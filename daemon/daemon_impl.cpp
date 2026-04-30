#include "daemon_impl.hpp"

#include "../lib/dedup_session.hpp"
#include "../lib/engine.hpp"
#include "../lib/lock_file.hpp"
#include "../lib/logging.hpp"
#include "../lib/scanner.hpp"
#include "../lib/signal_watcher.hpp"
#include "../lib/watcher.hpp"

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <optional>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <unordered_set>

namespace deduped {

namespace {

constexpr std::string_view kDuplicatePathNotWritableMessage = "duplicate path not writable, skipped";
constexpr std::string_view kLockFileName = "deduped.lock";

class ApplyResultLogger
{
public:
	void log(const ApplyResult& result, const bool reconciliation)
	{
		const auto linked_prefix = reconciliation ? "RECONCILE LINKED" : "LINKED";
		const auto failed_prefix = reconciliation ? "RECONCILE FAILED" : "FAILED";
		const auto skipped_prefix = reconciliation ? "RECONCILE SKIPPED" : "SKIPPED";

		if (result.status == ApplyStatus::Linked) {
			spdlog::info("{} {}", linked_prefix, result.pair.duplicate_path);
			return;
		}

		if (result.status == ApplyStatus::Failed) {
			spdlog::error("{} {}: {}", failed_prefix, result.pair.duplicate_path, result.message);
			return;
		}

		if (result.status == ApplyStatus::Skipped && result.message == kDuplicatePathNotWritableMessage &&
		    unwritable_duplicate_paths_logged_.insert(result.pair.duplicate_path).second) {
			spdlog::warn("{} {}: {}", skipped_prefix, result.pair.duplicate_path, result.message);
		}
	}

private:
	std::unordered_set<std::string> unwritable_duplicate_paths_logged_;
};

bool check_dir_access(const std::filesystem::path& dir, std::string_view label, const bool require_write)
{
	std::error_code ec;
	if (!std::filesystem::exists(dir, ec)) {
		spdlog::error("{} directory does not exist: {}", label, dir.string());
		return false;
	}
	if (!std::filesystem::is_directory(dir, ec)) {
		spdlog::error("{} path is not a directory: {}", label, dir.string());
		return false;
	}

	std::filesystem::directory_iterator it{dir, ec};
	if (ec) {
		spdlog::error("{} directory is not readable/traversable: {}", label, dir.string());
		return false;
	}

	if (!require_write) {
		return true;
	}

	const auto probe =
	    dir / (".deduped_write_probe_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	{
		std::ofstream f(probe, std::ios::trunc);
		if (!f) {
			spdlog::error("{} directory is not writable: {}", label, dir.string());
			return false;
		}
	}

	std::filesystem::remove(probe, ec);
	if (ec) {
		spdlog::error("{} directory write probe cleanup failed: {}", label, dir.string());
		return false;
	}

	return true;
}

int init_daemon_without_watcher_impl(const std::string& config_dir, const std::vector<std::string>& data_dirs,
                                     const std::string& log_level, bool apply_flag, ApplyResultLogger& apply_logger,
                                     std::optional<LockFile>* held_lock = nullptr)
{
	if (!configure_log_level(log_level)) {
		return EXIT_FAILURE;
	}

	const std::filesystem::path config_root = std::filesystem::absolute(config_dir);

	if (!check_dir_access(config_root, "Config", true)) {
		return EXIT_FAILURE;
	}

	std::vector<std::filesystem::path> data_roots;
	data_roots.reserve(data_dirs.size());
	for (const auto& dir : data_dirs) {
		const auto abs_dir = std::filesystem::absolute(dir);
		if (!check_dir_access(abs_dir, "Data", false)) {
			return EXIT_FAILURE;
		}
		data_roots.push_back(abs_dir);
	}

	if (data_roots.empty()) {
		spdlog::error("No data directories specified");
		return EXIT_FAILURE;
	}

	const std::filesystem::path db_path = config_root / "deduped.db";
	const auto lock_path = config_root / kLockFileName;

	std::optional<LockFile> local_lock;
	auto* lock = held_lock != nullptr ? held_lock : &local_lock;
	try {
		lock->emplace(lock_path);
	} catch (const std::exception& ex) {
		spdlog::error("{}", ex.what());
		return EXIT_FAILURE;
	}

	std::string data_str;
	for (const auto& root : data_roots) {
		if (!data_str.empty()) {
			data_str += ", ";
		}
		data_str += root.string();
	}

	spdlog::info("deduped started. config={} data=[{}] db={} apply={}", config_root.string(), data_str,
	             db_path.string(), apply_flag);

	DedupSession session{{
	    .db_path = db_path,
	    .data_roots = data_roots,
	    .engine_opts = {.dry_run = !apply_flag},
	}};

	try {
		session.recover();
	} catch (const std::exception& ex) {
		spdlog::error("Recovery error: {}", ex.what());
		return EXIT_FAILURE;
	} catch (...) {
		spdlog::critical("Recovery aborted by unknown exception");
		return EXIT_FAILURE;
	}

	spdlog::info("Running startup reconciliation scan...");
	try {
		EngineCallbacks cbs;
		cbs.on_dupe_found = [](const DupePair& p) {
			spdlog::info("RECONCILE DUPE  {} == {}", p.canonical_path, p.duplicate_path);
		};
		cbs.on_apply = [&](const ApplyResult& r) { apply_logger.log(r, true); };

		session.run_full_scan(cbs);
	} catch (const std::exception& ex) {
		spdlog::error("Reconciliation error: {}", ex.what());
		return EXIT_FAILURE;
	} catch (...) {
		spdlog::critical("Reconciliation aborted by unknown exception");
		return EXIT_FAILURE;
	}

	spdlog::info("Reconciliation complete. Watching {}...", data_str);
	return EXIT_SUCCESS;
}

} // namespace

int init_daemon_without_watcher(const std::string& config_dir, const std::vector<std::string>& data_dirs,
                                const std::string& log_level, bool apply_flag)
{
	ApplyResultLogger apply_logger;
	return init_daemon_without_watcher_impl(config_dir, data_dirs, log_level, apply_flag, apply_logger);
}

int run_daemon_impl(const std::string& config_dir, const std::vector<std::string>& data_dirs,
                    const std::string& log_level, bool apply_flag)
{
	ApplyResultLogger apply_logger;
	std::optional<LockFile> held_lock;

	const int init_result = init_daemon_without_watcher_impl(config_dir, data_dirs, log_level, apply_flag,
	                                                        apply_logger, &held_lock);
	if (init_result != EXIT_SUCCESS) {
		return init_result;
	}

	const std::filesystem::path config_root = std::filesystem::absolute(config_dir);
	const std::filesystem::path db_path = config_root / "deduped.db";
	std::vector<std::filesystem::path> data_roots;
	data_roots.reserve(data_dirs.size());
	for (const auto& dir : data_dirs) {
		data_roots.push_back(std::filesystem::absolute(dir));
	}

	EngineCallbacks watch_cbs;
	watch_cbs.on_dupe_found = [](const DupePair& p) {
		spdlog::info("DUPE  {} == {}", p.canonical_path, p.duplicate_path);
	};
	watch_cbs.on_apply = [&](const ApplyResult& r) { apply_logger.log(r, false); };

	const EngineOptions watch_engine_opts{.dry_run = !apply_flag};

	try {
		Watcher watcher(data_roots, [&](const WatchEvent& ev) {
			DedupSession event_session{{
			    .db_path = db_path,
			    .data_roots = data_roots,
			    .engine_opts = watch_engine_opts,
			}};

			if (ev.type == FileEvent::Modified) {
				spdlog::debug("EVENT modified {}", ev.path.string());
				event_session.handle_change(ev.path, watch_cbs);
			} else {
				spdlog::debug("EVENT deleted {}", ev.path.string());
				event_session.handle_removed(ev.path);
			}
		});

		SignalWatcher signal_watcher{[&] { watcher.stop(); }};

		watcher.run();
	} catch (const std::exception& ex) {
		spdlog::error("Watcher error: {}", ex.what());
		return EXIT_FAILURE;
	} catch (...) {
		spdlog::critical("Watcher aborted by unknown exception");
		return EXIT_FAILURE;
	}

	spdlog::info("deduped shutting down.");
	return EXIT_SUCCESS;
}

} // namespace deduped
