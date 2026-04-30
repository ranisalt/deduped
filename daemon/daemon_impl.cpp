#include "daemon_impl.hpp"

#include "../lib/engine.hpp"
#include "../lib/logging.hpp"
#include "../lib/repository.hpp"
#include "../lib/scanner.hpp"
#include "../lib/scope_exit.hpp"
#include "../lib/watcher.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <optional>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <thread>
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

class LockFile
{
public:
	explicit LockFile(std::filesystem::path p) : path_(std::move(p))
	{
		fd_ = ::open(path_.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
		if (fd_ == -1) {
			if (errno == EEXIST) {
				throw std::runtime_error("Lock already exists - daemon already running? " + path_.string());
			}

			const auto ec = std::error_code(errno, std::generic_category());
			throw std::runtime_error("Cannot create lock file: " + path_.string() + ": " + ec.message());
		}
	}

	~LockFile()
	{
		if (fd_ != -1) {
			::close(fd_);
		}

		std::error_code ec;
		std::filesystem::remove(path_, ec);
	}

	LockFile(const LockFile&) = delete;
	LockFile& operator=(const LockFile&) = delete;
	LockFile(LockFile&&) = delete;
	LockFile& operator=(LockFile&&) = delete;

private:
	std::filesystem::path path_;
	int fd_ = -1;
};

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

	const auto data_str = [&] {
		std::string data;
		for (size_t i = 0; i < data_roots.size(); ++i) {
			if (i > 0) {
				data += ", ";
			}
			data += data_roots[i].string();
		}
		return data;
	}();

	spdlog::info("deduped started. config={} data=[{}] db={} apply={}", config_root.string(), data_str,
	             db_path.string(), apply_flag);

	Repository repo{db_path};
	EngineOptions engine_opts;
	engine_opts.dry_run = !apply_flag;

	EngineCallbacks recovery_cbs;
	recovery_cbs.on_apply = [](const ApplyResult& r) {
		if (r.status == ApplyStatus::Linked) {
			spdlog::warn("RECOVERED LINKED {}", r.pair.duplicate_path);
		} else if (r.status == ApplyStatus::Failed) {
			spdlog::warn("RECOVERY FAILED {}: {}", r.pair.duplicate_path, r.message);
		}
	};
	recover_pending_operations(repo, recovery_cbs);

	spdlog::info("Running startup reconciliation scan...");
	try {
		ScanOptions scan_opts;
		scan_opts.roots = data_roots;

		EngineCallbacks cbs;
		cbs.on_dupe_found = [](const DupePair& p) {
			spdlog::info("RECONCILE DUPE  {} == {}", p.canonical_path, p.duplicate_path);
		};
		cbs.on_apply = [&](const ApplyResult& r) {
			apply_logger.log(r, true);
		};

		run_engine(repo, scan_opts, engine_opts, cbs);
	} catch (const std::exception& ex) {
		spdlog::error("Reconciliation error: {}", ex.what());
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
	std::vector<std::filesystem::path> data_roots;
	data_roots.reserve(data_dirs.size());
	for (const auto& dir : data_dirs) {
		data_roots.push_back(std::filesystem::absolute(dir));
	}

	EngineCallbacks watch_cbs;
	watch_cbs.on_dupe_found = [](const DupePair& p) {
		spdlog::info("DUPE  {} == {}", p.canonical_path, p.duplicate_path);
	};
	watch_cbs.on_apply = [&](const ApplyResult& r) {
		apply_logger.log(r, false);
	};

	EngineOptions watch_engine_opts;
	watch_engine_opts.dry_run = !apply_flag;

	try {
		Watcher watcher(data_roots, [&](const WatchEvent& ev) {
			Repository repo{config_root / "deduped.db"};

			if (ev.type == FileEvent::Modified) {
				spdlog::debug("EVENT modified {}", ev.path.string());
				handle_file_change(repo, ev.path, watch_engine_opts, watch_cbs);
			} else {
				spdlog::debug("EVENT deleted {}", ev.path.string());
				handle_file_removed(repo, ev.path);
			}
		});

		boost::asio::io_context signal_context;
		boost::asio::signal_set signals(signal_context, SIGINT, SIGTERM);
		signals.async_wait([&](const boost::system::error_code& ec, int) {
			if (!ec) {
				watcher.stop();
			}
			signal_context.stop();
		});
		std::jthread signal_thread([&](std::stop_token) { signal_context.run(); });
		auto stop_signal_context = scope_exit{[&] { signal_context.stop(); }};

		watcher.run();
	} catch (const std::exception& ex) {
		spdlog::error("Watcher error: {}", ex.what());
		return EXIT_FAILURE;
	}

	spdlog::info("deduped shutting down.");
	return EXIT_SUCCESS;
}

} // namespace deduped
