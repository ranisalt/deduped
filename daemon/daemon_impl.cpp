#include "daemon_impl.hpp"

#include "../lib/engine.hpp"
#include "../lib/logging.hpp"
#include "../lib/repository.hpp"
#include "../lib/scanner.hpp"
#include "../lib/scope_exit.hpp"
#include "../lib/watcher.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string_view>
#include <thread>

namespace deduped {

namespace {

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

class LockDir
{
public:
	explicit LockDir(std::filesystem::path p) : path_(std::move(p))
	{
		std::error_code ec;
		if (!std::filesystem::create_directory(path_, ec)) {
			throw std::runtime_error("Lock already exists - daemon already running? " + path_.string());
		}
		if (ec) {
			throw std::runtime_error("Cannot create lock directory: " + path_.string());
		}
	}

	~LockDir()
	{
		std::error_code ec;
		std::filesystem::remove(path_, ec);
	}

	LockDir(const LockDir&) = delete;
	LockDir& operator=(const LockDir&) = delete;

private:
	std::filesystem::path path_;
};

} // namespace

int init_daemon_without_watcher(const std::string& config_dir, const std::vector<std::string>& data_dirs,
                                const std::string& log_level, bool apply_flag)
{
	if (!configure_log_level(log_level)) {
		return EXIT_FAILURE;
	}

	const std::filesystem::path config_root = std::filesystem::absolute(config_dir);

	if (!check_dir_access(config_root, "Config", true)) {
		return EXIT_FAILURE;
	}

	// Convert all data directories to absolute paths and validate them
	std::vector<std::filesystem::path> data_roots;
	data_roots.reserve(data_dirs.size());
	for (const auto& dir : data_dirs) {
		const auto abs_dir = std::filesystem::absolute(dir);
		if (!check_dir_access(abs_dir, "Data", apply_flag)) {
			return EXIT_FAILURE;
		}
		data_roots.push_back(abs_dir);
	}

	if (data_roots.empty()) {
		spdlog::error("No data directories specified");
		return EXIT_FAILURE;
	}

	const std::filesystem::path db_path = config_root / "deduped.db";
	const auto lock_path = config_root / "deduped.lockdir";

	std::optional<LockDir> lock;
	try {
		lock.emplace(lock_path);
	} catch (const std::exception& ex) {
		spdlog::error("{}", ex.what());
		return EXIT_FAILURE;
	}

	const auto data_str = [&] {
		std::string s;
		for (size_t i = 0; i < data_roots.size(); ++i) {
			if (i > 0) s += ", ";
			s += data_roots[i].string();
		}
		return s;
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

	// Startup reconciliation scan.
	spdlog::info("Running startup reconciliation scan...");
	try {
		ScanOptions scan_opts;
		scan_opts.roots = data_roots;

		EngineCallbacks cbs;
		cbs.on_dupe_found = [](const DupePair& p) {
			spdlog::info("RECONCILE DUPE  {} == {}", p.canonical_path, p.duplicate_path);
		};
		cbs.on_apply = [](const ApplyResult& r) {
			if (r.status == ApplyStatus::Linked)
				spdlog::info("RECONCILE LINKED {}", r.pair.duplicate_path);
			else if (r.status == ApplyStatus::Failed)
				spdlog::error("RECONCILE FAILED {}: {}", r.pair.duplicate_path, r.message);
		};

		run_engine(repo, scan_opts, engine_opts, cbs);
	} catch (const std::exception& ex) {
		spdlog::error("Reconciliation error: {}", ex.what());
		return EXIT_FAILURE;
	}
	spdlog::info("Reconciliation complete. Watching {}...", data_str);

	return EXIT_SUCCESS;
}

int run_daemon_impl(const std::string& config_dir, const std::vector<std::string>& data_dirs,
                    const std::string& log_level, bool apply_flag)
{
	// Initialize daemon (validation, recovery, reconciliation)
	const int init_result = init_daemon_without_watcher(config_dir, data_dirs, log_level, apply_flag);
	if (init_result != EXIT_SUCCESS) {
		return init_result;
	}

	// Now get the data_roots for watcher setup
	const std::filesystem::path config_root = std::filesystem::absolute(config_dir);
	std::vector<std::filesystem::path> data_roots;
	data_roots.reserve(data_dirs.size());
	for (const auto& dir : data_dirs) {
		data_roots.push_back(std::filesystem::absolute(dir));
	}

	// inotify watch loop.
	EngineCallbacks watch_cbs;
	watch_cbs.on_dupe_found = [](const DupePair& p) {
		spdlog::info("DUPE  {} == {}", p.canonical_path, p.duplicate_path);
	};
	watch_cbs.on_apply = [](const ApplyResult& r) {
		if (r.status == ApplyStatus::Linked)
			spdlog::info("LINKED {}", r.pair.duplicate_path);
		else if (r.status == ApplyStatus::Failed)
			spdlog::error("FAILED {}: {}", r.pair.duplicate_path, r.message);
	};
	EngineOptions watch_engine_opts;
	watch_engine_opts.dry_run = !apply_flag;

	try {
		Watcher watcher(data_roots, [&](const WatchEvent& ev) {
			// Need repo and engine_opts for the callback - recreate them here
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
