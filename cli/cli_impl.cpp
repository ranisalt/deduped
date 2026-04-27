#include "cli_impl.hpp"

#include "../lib/engine.hpp"
#include "../lib/logging.hpp"
#include "../lib/repository.hpp"
#include "../lib/scanner.hpp"
#include "../lib/types.hpp"

#include <algorithm>
#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <spdlog/spdlog.h>
#include <thread>

namespace deduped {

namespace {

class SignalWatcher
{
public:
	SignalWatcher() : signals_(io_context_, SIGINT, SIGTERM)
	{
		signals_.async_wait([this](const boost::system::error_code& ec, int) {
			if (!ec) {
				termination_requested_.store(true);
			}
			io_context_.stop();
		});

		thread_ = std::jthread([this](std::stop_token) { io_context_.run(); });
	}

	~SignalWatcher()
	{
		boost::system::error_code ec;
		signals_.cancel(ec);
		io_context_.stop();
	}

	[[nodiscard]] bool termination_requested() const noexcept { return termination_requested_.load(); }

private:
	std::atomic<bool> termination_requested_{false};
	boost::asio::io_context io_context_;
	boost::asio::signal_set signals_;
	std::jthread thread_;
};

} // namespace

namespace {

struct ScanCounters
{
	std::size_t scanned{0};
	std::size_t hits{0};
	std::size_t misses{0};
	std::size_t dupes{0};
	std::size_t linked{0};
	std::size_t failed{0};
};

EngineCallbacks make_engine_callbacks(const SignalWatcher& signal_watcher, ScanCounters& c)
{
	constexpr std::size_t progress_interval = 1000;

	EngineCallbacks cbs;
	cbs.on_scan_decision = [&](const std::string& path, const ScanCacheStatus status) {
		++c.scanned;
		switch (status) {
			case ScanCacheStatus::Hit:
				++c.hits;
				break;
			case ScanCacheStatus::Miss:
				++c.misses;
				break;
		}
		spdlog::trace("{} {}", status == ScanCacheStatus::Hit ? "HIT" : "MISS", path);
		if (c.scanned % progress_interval == 0) {
			spdlog::info("Progress: scanned={} hits={} misses={} dupes={} linked={} failed={}", c.scanned, c.hits,
			             c.misses, c.dupes, c.linked, c.failed);
		}
	};
	cbs.on_shutdown_requested = [](const std::size_t pending, const std::size_t in_flight) {
		spdlog::warn("Ctrl+C detected. Application is closing; {} hash jobs pending ({} in flight).", pending,
		             in_flight);
	};
	cbs.should_abort = [&] { return signal_watcher.termination_requested(); };
	cbs.on_dupe_found = [&](const DupePair& p) {
		++c.dupes;
		spdlog::info("DUPE  {} == {}", p.canonical_path, p.duplicate_path);
	};
	cbs.on_apply = [&](const ApplyResult& r) {
		switch (r.status) {
			case ApplyStatus::Linked:
				++c.linked;
				spdlog::info("LINKED {}", r.pair.duplicate_path);
				break;
			case ApplyStatus::AlreadyLinked:
				spdlog::debug("ALREADY_LINKED {}", r.pair.duplicate_path);
				break;
			case ApplyStatus::Skipped:
				spdlog::debug("SKIPPED {}, {}", r.pair.duplicate_path, r.message);
				break;
			case ApplyStatus::Failed:
				++c.failed;
				spdlog::error("FAILED {}, {}", r.pair.duplicate_path, r.message);
				break;
		}
	};
	return cbs;
}

} // namespace

int run_cli_impl(const std::string& db_dir, const std::vector<std::string>& roots, const std::string& log_level,
                 bool apply_flag)
{
	if (!configure_log_level(log_level)) {
		return EXIT_FAILURE;
	}

	if (apply_flag) {
		spdlog::info("Running in apply mode, duplicates will be replaced with hardlinks.");
	} else {
		spdlog::info("Running in dry-run mode (no filesystem changes).");
	}

	const std::filesystem::path db_root = std::filesystem::absolute(db_dir);
	std::error_code mkdir_ec;
	std::filesystem::create_directories(db_root, mkdir_ec);
	if (mkdir_ec) {
		spdlog::error("Failed to create database directory {}: {}", db_root.string(), mkdir_ec.message());
		return EXIT_FAILURE;
	}

	const std::filesystem::path db_path = db_root / "deduped.db";
	spdlog::info("Using database: {}", db_path.string());
	Repository repo{db_path};
	SignalWatcher signal_watcher;

	ScanOptions scan_opts;
	scan_opts.roots.reserve(roots.size());
	std::transform(roots.begin(), roots.end(), std::back_inserter(scan_opts.roots),
	               [](const std::string& root) { return std::filesystem::absolute(root); });

	EngineOptions engine_opts;
	engine_opts.dry_run = !apply_flag;

	ScanCounters counters;
	const EngineCallbacks cbs = make_engine_callbacks(signal_watcher, counters);

	EngineCallbacks recovery_cbs;
	recovery_cbs.on_apply = [](const ApplyResult& r) {
		if (r.status == ApplyStatus::Linked) {
			spdlog::warn("RECOVERED LINKED {}", r.pair.duplicate_path);
		} else if (r.status == ApplyStatus::Failed) {
			spdlog::warn("RECOVERY FAILED {}: {}", r.pair.duplicate_path, r.message);
		}
	};
	recover_pending_operations(repo, recovery_cbs);

	try {
		run_engine(repo, scan_opts, engine_opts, cbs);
	} catch (const ScanInterrupted&) {
		spdlog::info("Done. Scanned={} Hits={} Misses={} Dupes={} Linked={} Failed={}", counters.scanned, counters.hits,
		             counters.misses, counters.dupes, counters.linked, counters.failed);
		spdlog::warn("Interrupted by signal; saved indexed progress to {}", db_path.string());
		return 130;
	} catch (const std::exception& ex) {
		spdlog::error("Engine error: {}", ex.what());
		return EXIT_FAILURE;
	}

	spdlog::info("Done. Scanned={} Hits={} Misses={} Dupes={} Linked={} Failed={}", counters.scanned, counters.hits,
	             counters.misses, counters.dupes, counters.linked, counters.failed);
	return counters.failed > 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}

} // namespace deduped
