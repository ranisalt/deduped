#include "cli_impl.hpp"

#include "../lib/dedup_session.hpp"
#include "../lib/engine.hpp"
#include "../lib/logging.hpp"
#include "../lib/scanner.hpp"
#include "../lib/signal_watcher.hpp"
#include "../lib/types.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <spdlog/spdlog.h>

namespace deduped {

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

	std::vector<std::filesystem::path> data_roots;
	data_roots.reserve(roots.size());
	std::transform(roots.begin(), roots.end(), std::back_inserter(data_roots),
	               [](const std::string& root) { return std::filesystem::absolute(root); });

	DedupSession session{{
	    .db_path = db_path,
	    .data_roots = std::move(data_roots),
	    .engine_opts = {.dry_run = !apply_flag},
	}};
	SignalWatcher signal_watcher;

	ScanCounters counters;
	const EngineCallbacks cbs = make_engine_callbacks(signal_watcher, counters);

	session.recover();

	try {
		session.run_full_scan(cbs);
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
