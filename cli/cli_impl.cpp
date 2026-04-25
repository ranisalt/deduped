#include "cli_impl.hpp"

#include "../lib/engine.hpp"
#include "../lib/logging.hpp"
#include "../lib/repository.hpp"
#include "../lib/scanner.hpp"
#include "../lib/types.hpp"

#include <cstdlib>
#include <filesystem>
#include <spdlog/spdlog.h>

namespace deduped {

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

	const std::filesystem::path db_path = std::filesystem::absolute(db_dir) / "deduped.db";
	Repository repo{db_path};

	ScanOptions scan_opts;
	scan_opts.roots.reserve(roots.size());
	std::ranges::transform(roots, std::back_inserter(scan_opts.roots),
	                       [](const std::string& root) { return std::filesystem::absolute(root); });

	EngineOptions engine_opts;
	engine_opts.dry_run = !apply_flag;

	std::size_t scanned{0}, dupes{0}, linked{0}, failed{0};

	EngineCallbacks cbs;
	cbs.on_scan = [&](const std::string&) { ++scanned; };
	cbs.on_dupe_found = [&](const DupePair& p) {
		++dupes;
		spdlog::info("DUPE  {} == {}", p.canonical_path, p.duplicate_path);
	};
	cbs.on_apply = [&](const ApplyResult& r) {
		switch (r.status) {
			case ApplyStatus::Linked:
				++linked;
				spdlog::info("LINKED {}", r.pair.duplicate_path);
				break;
			case ApplyStatus::AlreadyLinked:
				spdlog::debug("ALREADY_LINKED {}", r.pair.duplicate_path);
				break;
			case ApplyStatus::Skipped:
				spdlog::debug("SKIPPED {}, {}", r.pair.duplicate_path, r.message);
				break;
			case ApplyStatus::Failed:
				++failed;
				spdlog::error("FAILED {}, {}", r.pair.duplicate_path, r.message);
				break;
		}
	};

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
	} catch (const std::exception& ex) {
		spdlog::error("Engine error: {}", ex.what());
		return EXIT_FAILURE;
	}

	spdlog::info("Done. Scanned={} Dupes={} Linked={} Failed={}", scanned, dupes, linked, failed);
	return failed > 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}

} // namespace deduped
