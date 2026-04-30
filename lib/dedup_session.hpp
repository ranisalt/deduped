#pragma once

#include "engine.hpp"
#include "repository.hpp"
#include "scanner.hpp"

#include <filesystem>
#include <memory>
#include <optional>
#include <vector>

namespace deduped {

// Bundles the repeated init/recover/run sequence used by both the CLI and the
// daemon. Owns a Repository tied to a single database path; runs schema
// recovery and full scans against a stable set of data roots.
class DedupSession
{
public:
	struct Config
	{
		std::filesystem::path db_path;
		std::vector<std::filesystem::path> data_roots;
		EngineOptions engine_opts{};
	};

	explicit DedupSession(Config cfg);
	~DedupSession();

	DedupSession(const DedupSession&) = delete;
	DedupSession& operator=(const DedupSession&) = delete;
	DedupSession(DedupSession&&) = delete;
	DedupSession& operator=(DedupSession&&) = delete;

	// Recover any planned-but-not-completed hardlink operations from the op_log.
	// Forwards interesting events to `cbs.on_apply` if provided. A standard
	// recovery-log decoration is applied internally so callers do not need to
	// re-implement the "RECOVERED LINKED / RECOVERY FAILED" branching.
	void recover(const EngineCallbacks& cbs = {});

	// Full scan + dedupe over the configured data roots.
	std::vector<ApplyResult> run_full_scan(const EngineCallbacks& cbs = {});

	// Single-file event handlers (used by the daemon's inotify loop).
	std::optional<ApplyResult> handle_change(const std::filesystem::path& path, const EngineCallbacks& cbs = {});
	void handle_removed(const std::filesystem::path& path);

	[[nodiscard]] IRepository& repo() noexcept;
	[[nodiscard]] const Config& config() const noexcept { return cfg_; }

private:
	Config cfg_;
	std::unique_ptr<Repository> repo_;
};

} // namespace deduped
