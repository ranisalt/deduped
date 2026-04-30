#pragma once

#include "repository.hpp"
#include "scanner.hpp"
#include "types.hpp"

#include <cstddef>
#include <functional>
#include <optional>
#include <stdexcept>
#include <vector>

namespace deduped {

struct EngineOptions
{
	bool dry_run{true}; // true = report only; false = create hardlinks
};

class ScanInterrupted : public std::runtime_error
{
public:
	ScanInterrupted() : std::runtime_error("scan interrupted") {}

	ScanInterrupted(const std::size_t pending_hash_jobs, const std::size_t in_flight_hash_jobs) :
	    std::runtime_error("scan interrupted"),
	    pending_hash_jobs_(pending_hash_jobs),
	    in_flight_hash_jobs_(in_flight_hash_jobs)
	{}

	[[nodiscard]] std::size_t pending_hash_jobs() const noexcept { return pending_hash_jobs_; }
	[[nodiscard]] std::size_t in_flight_hash_jobs() const noexcept { return in_flight_hash_jobs_; }

private:
	std::size_t pending_hash_jobs_{0};
	std::size_t in_flight_hash_jobs_{0};
};

// Callback types for progress reporting.
enum class ScanCacheStatus
{
	Hit,
	Miss,
};

struct EngineCallbacks
{
	std::function<void(const std::string& path)> on_scan;                                            // may be nullptr
	std::function<void(const std::string& path, ScanCacheStatus status)> on_scan_decision;           // may be nullptr
	std::function<void(const DupePair&)> on_dupe_found;                                              // may be nullptr
	std::function<void(const ApplyResult&)> on_apply;                                                // may be nullptr
	std::function<void(std::size_t pending_jobs, std::size_t in_flight_jobs)> on_shutdown_requested; // may be nullptr
	std::function<bool()> should_abort; // may be nullptr; return true to stop
};

namespace detail {

using HashFileFn = std::function<Digest(const std::filesystem::path&)>;

struct ResolvedDigest
{
	Digest digest;
	bool reused_cached_digest{false};
};

// Internal helper shared by full scans and event handling.
// Exposed in the header so unit tests can verify the metadata-cache policy.
[[nodiscard]] ResolvedDigest resolve_digest(IRepository& repo, const std::filesystem::path& path,
                                            const FileMeta& current_meta, const HashFileFn& hash_file_fn);

} // namespace detail

// Run a full scan+dedupe cycle (used for startup reconciliation):
//   1. Walk roots, update the index (hash only when metadata is stale).
//   2. Find all sets of files sharing the same digest.
//   3. In dry_run mode: report pairs via callbacks.
//      In apply mode: create hardlinks and log operations.
//   4. Purge index entries for files no longer present on disk.
// Returns the list of results (apply mode) or planned pairs (dry-run mode).
std::vector<ApplyResult> run_engine(IRepository& repo, const ScanOptions& scan_opts, const EngineOptions& engine_opts,
                                    const EngineCallbacks& cbs = {});

// Recover interrupted hardlink operations recorded in the op log.
// Safe to run at startup before scanning or watching.
void recover_pending_operations(IRepository& repo, const EngineCallbacks& cbs = {});

// Handle a file-level inotify event: file created or written to completion.
//   - Hashes the file (uses cached hash when metadata is still valid).
//   - Upserts the index entry.
//   - If a duplicate is found, reports it via cbs.on_dupe_found and,
//     in non-dry_run mode, creates a hardlink.
// Returns the ApplyResult when a duplicate was found, nullopt otherwise.
std::optional<ApplyResult> handle_file_change(IRepository& repo, const std::filesystem::path& path,
                                              const EngineOptions& opts, const EngineCallbacks& cbs = {});

// Handle a file-level inotify event: file deleted or moved out.
//   - Removes the entry from the index.
//   - No-op if the path is not indexed.
void handle_file_removed(IRepository& repo, const std::filesystem::path& path);

} // namespace deduped
