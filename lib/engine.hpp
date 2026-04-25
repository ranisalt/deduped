#pragma once

#include "repository.hpp"
#include "scanner.hpp"
#include "types.hpp"

#include <functional>
#include <optional>
#include <vector>

namespace deduped {

struct EngineOptions
{
	bool dry_run{true}; // true = report only; false = create hardlinks
};

// Callback types for progress reporting.
using ProgressFn = std::function<void(const std::string& path)>; // called per file scanned
using DupeFoundFn = std::function<void(const DupePair&)>;        // called when a pair is identified
using ApplyResultFn = std::function<void(const ApplyResult&)>;   // called after each apply attempt

struct EngineCallbacks
{
	ProgressFn on_scan;        // may be nullptr
	DupeFoundFn on_dupe_found; // may be nullptr
	ApplyResultFn on_apply;    // may be nullptr
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
[[nodiscard]] ResolvedDigest resolve_digest(Repository& repo, const std::filesystem::path& path,
                                            const FileMeta& current_meta, const HashFileFn& hash_file_fn);

} // namespace detail

// Run a full scan+dedupe cycle (used for startup reconciliation):
//   1. Walk roots, update the index (hash only when metadata is stale).
//   2. Find all sets of files sharing the same digest.
//   3. In dry_run mode: report pairs via callbacks.
//      In apply mode: create hardlinks and log operations.
//   4. Purge index entries for files no longer present on disk.
// Returns the list of results (apply mode) or planned pairs (dry-run mode).
std::vector<ApplyResult> run_engine(Repository& repo, const ScanOptions& scan_opts, const EngineOptions& engine_opts,
                                    const EngineCallbacks& cbs = {});

// Recover interrupted hardlink operations recorded in the op log.
// Safe to run at startup before scanning or watching.
void recover_pending_operations(Repository& repo, const EngineCallbacks& cbs = {});

// Handle a file-level inotify event: file created or written to completion.
//   - Hashes the file (uses cached hash when metadata is still valid).
//   - Upserts the index entry.
//   - If a duplicate is found, reports it via cbs.on_dupe_found and,
//     in non-dry_run mode, creates a hardlink.
// Returns the ApplyResult when a duplicate was found, nullopt otherwise.
std::optional<ApplyResult> handle_file_change(Repository& repo, const std::filesystem::path& path,
                                              const EngineOptions& opts, const EngineCallbacks& cbs = {});

// Handle a file-level inotify event: file deleted or moved out.
//   - Removes the entry from the index.
//   - No-op if the path is not indexed.
void handle_file_removed(Repository& repo, const std::filesystem::path& path);

} // namespace deduped
