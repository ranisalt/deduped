#pragma once

#include "types.hpp"

#include <filesystem>
#include <memory>
#include <optional>
#include <vector>

namespace deduped {

// Repository owns the SQLite connection and executes all DB operations.
// All methods are thread-hostile (call from a single thread or serialize externally).
class Repository
{
public:
	// Open (or create) the database at `db_path`.
	// Initializes schema objects and enables WAL mode.
	explicit Repository(const std::filesystem::path& db_path);
	~Repository();

	Repository(const Repository&) = delete;
	Repository& operator=(const Repository&) = delete;
	Repository(Repository&&) = delete;
	Repository& operator=(Repository&&) = delete;

	// Upsert a file record.  If a row with the same path already exists its
	// metadata, digest, and last_seen timestamp are updated.
	void upsert(const IndexEntry& entry);

	// Look up a file by its absolute path.  Returns nullopt if not found.
	[[nodiscard]] std::optional<IndexEntry> find_by_path(const std::string& path) const;

	// Look up a file identity by device+inode. Returns nullopt if not found.
	// Path in the returned entry is one representative alias.
	[[nodiscard]] std::optional<IndexEntry> find_by_inode(std::uint64_t device, std::uint64_t inode) const;

	// Return all entries that share the given digest (for duplicate detection).
	[[nodiscard]] std::vector<IndexEntry> find_by_digest(const Digest& digest) const;

	// Mark all rows not seen since `cutoff_unix_s` as deleted (removes them).
	void remove_stale(std::int64_t cutoff_unix_s);

	// Remove a single file record by its absolute path.
	// No-op if the path is not in the index.
	void remove_by_path(const std::string& path);

	enum class OpStatus
	{
		Planned,
		Done,
		Failed
	};

	struct LoggedOp
	{
		std::int64_t id{};
		std::string canonical_path;
		std::string duplicate_path;
		std::string backup_path;
		OpStatus status{OpStatus::Planned};
		std::string message;
	};

	// Record an intended hardlink operation before it executes.
	// Returns the log rowid.
	[[nodiscard]] std::int64_t log_op_planned(const std::string& canonical, const std::string& duplicate,
	                                          const std::string& backup_path = {});

	// Update the outcome of a previously planned operation.
	void log_op_complete(std::int64_t op_id, OpStatus status, const std::string& message = {});

	// List operations matching the given status.
	[[nodiscard]] std::vector<LoggedOp> list_ops(OpStatus status) const;

	// Current UNIX timestamp in seconds (uses the SQLite connection for consistency).
	[[nodiscard]] std::int64_t now_unix_s() const;

private:
	struct Impl;
	std::unique_ptr<Impl> impl_;
};

} // namespace deduped
