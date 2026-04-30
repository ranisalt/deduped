#pragma once

#include "types.hpp"

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace deduped {

// Abstract index/operation log interface. The engine depends only on this so
// alternative backends (in-memory mocks, alternate databases) can be plugged
// in without touching the engine.
class IRepository
{
public:
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

	IRepository() = default;
	virtual ~IRepository() = default;

	IRepository(const IRepository&) = delete;
	IRepository& operator=(const IRepository&) = delete;
	IRepository(IRepository&&) = delete;
	IRepository& operator=(IRepository&&) = delete;

	virtual void upsert(const IndexEntry& entry) = 0;
	[[nodiscard]] virtual std::optional<IndexEntry> find_by_path(const std::string& path) const = 0;
	[[nodiscard]] virtual std::optional<IndexEntry> find_by_inode(std::uint64_t device, std::uint64_t inode) const = 0;
	[[nodiscard]] virtual std::vector<IndexEntry> find_by_digest(const Digest& digest) const = 0;
	virtual void remove_stale(std::int64_t cutoff_unix_s) = 0;
	virtual void remove_by_path(const std::string& path) = 0;

	[[nodiscard]] virtual std::int64_t log_op_planned(const std::string& canonical, const std::string& duplicate,
	                                                  const std::string& backup_path = {}) = 0;
	virtual void log_op_complete(std::int64_t op_id, OpStatus status, const std::string& message = {}) = 0;
	[[nodiscard]] virtual std::vector<LoggedOp> list_ops(OpStatus status) const = 0;

	[[nodiscard]] virtual std::int64_t now_unix_s() const = 0;
};

// SQLite-backed concrete repository. All methods are thread-hostile; serialize
// access externally if used from multiple threads.
class Repository final : public IRepository
{
public:
	// Open (or create) the database at `db_path`.
	// Initializes schema objects and enables WAL mode.
	explicit Repository(const std::filesystem::path& db_path);
	~Repository() override;

	void upsert(const IndexEntry& entry) override;
	[[nodiscard]] std::optional<IndexEntry> find_by_path(const std::string& path) const override;
	[[nodiscard]] std::optional<IndexEntry> find_by_inode(std::uint64_t device, std::uint64_t inode) const override;
	[[nodiscard]] std::vector<IndexEntry> find_by_digest(const Digest& digest) const override;
	void remove_stale(std::int64_t cutoff_unix_s) override;
	void remove_by_path(const std::string& path) override;

	[[nodiscard]] std::int64_t log_op_planned(const std::string& canonical, const std::string& duplicate,
	                                          const std::string& backup_path = {}) override;
	void log_op_complete(std::int64_t op_id, OpStatus status, const std::string& message = {}) override;
	[[nodiscard]] std::vector<LoggedOp> list_ops(OpStatus status) const override;

	[[nodiscard]] std::int64_t now_unix_s() const override;

private:
	struct Impl;
	std::unique_ptr<Impl> impl_;
};

} // namespace deduped
