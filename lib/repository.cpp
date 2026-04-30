#include "repository.hpp"

#include <array>
#include <cstring>
#include <format>
#include <span>
#include <sqlite3.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace deduped {

namespace {

constexpr const char* kSchema = R"sql(
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS inodes (
	id          INTEGER PRIMARY KEY,
	device      INTEGER NOT NULL,
	inode       INTEGER NOT NULL,
	size        INTEGER NOT NULL,
	mtime_ns    INTEGER NOT NULL,
	mode        INTEGER NOT NULL,
	uid         INTEGER NOT NULL,
	gid         INTEGER NOT NULL,
	digest      BLOB    NOT NULL,   -- 32 bytes BLAKE3
	last_seen   INTEGER NOT NULL,   -- Unix seconds
	UNIQUE(device, inode)
);

CREATE INDEX IF NOT EXISTS idx_inodes_digest ON inodes(digest);
CREATE INDEX IF NOT EXISTS idx_inodes_identity ON inodes(device, inode);

CREATE TABLE IF NOT EXISTS paths (
	path        TEXT    PRIMARY KEY,
	inode_id    INTEGER NOT NULL,
	last_seen   INTEGER NOT NULL,
	FOREIGN KEY(inode_id) REFERENCES inodes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_paths_inode_id ON paths(inode_id);

CREATE TABLE IF NOT EXISTS op_log (
    id          INTEGER PRIMARY KEY,
    canonical   TEXT    NOT NULL,
    duplicate   TEXT    NOT NULL,
	backup_path TEXT    NOT NULL DEFAULT '',
    status      TEXT    NOT NULL DEFAULT 'planned',  -- planned | done | failed
    message     TEXT    NOT NULL DEFAULT '',
    created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);
)sql";

constexpr int kCurrentSchemaVersion = 3;

// Index by enum value; keep in sync with Repository::OpStatus.
constexpr std::array<std::string_view, 3> kOpStatusNames{"planned", "done", "failed"};

[[nodiscard]] std::string_view op_status_to_str(Repository::OpStatus s) noexcept
{
	const auto idx = static_cast<std::size_t>(s);
	return idx < kOpStatusNames.size() ? kOpStatusNames[idx] : kOpStatusNames[0];
}

[[nodiscard]] Repository::OpStatus op_status_from_str(std::string_view s) noexcept
{
	for (std::size_t i = 0; i < kOpStatusNames.size(); ++i) {
		if (kOpStatusNames[i] == s) {
			return static_cast<Repository::OpStatus>(i);
		}
	}
	return Repository::OpStatus::Planned;
}

// RAII wrapper around sqlite3_stmt* with bind/step/column helpers and fluent
// chaining. All SQLite error paths raise std::runtime_error with the
// originating context tag prepended.
class Stmt
{
public:
	Stmt(sqlite3* db, std::string_view sql, std::string_view tag) : db_{db}, tag_{tag}
	{
		if (sqlite3_prepare_v2(db_, sql.data(), static_cast<int>(sql.size()), &s_, nullptr) != SQLITE_OK) {
			throw std::runtime_error(std::format("{}: prepare: {}", tag_, sqlite3_errmsg(db_)));
		}
	}
	~Stmt()
	{
		if (s_ != nullptr) sqlite3_finalize(s_);
	}

	Stmt(const Stmt&) = delete;
	Stmt& operator=(const Stmt&) = delete;
	Stmt(Stmt&&) = delete;
	Stmt& operator=(Stmt&&) = delete;

	Stmt& bind(int idx, std::int64_t v)
	{
		sqlite3_bind_int64(s_, idx, v);
		return *this;
	}
	Stmt& bind(int idx, std::uint64_t v) { return bind(idx, static_cast<std::int64_t>(v)); }
	Stmt& bind(int idx, std::string_view v)
	{
		sqlite3_bind_text(s_, idx, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
		return *this;
	}
	Stmt& bind_static(int idx, std::string_view v)
	{
		sqlite3_bind_text(s_, idx, v.data(), static_cast<int>(v.size()), SQLITE_STATIC);
		return *this;
	}
	Stmt& bind_blob(int idx, std::span<const std::uint8_t> v)
	{
		sqlite3_bind_blob(s_, idx, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
		return *this;
	}

	// Execute statement expecting no rows (DDL/DML).
	void exec()
	{
		const int rc = sqlite3_step(s_);
		if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
			throw std::runtime_error(std::format("{}: step: {}", tag_, sqlite3_errmsg(db_)));
		}
	}

	// Single-step. Returns SQLITE_ROW or SQLITE_DONE; throws on error.
	[[nodiscard]] int step()
	{
		const int rc = sqlite3_step(s_);
		if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
			throw std::runtime_error(std::format("{}: step: {}", tag_, sqlite3_errmsg(db_)));
		}
		return rc;
	}

	[[nodiscard]] sqlite3_stmt* raw() noexcept { return s_; }

private:
	sqlite3* db_;
	std::string_view tag_;
	sqlite3_stmt* s_{nullptr};
};

// Read a TEXT column as std::string (treats NULL as empty).
[[nodiscard]] std::string col_text(sqlite3_stmt* s, int idx)
{
	const auto* p = reinterpret_cast<const char*>(sqlite3_column_text(s, idx));
	return p != nullptr ? std::string{p} : std::string{};
}

// Build an IndexEntry from the current row of a 11-column SELECT.
[[nodiscard]] IndexEntry entry_from_row(sqlite3_stmt* s)
{
	IndexEntry e;
	e.id = sqlite3_column_int64(s, 0);
	e.path = col_text(s, 1);
	e.meta.size = static_cast<std::uint64_t>(sqlite3_column_int64(s, 2));
	e.meta.mtime_ns = sqlite3_column_int64(s, 3);
	e.meta.inode = static_cast<std::uint64_t>(sqlite3_column_int64(s, 4));
	e.meta.device = static_cast<std::uint64_t>(sqlite3_column_int64(s, 5));
	e.meta.mode = static_cast<std::uint32_t>(sqlite3_column_int64(s, 6));
	e.meta.uid = static_cast<std::uint32_t>(sqlite3_column_int64(s, 7));
	e.meta.gid = static_cast<std::uint32_t>(sqlite3_column_int64(s, 8));
	const void* blob = sqlite3_column_blob(s, 9);
	const int blob_len = sqlite3_column_bytes(s, 9);
	if (blob_len == static_cast<int>(kDigestBytes)) {
		std::memcpy(e.digest.data(), blob, kDigestBytes);
	}
	e.last_seen = sqlite3_column_int64(s, 10);
	return e;
}

[[nodiscard]] Repository::LoggedOp logged_op_from_row(sqlite3_stmt* s)
{
	Repository::LoggedOp op;
	op.id = sqlite3_column_int64(s, 0);
	op.canonical_path = col_text(s, 1);
	op.duplicate_path = col_text(s, 2);
	op.backup_path = col_text(s, 3);
	op.status = op_status_from_str(col_text(s, 4));
	op.message = col_text(s, 5);
	return op;
}

constexpr std::string_view kEntrySelectCols =
    "i.id,p.path,i.size,i.mtime_ns,i.inode,i.device,i.mode,i.uid,i.gid,i.digest,p.last_seen";

} // namespace

struct Repository::Impl
{
	sqlite3* db{nullptr};

	explicit Impl(const std::filesystem::path& path)
	{
		if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
			std::string msg = db != nullptr ? sqlite3_errmsg(db) : "sqlite3_open failed";
			sqlite3_close(db);
			throw std::runtime_error(std::format("Repository: {}", msg));
		}
		exec_sql(kSchema, "schema");
		set_user_version(kCurrentSchemaVersion);
	}

	~Impl()
	{
		if (db != nullptr) sqlite3_close(db);
	}

	void exec_sql(const char* sql, std::string_view tag)
	{
		char* err = nullptr;
		if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
			std::string msg = err != nullptr ? err : "(null)";
			sqlite3_free(err);
			throw std::runtime_error(std::format("{}: {}", tag, msg));
		}
	}

	void set_user_version(int version)
	{
		const auto sql = std::format("PRAGMA user_version = {};", version);
		exec_sql(sql.c_str(), "user_version");
	}

	// Run `fn` inside a BEGIN IMMEDIATE/COMMIT pair, ROLLBACK + rethrow on exception.
	template <class Fn>
	void in_transaction(Fn&& fn)
	{
		exec_sql("BEGIN IMMEDIATE TRANSACTION;", "begin");
		try {
			std::forward<Fn>(fn)();
			exec_sql("COMMIT;", "commit");
		} catch (...) {
			exec_sql("ROLLBACK;", "rollback");
			throw;
		}
	}

	[[nodiscard]] Stmt prepare(std::string_view sql, std::string_view tag) { return Stmt{db, sql, tag}; }

	[[nodiscard]] std::optional<std::int64_t> inode_id_for_path(const std::string& path)
	{
		auto st = prepare("SELECT inode_id FROM paths WHERE path=? LIMIT 1;", "inode_id_for_path");
		st.bind(1, path);
		if (st.step() == SQLITE_ROW) {
			return sqlite3_column_int64(st.raw(), 0);
		}
		return std::nullopt;
	}

	[[nodiscard]] std::int64_t upsert_inode_and_get_id(const IndexEntry& entry)
	{
		static constexpr std::string_view kUpsertInodeSql = R"sql(
INSERT INTO inodes(device, inode, size, mtime_ns, mode, uid, gid, digest, last_seen)
VALUES(?,?,?,?,?,?,?,?,?)
ON CONFLICT(device, inode) DO UPDATE SET
    size      = excluded.size,
    mtime_ns  = excluded.mtime_ns,
    mode      = excluded.mode,
    uid       = excluded.uid,
    gid       = excluded.gid,
    digest    = excluded.digest,
    last_seen = excluded.last_seen;
)sql";

		auto upsert = prepare(kUpsertInodeSql, "upsert_inode");
		upsert.bind(1, entry.meta.device)
		    .bind(2, entry.meta.inode)
		    .bind(3, entry.meta.size)
		    .bind(4, entry.meta.mtime_ns)
		    .bind(5, static_cast<std::int64_t>(entry.meta.mode))
		    .bind(6, static_cast<std::int64_t>(entry.meta.uid))
		    .bind(7, static_cast<std::int64_t>(entry.meta.gid))
		    .bind_blob(8, entry.digest)
		    .bind(9, entry.last_seen)
		    .exec();

		auto select_id = prepare("SELECT id FROM inodes WHERE device=? AND inode=? LIMIT 1;", "upsert_inode_get_id");
		select_id.bind(1, entry.meta.device).bind(2, entry.meta.inode);
		if (select_id.step() != SQLITE_ROW) {
			throw std::runtime_error(std::format("upsert_inode_and_get_id: {}", sqlite3_errmsg(db)));
		}
		return sqlite3_column_int64(select_id.raw(), 0);
	}

	void maybe_delete_orphan_inode(std::int64_t inode_id)
	{
		auto st = prepare(
		    "DELETE FROM inodes WHERE id=? "
		    "AND NOT EXISTS(SELECT 1 FROM paths WHERE inode_id=? LIMIT 1);",
		    "delete_orphan_inode");
		st.bind(1, inode_id).bind(2, inode_id).exec();
	}
};

Repository::Repository(const std::filesystem::path& db_path) : impl_(std::make_unique<Impl>(db_path)) {}

Repository::~Repository() = default;

void Repository::upsert(const IndexEntry& entry)
{
	impl_->in_transaction([&] {
		const auto previous_inode_id = impl_->inode_id_for_path(entry.path);
		const auto inode_id = impl_->upsert_inode_and_get_id(entry);

		auto upsert_path = impl_->prepare(
		    "INSERT INTO paths(path, inode_id, last_seen) VALUES(?,?,?) "
		    "ON CONFLICT(path) DO UPDATE SET inode_id=excluded.inode_id, last_seen=excluded.last_seen;",
		    "upsert_path");
		upsert_path.bind(1, entry.path).bind(2, inode_id).bind(3, entry.last_seen).exec();

		if (previous_inode_id.has_value() && *previous_inode_id != inode_id) {
			impl_->maybe_delete_orphan_inode(*previous_inode_id);
		}
	});
}

std::optional<IndexEntry> Repository::find_by_path(const std::string& path) const
{
	const auto sql = std::format("SELECT {} FROM paths p JOIN inodes i ON i.id=p.inode_id WHERE p.path=? LIMIT 1;",
	                             kEntrySelectCols);
	auto st = impl_->prepare(sql, "find_by_path");
	st.bind(1, path);
	if (st.step() == SQLITE_ROW) {
		return entry_from_row(st.raw());
	}
	return std::nullopt;
}

std::optional<IndexEntry> Repository::find_by_inode(std::uint64_t device, std::uint64_t inode) const
{
	static constexpr std::string_view kSql =
	    "SELECT i.id, "
	    "COALESCE((SELECT p.path FROM paths p WHERE p.inode_id=i.id ORDER BY p.path LIMIT 1), ''), "
	    "i.size,i.mtime_ns,i.inode,i.device,i.mode,i.uid,i.gid,i.digest,i.last_seen "
	    "FROM inodes i WHERE i.device=? AND i.inode=? LIMIT 1;";

	auto st = impl_->prepare(kSql, "find_by_inode");
	st.bind(1, device).bind(2, inode);
	if (st.step() == SQLITE_ROW) {
		return entry_from_row(st.raw());
	}
	return std::nullopt;
}

std::vector<IndexEntry> Repository::find_by_digest(const Digest& digest) const
{
	const auto sql = std::format(
	    "SELECT {} FROM inodes i JOIN paths p ON p.inode_id=i.id WHERE i.digest=? "
	    "ORDER BY p.path;",
	    kEntrySelectCols);
	auto st = impl_->prepare(sql, "find_by_digest");
	st.bind_blob(1, digest);

	std::vector<IndexEntry> results;
	while (st.step() == SQLITE_ROW) {
		results.push_back(entry_from_row(st.raw()));
	}
	return results;
}

void Repository::remove_stale(std::int64_t cutoff_unix_s)
{
	impl_->in_transaction([&] {
		auto stale_paths = impl_->prepare("DELETE FROM paths WHERE last_seen < ?;", "remove_stale_paths");
		stale_paths.bind(1, cutoff_unix_s).exec();

		auto prune_orphans =
		    impl_->prepare("DELETE FROM inodes WHERE NOT EXISTS(SELECT 1 FROM paths WHERE paths.inode_id=inodes.id);",
		                   "prune_orphan_inodes");
		prune_orphans.exec();
	});
}

void Repository::remove_by_path(const std::string& path)
{
	impl_->in_transaction([&] {
		const auto inode_id = impl_->inode_id_for_path(path);
		auto delete_path = impl_->prepare("DELETE FROM paths WHERE path=?;", "remove_by_path");
		delete_path.bind(1, path).exec();

		if (inode_id.has_value()) {
			impl_->maybe_delete_orphan_inode(*inode_id);
		}
	});
}

std::int64_t Repository::log_op_planned(const std::string& canonical, const std::string& duplicate,
                                        const std::string& backup_path)
{
	auto st = impl_->prepare("INSERT INTO op_log(canonical, duplicate, backup_path, status) VALUES(?,?,?,'planned');",
	                         "log_op_planned");
	st.bind(1, canonical).bind(2, duplicate).bind(3, backup_path).exec();
	return sqlite3_last_insert_rowid(impl_->db);
}

void Repository::log_op_complete(std::int64_t op_id, OpStatus status, const std::string& message)
{
	auto st = impl_->prepare("UPDATE op_log SET status=?, message=? WHERE id=?;", "log_op_complete");
	st.bind_static(1, op_status_to_str(status)).bind(2, message).bind(3, op_id).exec();
}

std::vector<Repository::LoggedOp> Repository::list_ops(OpStatus status) const
{
	auto st = impl_->prepare(
	    "SELECT id, canonical, duplicate, backup_path, status, message FROM op_log WHERE status=? ORDER BY id;",
	    "list_ops");
	st.bind_static(1, op_status_to_str(status));

	std::vector<LoggedOp> ops;
	while (st.step() == SQLITE_ROW) {
		ops.push_back(logged_op_from_row(st.raw()));
	}
	return ops;
}

std::int64_t Repository::now_unix_s() const
{
	auto st = impl_->prepare("SELECT strftime('%s','now');", "now_unix_s");
	if (st.step() != SQLITE_ROW) {
		throw std::runtime_error(std::format("now_unix_s: {}", sqlite3_errmsg(impl_->db)));
	}
	return sqlite3_column_int64(st.raw(), 0);
}

} // namespace deduped
