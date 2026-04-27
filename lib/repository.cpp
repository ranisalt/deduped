#include "repository.hpp"

#include <cstring>
#include <sqlite3.h>
#include <stdexcept>
#include <string>

namespace deduped {

static constexpr const char* kSchema = R"sql(
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

static constexpr int kCurrentSchemaVersion = 3;

constexpr const char* op_status_str(Repository::OpStatus s) noexcept
{
	switch (s) {
		case Repository::OpStatus::Done:
			return "done";
		case Repository::OpStatus::Failed:
			return "failed";
		default:
			return "planned";
	}
}

struct Stmt
{
	sqlite3_stmt* s{nullptr};
	explicit Stmt(sqlite3* db, const char* sql)
	{
		if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK) {
			throw std::runtime_error(std::string("SQLite prepare: ") + sqlite3_errmsg(db));
		}
	}
	~Stmt()
	{
		if (s) sqlite3_finalize(s);
	}
	Stmt(const Stmt&) = delete;
	Stmt& operator=(const Stmt&) = delete;
};

struct Repository::Impl
{
	sqlite3* db{nullptr};

	explicit Impl(const std::filesystem::path& path)
	{
		if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
			std::string msg = db ? sqlite3_errmsg(db) : "sqlite3_open failed";
			sqlite3_close(db);
			throw std::runtime_error("Repository: " + msg);
		}
		exec(kSchema);
		bootstrap_version();
	}

	~Impl()
	{
		if (db) sqlite3_close(db);
	}

	void exec(const char* sql)
	{
		char* err = nullptr;
		if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
			std::string msg = err ? err : "(null)";
			sqlite3_free(err);
			throw std::runtime_error("SQLite exec: " + msg);
		}
	}

	void bootstrap_version() { set_user_version(kCurrentSchemaVersion); }

	void set_user_version(const int version)
	{
		exec(("PRAGMA user_version = " + std::to_string(version) + ";").c_str());
	}

	int step_expect_done(sqlite3_stmt* s)
	{
		int rc = sqlite3_step(s);
		if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
			throw std::runtime_error(std::string("SQLite step: ") + sqlite3_errmsg(db));
		}
		return rc;
	}

	// Build an IndexEntry from the current row of a statement positioned on a row.
	static IndexEntry entry_from_row(sqlite3_stmt* s)
	{
		IndexEntry e;
		e.id = sqlite3_column_int64(s, 0);
		e.path = reinterpret_cast<const char*>(sqlite3_column_text(s, 1));
		e.meta.size = static_cast<std::uint64_t>(sqlite3_column_int64(s, 2));
		e.meta.mtime_ns = sqlite3_column_int64(s, 3);
		e.meta.inode = static_cast<std::uint64_t>(sqlite3_column_int64(s, 4));
		e.meta.device = static_cast<std::uint64_t>(sqlite3_column_int64(s, 5));
		e.meta.mode = static_cast<std::uint32_t>(sqlite3_column_int64(s, 6));
		e.meta.uid = static_cast<std::uint32_t>(sqlite3_column_int64(s, 7));
		e.meta.gid = static_cast<std::uint32_t>(sqlite3_column_int64(s, 8));
		const void* blob = sqlite3_column_blob(s, 9);
		int blob_len = sqlite3_column_bytes(s, 9);
		if (blob_len == static_cast<int>(kDigestBytes)) {
			std::memcpy(e.digest.data(), blob, kDigestBytes);
		}
		e.last_seen = sqlite3_column_int64(s, 10);
		return e;
	}

	static Repository::LoggedOp logged_op_from_row(sqlite3_stmt* s)
	{
		Repository::LoggedOp op;
		op.id = sqlite3_column_int64(s, 0);
		op.canonical_path = reinterpret_cast<const char*>(sqlite3_column_text(s, 1));
		op.duplicate_path = reinterpret_cast<const char*>(sqlite3_column_text(s, 2));
		op.backup_path = reinterpret_cast<const char*>(sqlite3_column_text(s, 3));

		const auto* status = reinterpret_cast<const char*>(sqlite3_column_text(s, 4));
		if (status != nullptr) {
			const std::string_view status_view{status};
			if (status_view == "done") {
				op.status = Repository::OpStatus::Done;
			} else if (status_view == "failed") {
				op.status = Repository::OpStatus::Failed;
			}
		}

		const auto* message = reinterpret_cast<const char*>(sqlite3_column_text(s, 5));
		if (message != nullptr) {
			op.message = message;
		}
		return op;
	}

	[[nodiscard]] std::optional<std::int64_t> inode_id_for_path(const std::string& path) const
	{
		Stmt st(db, "SELECT inode_id FROM paths WHERE path=? LIMIT 1;");
		sqlite3_bind_text(st.s, 1, path.c_str(), -1, SQLITE_TRANSIENT);
		const int rc = sqlite3_step(st.s);
		if (rc == SQLITE_ROW) {
			return sqlite3_column_int64(st.s, 0);
		}
		if (rc == SQLITE_DONE) {
			return std::nullopt;
		}
		throw std::runtime_error(std::string("inode_id_for_path: ") + sqlite3_errmsg(db));
	}

	[[nodiscard]] std::int64_t upsert_inode_and_get_id(const IndexEntry& entry)
	{
		static constexpr const char* kUpsertInodeSql = R"sql(
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

		Stmt upsert_inode(db, kUpsertInodeSql);
		sqlite3_bind_int64(upsert_inode.s, 1, static_cast<sqlite3_int64>(entry.meta.device));
		sqlite3_bind_int64(upsert_inode.s, 2, static_cast<sqlite3_int64>(entry.meta.inode));
		sqlite3_bind_int64(upsert_inode.s, 3, static_cast<sqlite3_int64>(entry.meta.size));
		sqlite3_bind_int64(upsert_inode.s, 4, entry.meta.mtime_ns);
		sqlite3_bind_int64(upsert_inode.s, 5, static_cast<sqlite3_int64>(entry.meta.mode));
		sqlite3_bind_int64(upsert_inode.s, 6, static_cast<sqlite3_int64>(entry.meta.uid));
		sqlite3_bind_int64(upsert_inode.s, 7, static_cast<sqlite3_int64>(entry.meta.gid));
		sqlite3_bind_blob(upsert_inode.s, 8, entry.digest.data(), static_cast<int>(kDigestBytes), SQLITE_TRANSIENT);
		sqlite3_bind_int64(upsert_inode.s, 9, entry.last_seen);
		step_expect_done(upsert_inode.s);

		Stmt inode_id_stmt(db, "SELECT id FROM inodes WHERE device=? AND inode=? LIMIT 1;");
		sqlite3_bind_int64(inode_id_stmt.s, 1, static_cast<sqlite3_int64>(entry.meta.device));
		sqlite3_bind_int64(inode_id_stmt.s, 2, static_cast<sqlite3_int64>(entry.meta.inode));
		const int rc = sqlite3_step(inode_id_stmt.s);
		if (rc != SQLITE_ROW) {
			throw std::runtime_error(std::string("upsert_inode_and_get_id: ") + sqlite3_errmsg(db));
		}
		return sqlite3_column_int64(inode_id_stmt.s, 0);
	}

	void maybe_delete_orphan_inode(const std::int64_t inode_id)
	{
		Stmt st(db,
		        "DELETE FROM inodes WHERE id=? "
		        "AND NOT EXISTS(SELECT 1 FROM paths WHERE inode_id=? LIMIT 1);");
		sqlite3_bind_int64(st.s, 1, inode_id);
		sqlite3_bind_int64(st.s, 2, inode_id);
		step_expect_done(st.s);
	}
};

Repository::Repository(const std::filesystem::path& db_path) : impl_(std::make_unique<Impl>(db_path)) {}

Repository::~Repository() = default;

void Repository::upsert(const IndexEntry& entry)
{
	impl_->exec("BEGIN IMMEDIATE TRANSACTION;");
	try {
		const auto previous_inode_id = impl_->inode_id_for_path(entry.path);
		const auto inode_id = impl_->upsert_inode_and_get_id(entry);

		Stmt upsert_path(impl_->db,
		                 "INSERT INTO paths(path, inode_id, last_seen) VALUES(?,?,?) "
		                 "ON CONFLICT(path) DO UPDATE SET inode_id=excluded.inode_id, last_seen=excluded.last_seen;");
		sqlite3_bind_text(upsert_path.s, 1, entry.path.c_str(), -1, SQLITE_TRANSIENT);
		sqlite3_bind_int64(upsert_path.s, 2, inode_id);
		sqlite3_bind_int64(upsert_path.s, 3, entry.last_seen);
		impl_->step_expect_done(upsert_path.s);

		if (previous_inode_id.has_value() && *previous_inode_id != inode_id) {
			impl_->maybe_delete_orphan_inode(*previous_inode_id);
		}

		impl_->exec("COMMIT;");
	} catch (...) {
		impl_->exec("ROLLBACK;");
		throw;
	}
}

std::optional<IndexEntry> Repository::find_by_path(const std::string& path) const
{
	static constexpr const char* kSql =
	    "SELECT i.id,p.path,i.size,i.mtime_ns,i.inode,i.device,i.mode,i.uid,i.gid,i.digest,p.last_seen "
	    "FROM paths p JOIN inodes i ON i.id=p.inode_id "
	    "WHERE p.path=? LIMIT 1;";

	Stmt st(impl_->db, kSql);
	sqlite3_bind_text(st.s, 1, path.c_str(), -1, SQLITE_TRANSIENT);
	int rc = sqlite3_step(st.s);
	if (rc == SQLITE_ROW) {
		return Impl::entry_from_row(st.s);
	}
	if (rc == SQLITE_DONE) {
		return std::nullopt;
	}
	throw std::runtime_error(std::string("find_by_path: ") + sqlite3_errmsg(impl_->db));
}

std::optional<IndexEntry> Repository::find_by_inode(const std::uint64_t device, const std::uint64_t inode) const
{
	static constexpr const char* kSql =
	    "SELECT i.id, "
	    "COALESCE((SELECT p.path FROM paths p WHERE p.inode_id=i.id ORDER BY p.path LIMIT 1), ''), "
	    "i.size,i.mtime_ns,i.inode,i.device,i.mode,i.uid,i.gid,i.digest,i.last_seen "
	    "FROM inodes i WHERE i.device=? AND i.inode=? LIMIT 1;";

	Stmt st(impl_->db, kSql);
	sqlite3_bind_int64(st.s, 1, static_cast<sqlite3_int64>(device));
	sqlite3_bind_int64(st.s, 2, static_cast<sqlite3_int64>(inode));
	const int rc = sqlite3_step(st.s);
	if (rc == SQLITE_ROW) {
		return Impl::entry_from_row(st.s);
	}
	if (rc == SQLITE_DONE) {
		return std::nullopt;
	}
	throw std::runtime_error(std::string("find_by_inode: ") + sqlite3_errmsg(impl_->db));
}

std::vector<IndexEntry> Repository::find_by_digest(const Digest& digest) const
{
	static constexpr const char* kSql =
	    "SELECT i.id,p.path,i.size,i.mtime_ns,i.inode,i.device,i.mode,i.uid,i.gid,i.digest,p.last_seen "
	    "FROM inodes i JOIN paths p ON p.inode_id=i.id "
	    "WHERE i.digest=? ORDER BY p.path;";

	Stmt st(impl_->db, kSql);
	sqlite3_bind_blob(st.s, 1, digest.data(), static_cast<int>(kDigestBytes), SQLITE_TRANSIENT);

	std::vector<IndexEntry> results;
	while (true) {
		const int rc = sqlite3_step(st.s);
		if (rc == SQLITE_DONE) {
			break;
		}
		if (rc != SQLITE_ROW) {
			throw std::runtime_error(std::string("find_by_digest: ") + sqlite3_errmsg(impl_->db));
		}
		results.push_back(Impl::entry_from_row(st.s));
	}
	return results;
}

void Repository::remove_stale(std::int64_t cutoff_unix_s)
{
	impl_->exec("BEGIN IMMEDIATE TRANSACTION;");
	try {
		Stmt stale_paths(impl_->db, "DELETE FROM paths WHERE last_seen < ?;");
		sqlite3_bind_int64(stale_paths.s, 1, cutoff_unix_s);
		impl_->step_expect_done(stale_paths.s);

		Stmt prune_orphans(impl_->db,
		                   "DELETE FROM inodes WHERE NOT EXISTS(SELECT 1 FROM paths WHERE paths.inode_id=inodes.id);");
		impl_->step_expect_done(prune_orphans.s);

		impl_->exec("COMMIT;");
	} catch (...) {
		impl_->exec("ROLLBACK;");
		throw;
	}
}

void Repository::remove_by_path(const std::string& path)
{
	impl_->exec("BEGIN IMMEDIATE TRANSACTION;");
	try {
		const auto inode_id = impl_->inode_id_for_path(path);
		Stmt delete_path(impl_->db, "DELETE FROM paths WHERE path=?;");
		sqlite3_bind_text(delete_path.s, 1, path.c_str(), -1, SQLITE_TRANSIENT);
		impl_->step_expect_done(delete_path.s);

		if (inode_id.has_value()) {
			impl_->maybe_delete_orphan_inode(*inode_id);
		}

		impl_->exec("COMMIT;");
	} catch (...) {
		impl_->exec("ROLLBACK;");
		throw;
	}
}

std::int64_t Repository::log_op_planned(const std::string& canonical, const std::string& duplicate,
                                        const std::string& backup_path)
{
	static constexpr const char* kSql =
	    "INSERT INTO op_log(canonical, duplicate, backup_path, status) VALUES(?,?,?,'planned');";

	Stmt st(impl_->db, kSql);
	sqlite3_bind_text(st.s, 1, canonical.c_str(), -1, SQLITE_TRANSIENT);
	sqlite3_bind_text(st.s, 2, duplicate.c_str(), -1, SQLITE_TRANSIENT);
	sqlite3_bind_text(st.s, 3, backup_path.c_str(), -1, SQLITE_TRANSIENT);
	impl_->step_expect_done(st.s);
	return sqlite3_last_insert_rowid(impl_->db);
}

void Repository::log_op_complete(std::int64_t op_id, OpStatus status, const std::string& message)
{
	Stmt st(impl_->db, "UPDATE op_log SET status=?, message=? WHERE id=?;");
	sqlite3_bind_text(st.s, 1, op_status_str(status), -1, SQLITE_STATIC);
	sqlite3_bind_text(st.s, 2, message.c_str(), -1, SQLITE_TRANSIENT);
	sqlite3_bind_int64(st.s, 3, op_id);
	impl_->step_expect_done(st.s);
}

std::vector<Repository::LoggedOp> Repository::list_ops(const OpStatus status) const
{
	static constexpr const char* kSql =
	    "SELECT id, canonical, duplicate, backup_path, status, message FROM op_log WHERE status=? ORDER BY id;";

	Stmt st(impl_->db, kSql);
	sqlite3_bind_text(st.s, 1, op_status_str(status), -1, SQLITE_STATIC);

	std::vector<LoggedOp> ops;
	while (true) {
		const int rc = sqlite3_step(st.s);
		if (rc == SQLITE_DONE) {
			break;
		}
		if (rc != SQLITE_ROW) {
			throw std::runtime_error(std::string("list_ops: ") + sqlite3_errmsg(impl_->db));
		}
		ops.push_back(Impl::logged_op_from_row(st.s));
	}
	return ops;
}

std::int64_t Repository::now_unix_s() const
{
	Stmt st(impl_->db, "SELECT strftime('%s','now');");
	const int rc = sqlite3_step(st.s);
	if (rc != SQLITE_ROW) {
		throw std::runtime_error(std::string("now_unix_s: ") + sqlite3_errmsg(impl_->db));
	}
	return sqlite3_column_int64(st.s, 0);
}

} // namespace deduped
