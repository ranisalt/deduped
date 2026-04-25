#include "repository.hpp"

#include <cstring>
#include <sqlite3.h>
#include <stdexcept>
#include <string>

namespace deduped {

static constexpr const char* kSchema = R"sql(
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY,
    path        TEXT    NOT NULL UNIQUE,
    size        INTEGER NOT NULL,
    mtime_ns    INTEGER NOT NULL,
    inode       INTEGER NOT NULL,
    device      INTEGER NOT NULL,
    mode        INTEGER NOT NULL,
    uid         INTEGER NOT NULL,
    gid         INTEGER NOT NULL,
    digest      BLOB    NOT NULL,   -- 32 bytes BLAKE3
    last_seen   INTEGER NOT NULL    -- Unix seconds
);

CREATE INDEX IF NOT EXISTS idx_files_digest ON files(digest);

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

static constexpr int kCurrentSchemaVersion = 2;

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

	void reset()
	{
		sqlite3_reset(s);
		sqlite3_clear_bindings(s);
	}
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

	void bootstrap_version()
	{
		ensure_backup_path_column();
		set_user_version(kCurrentSchemaVersion);
	}

	[[nodiscard]] int user_version() const
	{
		Stmt st(db, "PRAGMA user_version;");
		const int rc = sqlite3_step(st.s);
		if (rc != SQLITE_ROW) {
			throw std::runtime_error(std::string("PRAGMA user_version: ") + sqlite3_errmsg(db));
		}
		return sqlite3_column_int(st.s, 0);
	}

	void set_user_version(const int version)
	{
		exec(("PRAGMA user_version = " + std::to_string(version) + ";").c_str());
	}

	[[nodiscard]] bool table_has_column(const char* table_name, const char* column_name) const
	{
		Stmt st(db, ("PRAGMA table_info(" + std::string(table_name) + ");").c_str());
		while (sqlite3_step(st.s) == SQLITE_ROW) {
			const auto* value = reinterpret_cast<const char*>(sqlite3_column_text(st.s, 1));
			if (value != nullptr && std::string_view{value} == column_name) {
				return true;
			}
		}
		return false;
	}

	void ensure_backup_path_column()
	{
		if (!table_has_column("op_log", "backup_path")) {
			exec("ALTER TABLE op_log ADD COLUMN backup_path TEXT NOT NULL DEFAULT '';");
		}
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
};

Repository::Repository(const std::filesystem::path& db_path) : impl_(std::make_unique<Impl>(db_path)) {}

Repository::~Repository() = default;

void Repository::upsert(const IndexEntry& entry)
{
	static constexpr const char* kSql = R"sql(
        INSERT INTO files(path, size, mtime_ns, inode, device, mode, uid, gid, digest, last_seen)
        VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET
            size      = excluded.size,
            mtime_ns  = excluded.mtime_ns,
            inode     = excluded.inode,
            device    = excluded.device,
            mode      = excluded.mode,
            uid       = excluded.uid,
            gid       = excluded.gid,
            digest    = excluded.digest,
            last_seen = excluded.last_seen;
    )sql";

	Stmt st(impl_->db, kSql);
	sqlite3_bind_text(st.s, 1, entry.path.c_str(), -1, SQLITE_TRANSIENT);
	sqlite3_bind_int64(st.s, 2, static_cast<sqlite3_int64>(entry.meta.size));
	sqlite3_bind_int64(st.s, 3, entry.meta.mtime_ns);
	sqlite3_bind_int64(st.s, 4, static_cast<sqlite3_int64>(entry.meta.inode));
	sqlite3_bind_int64(st.s, 5, static_cast<sqlite3_int64>(entry.meta.device));
	sqlite3_bind_int64(st.s, 6, static_cast<sqlite3_int64>(entry.meta.mode));
	sqlite3_bind_int64(st.s, 7, static_cast<sqlite3_int64>(entry.meta.uid));
	sqlite3_bind_int64(st.s, 8, static_cast<sqlite3_int64>(entry.meta.gid));
	sqlite3_bind_blob(st.s, 9, entry.digest.data(), static_cast<int>(kDigestBytes), SQLITE_TRANSIENT);
	sqlite3_bind_int64(st.s, 10, entry.last_seen);
	impl_->step_expect_done(st.s);
}

std::optional<IndexEntry> Repository::find_by_path(const std::string& path) const
{
	static constexpr const char* kSql =
	    "SELECT id,path,size,mtime_ns,inode,device,mode,uid,gid,digest,last_seen "
	    "FROM files WHERE path=? LIMIT 1;";

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

std::vector<IndexEntry> Repository::find_by_digest(const Digest& digest) const
{
	static constexpr const char* kSql =
	    "SELECT id,path,size,mtime_ns,inode,device,mode,uid,gid,digest,last_seen "
	    "FROM files WHERE digest=? ORDER BY path;";

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
	Stmt st(impl_->db, "DELETE FROM files WHERE last_seen < ?;");
	sqlite3_bind_int64(st.s, 1, cutoff_unix_s);
	impl_->step_expect_done(st.s);
}

void Repository::remove_by_path(const std::string& path)
{
	Stmt st(impl_->db, "DELETE FROM files WHERE path=?;");
	sqlite3_bind_text(st.s, 1, path.c_str(), -1, SQLITE_TRANSIENT);
	impl_->step_expect_done(st.s);
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
