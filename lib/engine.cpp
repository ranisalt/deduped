#include "engine.hpp"

#include "hasher.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <map>
#include <random>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <system_error>
#include <unistd.h>

namespace deduped {

namespace {

[[nodiscard]] ApplyResult make_apply_result(const Repository::LoggedOp& op)
{
	ApplyResult result;
	result.pair.canonical_path = op.canonical_path;
	result.pair.duplicate_path = op.duplicate_path;
	return result;
}

void notify_apply(const EngineCallbacks& cbs, const ApplyResult& result)
{
	if (cbs.on_apply) {
		cbs.on_apply(result);
	}
}

} // namespace

namespace detail {

ResolvedDigest resolve_digest(Repository& repo, const std::filesystem::path& path, const FileMeta& current_meta,
                              const HashFileFn& hash_file_fn)
{
	const auto existing = repo.find_by_path(path.string());
	if (existing && !is_meta_stale(existing->meta, current_meta)) {
		return {.digest = existing->digest, .reused_cached_digest = true};
	}

	return {.digest = hash_file_fn(path), .reused_cached_digest = false};
}

} // namespace detail

namespace {

void upsert_file_index(Repository& repo, const std::filesystem::path& p, const std::int64_t now)
{
	const auto cur_meta = meta_from_path(p);
	const auto resolved = detail::resolve_digest(repo, p, cur_meta, hash_file);
	IndexEntry e;
	e.path = p.string();
	e.meta = cur_meta;
	e.digest = resolved.digest;
	e.last_seen = now;
	repo.upsert(e);
}

// Apply a single hardlink replacement safely:
//   1. Log the planned operation.
//   2. Verify preconditions (same device, not already same inode, not a symlink).
//   3. Rename duplicate to a temp name.
//   4. link(canonical, duplicate).
//   5. Remove the renamed temp file.
//   On any failure: restore from temp, update log.
ApplyResult apply_pair(Repository& repo, const DupePair& pair)
{
	ApplyResult result;
	result.pair = pair;
	std::optional<std::int64_t> op_id;

	// pre-flight checks
	try {
		namespace fs = std::filesystem;
		const auto& cp = pair.canonical_path;
		const auto& dp = pair.duplicate_path;

		// refuse symlinks
		if (fs::is_symlink(cp) || fs::is_symlink(dp)) {
			result.status = ApplyStatus::Skipped;
			result.message = "symlink detected, skipped";
			return result;
		}

		// cross-device check
		const auto cm = meta_from_path(cp);
		const auto dm = meta_from_path(dp);
		if (cm.device != dm.device) {
			result.status = ApplyStatus::Skipped;
			result.message = "cross-device, cannot hardlink";
			return result;
		}

		// Already the same inode.
		if (cm.inode == dm.inode) {
			result.status = ApplyStatus::AlreadyLinked;
			return result;
		}

		// Re-verify both files match the planned digest (guard against TOCTOU).
		const auto canonical_digest = hash_file(cp);
		if (canonical_digest != pair.digest) {
			result.status = ApplyStatus::Skipped;
			result.message = "canonical file changed between scan and apply, skipped";
			return result;
		}

		const auto duplicate_digest = hash_file(dp);
		if (duplicate_digest != pair.digest) {
			result.status = ApplyStatus::Skipped;
			result.message = "file changed between scan and apply, skipped";
			return result;
		}

		// log planned
		static thread_local std::mt19937_64 prng{std::random_device{}()};
		const auto now_val = std::chrono::steady_clock::now().time_since_epoch().count();
		const auto nonce = prng();
		const std::string tmp = dp + ".deduped_tmp." + std::to_string(now_val) + "." + std::to_string(nonce);
		op_id = repo.log_op_planned(cp, dp, tmp);

		// atomic swap using rename + link
		namespace fs = std::filesystem;
		std::error_code ec;
		fs::rename(dp, tmp, ec);
		if (ec) {
			throw std::system_error(ec, "rename to tmp");
		}

		if (::link(cp.c_str(), dp.c_str()) != 0) {
			// Restore original.
			const auto link_errno = errno;
			fs::rename(tmp, dp, ec);
			if (ec) {
				throw std::runtime_error("link failed and duplicate restore failed");
			}
			throw std::system_error(link_errno, std::generic_category(), "link");
		}

		// Remove backup.
		fs::remove(tmp, ec);
		if (ec) {
			throw std::system_error(ec, "remove backup");
		}

		repo.log_op_complete(*op_id, Repository::OpStatus::Done);
		result.status = ApplyStatus::Linked;

	} catch (const std::exception& ex) {
		if (op_id.has_value()) {
			repo.log_op_complete(*op_id, Repository::OpStatus::Failed, ex.what());
		}
		result.status = ApplyStatus::Failed;
		result.message = ex.what();
	}

	return result;
}

} // namespace

void recover_pending_operations(Repository& repo, const EngineCallbacks& cbs)
{
	namespace fs = std::filesystem;

	for (const auto& op : repo.list_ops(Repository::OpStatus::Planned)) {
		auto result = make_apply_result(op);
		const fs::path canonical{op.canonical_path};
		const fs::path duplicate{op.duplicate_path};
		const fs::path backup{op.backup_path};

		try {
			std::error_code ec;
			const bool canonical_exists = fs::exists(canonical, ec) && !ec;
			const bool duplicate_exists = fs::exists(duplicate, ec) && !ec;
			const bool backup_exists = !op.backup_path.empty() && fs::exists(backup, ec) && !ec;

			if (canonical_exists && duplicate_exists) {
				const auto canonical_meta = meta_from_path(canonical);
				const auto duplicate_meta = meta_from_path(duplicate);
				if (canonical_meta.inode == duplicate_meta.inode) {
					if (backup_exists && !fs::remove(backup, ec) && ec) {
						throw fs::filesystem_error("remove backup during recovery", backup, ec);
					}
					repo.log_op_complete(op.id, Repository::OpStatus::Done);
					result.status = ApplyStatus::Linked;
					notify_apply(cbs, result);
					continue;
				}
			}

			if (backup_exists && !duplicate_exists) {
				fs::rename(backup, duplicate, ec);
				if (ec) {
					throw fs::filesystem_error("restore duplicate during recovery", backup, duplicate, ec);
				}
				repo.log_op_complete(op.id, Repository::OpStatus::Failed, "recovered original after interrupted apply");
				result.status = ApplyStatus::Failed;
				result.message = "recovered original after interrupted apply";
				notify_apply(cbs, result);
				continue;
			}

			repo.log_op_complete(op.id, Repository::OpStatus::Failed,
			                     "interrupted apply could not be automatically recovered");
			result.status = ApplyStatus::Failed;
			result.message = "interrupted apply could not be automatically recovered";
		} catch (const std::exception& ex) {
			repo.log_op_complete(op.id, Repository::OpStatus::Failed, ex.what());
			result.status = ApplyStatus::Failed;
			result.message = ex.what();
		}

		notify_apply(cbs, result);
	}
}

std::vector<ApplyResult> run_engine(Repository& repo, const ScanOptions& scan_opts, const EngineOptions& engine_opts,
                                    const EngineCallbacks& cbs)
{
	const auto now = repo.now_unix_s();

	// Group paths by size first, only files sharing a size need hashing.
	std::map<std::uint64_t, std::vector<std::filesystem::path>> by_size;

	scan_files(scan_opts, [&](const std::filesystem::path& p) {
		if (cbs.on_scan) cbs.on_scan(p.string());

		try {
			const auto meta = meta_from_path(p);
			by_size[meta.size].push_back(p);
			upsert_file_index(repo, p, now);

		} catch (const std::exception& ex) {
			// Permission or I/O error for a single file; skip it.
			spdlog::debug("skipping {}: {}", p.string(), ex.what());
		}
	});

	// Only check digests for size groups with 2+ files.
	std::vector<ApplyResult> results;

	for (const auto& [size, paths] : by_size) {
		if (paths.size() < 2) continue;

		// Collect distinct digests within this size group.
		std::map<Digest, std::vector<IndexEntry>> by_digest;
		for (const auto& p : paths) {
			auto e = repo.find_by_path(p.string());
			if (e) by_digest[e->digest].push_back(*e);
		}

		for (const auto& [digest, entries] : by_digest) {
			if (entries.size() < 2) continue;

			auto sorted_entries = entries;
			std::ranges::sort(sorted_entries, {}, &IndexEntry::path);

			// Canonical is always the lexicographically smallest path.
			const auto& canonical = sorted_entries.front();
			for (std::size_t i = 1; i < sorted_entries.size(); ++i) {
				DupePair pair;
				pair.canonical_path = canonical.path;
				pair.duplicate_path = sorted_entries[i].path;
				pair.digest = digest;
				pair.size_bytes = size;

				if (cbs.on_dupe_found) cbs.on_dupe_found(pair);

				if (engine_opts.dry_run) {
					ApplyResult r;
					r.pair = pair;
					r.status = ApplyStatus::Skipped;
					r.message = "dry-run";
					if (cbs.on_apply) cbs.on_apply(r);
					results.push_back(std::move(r));
				} else {
					auto r = apply_pair(repo, pair);
					if (cbs.on_apply) cbs.on_apply(r);
					results.push_back(std::move(r));
				}
			}
		}
	}

	repo.remove_stale(now - 1); // keep records from this very second

	return results;
}

std::optional<ApplyResult> handle_file_change(Repository& repo, const std::filesystem::path& path,
                                              const EngineOptions& opts, const EngineCallbacks& cbs)
{
	namespace fs = std::filesystem;

	// Guard: only index regular files; skip symlinks.
	std::error_code ec;
	if (!fs::is_regular_file(path, ec) || fs::is_symlink(path, ec)) return std::nullopt;

	const auto now = repo.now_unix_s();

	try {
		const auto cur_meta = meta_from_path(path);
		const auto resolved = detail::resolve_digest(repo, path, cur_meta, hash_file);

		IndexEntry e;
		e.path = path.string();
		e.meta = cur_meta;
		e.digest = resolved.digest;
		e.last_seen = now;
		repo.upsert(e);

		if (cbs.on_scan) cbs.on_scan(path.string());

		// Find all other entries with the same digest.
		auto matches = repo.find_by_digest(resolved.digest);
		std::erase_if(matches, [&](const IndexEntry& m) { return m.path == path.string(); });

		if (matches.empty()) return std::nullopt;

		// Canonical = lexicographically smaller path.
		const std::string& other = matches.front().path;
		DupePair pair;
		if (other < path.string()) {
			pair.canonical_path = other;
			pair.duplicate_path = path.string();
		} else {
			pair.canonical_path = path.string();
			pair.duplicate_path = other;
		}
		pair.digest = resolved.digest;
		pair.size_bytes = cur_meta.size;

		if (cbs.on_dupe_found) cbs.on_dupe_found(pair);

		if (opts.dry_run) {
			ApplyResult r;
			r.pair = pair;
			r.status = ApplyStatus::Skipped;
			r.message = "dry-run";
			if (cbs.on_apply) cbs.on_apply(r);
			return r;
		}

		auto r = apply_pair(repo, pair);
		if (cbs.on_apply) cbs.on_apply(r);
		return r;

	} catch (const std::exception& ex) {
		// I/O error or permission problem for this one file; skip it.
		spdlog::debug("handle_file_change skipping {}: {}", path.string(), ex.what());
		return std::nullopt;
	}
}

void handle_file_removed(Repository& repo, const std::filesystem::path& path) { repo.remove_by_path(path.string()); }

} // namespace deduped
