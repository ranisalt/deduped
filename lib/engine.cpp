#include "engine.hpp"

#include "hasher.hpp"
#include "scan_coordinator.hpp"
#include "scope_exit.hpp"

#include <algorithm>
#include <boost/container/flat_map.hpp>
#include <cerrno>
#include <chrono>
#include <expected>
#include <filesystem>
#include <random>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <system_error>
#include <unistd.h>

namespace deduped {

namespace {

constexpr std::string_view kDuplicatePathNotWritableMessage = "duplicate path not writable, skipped";

ApplyResult apply_pair(IRepository& repo, const DupePair& pair, const HashShouldAbortFn& should_abort);

template <typename AbortFn>
std::vector<ApplyResult> build_apply_results(IRepository& repo, const EngineOptions& engine_opts,
                                             const EngineCallbacks& cbs, const std::vector<IndexEntry>& cached_entries,
                                             const std::vector<IndexEntry>& hashed_entries,
                                             AbortFn&& throw_if_main_aborted)
{
	boost::container::flat_map<std::uint64_t, std::vector<IndexEntry>> indexed_by_size;
	for (const auto& entry : cached_entries) {
		indexed_by_size[entry.meta.size].push_back(entry);
	}
	for (const auto& entry : hashed_entries) {
		indexed_by_size[entry.meta.size].push_back(entry);
	}

	std::vector<ApplyResult> results;

	for (const auto& [size, entries_for_size] : indexed_by_size) {
		throw_if_main_aborted();

		if (entries_for_size.size() < 2) continue;

		boost::container::flat_map<Digest, std::vector<IndexEntry>> by_digest;
		for (const auto& entry : entries_for_size) {
			by_digest[entry.digest].push_back(entry);
		}

		for (const auto& [digest, entries] : by_digest) {
			throw_if_main_aborted();

			if (entries.size() < 2) continue;

			const auto canonical_it =
			    std::min_element(entries.begin(), entries.end(),
			                     [](const IndexEntry& a, const IndexEntry& b) { return a.path < b.path; });
			const auto& canonical = *canonical_it;
			for (const auto& entry : entries) {
				if (entry.path == canonical.path) {
					continue;
				}

				const bool already_linked_to_canonical =
				    entry.meta.device == canonical.meta.device && entry.meta.inode == canonical.meta.inode;
				if (already_linked_to_canonical && engine_opts.dry_run) {
					continue;
				}
				throw_if_main_aborted();

				DupePair pair;
				pair.canonical_path = canonical.path;
				pair.duplicate_path = entry.path;
				pair.digest = digest;
				pair.size_bytes = size;

				if (cbs.on_dupe_found) cbs.on_dupe_found(pair);
				throw_if_main_aborted();

				if (engine_opts.dry_run) {
					ApplyResult result;
					result.pair = pair;
					result.status = ApplyStatus::Skipped;
					result.message = "dry-run";
					if (cbs.on_apply) cbs.on_apply(result);
					results.push_back(std::move(result));
				} else {
					auto result = apply_pair(repo, pair, cbs.should_abort);
					if (cbs.on_apply) cbs.on_apply(result);
					results.push_back(std::move(result));
				}
			}
		}
	}

	return results;
}

[[nodiscard]] std::expected<void, ApplyResult> validate_apply_preconditions(const DupePair& pair,
                                                                            const HashShouldAbortFn& should_abort)
{
	namespace fs = std::filesystem;
	ApplyResult result;
	result.pair = pair;

	const auto& cp = pair.canonical_path;
	const auto& dp = pair.duplicate_path;

	if (fs::is_symlink(cp) || fs::is_symlink(dp)) {
		result.status = ApplyStatus::Skipped;
		result.message = "symlink detected, skipped";
		return std::unexpected(result);
	}

	const auto cm = meta_from_path(cp);
	const auto dm = meta_from_path(dp);
	if (cm.device != dm.device) {
		result.status = ApplyStatus::Skipped;
		result.message = "cross-device, cannot hardlink";
		return std::unexpected(result);
	}

	if (cm.inode == dm.inode) {
		result.status = ApplyStatus::AlreadyLinked;
		return std::unexpected(result);
	}

	const auto canonical_digest = hash_file(cp, should_abort);
	if (canonical_digest != pair.digest) {
		result.status = ApplyStatus::Skipped;
		result.message = "canonical file changed between scan and apply, skipped";
		return std::unexpected(result);
	}

	const auto duplicate_digest = hash_file(dp, should_abort);
	if (duplicate_digest != pair.digest) {
		result.status = ApplyStatus::Skipped;
		result.message = "file changed between scan and apply, skipped";
		return std::unexpected(result);
	}

	return {};
}

[[nodiscard]] std::string make_temp_backup_path(const std::string& duplicate_path)
{
	static thread_local std::mt19937_64 prng{std::random_device{}()};
	const auto now_val = std::chrono::steady_clock::now().time_since_epoch().count();
	const auto nonce = prng();
	return duplicate_path + ".deduped_tmp." + std::to_string(now_val) + "." + std::to_string(nonce);
}

void perform_atomic_hardlink_swap(const std::string& canonical_path, const std::string& duplicate_path,
                                  const std::string& tmp_backup_path)
{
	namespace fs = std::filesystem;
	std::error_code ec;
	fs::rename(duplicate_path, tmp_backup_path, ec);
	if (ec) {
		throw std::system_error(ec, "rename to tmp");
	}

	if (::link(canonical_path.c_str(), duplicate_path.c_str()) != 0) {
		const auto link_errno = errno;
		fs::rename(tmp_backup_path, duplicate_path, ec);
		if (ec) {
			throw std::runtime_error("link failed and duplicate restore failed");
		}
		throw std::system_error(link_errno, std::generic_category(), "link");
	}

	fs::remove(tmp_backup_path, ec);
	if (ec) {
		throw std::system_error(ec, "remove backup");
	}
}

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

void throw_if_aborted(const EngineCallbacks& cbs)
{
	if (cbs.should_abort && cbs.should_abort()) {
		throw ScanInterrupted{};
	}
}

} // namespace

namespace detail {

ResolvedDigest resolve_digest(IRepository& repo, const std::filesystem::path& path, const FileMeta& current_meta,
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
// Apply a single hardlink replacement safely:
//   1. Log the planned operation.
//   2. Verify preconditions (same device, not already same inode, not a symlink).
//   3. Rename duplicate to a temp name.
//   4. link(canonical, duplicate).
//   5. Remove the renamed temp file.
//   On any failure: restore from temp, update log.
ApplyResult apply_pair(IRepository& repo, const DupePair& pair, const HashShouldAbortFn& should_abort = {})
{
	ApplyResult result;
	result.pair = pair;
	std::optional<std::int64_t> op_id;
	const auto throw_if_apply_aborted = [&] {
		if (should_abort && should_abort()) {
			throw ScanInterrupted{};
		}
	};

	// pre-flight checks
	try {
		if (const auto preflight_result = validate_apply_preconditions(pair, should_abort);
		    !preflight_result.has_value()) {
			return preflight_result.error();
		}

		throw_if_apply_aborted();

		// log planned
		const auto& cp = pair.canonical_path;
		const auto& dp = pair.duplicate_path;
		const std::string tmp = make_temp_backup_path(dp);
		op_id = repo.log_op_planned(cp, dp, tmp);
		perform_atomic_hardlink_swap(cp, dp, tmp);

		repo.log_op_complete(*op_id, Repository::OpStatus::Done);
		result.status = ApplyStatus::Linked;

	} catch (const HashInterrupted&) {
		throw ScanInterrupted{};
	} catch (const ScanInterrupted&) {
		throw;
	} catch (const std::system_error& ex) {
		if (op_id.has_value() &&
		    (ex.code() == std::errc::permission_denied || ex.code() == std::errc::operation_not_permitted)) {
			repo.log_op_complete(*op_id, Repository::OpStatus::Failed, kDuplicatePathNotWritableMessage.data());
			result.status = ApplyStatus::Skipped;
			result.message = kDuplicatePathNotWritableMessage;
			return result;
		}

		if (op_id.has_value()) {
			repo.log_op_complete(*op_id, Repository::OpStatus::Failed, ex.what());
		}
		result.status = ApplyStatus::Failed;
		result.message = ex.what();
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

void recover_pending_operations(IRepository& repo, const EngineCallbacks& cbs)
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

std::vector<ApplyResult> run_engine(IRepository& repo, const ScanOptions& scan_opts, const EngineOptions& engine_opts,
                                    const EngineCallbacks& cbs)
{
	const auto now = repo.now_unix_s();

	detail::ScanCoordinator coord{repo, cbs, now};
	detail::InterruptState interrupted;

	try {
		scan_files(scan_opts, [&](const std::filesystem::path& p) {
			coord.drain_completed_results();
			coord.throw_if_hashing_aborted();

			if (cbs.on_scan) {
				cbs.on_scan(p.string());
			}
			coord.throw_if_hashing_aborted();

			coord.process_scanned_path(p);
		});
	} catch (const ScanInterrupted& ex) {
		interrupted = std::unexpected(ex);
		if (interrupted.error().pending_hash_jobs() == 0 && interrupted.error().in_flight_hash_jobs() == 0 &&
		    cbs.should_abort && cbs.should_abort()) {
			interrupted = std::unexpected(coord.make_interrupt());
		}
	} catch (...) {
		static_cast<void>(coord.finish_hashing(detail::InterruptState{}));
		throw;
	}

	interrupted = coord.finish_hashing(interrupted);
	coord.release_completion_work();
	if (!interrupted.has_value()) {
		throw interrupted.error();
	}

	auto throw_if_main_aborted = [&] { coord.throw_if_main_aborted(); };
	auto results = build_apply_results(repo, engine_opts, cbs, coord.cached_entries(), coord.hashed_entries(),
	                                   throw_if_main_aborted);

	coord.throw_if_main_aborted();
	repo.remove_stale(now - 1);

	return results;
}

std::optional<ApplyResult> handle_file_change(IRepository& repo, const std::filesystem::path& path,
                                              const EngineOptions& opts, const EngineCallbacks& cbs)
{
	namespace fs = std::filesystem;

	// Guard: only index regular files; skip symlinks.
	std::error_code ec;
	if (!fs::is_regular_file(path, ec) || fs::is_symlink(path, ec)) return std::nullopt;

	const auto now = repo.now_unix_s();

	try {
		const auto cur_meta = meta_from_path(path);
		const auto resolved = detail::resolve_digest(
		    repo, path, cur_meta, [&](const fs::path& candidate) { return hash_file(candidate, cbs.should_abort); });

		IndexEntry e;
		e.path = path.string();
		e.meta = cur_meta;
		e.digest = resolved.digest;
		e.last_seen = now;
		repo.upsert(e);

		if (cbs.on_scan) cbs.on_scan(path.string());
		if (cbs.on_scan_decision) {
			cbs.on_scan_decision(path.string(),
			                     resolved.reused_cached_digest ? ScanCacheStatus::Hit : ScanCacheStatus::Miss);
		}

		// Find all other entries with the same digest.
		auto matches = repo.find_by_digest(resolved.digest);
		matches.erase(std::remove_if(matches.begin(), matches.end(),
		                             [&](const IndexEntry& m) { return m.path == path.string(); }),
		              matches.end());

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
		throw_if_aborted(cbs);

		if (opts.dry_run) {
			ApplyResult r;
			r.pair = pair;
			r.status = ApplyStatus::Skipped;
			r.message = "dry-run";
			if (cbs.on_apply) cbs.on_apply(r);
			return r;
		}

		auto r = apply_pair(repo, pair, cbs.should_abort);
		if (cbs.on_apply) cbs.on_apply(r);
		return r;

	} catch (const ScanInterrupted&) {
		throw;
	} catch (const std::exception& ex) {
		// I/O error or permission problem for this one file; skip it.
		spdlog::debug("handle_file_change skipping {}: {}", path.string(), ex.what());
		return std::nullopt;
	}
}

void handle_file_removed(IRepository& repo, const std::filesystem::path& path) { repo.remove_by_path(path.string()); }

} // namespace deduped
