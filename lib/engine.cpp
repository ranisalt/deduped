#include "engine.hpp"

#include "hasher.hpp"
#include "scope_exit.hpp"

#include <algorithm>
#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/container_hash/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <cerrno>
#include <chrono>
#include <expected>
#include <filesystem>
#include <map>
#include <queue>
#include <random>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

namespace deduped {

namespace {

struct HashJob
{
	std::filesystem::path path;
	FileMeta meta;
};

using DeferredHashJobs = boost::container::small_vector<HashJob, 2>;

struct InodeKey
{
	std::uint64_t device{};
	std::uint64_t inode{};
	[[nodiscard]] bool operator==(const InodeKey&) const noexcept = default;
};

struct InodeKeyHash
{
	std::size_t operator()(const InodeKey& k) const noexcept
	{
		std::size_t h = 0;
		boost::hash_combine(h, k.device);
		boost::hash_combine(h, k.inode);
		return h;
	}
};

using InodeDigestCache = boost::unordered_flat_map<InodeKey, Digest, InodeKeyHash>;
using InFlightInodes = boost::unordered_flat_set<InodeKey, InodeKeyHash>;
using DeferredSameInodeMap = boost::unordered_flat_map<InodeKey, DeferredHashJobs, InodeKeyHash>;

ApplyResult apply_pair(Repository& repo, const DupePair& pair, const HashShouldAbortFn& should_abort);

template <typename AbortFn>
std::vector<ApplyResult> build_apply_results(Repository& repo, const EngineOptions& engine_opts,
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

template <typename SubmitFn, typename DrainFn, typename AbortFn>
void process_scanned_path(Repository& repo, const std::filesystem::path& path, std::int64_t now,
                          InodeDigestCache& inode_cache, InFlightInodes& in_flight_inodes,
                          DeferredSameInodeMap& deferred_same_inode, std::vector<IndexEntry>& cached_entries,
                          const EngineCallbacks& cbs, SubmitFn&& submit_hash_job, DrainFn&& drain_completed_results,
                          AbortFn&& throw_if_hashing_aborted)
{
	try {
		const auto meta = meta_from_path(path);
		const InodeKey inode_key{meta.device, meta.inode};

		if (const auto it = inode_cache.find(inode_key); it != inode_cache.end()) {
			IndexEntry entry;
			entry.path = path.string();
			entry.meta = meta;
			entry.digest = it->second;
			entry.last_seen = now;
			repo.upsert(entry);
			cached_entries.push_back(std::move(entry));
			if (cbs.on_scan_decision) {
				cbs.on_scan_decision(path.string(), ScanCacheStatus::Hit);
			}
			drain_completed_results();
			throw_if_hashing_aborted();
			return;
		}

		const auto existing = repo.find_by_inode(meta.device, meta.inode);
		if (existing && !is_meta_stale(existing->meta, meta)) {
			IndexEntry entry;
			entry.path = path.string();
			entry.meta = meta;
			entry.digest = existing->digest;
			entry.last_seen = now;
			inode_cache[inode_key] = existing->digest;
			repo.upsert(entry);
			cached_entries.push_back(std::move(entry));
			if (cbs.on_scan_decision) {
				cbs.on_scan_decision(path.string(), ScanCacheStatus::Hit);
			}
			drain_completed_results();
			throw_if_hashing_aborted();
			return;
		}

		if (in_flight_inodes.find(inode_key) != in_flight_inodes.end()) {
			deferred_same_inode[inode_key].push_back(HashJob{.path = path, .meta = meta});
			if (cbs.on_scan_decision) {
				cbs.on_scan_decision(path.string(), ScanCacheStatus::Hit);
			}
			drain_completed_results();
			throw_if_hashing_aborted();
			return;
		}

		in_flight_inodes.insert(inode_key);
		submit_hash_job(HashJob{.path = path, .meta = meta});
		if (cbs.on_scan_decision) {
			cbs.on_scan_decision(path.string(), ScanCacheStatus::Miss);
		}
		drain_completed_results();
		throw_if_hashing_aborted();

	} catch (const ScanInterrupted&) {
		throw;
	} catch (const std::exception& ex) {
		spdlog::debug("skipping {}: {}", path.string(), ex.what());
	}
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
// Apply a single hardlink replacement safely:
//   1. Log the planned operation.
//   2. Verify preconditions (same device, not already same inode, not a symlink).
//   3. Rename duplicate to a temp name.
//   4. link(canonical, duplicate).
//   5. Remove the renamed temp file.
//   On any failure: restore from temp, update log.
ApplyResult apply_pair(Repository& repo, const DupePair& pair, const HashShouldAbortFn& should_abort = {})
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

	boost::asio::io_context completion_context;
	auto completion_work = boost::asio::make_work_guard(completion_context);
	std::queue<std::optional<IndexEntry>> completed_results;
	std::vector<IndexEntry> hashed_entries;
	std::vector<IndexEntry> cached_entries;
	InodeDigestCache inode_cache;
	InFlightInodes in_flight_inodes;
	DeferredSameInodeMap deferred_same_inode;

	std::atomic<bool> abort_workers{false};

	const auto worker_count = std::max(1u, std::thread::hardware_concurrency());
	boost::asio::thread_pool hash_pool{worker_count};
	std::atomic<std::size_t> queued_hash_jobs{0};
	std::atomic<std::size_t> active_hashes{0};

	bool shutdown_notified{false};
	using InterruptState = std::expected<void, ScanInterrupted>;

	auto make_interrupt = [&]() {
		const auto queued = queued_hash_jobs.load();
		const auto in_flight_hash_jobs = active_hashes.load();
		return ScanInterrupted{queued + in_flight_hash_jobs, in_flight_hash_jobs};
	};

	auto notify_shutdown_requested = [&](const ScanInterrupted& interrupted) {
		if (shutdown_notified) {
			return;
		}
		shutdown_notified = true;
		if (cbs.on_shutdown_requested) {
			cbs.on_shutdown_requested(interrupted.pending_hash_jobs(), interrupted.in_flight_hash_jobs());
		}
	};

	auto capture_hashing_abort = [&]() -> InterruptState {
		if (!(cbs.should_abort && cbs.should_abort())) {
			return {};
		}

		auto interrupted = make_interrupt();
		notify_shutdown_requested(interrupted);
		return std::unexpected(interrupted);
	};

	auto throw_if_hashing_aborted = [&]() {
		if (auto interrupted = capture_hashing_abort(); !interrupted.has_value()) {
			throw interrupted.error();
		}
	};

	auto throw_if_main_aborted = [&]() {
		if (!(cbs.should_abort && cbs.should_abort())) {
			return;
		}

		const ScanInterrupted interrupted{};
		notify_shutdown_requested(interrupted);
		throw interrupted;
	};

	auto set_abort_state = [&](const ScanInterrupted& interrupted) {
		abort_workers = true;
		queued_hash_jobs.store(0);
		hash_pool.stop();
		completion_work.reset();
		notify_shutdown_requested(interrupted);
	};

	auto drain_completed_results = [&]() {
		completion_context.poll();

		while (!completed_results.empty()) {
			auto maybe_entry = std::move(completed_results.front());
			completed_results.pop();

			if (!maybe_entry) {
				continue;
			}

			auto entry = std::move(*maybe_entry);
			const InodeKey key{entry.meta.device, entry.meta.inode};
			inode_cache[key] = entry.digest;
			in_flight_inodes.erase(key);
			repo.upsert(entry);
			hashed_entries.push_back(std::move(entry));

			if (auto it = deferred_same_inode.find(key); it != deferred_same_inode.end()) {
				for (const auto& deferred : it->second) {
					IndexEntry deferred_entry;
					deferred_entry.path = deferred.path.string();
					deferred_entry.meta = deferred.meta;
					deferred_entry.digest = inode_cache[key];
					deferred_entry.last_seen = now;
					repo.upsert(deferred_entry);
					cached_entries.push_back(std::move(deferred_entry));
				}
				deferred_same_inode.erase(it);
			}
		}
	};

	auto capture_shutdown_during_hashing = [&](InterruptState& interrupted) {
		if (!interrupted.has_value()) {
			return;
		}

		if (auto requested = capture_hashing_abort(); !requested.has_value()) {
			interrupted = std::move(requested);
			set_abort_state(interrupted.error());
		}
	};

	auto finish_hashing = [&](InterruptState interrupted) -> InterruptState {
		if (!interrupted.has_value()) {
			set_abort_state(interrupted.error());
		} else {
			capture_shutdown_during_hashing(interrupted);
		}

		for (;;) {
			capture_shutdown_during_hashing(interrupted);
			drain_completed_results();
			capture_shutdown_during_hashing(interrupted);

			if (queued_hash_jobs.load() == 0 && active_hashes.load() == 0 && completed_results.empty()) {
				break;
			}

			static_cast<void>(completion_context.run_one_for(std::chrono::milliseconds(10)));
		}
		drain_completed_results();

		hash_pool.join();

		if (interrupted.has_value() && cbs.should_abort) {
			const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(25);
			while (interrupted.has_value() && std::chrono::steady_clock::now() < deadline) {
				capture_shutdown_during_hashing(interrupted);
				if (interrupted.has_value()) {
					std::this_thread::sleep_for(std::chrono::milliseconds(1));
				}
			}
		}

		return interrupted;
	};

	auto submit_hash_job = [&](HashJob job) {
		queued_hash_jobs.fetch_add(1);
		boost::asio::post(hash_pool, [&, job = std::move(job)]() mutable {
			// This job has started executing, so it's no longer "queued".
			queued_hash_jobs.fetch_sub(1);

			if (abort_workers.load()) {
				boost::asio::post(completion_context, [&completed_results] { completed_results.push(std::nullopt); });
				return;
			}

			active_hashes.fetch_add(1);
			auto active_hash = scope_exit{[&] { active_hashes.fetch_sub(1); }};

			try {
				IndexEntry entry;
				entry.path = job.path.string();
				entry.meta = job.meta;
				entry.digest = hash_file(
				    job.path, [&] { return abort_workers.load() || (cbs.should_abort && cbs.should_abort()); });
				entry.last_seen = now;
				boost::asio::post(completion_context, [&completed_results, entry = std::move(entry)]() mutable {
					completed_results.push(std::move(entry));
				});
			} catch (const HashInterrupted&) {
				boost::asio::post(completion_context, [&completed_results] { completed_results.push(std::nullopt); });
				return;
			} catch (const std::exception& ex) {
				spdlog::debug("skipping {}: {}", job.path.string(), ex.what());
				boost::asio::post(completion_context, [&completed_results] { completed_results.push(std::nullopt); });
			}
		});
	};

	InterruptState interrupted;
	try {
		scan_files(scan_opts, [&](const std::filesystem::path& p) {
			drain_completed_results();
			throw_if_hashing_aborted();

			if (cbs.on_scan) {
				cbs.on_scan(p.string());
			}
			throw_if_hashing_aborted();

			process_scanned_path(repo, p, now, inode_cache, in_flight_inodes, deferred_same_inode, cached_entries, cbs,
			                     submit_hash_job, drain_completed_results, throw_if_hashing_aborted);
		});
	} catch (const ScanInterrupted& ex) {
		interrupted = std::unexpected(ex);
		if (interrupted.error().pending_hash_jobs() == 0 && interrupted.error().in_flight_hash_jobs() == 0 &&
		    cbs.should_abort && cbs.should_abort()) {
			interrupted = std::unexpected(make_interrupt());
		}
	} catch (...) {
		static_cast<void>(finish_hashing(InterruptState{}));
		throw;
	}

	interrupted = finish_hashing(interrupted);
	completion_work.reset();
	if (!interrupted.has_value()) {
		throw interrupted.error();
	}

	auto results = build_apply_results(repo, engine_opts, cbs, cached_entries, hashed_entries, throw_if_main_aborted);

	throw_if_main_aborted();
	repo.remove_stale(now - 1);

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

void handle_file_removed(Repository& repo, const std::filesystem::path& path) { repo.remove_by_path(path.string()); }

} // namespace deduped
