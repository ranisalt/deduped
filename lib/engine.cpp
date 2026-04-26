#include "engine.hpp"

#include "hasher.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <flat_map>
#include <map>
#include <mutex>
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

	// pre-flight checks
	try {
		namespace fs = std::filesystem;
		auto throw_if_apply_aborted = [&] {
			if (should_abort && should_abort()) {
				throw ScanInterrupted{};
			}
		};

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
		const auto canonical_digest = hash_file(cp, should_abort);
		if (canonical_digest != pair.digest) {
			result.status = ApplyStatus::Skipped;
			result.message = "canonical file changed between scan and apply, skipped";
			return result;
		}

		const auto duplicate_digest = hash_file(dp, should_abort);
		if (duplicate_digest != pair.digest) {
			result.status = ApplyStatus::Skipped;
			result.message = "file changed between scan and apply, skipped";
			return result;
		}

		throw_if_apply_aborted();

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

	struct HashJob
	{
		std::filesystem::path path;
		FileMeta meta;
	};

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
			std::size_t h = k.device;
			h ^= k.inode + 0x9e3779b9u + (h << 6) + (h >> 2);
			return h;
		}
	};

	struct ActiveHashGuard
	{
		std::atomic<std::size_t>& active_hashes;
		std::condition_variable& results_cv;

		~ActiveHashGuard()
		{
			active_hashes.fetch_sub(1);
			results_cv.notify_all();
		}
	};

	struct WorkerExitGuard
	{
		std::atomic<std::size_t>& live_workers;
		std::condition_variable& results_cv;

		~WorkerExitGuard()
		{
			live_workers.fetch_sub(1);
			results_cv.notify_all();
		}
	};

	std::queue<HashJob> jobs;
	std::queue<IndexEntry> ready_results;
	std::vector<IndexEntry> hashed_entries;
	std::vector<IndexEntry> cached_entries;
	std::unordered_map<InodeKey, Digest, InodeKeyHash> inode_cache;
	std::unordered_set<InodeKey, InodeKeyHash> in_flight_inodes;
	std::unordered_map<InodeKey, std::vector<HashJob>, InodeKeyHash> deferred_same_inode;

	std::mutex jobs_mutex;
	std::condition_variable jobs_cv;
	std::mutex results_mutex;
	std::condition_variable results_cv;
	bool producer_done{false};
	std::atomic<bool> abort_workers{false};

	const auto worker_count = std::max(1u, std::thread::hardware_concurrency());
	std::atomic<std::size_t> active_hashes{0};
	std::atomic<std::size_t> live_workers{worker_count};
	std::vector<std::thread> workers;
	workers.reserve(worker_count);

	bool shutdown_notified{false};

	auto make_interrupt = [&]() {
		std::size_t queued_hash_jobs = 0;
		{
			std::lock_guard lock{jobs_mutex};
			queued_hash_jobs = jobs.size();
		}
		const auto in_flight_hash_jobs = active_hashes.load();
		return ScanInterrupted{queued_hash_jobs + in_flight_hash_jobs, in_flight_hash_jobs};
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

	auto capture_hashing_abort = [&]() -> std::optional<ScanInterrupted> {
		if (!(cbs.should_abort && cbs.should_abort())) {
			return std::nullopt;
		}

		auto interrupted = make_interrupt();
		notify_shutdown_requested(interrupted);
		return interrupted;
	};

	auto throw_if_hashing_aborted = [&]() {
		if (auto interrupted = capture_hashing_abort(); interrupted.has_value()) {
			throw *interrupted;
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

	auto drain_ready_results = [&]() {
		std::queue<IndexEntry> drained;
		{
			std::lock_guard lock{results_mutex};
			std::swap(drained, ready_results);
		}
		while (!drained.empty()) {
			auto entry = std::move(drained.front());
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
			drained.pop();
		}
	};

	auto set_abort_state = [&](const ScanInterrupted& interrupted) {
		abort_workers = true;
		{
			std::lock_guard lock{jobs_mutex};
			producer_done = true;
			std::queue<HashJob> discarded_jobs;
			std::swap(jobs, discarded_jobs);
		}
		notify_shutdown_requested(interrupted);
		jobs_cv.notify_all();
	};

	auto capture_shutdown_during_hashing = [&](std::optional<ScanInterrupted>& interrupted) {
		if (interrupted.has_value()) {
			return;
		}

		if (auto requested = capture_hashing_abort(); requested.has_value()) {
			interrupted = std::move(requested);
			set_abort_state(*interrupted);
		}
	};

	auto finish_hashing = [&](std::optional<ScanInterrupted> interrupted) -> std::optional<ScanInterrupted> {
		if (interrupted.has_value()) {
			set_abort_state(*interrupted);
		} else {
			{
				std::lock_guard lock{jobs_mutex};
				producer_done = true;
			}
			jobs_cv.notify_all();
			capture_shutdown_during_hashing(interrupted);
		}

		std::unique_lock results_lock{results_mutex};
		for (;;) {
			capture_shutdown_during_hashing(interrupted);
			results_lock.unlock();
			drain_ready_results();
			capture_shutdown_during_hashing(interrupted);
			results_lock.lock();

			if (live_workers.load() == 0 && ready_results.empty()) {
				break;
			}

			results_cv.wait_for(results_lock, std::chrono::milliseconds(10), [&] {
				return !ready_results.empty() || live_workers.load() == 0 ||
				       (!interrupted.has_value() && cbs.should_abort && cbs.should_abort());
			});
		}
		results_lock.unlock();
		drain_ready_results();

		for (auto& worker : workers) {
			if (worker.joinable()) {
				worker.join();
			}
		}

		if (!interrupted.has_value() && cbs.should_abort) {
			const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(25);
			while (!interrupted.has_value() && std::chrono::steady_clock::now() < deadline) {
				capture_shutdown_during_hashing(interrupted);
				if (!interrupted.has_value()) {
					std::this_thread::sleep_for(std::chrono::milliseconds(1));
				}
			}
		}

		return interrupted;
	};

	for (unsigned int i = 0; i < worker_count; ++i) {
		workers.emplace_back([&] {
			const WorkerExitGuard worker_exit{live_workers, results_cv};

			for (;;) {
				HashJob job;
				{
					std::unique_lock lock{jobs_mutex};
					jobs_cv.wait(lock, [&] { return abort_workers.load() || producer_done || !jobs.empty(); });
					if (abort_workers.load()) {
						return;
					}
					if (jobs.empty()) {
						if (producer_done) {
							return;
						}
						continue;
					}

					job = std::move(jobs.front());
					jobs.pop();
					active_hashes.fetch_add(1);
				}

				const ActiveHashGuard active_hash{active_hashes, results_cv};
				try {
					IndexEntry entry;
					entry.path = job.path.string();
					entry.meta = job.meta;
					entry.digest = hash_file(
					    job.path, [&] { return abort_workers.load() || (cbs.should_abort && cbs.should_abort()); });
					entry.last_seen = now;

					{
						std::lock_guard lock{results_mutex};
						ready_results.push(std::move(entry));
					}
					results_cv.notify_one();
				} catch (const HashInterrupted&) {
					return;
				} catch (const std::exception& ex) {
					spdlog::debug("skipping {}: {}", job.path.string(), ex.what());
				}
			}
		});
	}

	std::optional<ScanInterrupted> interrupted;
	try {
		scan_files(scan_opts, [&](const std::filesystem::path& p) {
			drain_ready_results();
			throw_if_hashing_aborted();

			if (cbs.on_scan) {
				cbs.on_scan(p.string());
			}
			throw_if_hashing_aborted();

			try {
				const auto meta = meta_from_path(p);
				const InodeKey inode_key{meta.device, meta.inode};

				if (const auto it = inode_cache.find(inode_key); it != inode_cache.end()) {
					IndexEntry entry;
					entry.path = p.string();
					entry.meta = meta;
					entry.digest = it->second;
					entry.last_seen = now;
					repo.upsert(entry);
					cached_entries.push_back(std::move(entry));
					if (cbs.on_scan_decision) {
						cbs.on_scan_decision(p.string(), ScanCacheStatus::Hit);
					}
					drain_ready_results();
					throw_if_hashing_aborted();
					return;
				}

				const auto existing = repo.find_by_path(p.string());
				if (existing && !is_meta_stale(existing->meta, meta)) {
					IndexEntry entry;
					entry.path = p.string();
					entry.meta = meta;
					entry.digest = existing->digest;
					entry.last_seen = now;
					inode_cache[inode_key] = existing->digest;
					repo.upsert(entry);
					cached_entries.push_back(std::move(entry));
					if (cbs.on_scan_decision) {
						cbs.on_scan_decision(p.string(), ScanCacheStatus::Hit);
					}
					drain_ready_results();
					throw_if_hashing_aborted();
					return;
				}

				if (in_flight_inodes.contains(inode_key)) {
					deferred_same_inode[inode_key].push_back(HashJob{.path = p, .meta = meta});
					if (cbs.on_scan_decision) {
						cbs.on_scan_decision(p.string(), ScanCacheStatus::Hit);
					}
					drain_ready_results();
					throw_if_hashing_aborted();
					return;
				}

				{
					std::lock_guard lock{jobs_mutex};
					jobs.push(HashJob{.path = p, .meta = meta});
				}
				in_flight_inodes.insert(inode_key);
				jobs_cv.notify_one();
				if (cbs.on_scan_decision) {
					cbs.on_scan_decision(p.string(), ScanCacheStatus::Miss);
				}
				drain_ready_results();
				throw_if_hashing_aborted();

			} catch (const ScanInterrupted&) {
				throw;
			} catch (const std::exception& ex) {
				spdlog::debug("skipping {}: {}", p.string(), ex.what());
			}
		});
	} catch (const ScanInterrupted& ex) {
		interrupted = ex;
		if (interrupted->pending_hash_jobs() == 0 && interrupted->in_flight_hash_jobs() == 0 && cbs.should_abort &&
		    cbs.should_abort()) {
			interrupted = make_interrupt();
		}
	} catch (...) {
		static_cast<void>(finish_hashing(std::nullopt));
		throw;
	}

	interrupted = finish_hashing(interrupted);
	if (interrupted.has_value()) {
		throw *interrupted;
	}

	auto indexed_by_size = [&]() {
		std::flat_map<std::uint64_t, std::vector<IndexEntry>> grouped_entries;
		for (const auto& entry : cached_entries) {
			grouped_entries[entry.meta.size].push_back(entry);
		}
		for (const auto& entry : hashed_entries) {
			grouped_entries[entry.meta.size].push_back(entry);
		}
		return grouped_entries;
	}();

	std::vector<ApplyResult> results;

	for (const auto& [size, entries_for_size] : indexed_by_size) {
		throw_if_main_aborted();

		if (entries_for_size.size() < 2) continue;

		std::flat_map<Digest, std::vector<IndexEntry>> by_digest;
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
