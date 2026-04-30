#include "scan_coordinator.hpp"

#include "hasher.hpp"
#include "scope_exit.hpp"

#include <algorithm>
#include <boost/asio/post.hpp>
#include <chrono>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <thread>
#include <utility>

namespace deduped::detail {

namespace {

unsigned int worker_count() { return std::max(1u, std::thread::hardware_concurrency()); }

} // namespace

ScanCoordinator::ScanCoordinator(IRepository& repo, const EngineCallbacks& cbs, std::int64_t now) :
    repo_(repo),
    cbs_(cbs),
    now_(now),
    completion_work_(boost::asio::make_work_guard(completion_context_)),
    hash_pool_(worker_count())
{}

ScanCoordinator::~ScanCoordinator() = default;

ScanInterrupted ScanCoordinator::make_interrupt() const
{
	const auto queued = queued_hash_jobs_.load();
	const auto in_flight = active_hashes_.load();
	return ScanInterrupted{queued + in_flight, in_flight};
}

void ScanCoordinator::notify_shutdown_requested(const ScanInterrupted& interrupted)
{
	if (shutdown_notified_) {
		return;
	}
	shutdown_notified_ = true;
	if (cbs_.on_shutdown_requested) {
		cbs_.on_shutdown_requested(interrupted.pending_hash_jobs(), interrupted.in_flight_hash_jobs());
	}
}

InterruptState ScanCoordinator::capture_hashing_abort()
{
	if (!(cbs_.should_abort && cbs_.should_abort())) {
		return {};
	}

	auto interrupted = make_interrupt();
	notify_shutdown_requested(interrupted);
	return std::unexpected(interrupted);
}

void ScanCoordinator::throw_if_hashing_aborted()
{
	if (auto interrupted = capture_hashing_abort(); !interrupted.has_value()) {
		throw interrupted.error();
	}
}

void ScanCoordinator::throw_if_main_aborted()
{
	if (!(cbs_.should_abort && cbs_.should_abort())) {
		return;
	}

	const ScanInterrupted interrupted{};
	notify_shutdown_requested(interrupted);
	throw interrupted;
}

void ScanCoordinator::set_abort_state(const ScanInterrupted& interrupted)
{
	abort_workers_ = true;
	queued_hash_jobs_.store(0);
	hash_pool_.stop();
	completion_work_.reset();
	notify_shutdown_requested(interrupted);
}

void ScanCoordinator::capture_shutdown_during_hashing(InterruptState& interrupted)
{
	if (!interrupted.has_value()) {
		return;
	}
	if (auto requested = capture_hashing_abort(); !requested.has_value()) {
		interrupted = std::move(requested);
		set_abort_state(interrupted.error());
	}
}

void ScanCoordinator::release_completion_work() { completion_work_.reset(); }

void ScanCoordinator::drain_completed_results()
{
	completion_context_.poll();

	while (!completed_results_.empty()) {
		auto maybe_entry = std::move(completed_results_.front());
		completed_results_.pop();

		if (!maybe_entry) {
			continue;
		}

		auto entry = std::move(*maybe_entry);
		const InodeKey key{entry.meta.device, entry.meta.inode};
		inode_cache_[key] = entry.digest;
		in_flight_inodes_.erase(key);
		repo_.upsert(entry);
		hashed_entries_.push_back(std::move(entry));

		if (auto it = deferred_same_inode_.find(key); it != deferred_same_inode_.end()) {
			for (const auto& deferred : it->second) {
				IndexEntry deferred_entry;
				deferred_entry.path = deferred.path.string();
				deferred_entry.meta = deferred.meta;
				deferred_entry.digest = inode_cache_[key];
				deferred_entry.last_seen = now_;
				repo_.upsert(deferred_entry);
				cached_entries_.push_back(std::move(deferred_entry));
			}
			deferred_same_inode_.erase(it);
		}
	}
}

void ScanCoordinator::submit_hash_job(HashJob job)
{
	queued_hash_jobs_.fetch_add(1);
	boost::asio::post(hash_pool_, [this, job = std::move(job)]() mutable {
		// This job has started executing, so it's no longer "queued".
		queued_hash_jobs_.fetch_sub(1);

		if (abort_workers_.load()) {
			boost::asio::post(completion_context_, [this] { completed_results_.push(std::nullopt); });
			return;
		}

		active_hashes_.fetch_add(1);
		auto active_hash = scope_exit{[this] { active_hashes_.fetch_sub(1); }};

		try {
			IndexEntry entry;
			entry.path = job.path.string();
			entry.meta = job.meta;
			entry.digest = hash_file(
			    job.path, [this] { return abort_workers_.load() || (cbs_.should_abort && cbs_.should_abort()); });
			entry.last_seen = now_;
			boost::asio::post(completion_context_, [this, entry = std::move(entry)]() mutable {
				completed_results_.push(std::move(entry));
			});
		} catch (const HashInterrupted&) {
			boost::asio::post(completion_context_, [this] { completed_results_.push(std::nullopt); });
			return;
		} catch (const std::exception& ex) {
			spdlog::debug("skipping {}: {}", job.path.string(), ex.what());
			boost::asio::post(completion_context_, [this] { completed_results_.push(std::nullopt); });
		}
	});
}

void ScanCoordinator::process_scanned_path(const std::filesystem::path& path)
{
	try {
		const auto meta = meta_from_path(path);
		const InodeKey inode_key{meta.device, meta.inode};

		auto upsert_cached = [&](const Digest& digest) {
			IndexEntry entry;
			entry.path = path.string();
			entry.meta = meta;
			entry.digest = digest;
			entry.last_seen = now_;
			repo_.upsert(entry);
			cached_entries_.push_back(std::move(entry));
		};

		auto notify_decision = [&](ScanCacheStatus status) {
			if (cbs_.on_scan_decision) {
				cbs_.on_scan_decision(path.string(), status);
			}
		};

		if (const auto it = inode_cache_.find(inode_key); it != inode_cache_.end()) {
			upsert_cached(it->second);
			notify_decision(ScanCacheStatus::Hit);
			drain_completed_results();
			throw_if_hashing_aborted();
			return;
		}

		if (const auto existing = repo_.find_by_inode(meta.device, meta.inode);
		    existing && !is_meta_stale(existing->meta, meta)) {
			inode_cache_[inode_key] = existing->digest;
			upsert_cached(existing->digest);
			notify_decision(ScanCacheStatus::Hit);
			drain_completed_results();
			throw_if_hashing_aborted();
			return;
		}

		if (in_flight_inodes_.find(inode_key) != in_flight_inodes_.end()) {
			deferred_same_inode_[inode_key].push_back(HashJob{.path = path, .meta = meta});
			notify_decision(ScanCacheStatus::Hit);
			drain_completed_results();
			throw_if_hashing_aborted();
			return;
		}

		in_flight_inodes_.insert(inode_key);
		submit_hash_job(HashJob{.path = path, .meta = meta});
		notify_decision(ScanCacheStatus::Miss);
		drain_completed_results();
		throw_if_hashing_aborted();

	} catch (const ScanInterrupted&) {
		throw;
	} catch (const std::exception& ex) {
		spdlog::debug("skipping {}: {}", path.string(), ex.what());
	}
}

InterruptState ScanCoordinator::finish_hashing(InterruptState interrupted)
{
	if (!interrupted.has_value()) {
		set_abort_state(interrupted.error());
	} else {
		capture_shutdown_during_hashing(interrupted);
	}

	for (;;) {
		capture_shutdown_during_hashing(interrupted);
		drain_completed_results();
		capture_shutdown_during_hashing(interrupted);

		if (queued_hash_jobs_.load() == 0 && active_hashes_.load() == 0 && completed_results_.empty()) {
			break;
		}

		static_cast<void>(completion_context_.run_one_for(std::chrono::milliseconds(10)));
	}
	drain_completed_results();

	hash_pool_.join();

	if (interrupted.has_value() && cbs_.should_abort) {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(25);
		while (interrupted.has_value() && std::chrono::steady_clock::now() < deadline) {
			capture_shutdown_during_hashing(interrupted);
			if (interrupted.has_value()) {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}
	}

	return interrupted;
}

} // namespace deduped::detail
