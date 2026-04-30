#pragma once

// Internal coordinator owning the threading state used by the full-scan
// execution path of `run_engine`. Not part of the public engine API: included
// only by engine.cpp.

#include "engine.hpp"
#include "repository.hpp"
#include "types.hpp"

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/container_hash/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <optional>
#include <queue>
#include <vector>

namespace deduped::detail {

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
		std::size_t h = 0;
		boost::hash_combine(h, k.device);
		boost::hash_combine(h, k.inode);
		return h;
	}
};

using DeferredHashJobs = boost::container::small_vector<HashJob, 2>;
using InodeDigestCache = boost::unordered_flat_map<InodeKey, Digest, InodeKeyHash>;
using InFlightInodes = boost::unordered_flat_set<InodeKey, InodeKeyHash>;
using DeferredSameInodeMap = boost::unordered_flat_map<InodeKey, DeferredHashJobs, InodeKeyHash>;

using InterruptState = std::expected<void, ScanInterrupted>;

// Coordinator that owns the thread pool, inode caches, and shutdown-state
// machinery used during the scan phase of `run_engine`. All public methods are
// expected to be called from the orchestrator thread; internally posts work to
// a worker pool and drains completions on the orchestrator thread.
class ScanCoordinator
{
public:
	ScanCoordinator(IRepository& repo, const EngineCallbacks& cbs, std::int64_t now);
	~ScanCoordinator();

	ScanCoordinator(const ScanCoordinator&) = delete;
	ScanCoordinator& operator=(const ScanCoordinator&) = delete;
	ScanCoordinator(ScanCoordinator&&) = delete;
	ScanCoordinator& operator=(ScanCoordinator&&) = delete;

	// Process a single path discovered by the scan walker. Hashes async (via
	// pool) when needed, otherwise upserts immediately from cache.
	void process_scanned_path(const std::filesystem::path& path);

	// Drain finished hash jobs from the completion queue, upserting results.
	void drain_completed_results();

	// Throws ScanInterrupted if abort requested. Notifies shutdown-requested
	// callback exactly once across the lifetime of this coordinator.
	void throw_if_hashing_aborted();
	void throw_if_main_aborted();

	// Build the current-state ScanInterrupted (queued + in-flight counts).
	[[nodiscard]] ScanInterrupted make_interrupt() const;

	// Wait for all pending hash jobs to drain and return final interrupt state.
	// Called once at the end of the scan loop. After return, the worker pool is
	// joined.
	[[nodiscard]] InterruptState finish_hashing(InterruptState initial);

	// Release the io_context work guard once all work has been drained.
	void release_completion_work();

	[[nodiscard]] const std::vector<IndexEntry>& cached_entries() const noexcept { return cached_entries_; }
	[[nodiscard]] const std::vector<IndexEntry>& hashed_entries() const noexcept { return hashed_entries_; }

private:
	[[nodiscard]] InterruptState capture_hashing_abort();
	void notify_shutdown_requested(const ScanInterrupted& interrupted);
	void set_abort_state(const ScanInterrupted& interrupted);
	void capture_shutdown_during_hashing(InterruptState& interrupted);
	void submit_hash_job(HashJob job);

	IRepository& repo_;
	const EngineCallbacks& cbs_;
	std::int64_t now_;

	boost::asio::io_context completion_context_;
	boost::asio::executor_work_guard<boost::asio::io_context::executor_type> completion_work_;
	std::queue<std::optional<IndexEntry>> completed_results_;
	std::vector<IndexEntry> hashed_entries_;
	std::vector<IndexEntry> cached_entries_;
	InodeDigestCache inode_cache_;
	InFlightInodes in_flight_inodes_;
	DeferredSameInodeMap deferred_same_inode_;

	std::atomic<bool> abort_workers_{false};
	boost::asio::thread_pool hash_pool_;
	std::atomic<std::size_t> queued_hash_jobs_{0};
	std::atomic<std::size_t> active_hashes_{0};
	bool shutdown_notified_{false};
};

} // namespace deduped::detail
