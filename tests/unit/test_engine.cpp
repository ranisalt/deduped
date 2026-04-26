#include "../../lib/engine.hpp"
#include "../../lib/repository.hpp"
#include "../helpers/temp_dir.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <map>
#include <thread>
#include <vector>

using namespace deduped;
using namespace deduped::test;
namespace fs = std::filesystem;

namespace {

Digest make_digest(const std::uint8_t byte)
{
	Digest digest;
	digest.fill(byte);
	return digest;
}

IndexEntry make_index_entry(const fs::path& path, const FileMeta& meta, const Digest& digest,
                            const std::int64_t last_seen)
{
	IndexEntry entry;
	entry.path = path.string();
	entry.meta = meta;
	entry.digest = digest;
	entry.last_seen = last_seen;
	return entry;
}

} // namespace

class EngineTest : public ::testing::Test
{
protected:
	void SetUp() override
	{
		repo = std::make_unique<Repository>(td.path() / "engine_test.db");
		scan_opts.roots = {td.path()};
	}

	TempDir td;
	std::unique_ptr<Repository> repo;
	ScanOptions scan_opts;
};

// In dry-run mode no filesystem changes are made and dupes are reported.
TEST_F(EngineTest, DryRunReportsDuplicatesWithoutModifyingFilesystem)
{
	const auto p1 = td.write_file("a.txt", "same content");
	const auto p2 = td.write_file("b.txt", "same content");
	const auto p3 = td.write_file("c.txt", "different");

	EngineOptions opts;
	opts.dry_run = true;

	std::vector<DupePair> found;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& p) { found.push_back(p); };

	const auto results = run_engine(*repo, scan_opts, opts, cbs);

	EXPECT_EQ(found.size(), 1u);
	// Files must still exist and be unchanged.
	EXPECT_TRUE(std::filesystem::exists(p1));
	EXPECT_TRUE(std::filesystem::exists(p2));
}

// Dry-run must never create hardlinks.
TEST_F(EngineTest, DryRunDoesNotCreateHardlinks)
{
	td.write_file("x.txt", "dup");
	td.write_file("y.txt", "dup");

	EngineOptions opts;
	opts.dry_run = true;

	run_engine(*repo, scan_opts, opts);

	// Inodes must differ (no linking performed).
	const auto m_x = meta_from_path(td.path() / "x.txt");
	const auto m_y = meta_from_path(td.path() / "y.txt");
	EXPECT_NE(m_x.inode, m_y.inode);
}

// Unique files must never be reported as duplicates.
TEST_F(EngineTest, UniqueFilesNotReportedAsDuplicates)
{
	td.write_file("a.txt", "aaa");
	td.write_file("b.txt", "bbb");
	td.write_file("c.txt", "ccc");

	EngineOptions opts;
	opts.dry_run = true;

	std::vector<DupePair> found;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& p) { found.push_back(p); };

	run_engine(*repo, scan_opts, opts, cbs);
	EXPECT_TRUE(found.empty());
}

// Second scan of the same directory must be idempotent (no extra dupes).
TEST_F(EngineTest, RescanIsIdempotent)
{
	td.write_file("a.txt", "same");
	td.write_file("b.txt", "same");

	EngineOptions opts;
	opts.dry_run = true;

	std::size_t count1 = 0, count2 = 0;
	EngineCallbacks cbs1;
	cbs1.on_dupe_found = [&](const DupePair&) { ++count1; };
	run_engine(*repo, scan_opts, opts, cbs1);

	EngineCallbacks cbs2;
	cbs2.on_dupe_found = [&](const DupePair&) { ++count2; };
	run_engine(*repo, scan_opts, opts, cbs2);

	EXPECT_EQ(count1, count2);
}

TEST_F(EngineTest, ResolveDigestReusesCachedDigestWhenMetadataIsUnchanged)
{
	const auto path = td.write_file("cached.txt", "same content");
	const auto meta = meta_from_path(path);
	const auto cached_digest = make_digest(0x11);
	repo->upsert(make_index_entry(path, meta, cached_digest, 10));

	std::size_t hash_calls = 0;
	const auto result = detail::resolve_digest(*repo, path, meta, [&](const fs::path&) {
		++hash_calls;
		return make_digest(0x22);
	});

	EXPECT_TRUE(result.reused_cached_digest);
	EXPECT_EQ(hash_calls, 0u);
	EXPECT_EQ(result.digest, cached_digest);
}

TEST_F(EngineTest, ResolveDigestReusesCachedDigestWhenModeChanges)
{
	const auto path = td.write_file("mode.txt", "same content");
	const auto original_meta = meta_from_path(path);
	const auto cached_digest = make_digest(0x33);
	repo->upsert(make_index_entry(path, original_meta, cached_digest, 10));

	fs::permissions(path, fs::perms::owner_exec, fs::perm_options::add);
	const auto updated_meta = meta_from_path(path);

	std::size_t hash_calls = 0;
	const auto refreshed_digest = make_digest(0x44);
	const auto result = detail::resolve_digest(*repo, path, updated_meta, [&](const fs::path&) {
		++hash_calls;
		return refreshed_digest;
	});

	EXPECT_TRUE(result.reused_cached_digest);
	EXPECT_EQ(hash_calls, 0u);
	EXPECT_EQ(result.digest, cached_digest);
}

TEST_F(EngineTest, ScanDecisionReportsHitAndMiss)
{
	const auto data_root = td.path() / "data";
	fs::create_directories(data_root);
	const auto hit_path = td.write_file("data/hit.txt", "same content");
	const auto miss_path = td.write_file("data/miss.txt", "different content");
	const auto hit_meta = meta_from_path(hit_path);
	const auto cached_digest = make_digest(0x51);
	repo->upsert(make_index_entry(hit_path, hit_meta, cached_digest, 10));

	EngineOptions opts;
	opts.dry_run = true;
	ScanOptions data_scan_opts;
	data_scan_opts.roots = {data_root};

	std::map<std::string, ScanCacheStatus> decisions;
	EngineCallbacks cbs;
	cbs.on_scan_decision = [&](const std::string& path, const ScanCacheStatus status) { decisions[path] = status; };

	run_engine(*repo, data_scan_opts, opts, cbs);

	ASSERT_EQ(decisions.size(), 2u);
	EXPECT_EQ(decisions.at(hit_path.string()), ScanCacheStatus::Hit);
	EXPECT_EQ(decisions.at(miss_path.string()), ScanCacheStatus::Miss);
}

TEST_F(EngineTest, SameInodeAliasesAreNotReportedAsDuplicates)
{
	const auto data_root = td.path() / "data";
	fs::create_directories(data_root);
	const auto canonical = td.write_file("data/canonical.txt", "same content");
	const auto alias = data_root / "alias.txt";
	fs::create_hard_link(canonical, alias);
	ScanOptions alias_scan_opts;
	alias_scan_opts.roots = {data_root};

	EngineOptions opts;
	opts.dry_run = true;

	std::vector<DupePair> found;
	std::size_t hit_count = 0;
	std::size_t miss_count = 0;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& pair) { found.push_back(pair); };
	cbs.on_scan_decision = [&](const std::string&, const ScanCacheStatus status) {
		if (status == ScanCacheStatus::Hit) {
			++hit_count;
		} else {
			++miss_count;
		}
	};

	const auto results = run_engine(*repo, alias_scan_opts, opts, cbs);

	EXPECT_TRUE(found.empty());
	EXPECT_TRUE(results.empty());
	EXPECT_EQ(hit_count, 1u);
	EXPECT_EQ(miss_count, 1u);
	ASSERT_TRUE(repo->find_by_path(canonical.string()).has_value());
	ASSERT_TRUE(repo->find_by_path(alias.string()).has_value());
}

TEST_F(EngineTest, InterruptedDryRunPersistsPartialIndex)
{
	const auto a = td.write_file("a.txt", "aaa");
	const auto b = td.write_file("b.txt", "bbb");

	EngineOptions opts;
	opts.dry_run = true;

	Repository observer(td.path() / "engine_test.db");

	std::size_t scanned = 0;
	std::string first_scanned_path;
	EngineCallbacks cbs;
	cbs.on_scan = [&](const std::string& path) {
		++scanned;
		if (scanned == 1) {
			first_scanned_path = path;
			return;
		}
		if (scanned != 2) {
			return;
		}

		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		while (std::chrono::steady_clock::now() < deadline) {
			if (observer.find_by_path(first_scanned_path).has_value()) {
				break;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}

		throw ScanInterrupted{};
	};

	EXPECT_THROW(run_engine(*repo, scan_opts, opts, cbs), ScanInterrupted);

	const bool has_a = repo->find_by_path(a.string()).has_value();
	const bool has_b = repo->find_by_path(b.string()).has_value();
	EXPECT_TRUE(has_a || has_b);
}

TEST_F(EngineTest, HashResultIsPersistedBeforeScanCompletes)
{
	const auto worker_count = std::max(1u, std::thread::hardware_concurrency());
	const auto job_count = worker_count + 8;
	const std::string payload(1024 * 1024, 'y');
	std::vector<fs::path> file_paths;
	file_paths.reserve(job_count);
	for (unsigned int i = 0; i < job_count; ++i) {
		file_paths.push_back(td.write_file("file-" + std::to_string(i) + ".bin", payload));
	}

	EngineOptions opts;
	opts.dry_run = true;

	Repository observer(td.path() / "engine_test.db");

	std::atomic<bool> abort_requested = false;
	std::atomic<bool> persisted_during_scan = false;
	std::jthread interrupter([&](std::stop_token stop_token) {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!stop_token.stop_requested() && std::chrono::steady_clock::now() < deadline) {
			for (const auto& path : file_paths) {
				if (observer.find_by_path(path.string()).has_value()) {
					persisted_during_scan = true;
					abort_requested = true;
					return;
				}
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}
	});

	EngineCallbacks cbs;
	cbs.should_abort = [&] { return abort_requested.load(); };

	EXPECT_THROW(run_engine(*repo, scan_opts, opts, cbs), ScanInterrupted);
	EXPECT_TRUE(persisted_during_scan.load());
}

TEST_F(EngineTest, RestartAfterInterruptReusesPersistedDigest)
{
	const auto worker_count = std::max(1u, std::thread::hardware_concurrency());
	const auto job_count = worker_count + 8;
	std::vector<fs::path> file_paths;
	file_paths.reserve(job_count);
	for (unsigned int i = 0; i < job_count; ++i) {
		file_paths.push_back(td.write_file("file-" + std::to_string(i) + ".bin",
		                                   std::string(1024 * 1024, static_cast<char>('a' + (i % 26)))));
	}

	EngineOptions opts;
	opts.dry_run = true;

	Repository observer(td.path() / "engine_test.db");
	std::atomic<bool> abort_requested = false;
	std::jthread interrupter([&](std::stop_token stop_token) {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!stop_token.stop_requested() && std::chrono::steady_clock::now() < deadline) {
			for (const auto& path : file_paths) {
				if (observer.find_by_path(path.string()).has_value()) {
					abort_requested = true;
					return;
				}
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}
	});

	EngineCallbacks interrupted_cbs;
	interrupted_cbs.should_abort = [&] { return abort_requested.load(); };

	EXPECT_THROW(run_engine(*repo, scan_opts, opts, interrupted_cbs), ScanInterrupted);

	std::optional<IndexEntry> persisted_entry;
	for (const auto& path : file_paths) {
		persisted_entry = repo->find_by_path(path.string());
		if (persisted_entry.has_value()) {
			break;
		}
	}
	ASSERT_TRUE(persisted_entry.has_value());

	const auto original_digest = persisted_entry->digest;
	const auto fake_digest = make_digest(0x77);
	ASSERT_NE(original_digest, fake_digest);

	persisted_entry->digest = fake_digest;
	repo->upsert(*persisted_entry);

	run_engine(*repo, scan_opts, opts);

	const auto restarted_entry = repo->find_by_path(persisted_entry->path);
	ASSERT_TRUE(restarted_entry.has_value());
	EXPECT_EQ(restarted_entry->digest, fake_digest);
}

TEST_F(EngineTest, AbortRequestedDuringDuplicateReportingStopsEngine)
{
	td.write_file("a.txt", "same");
	td.write_file("b.txt", "same");
	td.write_file("c.txt", "same");

	EngineOptions opts;
	opts.dry_run = true;

	bool abort_requested = false;
	std::size_t dupes_seen = 0;
	EngineCallbacks cbs;
	cbs.should_abort = [&] { return abort_requested; };
	cbs.on_dupe_found = [&](const DupePair&) {
		++dupes_seen;
		abort_requested = true;
	};

	EXPECT_THROW(run_engine(*repo, scan_opts, opts, cbs), ScanInterrupted);
	EXPECT_EQ(dupes_seen, 1u);
}

TEST_F(EngineTest, AbortRequestedDuringHashingReportsShutdownStats)
{
	const auto worker_count = std::max(1u, std::thread::hardware_concurrency());
	const auto job_count = worker_count + 8;
	const std::string payload(1024 * 1024, 'x');
	std::vector<fs::path> file_paths;
	file_paths.reserve(job_count);
	for (unsigned int i = 0; i < job_count; ++i) {
		file_paths.push_back(td.write_file("file-" + std::to_string(i) + ".bin", payload));
	}

	EngineOptions opts;
	opts.dry_run = true;

	Repository observer(td.path() / "engine_test.db");
	std::atomic<bool> abort_requested = false;
	bool shutdown_requested = false;
	std::size_t notified_pending = 0;
	std::size_t notified_in_flight = 0;

	std::jthread interrupter([&](std::stop_token stop_token) {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!stop_token.stop_requested() && std::chrono::steady_clock::now() < deadline) {
			for (const auto& path : file_paths) {
				if (observer.find_by_path(path.string()).has_value()) {
					abort_requested = true;
					return;
				}
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}
	});

	EngineCallbacks cbs;
	cbs.on_shutdown_requested = [&](const std::size_t pending_hash_jobs, const std::size_t in_flight_hash_jobs) {
		shutdown_requested = true;
		notified_pending = pending_hash_jobs;
		notified_in_flight = in_flight_hash_jobs;
	};
	cbs.should_abort = [&] { return abort_requested.load(); };

	try {
		static_cast<void>(run_engine(*repo, scan_opts, opts, cbs));
		FAIL() << "expected interrupt";
	} catch (const ScanInterrupted& ex) {
		EXPECT_TRUE(shutdown_requested);
		EXPECT_EQ(notified_pending, ex.pending_hash_jobs());
		EXPECT_EQ(notified_in_flight, ex.in_flight_hash_jobs());
	}
}
