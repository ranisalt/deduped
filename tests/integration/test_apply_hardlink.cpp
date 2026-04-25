#include "../../lib/engine.hpp"
#include "../../lib/repository.hpp"
#include "../../lib/types.hpp"
#include "../helpers/scope_exit.hpp"
#include "../helpers/temp_dir.hpp"

#include <fstream>
#include <gtest/gtest.h>
#include <unistd.h>

using namespace deduped;
using namespace deduped::test;
namespace fs = std::filesystem;

namespace {

} // namespace

class ApplyHardlinkTest : public ::testing::Test
{
protected:
	void SetUp() override
	{
		repo = std::make_unique<Repository>(td.path() / "apply.db");
		scan_opts.roots = {td.path()};
	}
	TempDir td;
	std::unique_ptr<Repository> repo;
	ScanOptions scan_opts;
};

// Apply mode must create a hardlink (shared inode) for confirmed duplicates.
TEST_F(ApplyHardlinkTest, ApplyCreatesHardlink)
{
	const auto p1 = td.write_file("a.txt", "shared content");
	const auto p2 = td.write_file("b.txt", "shared content");

	EngineOptions opts;
	opts.dry_run = false;
	std::vector<ApplyResult> results;
	EngineCallbacks cbs;
	cbs.on_apply = [&](const ApplyResult& r) { results.push_back(r); };

	run_engine(*repo, scan_opts, opts, cbs);

	ASSERT_EQ(results.size(), 1u);
	EXPECT_EQ(results[0].status, ApplyStatus::Linked);

	// Inodes must now be identical.
	const auto m1 = meta_from_path(p1);
	const auto m2 = meta_from_path(p2);
	EXPECT_EQ(m1.inode, m2.inode);
}

// After apply, a re-scan must report the pair as already-linked (idempotent).
TEST_F(ApplyHardlinkTest, RescanAfterApplyIsIdempotent)
{
	td.write_file("a.txt", "same");
	td.write_file("b.txt", "same");

	EngineOptions apply_opts;
	apply_opts.dry_run = false;
	run_engine(*repo, scan_opts, apply_opts);

	// Second run.
	std::vector<ApplyResult> second_results;
	EngineCallbacks cbs2;
	cbs2.on_apply = [&](const ApplyResult& r) { second_results.push_back(r); };
	run_engine(*repo, scan_opts, apply_opts, cbs2);

	ASSERT_EQ(second_results.size(), 1u);
	EXPECT_EQ(second_results[0].status, ApplyStatus::AlreadyLinked);
}

// Files that already share an inode must not be relinked.
TEST_F(ApplyHardlinkTest, AlreadyLinkedFilesAreNotRelinked)
{
	const auto p1 = td.write_file("x.txt", "data");
	// Hard-link manually so both have same inode.
	const auto p2 = td.path() / "y.txt";
	std::filesystem::create_hard_link(p1, p2);

	EngineOptions opts;
	opts.dry_run = false;
	std::vector<ApplyResult> results;
	EngineCallbacks cbs;
	cbs.on_apply = [&](const ApplyResult& r) { results.push_back(r); };
	run_engine(*repo, scan_opts, opts, cbs);

	ASSERT_EQ(results.size(), 1u);
	EXPECT_EQ(results[0].status, ApplyStatus::AlreadyLinked);
}

TEST_F(ApplyHardlinkTest, SkipsCrossDeviceFiles)
{
	const auto other_device_base = fs::path{"/dev/shm"};
	if (!fs::exists(other_device_base) || !fs::is_directory(other_device_base)) {
		GTEST_SKIP() << "/dev/shm is not available for cross-device testing";
	}
	if (meta_from_path(td.path()).device == meta_from_path(other_device_base).device) {
		GTEST_SKIP() << "/dev/shm is on the same device as the main temp dir";
	}

	const auto root1 = td.path() / "root1";
	fs::create_directories(root1);

	const auto other_root =
	    other_device_base / ("deduped-cross-device-" + std::to_string(::getpid()) + "-" +
	                         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	fs::create_directories(other_root);
	const auto cleanup = scope_exit{[other_root] {
		std::error_code ec;
		fs::remove_all(other_root, ec);
	}};

	const auto p1 = td.write_file("root1/a.txt", "same content");
	const auto p2 = other_root / "b.txt";
	{
		std::ofstream out{p2};
		out << "same content";
	}

	ScanOptions opts;
	opts.roots = {root1, other_root};

	EngineOptions engine_opts;
	engine_opts.dry_run = false;

	std::vector<ApplyResult> results;
	EngineCallbacks cbs;
	cbs.on_apply = [&](const ApplyResult& r) { results.push_back(r); };

	run_engine(*repo, opts, engine_opts, cbs);

	ASSERT_EQ(results.size(), 1u);
	EXPECT_EQ(results[0].status, ApplyStatus::Skipped);
	EXPECT_EQ(results[0].message, "cross-device, cannot hardlink");
	EXPECT_NE(meta_from_path(p1).inode, meta_from_path(p2).inode);
}

// Original content must be intact after apply (hardlink preserves content).
TEST_F(ApplyHardlinkTest, ContentPreservedAfterHardlink)
{
	constexpr const char* kContent = "precious file content";
	const auto p1 = td.write_file("orig.txt", kContent);
	const auto p2 = td.write_file("dup.txt", kContent);

	EngineOptions opts;
	opts.dry_run = false;
	run_engine(*repo, scan_opts, opts);

	std::ifstream f(p2, std::ios::binary);
	const std::string actual{std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
	EXPECT_EQ(actual, kContent);
}

// If the canonical file changes after pair detection but before apply, apply
// must skip linking to avoid linking against stale assumptions.
TEST_F(ApplyHardlinkTest, SkipsApplyWhenCanonicalChangesBeforeLink)
{
	const auto canonical = td.write_file("canonical.txt", "same content");
	const auto duplicate = td.write_file("duplicate.txt", "same content");

	EngineOptions opts;
	opts.dry_run = false;

	std::vector<ApplyResult> results;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair&) {
		std::ofstream f(canonical, std::ios::trunc);
		f << "canonical changed concurrently";
	};
	cbs.on_apply = [&](const ApplyResult& r) { results.push_back(r); };

	run_engine(*repo, scan_opts, opts, cbs);

	ASSERT_EQ(results.size(), 1u);
	EXPECT_EQ(results[0].status, ApplyStatus::Skipped);
	EXPECT_EQ(results[0].message, "canonical file changed between scan and apply, skipped");

	const auto cm = meta_from_path(canonical);
	const auto dm = meta_from_path(duplicate);
	EXPECT_NE(cm.inode, dm.inode);
}

// If the duplicate file changes after pair detection but before apply, apply
// must skip linking to avoid replacing modified content.
TEST_F(ApplyHardlinkTest, SkipsApplyWhenDuplicateChangesBeforeLink)
{
	const auto canonical = td.write_file("canonical.txt", "same content");
	const auto duplicate = td.write_file("duplicate.txt", "same content");

	EngineOptions opts;
	opts.dry_run = false;

	std::vector<ApplyResult> results;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair&) {
		std::ofstream f(duplicate, std::ios::trunc);
		f << "duplicate changed concurrently";
	};
	cbs.on_apply = [&](const ApplyResult& r) { results.push_back(r); };

	run_engine(*repo, scan_opts, opts, cbs);

	ASSERT_EQ(results.size(), 1u);
	EXPECT_EQ(results[0].status, ApplyStatus::Skipped);
	EXPECT_EQ(results[0].message, "file changed between scan and apply, skipped");

	const auto cm = meta_from_path(canonical);
	const auto dm = meta_from_path(duplicate);
	EXPECT_NE(cm.inode, dm.inode);
}
