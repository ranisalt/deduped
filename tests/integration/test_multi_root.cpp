#include "../../lib/engine.hpp"
#include "../../lib/repository.hpp"
#include "../../lib/scanner.hpp"
#include "../../lib/types.hpp"
#include "../helpers/temp_dir.hpp"

#include <gtest/gtest.h>

using namespace deduped;
using namespace deduped::test;

class MultiRootTest : public ::testing::Test
{
protected:
	void SetUp() override
	{
		repo = std::make_unique<Repository>(td.path() / "multi_root.db");
		root1 = td.path() / "root1";
		root2 = td.path() / "root2";
		std::filesystem::create_directories(root1);
		std::filesystem::create_directories(root2);
	}

	TempDir td;
	std::filesystem::path root1;
	std::filesystem::path root2;
	std::unique_ptr<Repository> repo;
};

// When scanning multiple roots, duplicates across roots should be found.
TEST_F(MultiRootTest, FindsDuplicatesAcrossRoots)
{
	const auto p1 = td.write_file("root1/a.txt", "shared content");
	const auto p2 = td.write_file("root2/b.txt", "shared content");

	ScanOptions scan_opts;
	scan_opts.roots = {root1, root2};

	std::vector<DupePair> dupes;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& pair) { dupes.push_back(pair); };

	EngineOptions engine_opts;
	engine_opts.dry_run = true;

	run_engine(*repo, scan_opts, engine_opts, cbs);

	ASSERT_EQ(dupes.size(), 1u) << "Expected 1 dupe pair across roots";
	EXPECT_EQ(dupes[0].canonical_path, p1);
	EXPECT_EQ(dupes[0].duplicate_path, p2);
}

// When applying deduplication across multiple roots, hardlinks should be created.
TEST_F(MultiRootTest, CreatesHardlinksAcrossRoots)
{
	const auto p1 = td.write_file("root1/file.txt", "duplicate content");
	const auto p2 = td.write_file("root2/file.txt", "duplicate content");

	ScanOptions scan_opts;
	scan_opts.roots = {root1, root2};

	EngineOptions engine_opts;
	engine_opts.dry_run = false;

	std::vector<ApplyResult> results;
	EngineCallbacks cbs;
	cbs.on_apply = [&](const ApplyResult& r) { results.push_back(r); };

	run_engine(*repo, scan_opts, engine_opts, cbs);

	ASSERT_EQ(results.size(), 1u) << "Expected 1 apply operation across roots";
	EXPECT_EQ(results[0].status, ApplyStatus::Linked);

	// Inodes must now be identical across roots.
	const auto m1 = meta_from_path(p1);
	const auto m2 = meta_from_path(p2);
	EXPECT_EQ(m1.inode, m2.inode) << "Files across roots should share inode after hardlinking";
}

// Multiple roots with duplicates within and across roots should all be handled.
TEST_F(MultiRootTest, HandlesMixedDuplicates)
{
	// Within root1: a.txt and b.txt have the same content
	const auto p_a = td.write_file("root1/a.txt", "content_A");
	const auto p_b = td.write_file("root1/b.txt", "content_A");

	// Within root2: c.txt and d.txt have the same content
	const auto p_c = td.write_file("root2/c.txt", "content_B");
	const auto p_d = td.write_file("root2/d.txt", "content_B");

	// Across roots: e.txt and f.txt have the same content
	const auto p_e = td.write_file("root1/e.txt", "content_C");
	const auto p_f = td.write_file("root2/f.txt", "content_C");

	ScanOptions scan_opts;
	scan_opts.roots = {root1, root2};

	std::vector<DupePair> dupes;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& pair) { dupes.push_back(pair); };

	EngineOptions engine_opts;
	engine_opts.dry_run = true;

	run_engine(*repo, scan_opts, engine_opts, cbs);

	// We expect 3 dupe pairs (one within each root, and one across roots)
	ASSERT_EQ(dupes.size(), 3u) << "Expected exactly 3 dupe pairs total";

	// Check that we have cross-root duplication detected
	const auto has_cross_root = std::any_of(dupes.begin(), dupes.end(), [&](const DupePair& p) {
		const auto canonical_path = std::filesystem::path{p.canonical_path};
		const auto duplicate_path = std::filesystem::path{p.duplicate_path};
		const auto p_root1 =
		    canonical_path.parent_path() == root1 || canonical_path.parent_path().parent_path() == root1;
		const auto p_root2 =
		    duplicate_path.parent_path() == root2 || duplicate_path.parent_path().parent_path() == root2;
		return (p_root1 && p_root2) || (p_root2 && p_root1);
	});
	EXPECT_TRUE(has_cross_root) << "Should detect at least one cross-root duplicate";
}

// Re-scanning multiple roots after apply should be idempotent.
TEST_F(MultiRootTest, RescanMultipleRootsIsIdempotent)
{
	td.write_file("root1/file1.txt", "content");
	td.write_file("root2/file2.txt", "content");

	ScanOptions scan_opts;
	scan_opts.roots = {root1, root2};

	EngineOptions apply_opts;
	apply_opts.dry_run = false;

	// First apply
	run_engine(*repo, scan_opts, apply_opts);

	// Second scan after apply
	std::vector<ApplyResult> second_results;
	EngineCallbacks cbs2;
	cbs2.on_apply = [&](const ApplyResult& r) { second_results.push_back(r); };

	run_engine(*repo, scan_opts, apply_opts, cbs2);

	ASSERT_EQ(second_results.size(), 1u);
	EXPECT_EQ(second_results[0].status, ApplyStatus::AlreadyLinked);
}

// Scanning multiple roots with nested subdirectories.
TEST_F(MultiRootTest, HandlesNestedStructureAcrossRoots)
{
	const auto p1 = td.write_file("root1/sub1/sub2/file.txt", "content");
	const auto p2 = td.write_file("root2/deep/nested/path/file.txt", "content");

	ScanOptions scan_opts;
	scan_opts.roots = {root1, root2};

	std::vector<DupePair> dupes;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& pair) { dupes.push_back(pair); };

	EngineOptions engine_opts;
	engine_opts.dry_run = true;

	run_engine(*repo, scan_opts, engine_opts, cbs);

	EXPECT_EQ(dupes.size(), 1u) << "Nested files across roots should be detected";
}

// Many roots should work correctly.
TEST_F(MultiRootTest, HandlesMultipleManyRoots)
{
	std::vector<std::filesystem::path> roots;
	const auto content = "identical";

	// Create 5 roots with identical files in each
	for (int i = 0; i < 5; ++i) {
		const auto root = td.path() / ("root" + std::to_string(i));
		std::filesystem::create_directories(root);
		roots.push_back(root);
		td.write_file("root" + std::to_string(i) + "/file.txt", content);
	}

	ScanOptions scan_opts;
	scan_opts.roots = roots;

	std::vector<DupePair> dupes;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& pair) { dupes.push_back(pair); };

	EngineOptions engine_opts;
	engine_opts.dry_run = true;

	run_engine(*repo, scan_opts, engine_opts, cbs);

	// With 5 identical files, we expect one canonical plus 4 duplicates.
	EXPECT_EQ(dupes.size(), 4u) << "Multiple roots with identical files should create 4 dupe pairs";
}
