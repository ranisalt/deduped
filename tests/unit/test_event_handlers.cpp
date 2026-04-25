#include "../../lib/engine.hpp"
#include "../../lib/repository.hpp"
#include "../../lib/types.hpp"
#include "../helpers/temp_dir.hpp"

#include <fstream>
#include <gtest/gtest.h>

using namespace deduped;
using namespace deduped::test;

class EventHandlerTest : public ::testing::Test
{
protected:
	void SetUp() override { repo = std::make_unique<Repository>(td.path() / "events.db"); }

	TempDir td;
	std::unique_ptr<Repository> repo;
};

// handle_file_change indexes a new file and returns nullopt (no duplicate yet).
TEST_F(EventHandlerTest, NewUniqueFileIsIndexed)
{
	const auto p = td.write_file("a.txt", "unique content");

	EngineOptions opts;
	opts.dry_run = true;
	const auto result = handle_file_change(*repo, p, opts);

	EXPECT_FALSE(result.has_value());
	EXPECT_TRUE(repo->find_by_path(p.string()).has_value());
}

// handle_file_change detects a duplicate when a second identical file is indexed.
TEST_F(EventHandlerTest, DuplicateFileIsDetected)
{
	const auto p1 = td.write_file("a.txt", "same");
	const auto p2 = td.write_file("b.txt", "same");

	EngineOptions opts;
	opts.dry_run = true;

	handle_file_change(*repo, p1, opts);

	std::vector<DupePair> found;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& pair) { found.push_back(pair); };

	const auto result = handle_file_change(*repo, p2, opts, cbs);

	EXPECT_TRUE(result.has_value());
	EXPECT_EQ(found.size(), 1u);
}

// handle_file_change in apply mode creates a hardlink for a confirmed duplicate.
TEST_F(EventHandlerTest, ApplyModeCreatesHardlink)
{
	const auto p1 = td.write_file("canon.txt", "identical");
	const auto p2 = td.write_file("dup.txt", "identical");

	EngineOptions opts;
	opts.dry_run = false;

	handle_file_change(*repo, p1, opts);
	const auto result = handle_file_change(*repo, p2, opts);

	ASSERT_TRUE(result.has_value());
	EXPECT_EQ(result->status, ApplyStatus::Linked);

	// Inodes must now match.
	EXPECT_EQ(meta_from_path(p1).inode, meta_from_path(p2).inode);
}

// handle_file_removed deletes the entry from the DB.
TEST_F(EventHandlerTest, RemovedFileIsDeletedFromIndex)
{
	const auto p = td.write_file("gone.txt", "will be removed");

	EngineOptions opts;
	opts.dry_run = true;
	handle_file_change(*repo, p, opts);
	EXPECT_TRUE(repo->find_by_path(p.string()).has_value());

	handle_file_removed(*repo, p);
	EXPECT_FALSE(repo->find_by_path(p.string()).has_value());
}

// handle_file_removed is a no-op for paths not in the DB.
TEST_F(EventHandlerTest, RemoveUnknownPathIsNoOp)
{
	EXPECT_NO_THROW(handle_file_removed(*repo, td.path() / "nonexistent.txt"));
}

// Modifying a file content causes its hash to be updated in the DB.
TEST_F(EventHandlerTest, ModifiedFileCausesRehash)
{
	const auto p = td.write_file("mutable.txt", "version one");

	EngineOptions opts;
	opts.dry_run = true;
	handle_file_change(*repo, p, opts);
	const auto first = repo->find_by_path(p.string());
	ASSERT_TRUE(first.has_value());

	// Overwrite with different content and re-signal.
	{
		std::ofstream f(p, std::ios::trunc);
		f << "version two";
	}

	handle_file_change(*repo, p, opts);
	const auto second = repo->find_by_path(p.string());
	ASSERT_TRUE(second.has_value());

	EXPECT_NE(first->digest, second->digest);
}

// Removing the canonical of a dupe set de-indexes it. The remaining duplicate
// is still in the index (its entry is not touched by handle_file_removed).
TEST_F(EventHandlerTest, RemovingCanonicalDeIndexesItButLeavesOtherEntries)
{
	const auto canon = td.write_file("canon.txt", "shared");
	const auto dup = td.write_file("dup.txt", "shared");

	EngineOptions opts;
	opts.dry_run = true;

	handle_file_change(*repo, canon, opts);
	handle_file_change(*repo, dup, opts);

	ASSERT_TRUE(repo->find_by_path(canon.string()).has_value());
	ASSERT_TRUE(repo->find_by_path(dup.string()).has_value());

	handle_file_removed(*repo, canon);

	EXPECT_FALSE(repo->find_by_path(canon.string()).has_value());
	// The duplicate entry remains; it is now the only copy of that digest.
	EXPECT_TRUE(repo->find_by_path(dup.string()).has_value());
}
