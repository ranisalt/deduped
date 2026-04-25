#include "../../lib/repository.hpp"
#include "../../lib/types.hpp"
#include "../helpers/temp_dir.hpp"

#include <gtest/gtest.h>

using namespace deduped;
using namespace deduped::test;

class RepositoryTest : public ::testing::Test
{
protected:
	void SetUp() override { repo = std::make_unique<Repository>(td.path() / "test.db"); }

	TempDir td;
	std::unique_ptr<Repository> repo;

	static IndexEntry make_entry(const std::string& path, std::uint64_t size = 10)
	{
		IndexEntry e;
		e.path = path;
		e.meta.size = size;
		e.meta.mtime_ns = 1000000;
		e.meta.inode = 42;
		e.meta.device = 7;
		e.meta.mode = 0644;
		e.meta.uid = 1000;
		e.meta.gid = 1000;
		e.digest.fill(0xab);
		e.last_seen = 1000;
		return e;
	}
};

TEST_F(RepositoryTest, UpsertAndFindByPath)
{
	const auto e = make_entry("/tmp/file.txt");
	repo->upsert(e);
	const auto found = repo->find_by_path("/tmp/file.txt");
	ASSERT_TRUE(found.has_value());
	EXPECT_EQ(found->path, "/tmp/file.txt");
	EXPECT_EQ(found->meta.size, 10u);
}

TEST_F(RepositoryTest, FindByPathMissingReturnsNullopt) { EXPECT_EQ(repo->find_by_path("/nonexistent"), std::nullopt); }

TEST_F(RepositoryTest, UpsertUpdatesExistingRow)
{
	auto e = make_entry("/tmp/file.txt", 10);
	repo->upsert(e);
	e.meta.size = 20;
	repo->upsert(e);
	const auto found = repo->find_by_path("/tmp/file.txt");
	ASSERT_TRUE(found.has_value());
	EXPECT_EQ(found->meta.size, 20u);
}

TEST_F(RepositoryTest, FindByDigestReturnsAllMatches)
{
	Digest d;
	d.fill(0xcd);
	auto e1 = make_entry("/a");
	e1.digest = d;
	auto e2 = make_entry("/b");
	e2.digest = d;
	auto e3 = make_entry("/c");
	e3.digest = {}; // different digest

	repo->upsert(e1);
	repo->upsert(e2);
	repo->upsert(e3);

	const auto matches = repo->find_by_digest(d);
	EXPECT_EQ(matches.size(), 2u);
}

TEST_F(RepositoryTest, RemoveStaleDeletesOldRows)
{
	auto e1 = make_entry("/old");
	e1.last_seen = 100;
	auto e2 = make_entry("/new");
	e2.last_seen = 1000;
	repo->upsert(e1);
	repo->upsert(e2);

	repo->remove_stale(500); // removes anything with last_seen < 500

	EXPECT_EQ(repo->find_by_path("/old"), std::nullopt);
	ASSERT_TRUE(repo->find_by_path("/new").has_value());
}

TEST_F(RepositoryTest, OpLogRoundtrip)
{
	const auto op_id = repo->log_op_planned("/canonical", "/duplicate");
	EXPECT_GT(op_id, 0);
	ASSERT_NO_THROW(repo->log_op_complete(op_id, Repository::OpStatus::Done));
}

TEST_F(RepositoryTest, NowUnixSReturnsPositiveValue) { EXPECT_GT(repo->now_unix_s(), 0); }
