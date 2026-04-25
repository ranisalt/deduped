#include "../../lib/types.hpp"
#include "../helpers/temp_dir.hpp"

#include <chrono>
#include <gtest/gtest.h>
#include <thread>

using namespace deduped;
using namespace deduped::test;

TEST(MetaFromPath, PopulatesAllFields)
{
	TempDir td;
	const auto p = td.write_file("x.txt", "data");
	const auto m = meta_from_path(p);
	EXPECT_EQ(m.size, 4u);
	EXPECT_GT(m.mtime_ns, 0);
	EXPECT_GT(m.inode, 0u);
	EXPECT_GT(m.device, 0u);
	EXPECT_NE(m.mode, 0u);
}

TEST(MetaFromPath, SizeMatchesFileContent)
{
	TempDir td;
	const std::string content(1234, 'z');
	const auto p = td.write_file("big.txt", content);
	EXPECT_EQ(meta_from_path(p).size, 1234u);
}

TEST(MetaFromPath, ThrowsOnMissingFile) { EXPECT_THROW(meta_from_path("/does/not/exist"), std::system_error); }

TEST(IsMetaStaleLive, DetectsMtimeChange)
{
	TempDir td;
	const auto p = td.write_file("f.txt", "v1");
	const auto m1 = meta_from_path(p);

	// Sleep enough for mtime resolution on most filesystems (1 second).
	std::this_thread::sleep_for(std::chrono::milliseconds(1100));
	// Rewrite to force mtime update.
	td.write_file("f.txt", "v2");

	const auto m2 = meta_from_path(p);
	// Size OR mtime will differ; is_meta_stale must report true.
	EXPECT_TRUE(is_meta_stale(m1, m2));
}
