#include "../../lib/types.hpp"

#include <gtest/gtest.h>

using namespace deduped;

TEST(DigestToHex, AllZeros)
{
	Digest d{};
	EXPECT_EQ(digest_to_hex(d), std::string(64, '0'));
}

TEST(DigestToHex, AllOnes)
{
	Digest d;
	d.fill(0xff);
	EXPECT_EQ(digest_to_hex(d), std::string(64, 'f'));
}

TEST(DigestToHex, KnownBytes)
{
	Digest d{};
	d[0] = 0xde;
	d[1] = 0xad;
	const auto hex = digest_to_hex(d);
	EXPECT_EQ(hex.substr(0, 4), "dead");
}

TEST(IsMetaStale, IdenticalMetaNotStale)
{
	FileMeta m;
	m.size = 100;
	m.mtime_ns = 1000;
	m.inode = 5;
	m.device = 2;
	m.mode = 0644;
	m.uid = 1000;
	m.gid = 1000;
	EXPECT_FALSE(is_meta_stale(m, m));
}

TEST(IsMetaStale, SizeChangeMakesStale)
{
	FileMeta stored, current;
	stored.size = 100;
	current.size = 101;
	EXPECT_TRUE(is_meta_stale(stored, current));
}

TEST(IsMetaStale, MtimeChangeMakesStale)
{
	FileMeta stored, current;
	stored.mtime_ns = 1000;
	current.mtime_ns = 1001;
	EXPECT_TRUE(is_meta_stale(stored, current));
}

TEST(IsMetaStale, InodeChangeMakesStale)
{
	FileMeta stored, current;
	stored.inode = 10;
	current.inode = 11;
	EXPECT_TRUE(is_meta_stale(stored, current));
}

TEST(IsMetaStale, DeviceChangeMakesStale)
{
	FileMeta stored, current;
	stored.device = 1;
	current.device = 2;
	EXPECT_TRUE(is_meta_stale(stored, current));
}

TEST(IsMetaStale, ModeChangeDoesNotMakeStale)
{
	FileMeta stored, current;
	stored.mode = 0644;
	current.mode = 0600;
	EXPECT_FALSE(is_meta_stale(stored, current));
}

TEST(IsMetaStale, UidChangeDoesNotMakeStale)
{
	FileMeta stored, current;
	stored.uid = 1000;
	current.uid = 0;
	EXPECT_FALSE(is_meta_stale(stored, current));
}

TEST(IsMetaStale, GidChangeDoesNotMakeStale)
{
	FileMeta stored, current;
	stored.gid = 1000;
	current.gid = 0;
	EXPECT_FALSE(is_meta_stale(stored, current));
}
