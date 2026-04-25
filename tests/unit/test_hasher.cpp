#include "../../lib/hasher.hpp"
#include "../helpers/temp_dir.hpp"

#include <gtest/gtest.h>

using namespace deduped;
using namespace deduped::test;

TEST(HashFile, Deterministic)
{
	TempDir td;
	const auto p = td.write_file("a.txt", "hello world");
	const auto h1 = hash_file(p);
	const auto h2 = hash_file(p);
	EXPECT_EQ(h1, h2);
}

TEST(HashFile, EmptyFile)
{
	TempDir td;
	const auto p = td.write_file("empty.txt", "");
	// Must not throw and must return a valid 32-byte digest.
	Digest d;
	ASSERT_NO_THROW(d = hash_file(p));
	// BLAKE3 of empty input is a known constant, verify it's not all zeros.
	Digest zeros{};
	EXPECT_NE(d, zeros);
}

TEST(HashFile, DifferentContentDifferentDigest)
{
	TempDir td;
	const auto p1 = td.write_file("a.txt", "hello");
	const auto p2 = td.write_file("b.txt", "world");
	EXPECT_NE(hash_file(p1), hash_file(p2));
}

TEST(HashFile, SameContentSameDigest)
{
	TempDir td;
	const auto p1 = td.write_file("a.txt", "identical content");
	const auto p2 = td.write_file("b.txt", "identical content");
	EXPECT_EQ(hash_file(p1), hash_file(p2));
}

TEST(HashFile, ThrowsOnMissingFile) { EXPECT_THROW(hash_file("/nonexistent/path/file.txt"), std::system_error); }
