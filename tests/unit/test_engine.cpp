#include "../../lib/engine.hpp"
#include "../../lib/repository.hpp"
#include "../helpers/temp_dir.hpp"

#include <filesystem>
#include <gtest/gtest.h>

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

TEST_F(EngineTest, ResolveDigestRehashesWhenModeChanges)
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

	EXPECT_FALSE(result.reused_cached_digest);
	EXPECT_EQ(hash_calls, 1u);
	EXPECT_EQ(result.digest, refreshed_digest);
}
