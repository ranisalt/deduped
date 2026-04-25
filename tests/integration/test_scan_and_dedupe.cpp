#include "../../lib/engine.hpp"
#include "../../lib/repository.hpp"
#include "../../lib/scanner.hpp"
#include "../helpers/temp_dir.hpp"

#include <gtest/gtest.h>

using namespace deduped;
using namespace deduped::test;

class ScanDedupeTest : public ::testing::Test
{
protected:
	void SetUp() override
	{
		repo = std::make_unique<Repository>(td.path() / "integ.db");
		data_root = td.path() / "data";
		std::filesystem::create_directories(data_root);
		scan_opts.roots = {data_root};
	}
	TempDir td;
	std::filesystem::path data_root;
	std::unique_ptr<Repository> repo;
	ScanOptions scan_opts;
};

TEST_F(ScanDedupeTest, ScannerSkipsSymlinks)
{
	const auto real = td.write_file("data/real.txt", "content");
	const auto link = data_root / "link.txt";
	std::filesystem::create_symlink(real, link);

	std::vector<std::filesystem::path> visited;
	scan_files(scan_opts, [&](const auto& p) { visited.push_back(p); });

	for (const auto& p : visited) {
		EXPECT_FALSE(std::filesystem::is_symlink(p)) << "symlink was visited: " << p;
	}
}

TEST_F(ScanDedupeTest, ScannerFindsRegularFiles)
{
	td.write_file("data/a.txt", "a");
	td.write_file("data/b.txt", "b");
	td.write_file("data/sub/c.txt", "c");

	std::vector<std::filesystem::path> visited;
	scan_files(scan_opts, [&](const auto& p) { visited.push_back(p); });
	EXPECT_EQ(visited.size(), 3u);
}

TEST_F(ScanDedupeTest, EngineIndexesThenFindsNoDupesForUniqueFiles)
{
	td.write_file("data/u1.txt", "unique1");
	td.write_file("data/u2.txt", "unique2");

	EngineOptions opts;
	opts.dry_run = true;
	std::vector<DupePair> dupes;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& p) { dupes.push_back(p); };

	run_engine(*repo, scan_opts, opts, cbs);
	EXPECT_TRUE(dupes.empty());
}

TEST_F(ScanDedupeTest, EngineFindsAndReportsDupesAcrossSubdirectories)
{
	td.write_file("data/dir1/file.txt", "duplicate content");
	td.write_file("data/dir2/file.txt", "duplicate content");

	EngineOptions opts;
	opts.dry_run = true;
	std::vector<DupePair> dupes;
	EngineCallbacks cbs;
	cbs.on_dupe_found = [&](const DupePair& p) { dupes.push_back(p); };

	run_engine(*repo, scan_opts, opts, cbs);
	EXPECT_EQ(dupes.size(), 1u);
}
