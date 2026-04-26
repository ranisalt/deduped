#include "../../lib/scanner.hpp"
#include "../../lib/scope_exit.hpp"
#include "../helpers/temp_dir.hpp"

#include <algorithm>
#include <filesystem>
#include <gtest/gtest.h>
#include <unistd.h>
#include <vector>

using namespace deduped;
using namespace deduped::test;
namespace fs = std::filesystem;

namespace {

void skip_if_running_as_root_for_scanner_tests()
{
	if (::geteuid() == 0) {
		GTEST_SKIP() << "permission-denied behavior is not observable when running as root";
	}
}

} // namespace

TEST(ScannerTest, UnreadableRootFails)
{
	skip_if_running_as_root_for_scanner_tests();

	TempDir td;
	const auto root = td.path() / "blocked-root";
	fs::create_directories(root);
	const auto restore_permissions = scope_exit{[root] {
		std::error_code ec;
		fs::permissions(root, fs::perms::owner_all, fs::perm_options::replace, ec);
	}};
	fs::permissions(root, fs::perms::none, fs::perm_options::replace);

	ScanOptions opts;
	opts.roots = {root};

	EXPECT_THROW(scan_files(opts, [](const fs::path&) {}), fs::filesystem_error);
}

TEST(ScannerTest, UnreadableSubtreeIsIgnored)
{
	skip_if_running_as_root_for_scanner_tests();

	TempDir td;
	const auto root = td.path() / "root";
	const auto blocked = root / "blocked";
	fs::create_directories(blocked);
	const auto visible = td.write_file("root/visible.txt", "ok");
	td.write_file("root/blocked/hidden.txt", "nope");

	const auto restore_permissions = scope_exit{[blocked] {
		std::error_code ec;
		fs::permissions(blocked, fs::perms::owner_all, fs::perm_options::replace, ec);
	}};
	fs::permissions(blocked, fs::perms::none, fs::perm_options::replace);

	std::vector<fs::path> visited;
	ScanOptions opts;
	opts.roots = {root};

	ASSERT_NO_THROW(scan_files(opts, [&](const fs::path& p) { visited.push_back(p); }));
	EXPECT_NE(std::find(visited.begin(), visited.end(), fs::canonical(visible)), visited.end());
}

TEST(ScannerTest, SymlinkedDirectoriesAreNotTraversed)
{
	TempDir td;
	const auto root = td.path() / "root";
	fs::create_directories(root);
	const auto external = td.path() / "external";
	fs::create_directories(external);

	const auto visible = td.write_file("root/visible.txt", "ok");
	const auto hidden = td.write_file("external/hidden.txt", "nope");
	fs::create_directory_symlink(external, root / "linked-dir");

	std::vector<fs::path> visited;
	ScanOptions opts;
	opts.roots = {root};

	ASSERT_NO_THROW(scan_files(opts, [&](const fs::path& p) { visited.push_back(p); }));
	EXPECT_NE(std::find(visited.begin(), visited.end(), fs::canonical(visible)), visited.end());
	EXPECT_EQ(std::find(visited.begin(), visited.end(), fs::canonical(hidden)), visited.end());
}
