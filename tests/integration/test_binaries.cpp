#include "../../cli/cli_impl.hpp"
#include "../../daemon/daemon_impl.hpp"
#include "../../lib/repository.hpp"
#include "../../lib/types.hpp"
#include "../helpers/scope_exit.hpp"
#include "../helpers/temp_dir.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace deduped;
using namespace deduped::test;
namespace fs = std::filesystem;

namespace {

void skip_if_running_as_root_for_binary_tests()
{
	if (::geteuid() == 0) {
		GTEST_SKIP() << "permission-denied behavior is not observable when running as root";
	}
}


} // namespace


TEST(CliBinaryTest, ApplyCreatesHardlinksAcrossMultipleRoots)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root1 = td.path() / "root1";
	const auto root2 = td.path() / "root2";
	fs::create_directories(root1);
	fs::create_directories(root2);

	const auto p1 = td.write_file("root1/a.txt", "same content");
	const auto p2 = td.write_file("root2/b.txt", "same content");

	const int exit_code = run_cli_impl(config.string(), {root1.string(), root2.string()}, "", true);

	EXPECT_EQ(exit_code, 0);
	EXPECT_EQ(meta_from_path(p1).inode, meta_from_path(p2).inode);
}

TEST(CliBinaryTest, ApplyOnAlreadyLinkedFilesSucceeds)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);

	const auto canonical = td.write_file("root/a.txt", "same content");
	const auto duplicate = root / "b.txt";
	fs::create_hard_link(canonical, duplicate);

	const int exit_code = run_cli_impl(config.string(), {root.string()}, "", true);

	EXPECT_EQ(exit_code, 0);
	EXPECT_EQ(meta_from_path(canonical).inode, meta_from_path(duplicate).inode);
}

TEST(CliBinaryTest, ApplySkipsCrossDeviceDuplicates)
{
	const auto other_device_base = fs::path{"/dev/shm"};
	if (!fs::exists(other_device_base) || !fs::is_directory(other_device_base)) {
		GTEST_SKIP() << "/dev/shm is not available for cross-device testing";
	}

	TempDir td;
	if (meta_from_path(td.path()).device == meta_from_path(other_device_base).device) {
		GTEST_SKIP() << "/dev/shm is on the same device as the main temp dir";
	}

	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);

	const auto other_root =
	    other_device_base / ("deduped-cli-cross-device-" + std::to_string(::getpid()) + "-" +
	                         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	fs::create_directories(other_root);
	const auto cleanup = scope_exit{[other_root] {
		std::error_code ec;
		fs::remove_all(other_root, ec);
	}};

	const auto canonical = td.write_file("root/a.txt", "same content");
	const auto duplicate = other_root / "b.txt";
	{
		std::ofstream out{duplicate};
		out << "same content";
	}

	const int exit_code = run_cli_impl(config.string(), {root.string(), other_root.string()}, "", true);

	EXPECT_EQ(exit_code, 0);
	EXPECT_NE(meta_from_path(canonical).inode, meta_from_path(duplicate).inode);
}

TEST(CliBinaryTest, UnreadableRootFails)
{
	skip_if_running_as_root_for_binary_tests();

	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "blocked-root";
	fs::create_directories(root);

	const auto restore_permissions = scope_exit{[root] {
		std::error_code ec;
		fs::permissions(root, fs::perms::owner_all, fs::perm_options::replace, ec);
	}};
	fs::permissions(root, fs::perms::none, fs::perm_options::replace);

	const int exit_code = run_cli_impl(config.string(), {root.string()}, "", false);

	EXPECT_NE(exit_code, 0);
}

TEST(DaemonInitTest, InitializesWithValidDirectories)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);
	td.write_file("root/file.txt", "content");

	const int exit_code = init_daemon_without_watcher(config.string(), {root.string()}, "info", false);

	EXPECT_EQ(exit_code, 0);
	EXPECT_TRUE(fs::exists(config / "deduped.db"));
	Repository repo{config / "deduped.db"};
	auto entry = repo.find_by_path((root / "file.txt").string());
	EXPECT_TRUE(entry.has_value());
}

TEST(DaemonInitTest, WithDuplicatesRunsReconciliationScan)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);
	td.write_file("root/a.txt", "duplicate content");
	td.write_file("root/b.txt", "duplicate content");

	const int exit_code = init_daemon_without_watcher(config.string(), {root.string()}, "info", false);

	EXPECT_EQ(exit_code, 0);
	Repository repo{config / "deduped.db"};
	EXPECT_TRUE(repo.find_by_path((root / "a.txt").string()).has_value());
	EXPECT_TRUE(repo.find_by_path((root / "b.txt").string()).has_value());
}

TEST(DaemonInitTest, WithApplyFlagExecutesRecoveryAndReconciliation)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);
	const auto p1 = td.write_file("root/a.txt", "same content");
	const auto p2 = td.write_file("root/b.txt", "same content");

	const int exit_code = init_daemon_without_watcher(config.string(), {root.string()}, "info", true);

	EXPECT_EQ(exit_code, 0);
	EXPECT_EQ(meta_from_path(p1).inode, meta_from_path(p2).inode);
}

TEST(DaemonInitTest, HandlesMultipleDataDirectories)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root1 = td.path() / "root1";
	const auto root2 = td.path() / "root2";
	fs::create_directories(root1);
	fs::create_directories(root2);
	const auto p1 = td.write_file("root1/file.txt", "content");
	const auto p2 = td.write_file("root2/file.txt", "content");

	const int exit_code = init_daemon_without_watcher(config.string(), {root1.string(), root2.string()}, "info", false);

	EXPECT_EQ(exit_code, 0);
	Repository repo{config / "deduped.db"};
	EXPECT_TRUE(repo.find_by_path(p1.string()).has_value());
	EXPECT_TRUE(repo.find_by_path(p2.string()).has_value());
}

TEST(DaemonInitTest, FailsWithEmptyDataDirectoryList)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);

	const int exit_code = init_daemon_without_watcher(config.string(), {}, "info", false);

	EXPECT_NE(exit_code, 0);
}

TEST(DaemonInitTest, FailsWithUnreadableDataDirectory)
{
	skip_if_running_as_root_for_binary_tests();

	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "blocked-root";
	fs::create_directories(root);

	const auto restore_permissions = scope_exit{[root] {
		std::error_code ec;
		fs::permissions(root, fs::perms::owner_all, fs::perm_options::replace, ec);
	}};
	fs::permissions(root, fs::perms::none, fs::perm_options::replace);

	const int exit_code = init_daemon_without_watcher(config.string(), {root.string()}, "info", false);

	EXPECT_NE(exit_code, 0);
}

TEST(DaemonInitTest, FailsWithUnreadableConfigDirectory)
{
	skip_if_running_as_root_for_binary_tests();

	TempDir td;
	const auto config = td.path() / "blocked-config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);

	const auto restore_permissions = scope_exit{[config] {
		std::error_code ec;
		fs::permissions(config, fs::perms::owner_all, fs::perm_options::replace, ec);
	}};
	fs::permissions(config, fs::perms::none, fs::perm_options::replace);

	const int exit_code = init_daemon_without_watcher(config.string(), {root.string()}, "info", false);

	EXPECT_NE(exit_code, 0);
}

TEST(DaemonInitTest, FailsWithUnwritableConfigDirectory)
{
	skip_if_running_as_root_for_binary_tests();

	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);

	const auto restore_perms = fs::status(config).permissions();
	const auto restore_permissions = scope_exit{[config, restore_perms] {
		std::error_code ec;
		fs::permissions(config, restore_perms, fs::perm_options::replace, ec);
	}};
	fs::permissions(config, fs::perms::owner_read | fs::perms::owner_exec, fs::perm_options::replace);

	const int exit_code = init_daemon_without_watcher(config.string(), {root.string()}, "info", false);

	EXPECT_NE(exit_code, 0);
}

TEST(DaemonInitTest, FailsWithDataPathThatIsNotDirectory)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto data_file = td.write_file("data-file", "x");

	const int exit_code = init_daemon_without_watcher(config.string(), {data_file.string()}, "info", false);

	EXPECT_NE(exit_code, 0);
}

TEST(DaemonInitTest, FailsWithConfigPathThatIsNotDirectory)
{
	TempDir td;
	const auto config_file = td.write_file("config-file", "x");
	const auto root = td.path() / "root";
	fs::create_directories(root);

	const int exit_code = init_daemon_without_watcher(config_file.string(), {root.string()}, "info", false);

	EXPECT_NE(exit_code, 0);
}

TEST(CliBinaryTest, InvalidLogLevelFails)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);

	const int exit_code = run_cli_impl(config.string(), {root.string()}, "nope", false);

	EXPECT_NE(exit_code, 0);
}

TEST(DaemonBinaryTest, InvalidLogLevelFails)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);

	const int exit_code = init_daemon_without_watcher(config.string(), {root.string()}, "nope", false);

	EXPECT_NE(exit_code, 0);
}

TEST(DaemonBinaryTest, UnreadableRootFailsFast)
{
	skip_if_running_as_root_for_binary_tests();

	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "blocked-root";
	fs::create_directories(root);
	const auto restore_permissions = scope_exit{[root] {
		std::error_code ec;
		fs::permissions(root, fs::perms::owner_all, fs::perm_options::replace, ec);
	}};
	fs::permissions(root, fs::perms::none, fs::perm_options::replace);

	const int exit_code = init_daemon_without_watcher(config.string(), {root.string()}, "", false);

	EXPECT_NE(exit_code, 0);
}


TEST(DaemonBinaryTest, MissingConfigDirectoryFails)
{
	TempDir td;
	const auto root = td.path() / "root";
	fs::create_directories(root);
	const auto missing_config = td.path() / "does-not-exist";

	const int exit_code = init_daemon_without_watcher(missing_config.string(), {root.string()}, "", false);

	EXPECT_NE(exit_code, 0);
}


TEST(DaemonBinaryTest, ExistingLockDirPreventsStart)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);
	fs::create_directories(config / "deduped.lockdir");

	const int exit_code = init_daemon_without_watcher(config.string(), {root.string()}, "", false);

	EXPECT_NE(exit_code, 0);
}
