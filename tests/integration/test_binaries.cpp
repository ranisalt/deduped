#include "../../cli/cli_impl.hpp"
#include "../../daemon/daemon_impl.hpp"
#include "../../lib/repository.hpp"
#include "../../lib/scope_exit.hpp"
#include "../../lib/types.hpp"
#include "../helpers/temp_dir.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>
#include <sstream>
#include <string>
#include <thread>
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

TEST(CliBinaryTest, InterruptLogsClosingMessageWithPendingHashJobs)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);

	const auto worker_count = std::max(1u, std::thread::hardware_concurrency());
	const auto job_count = worker_count + 8;
	const std::string payload(1024 * 1024, 'z');
	std::vector<fs::path> file_paths;
	file_paths.reserve(job_count);
	for (unsigned int i = 0; i < job_count; ++i) {
		file_paths.push_back(td.write_file("root/file-" + std::to_string(i) + ".bin", payload));
	}

	Repository observer(config / "deduped.db");

	std::ostringstream captured_logs;
	auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured_logs);
	auto logger = std::make_shared<spdlog::logger>("test-cli-shutdown", sink);
	auto previous_logger = spdlog::default_logger();
	const auto previous_level = spdlog::default_logger()->level();
	spdlog::set_default_logger(logger);
	spdlog::set_level(spdlog::level::info);

	const auto restore_logger = scope_exit{[&] {
		spdlog::set_default_logger(previous_logger);
		spdlog::set_level(previous_level);
	}};

	std::atomic<bool> interrupted = false;
	std::jthread interrupter([&](std::stop_token stop_token) {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!stop_token.stop_requested() && std::chrono::steady_clock::now() < deadline) {
			for (const auto& path : file_paths) {
				if (observer.find_by_path(path.string()).has_value()) {
					interrupted = true;
					::raise(SIGINT);
					return;
				}
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}
	});

	const int exit_code = run_cli_impl(config.string(), {root.string()}, "info", false);

	EXPECT_EQ(exit_code, 130);
	EXPECT_TRUE(interrupted.load());
	const auto logs = captured_logs.str();
	EXPECT_NE(logs.find("Application is closing"), std::string::npos);
	EXPECT_NE(logs.find("hash jobs pending"), std::string::npos);
	EXPECT_NE(logs.find("Done. Scanned="), std::string::npos);
}

TEST(CliBinaryTest, ProgressLogsHitAndMissTotals)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);

	for (int i = 0; i < 1000; ++i) {
		td.write_file("root/file-" + std::to_string(i) + ".txt", "content-" + std::to_string(i));
	}

	std::ostringstream first_run_logs;
	auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(first_run_logs);
	auto logger = std::make_shared<spdlog::logger>("test-cli-progress", sink);
	auto previous_logger = spdlog::default_logger();
	const auto previous_level = spdlog::default_logger()->level();
	spdlog::set_default_logger(logger);
	spdlog::set_level(spdlog::level::info);

	const auto restore_logger = scope_exit{[&] {
		spdlog::set_default_logger(previous_logger);
		spdlog::set_level(previous_level);
	}};

	EXPECT_EQ(run_cli_impl(config.string(), {root.string()}, "info", false), 0);
	EXPECT_NE(first_run_logs.str().find("Progress: scanned=1000 hits=0 misses=1000"), std::string::npos);
	EXPECT_NE(first_run_logs.str().find("Done. Scanned=1000 Hits=0 Misses=1000 Dupes=0 Linked=0 Failed=0"),
	          std::string::npos);

	first_run_logs.str("");
	first_run_logs.clear();

	EXPECT_EQ(run_cli_impl(config.string(), {root.string()}, "info", false), 0);
	EXPECT_NE(first_run_logs.str().find("Progress: scanned=1000 hits=1000 misses=0"), std::string::npos);
	EXPECT_NE(first_run_logs.str().find("Done. Scanned=1000 Hits=1000 Misses=0 Dupes=0 Linked=0 Failed=0"),
	          std::string::npos);
}

TEST(CliBinaryTest, PersistsIndexEntriesBetweenRuns)
{
	TempDir td;
	const auto config = td.path() / "config";
	fs::create_directories(config);
	const auto root = td.path() / "root";
	fs::create_directories(root);

	const auto p1 = td.write_file("root/file-1.txt", "content-1");
	const auto p2 = td.write_file("root/file-2.txt", "content-2");
	const auto p3 = td.write_file("root/file-3.txt", "content-3");

	EXPECT_EQ(run_cli_impl(config.string(), {root.string()}, "info", false), 0);

	Repository repo(config / "deduped.db");
	EXPECT_TRUE(repo.find_by_path(p1.string()).has_value());
	EXPECT_TRUE(repo.find_by_path(p2.string()).has_value());
	EXPECT_TRUE(repo.find_by_path(p3.string()).has_value());

	EXPECT_EQ(run_cli_impl(config.string(), {root.string()}, "info", false), 0);
	EXPECT_TRUE(repo.find_by_path(p1.string()).has_value());
	EXPECT_TRUE(repo.find_by_path(p2.string()).has_value());
	EXPECT_TRUE(repo.find_by_path(p3.string()).has_value());
}

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
