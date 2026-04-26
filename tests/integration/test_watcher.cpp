#include "../../lib/scope_exit.hpp"
#include "../../lib/watcher.hpp"
#include "../helpers/temp_dir.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace deduped;
using namespace deduped::test;
namespace fs = std::filesystem;

namespace {

// Run `watcher.run()` in a background thread; return the thread.
std::thread run_async(Watcher& w)
{
	return std::thread{[&] { w.run(); }};
}

// Give inotify time to deliver queued events. 200ms avoids flakes on loaded CI
// systems where 50ms is insufficient for kernel delivery.
void settle() { std::this_thread::sleep_for(std::chrono::milliseconds(200)); }

void skip_if_running_as_root_for_watcher_tests()
{
	if (::geteuid() == 0) {
		GTEST_SKIP() << "permission-denied behavior is not observable when running as root";
	}
}

} // namespace

// Creating a file and closing it fires a Modified event.
TEST(WatcherTest, FileCreationFiresModifiedEvent)
{
	TempDir td;

	std::vector<WatchEvent> events;
	std::mutex mtx;

	Watcher w{td.path(), [&](const WatchEvent& ev) {
		          std::lock_guard lk{mtx};
		          events.push_back(ev);
	          }};

	auto t = run_async(w);

	// Create file.
	const auto p = td.write_file("hello.txt", "content");
	settle();
	w.stop();
	t.join();

	std::lock_guard lk{mtx};
	const bool saw_modified = std::any_of(events.begin(), events.end(), [&](const WatchEvent& ev) {
		return ev.type == FileEvent::Modified && ev.path == p;
	});
	EXPECT_TRUE(saw_modified) << "Expected Modified event for " << p;
}

// Deleting a file fires a Deleted event.
TEST(WatcherTest, FileDeletionFiresDeletedEvent)
{
	TempDir td;
	const auto p = td.write_file("victim.txt", "bye");

	std::vector<WatchEvent> events;
	std::mutex mtx;

	Watcher w{td.path(), [&](const WatchEvent& ev) {
		          std::lock_guard lk{mtx};
		          events.push_back(ev);
	          }};

	auto t = run_async(w);

	fs::remove(p);
	settle();
	w.stop();
	t.join();

	std::lock_guard lk{mtx};
	const bool saw_deleted = std::any_of(events.begin(), events.end(), [&](const WatchEvent& ev) {
		return ev.type == FileEvent::Deleted && ev.path == p;
	});
	EXPECT_TRUE(saw_deleted) << "Expected Deleted event for " << p;
}

// Overwriting a file fires a Modified event.
TEST(WatcherTest, FileModificationFiresModifiedEvent)
{
	TempDir td;
	const auto p = td.write_file("editable.txt", "v1");

	std::vector<WatchEvent> events;
	std::mutex mtx;

	Watcher w{td.path(), [&](const WatchEvent& ev) {
		          std::lock_guard lk{mtx};
		          events.push_back(ev);
	          }};

	auto t = run_async(w);

	{
		std::ofstream f{p, std::ios::trunc};
		f << "v2";
	}
	settle();
	w.stop();
	t.join();

	std::lock_guard lk{mtx};
	const bool saw_modified = std::any_of(events.begin(), events.end(), [&](const WatchEvent& ev) {
		return ev.type == FileEvent::Modified && ev.path == p;
	});
	EXPECT_TRUE(saw_modified) << "Expected Modified event for " << p;
}

// Files created in a newly created subdirectory are also reported.
TEST(WatcherTest, FilesInNewSubdirAreWatched)
{
	TempDir td;

	std::vector<WatchEvent> events;
	std::mutex mtx;

	Watcher w{td.path(), [&](const WatchEvent& ev) {
		          std::lock_guard lk{mtx};
		          events.push_back(ev);
	          }};

	auto t = run_async(w);

	// Create a subdirectory, then a file inside it.
	const auto sub = td.path() / "subdir";
	fs::create_directory(sub);
	settle(); // let watcher pick up the directory

	const auto p = sub / "nested.txt";
	{
		std::ofstream f{p};
		f << "nested";
	}
	settle();
	w.stop();
	t.join();

	std::lock_guard lk{mtx};
	const bool saw_nested = std::any_of(events.begin(), events.end(), [&](const WatchEvent& ev) {
		return ev.type == FileEvent::Modified && ev.path == p;
	});
	EXPECT_TRUE(saw_nested) << "Expected Modified event for nested file " << p;
}

TEST(WatcherTest, MultipleRootsAreWatched)
{
	TempDir td;
	const auto root1 = td.path() / "root1";
	const auto root2 = td.path() / "root2";
	fs::create_directories(root1);
	fs::create_directories(root2);

	std::vector<WatchEvent> events;
	std::mutex mtx;

	Watcher w{{root1, root2}, [&](const WatchEvent& ev) {
		          std::lock_guard lk{mtx};
		          events.push_back(ev);
	          }};

	auto t = run_async(w);

	const auto p1 = td.write_file("root1/first.txt", "one");
	const auto p2 = td.write_file("root2/second.txt", "two");
	settle();
	w.stop();
	t.join();

	std::lock_guard lk{mtx};
	const bool saw_first = std::any_of(events.begin(), events.end(), [&](const WatchEvent& ev) {
		return ev.type == FileEvent::Modified && ev.path == p1;
	});
	const bool saw_second = std::any_of(events.begin(), events.end(), [&](const WatchEvent& ev) {
		return ev.type == FileEvent::Modified && ev.path == p2;
	});
	EXPECT_TRUE(saw_first);
	EXPECT_TRUE(saw_second);
}

TEST(WatcherTest, UnreadableRootFails)
{
	skip_if_running_as_root_for_watcher_tests();

	TempDir td;
	const auto root = td.path() / "blocked-root";
	fs::create_directories(root);
	const auto restore_permissions = scope_exit{[root] {
		std::error_code ec;
		fs::permissions(root, fs::perms::owner_all, fs::perm_options::replace, ec);
	}};
	fs::permissions(root, fs::perms::none, fs::perm_options::replace);

	const auto construct_watcher = [&] { Watcher watcher{root, [](const WatchEvent&) {}}; };
	EXPECT_THROW(construct_watcher(), fs::filesystem_error);
}

TEST(WatcherTest, UnreadableSubtreeIsIgnored)
{
	skip_if_running_as_root_for_watcher_tests();

	TempDir td;
	const auto root = td.path() / "root";
	const auto blocked = root / "blocked";
	fs::create_directories(blocked);
	const auto restore_permissions = scope_exit{[blocked] {
		std::error_code ec;
		fs::permissions(blocked, fs::perms::owner_all, fs::perm_options::replace, ec);
	}};
	fs::permissions(blocked, fs::perms::none, fs::perm_options::replace);

	std::vector<WatchEvent> events;
	std::mutex mtx;

	Watcher w{root, [&](const WatchEvent& ev) {
		          std::lock_guard lk{mtx};
		          events.push_back(ev);
	          }};

	auto t = run_async(w);
	const auto visible = td.write_file("root/visible.txt", "ok");
	settle();
	w.stop();
	t.join();

	std::lock_guard lk{mtx};
	const bool saw_visible = std::any_of(events.begin(), events.end(), [&](const WatchEvent& ev) {
		return ev.type == FileEvent::Modified && ev.path == visible;
	});
	EXPECT_TRUE(saw_visible);
}

// stop() before run() causes run() to return immediately.
TEST(WatcherTest, StopBeforeRunReturnsImmediately)
{
	TempDir td;
	Watcher w{td.path(), [](const WatchEvent&) {}};
	w.stop();

	bool finished = false;
	std::thread t{[&] {
		w.run();
		finished = true;
	}};
	t.join();

	EXPECT_TRUE(finished);
}
