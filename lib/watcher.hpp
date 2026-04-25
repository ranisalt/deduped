#pragma once

#include <filesystem>
#include <functional>
#include <memory>

namespace deduped {

enum class FileEvent
{
	Modified, // File created, written, or moved into the watched tree.
	Deleted,  // File deleted or moved out of the watched tree.
};

struct WatchEvent
{
	std::filesystem::path path;
	FileEvent type;
};

// Recursive inotify-based directory watcher (Linux only).
//
// Watches one or more root directories and all their subdirectories. For each
// regular-file event, the callback is called with a WatchEvent describing what
// happened.
//
// - IN_CLOSE_WRITE and IN_MOVED_TO  -> FileEvent::Modified
// - IN_DELETE and IN_MOVED_FROM     -> FileEvent::Deleted
// - New subdirectories are automatically watched.
// - Symlinks are never followed.
//
// Thread safety: run() blocks the calling thread.  stop() is safe to call
// from any other thread (writes to an internal pipe).
class Watcher
{
public:
	using Callback = std::function<void(const WatchEvent&)>;

	// Construct and start watching `root` recursively.
	// Throws std::system_error if inotify_init1 or the initial watches fail.
	explicit Watcher(std::filesystem::path root, Callback cb);

	// Construct and start watching all `roots` recursively.
	// Throws if any root itself is unreadable or cannot be watched.
	explicit Watcher(std::vector<std::filesystem::path> roots, Callback cb);
	~Watcher();

	Watcher(const Watcher&) = delete;
	Watcher& operator=(const Watcher&) = delete;

	// Block the calling thread until stop() is called.
	// Calls cb for each file-level event.
	void run();

	// Signal run() to return.  Thread-safe.
	void stop() noexcept;

private:
	struct Impl;
	std::unique_ptr<Impl> impl_;
};

} // namespace deduped
