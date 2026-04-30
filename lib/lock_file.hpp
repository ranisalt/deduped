#pragma once

#include <filesystem>

namespace deduped {

// Advisory whole-file lock acquired via flock(LOCK_EX | LOCK_NB).
//
// Kernel-managed: the lock is released automatically when the holding process
// exits (even on SIGKILL or crash), so a stale lock file never blocks a
// restart. The lock file itself is created on construction and removed on
// destruction (best effort) when the lock was held.
class LockFile
{
public:
	// Throws std::runtime_error if the lock is already held by another process,
	// or if the file cannot be opened.
	explicit LockFile(std::filesystem::path path);
	~LockFile();

	LockFile(const LockFile&) = delete;
	LockFile& operator=(const LockFile&) = delete;
	LockFile(LockFile&&) = delete;
	LockFile& operator=(LockFile&&) = delete;

private:
	std::filesystem::path path_;
	int fd_{-1};
};

} // namespace deduped
