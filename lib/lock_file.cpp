#include "lock_file.hpp"

#include <cerrno>
#include <fcntl.h>
#include <format>
#include <stdexcept>
#include <sys/file.h>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace deduped {

LockFile::LockFile(std::filesystem::path path) : path_(std::move(path))
{
	fd_ = ::open(path_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
	if (fd_ == -1) {
		const auto ec = std::error_code(errno, std::generic_category());
		throw std::runtime_error(std::format("Cannot open lock file {}: {}", path_.string(), ec.message()));
	}

	if (::flock(fd_, LOCK_EX | LOCK_NB) == -1) {
		const int saved_errno = errno;
		::close(fd_);
		fd_ = -1;
		if (saved_errno == EWOULDBLOCK) {
			throw std::runtime_error(std::format("Lock already held by another process: {}", path_.string()));
		}
		const auto ec = std::error_code(saved_errno, std::generic_category());
		throw std::runtime_error(std::format("flock failed on {}: {}", path_.string(), ec.message()));
	}
}

LockFile::~LockFile()
{
	if (fd_ == -1) {
		return;
	}
	// Best-effort cleanup of the on-disk file before releasing the lock so a
	// crashing process leaves the empty file behind harmlessly. The kernel will
	// release the flock on close anyway.
	std::error_code ec;
	std::filesystem::remove(path_, ec);
	::close(fd_);
}

} // namespace deduped
