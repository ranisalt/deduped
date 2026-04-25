#include "types.hpp"

#include <cerrno>
#include <format>
#include <stdexcept>
#include <sys/stat.h>

namespace deduped {

std::string digest_to_hex(const Digest& d)
{
	static constexpr char kHex[] = "0123456789abcdef";
	std::string out;
	out.reserve(kDigestBytes * 2);
	for (std::uint8_t b : d) {
		out += kHex[b >> 4];
		out += kHex[b & 0xf];
	}
	return out;
}

bool is_meta_stale(const FileMeta& stored, const FileMeta& current) noexcept
{
	return stored.size != current.size || stored.mtime_ns != current.mtime_ns || stored.inode != current.inode ||
	       stored.device != current.device || stored.mode != current.mode || stored.uid != current.uid ||
	       stored.gid != current.gid;
}

FileMeta meta_from_path(const std::filesystem::path& p)
{
	struct stat st{};
	if (::stat(p.c_str(), &st) != 0) {
		throw std::system_error(errno, std::generic_category(), std::format("stat({})", p.string()));
	}

	FileMeta m;
	m.size = static_cast<std::uint64_t>(st.st_size);
#if defined(__APPLE__)
	m.mtime_ns = st.st_mtimespec.tv_sec * 1'000'000'000LL + st.st_mtimespec.tv_nsec;
#else
	m.mtime_ns = st.st_mtim.tv_sec * 1'000'000'000LL + st.st_mtim.tv_nsec;
#endif
	m.inode = static_cast<std::uint64_t>(st.st_ino);
	m.device = static_cast<std::uint64_t>(st.st_dev);
	m.mode = static_cast<std::uint32_t>(st.st_mode);
	m.uid = static_cast<std::uint32_t>(st.st_uid);
	m.gid = static_cast<std::uint32_t>(st.st_gid);
	return m;
}

} // namespace deduped
