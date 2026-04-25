#include "hasher.hpp"

#include <array>
#include <blake3.h>
#include <cerrno>
#include <cstdio>
#include <memory>
#include <stdexcept>

namespace deduped {

namespace {
constexpr std::size_t kChunkBytes = 1024UL * 1024UL; // 1 MiB

struct FileCloser
{
	void operator()(std::FILE* file) const noexcept
	{
		if (file != nullptr) {
			std::fclose(file);
		}
	}
};
} // namespace

Digest hash_file(const std::filesystem::path& path)
{
	// Open in binary mode.
	std::unique_ptr<std::FILE, FileCloser> file{std::fopen(path.c_str(), "rb")};
	if (!file) {
		throw std::system_error(errno, std::generic_category(), "hash_file: fopen(" + path.string() + ')');
	}

	blake3_hasher hasher;
	blake3_hasher_init(&hasher);

	std::array<std::uint8_t, kChunkBytes> buf;
	std::size_t n = 0;
	while ((n = std::fread(buf.data(), 1, buf.size(), file.get())) > 0) {
		blake3_hasher_update(&hasher, buf.data(), n);
	}

	if (std::ferror(file.get())) {
		throw std::system_error(errno, std::generic_category(), "hash_file: fread(" + path.string() + ')');
	}

	Digest digest;
	blake3_hasher_finalize(&hasher, digest.data(), digest.size());
	return digest;
}

} // namespace deduped
