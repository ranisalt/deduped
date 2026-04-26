#include "hasher.hpp"

#include <array>
#include <blake3.h>
#include <cerrno>
#include <fstream>
#include <stdexcept>

namespace deduped {

namespace {
constexpr std::size_t kChunkBytes = 1024UL * 1024UL; // 1 MiB

void throw_if_aborted(const HashShouldAbortFn& should_abort)
{
	if (should_abort && should_abort()) {
		throw HashInterrupted{};
	}
}
} // namespace

Digest hash_file(const std::filesystem::path& path, const HashShouldAbortFn& should_abort)
{
	throw_if_aborted(should_abort);

	std::ifstream file{path, std::ios::binary};
	if (!file) {
		throw std::system_error(errno, std::generic_category(), "hash_file: open(" + path.string() + ')');
	}

	blake3_hasher hasher;
	blake3_hasher_init(&hasher);

	std::array<char, kChunkBytes> buf;
	while (file.read(buf.data(), static_cast<std::streamsize>(buf.size())) || file.gcount() > 0) {
		blake3_hasher_update(&hasher, buf.data(), static_cast<std::size_t>(file.gcount()));
		throw_if_aborted(should_abort);
	}

	if (file.bad()) {
		throw std::runtime_error("hash_file: read error on " + path.string());
	}

	throw_if_aborted(should_abort);

	Digest digest;
	blake3_hasher_finalize(&hasher, digest.data(), digest.size());
	return digest;
}

} // namespace deduped
