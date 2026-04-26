#pragma once

#include "types.hpp"

#include <filesystem>
#include <functional>
#include <stdexcept>

namespace deduped {

using HashShouldAbortFn = std::function<bool()>;

class HashInterrupted : public std::runtime_error
{
public:
	HashInterrupted() : std::runtime_error("hash interrupted") {}
};

// Compute the BLAKE3 hash of the file at `path`.
// Reads the file in chunks to bound memory use regardless of file size.
// Throws std::system_error on I/O failure.
[[nodiscard]] Digest hash_file(const std::filesystem::path& path, const HashShouldAbortFn& should_abort = {});

} // namespace deduped
