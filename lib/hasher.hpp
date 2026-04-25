#pragma once

#include "types.hpp"

#include <filesystem>

namespace deduped {

// Compute the BLAKE3 hash of the file at `path`.
// Reads the file in chunks to bound memory use regardless of file size.
// Throws std::system_error on I/O failure.
[[nodiscard]] Digest hash_file(const std::filesystem::path& path);

} // namespace deduped
