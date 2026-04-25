#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <string>

namespace deduped {

constexpr std::size_t kDigestBytes = 32;
using Digest = std::array<std::uint8_t, kDigestBytes>;

// Hex-encode a digest for display / storage.
std::string digest_to_hex(const Digest& d);

struct FileMeta
{
	std::uint64_t size{};    // bytes
	std::int64_t mtime_ns{}; // modification time (nanoseconds since epoch)
	std::uint64_t inode{};
	std::uint64_t device{};
	std::uint32_t mode{}; // permission bits + type
	std::uint32_t uid{};
	std::uint32_t gid{};
};

// Returns true when any field that would invalidate a stored hash has changed.
[[nodiscard]] bool is_meta_stale(const FileMeta& stored, const FileMeta& current) noexcept;

// Populate FileMeta from an existing directory entry / stat result.
[[nodiscard]] FileMeta meta_from_path(const std::filesystem::path& p);

struct IndexEntry
{
	std::int64_t id{}; // SQLite rowid
	std::string path;  // canonical absolute path
	FileMeta meta;
	Digest digest;
	std::int64_t last_seen{}; // Unix seconds of the most recent scan that confirmed this file
};

struct DupePair
{
	std::string canonical_path; // file to keep (first seen / lower path)
	std::string duplicate_path; // file that can be replaced with a hardlink
	Digest digest;
	std::uint64_t size_bytes{};
};

enum class ApplyStatus
{
	Linked,        // hardlink successfully created
	AlreadyLinked, // inode was already shared, no change needed
	Skipped,       // cross-device, symlink, permission issue, not an error
	Failed,        // unexpected error (stored in message)
};

struct ApplyResult
{
	DupePair pair;
	ApplyStatus status{ApplyStatus::Skipped};
	std::string message; // populated on Skipped / Failed
};

} // namespace deduped
