#pragma once

#include "types.hpp"

#include <filesystem>
#include <functional>
#include <vector>

namespace deduped {

struct ScanOptions
{
	// Root directories to recurse into.
	std::vector<std::filesystem::path> roots;

	// Symlinks are never followed, they are always skipped.
	// Cross-device files under the same root are hashed and indexed but
	// cannot be hardlinked; the engine skips them during apply.
};

// Enumerate all regular files reachable from opts.roots (non-recursive
// symlinks skipped) and call visitor for each canonical path.
// visitor(path) is called in directory-traversal order.
// Throws std::filesystem::filesystem_error when a root itself is unreadable.
// Unreadable subtrees are logged and skipped.
void scan_files(const ScanOptions& opts, const std::function<void(const std::filesystem::path&)>& visitor);

} // namespace deduped
