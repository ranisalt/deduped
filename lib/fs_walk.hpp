#pragma once

#include <filesystem>
#include <functional>

namespace deduped {

// Recursively iterates `root`, skipping symlink subtrees and unreadable
// directories (which are logged as warnings). Calls `visitor(entry)` for each
// non-symlink descendant. Throws std::filesystem::filesystem_error if `root`
// itself is unreadable.
void for_each_descendant(const std::filesystem::path& root,
                         const std::function<void(const std::filesystem::directory_entry&)>& visitor);

} // namespace deduped
