#include "fs_walk.hpp"

#include <spdlog/spdlog.h>

namespace deduped {

namespace {

void log_unreadable_subtree(const std::filesystem::path& path, const std::error_code& ec)
{
	spdlog::warn("Ignoring unreadable subtree {}: {}", path.string(), ec.message());
}

} // namespace

void for_each_descendant(const std::filesystem::path& root,
                         const std::function<void(const std::filesystem::directory_entry&)>& visitor)
{
	namespace fs = std::filesystem;

	std::error_code ec;
	fs::directory_iterator probe{root, ec};
	if (ec) {
		throw fs::filesystem_error("path is unreadable", root, ec);
	}

	fs::recursive_directory_iterator it{root, fs::directory_options::skip_permission_denied, ec};
	if (ec) {
		throw fs::filesystem_error("path is unreadable", root, ec);
	}

	const auto end = fs::recursive_directory_iterator{};
	while (it != end) {
		const auto entry = *it;

		std::error_code entry_ec;
		const bool is_symlink = entry.is_symlink(entry_ec);
		if (entry_ec) {
			log_unreadable_subtree(entry.path(), entry_ec);
		} else if (is_symlink) {
			// Don't recurse into symlinked directories.
			if (entry.is_directory(entry_ec) && !entry_ec) {
				it.disable_recursion_pending();
			}
		} else {
			visitor(entry);
		}

		it.increment(ec);
		if (ec) {
			log_unreadable_subtree(entry.path(), ec);
			ec.clear();
		}
	}
}

} // namespace deduped
