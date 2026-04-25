#include "scanner.hpp"

#include <spdlog/spdlog.h>

namespace deduped {

namespace {

void log_unreadable_subtree(const std::filesystem::path& path, const std::error_code& ec)
{
	spdlog::warn("Ignoring unreadable subtree {}: {}", path.string(), ec.message());
}

void scan_root(const std::filesystem::path& root, const std::function<void(const std::filesystem::path&)>& visitor)
{
	namespace fs = std::filesystem;

	std::error_code ec;
	fs::directory_iterator root_probe{root, ec};
	if (ec) {
		throw fs::filesystem_error("scan root is unreadable", root, ec);
	}

	fs::recursive_directory_iterator it{root, fs::directory_options::skip_permission_denied, ec};
	if (ec) {
		throw fs::filesystem_error("scan root is unreadable", root, ec);
	}

	const auto end = fs::recursive_directory_iterator{};
	while (it != end) {
		const auto entry = *it;

		std::error_code entry_ec;
		const bool is_symlink = entry.is_symlink(entry_ec);
		if (entry_ec) {
			log_unreadable_subtree(entry.path(), entry_ec);
		} else if (is_symlink) {
			if (entry.is_directory(entry_ec) && !entry_ec) {
				it.disable_recursion_pending();
			}
		} else if (entry.is_regular_file(entry_ec) && !entry_ec) {
			const auto canonical = fs::canonical(entry.path(), entry_ec);
			if (entry_ec) {
				log_unreadable_subtree(entry.path(), entry_ec);
			} else {
				visitor(canonical);
			}
		} else if (entry_ec) {
			log_unreadable_subtree(entry.path(), entry_ec);
		}

		it.increment(ec);
		if (ec) {
			log_unreadable_subtree(entry.path(), ec);
			ec.clear();
		}
	}
}

} // namespace

void scan_files(const ScanOptions& opts, const std::function<void(const std::filesystem::path&)>& visitor)
{
	for (const auto& root : opts.roots) {
		scan_root(root, visitor);
	}
}

} // namespace deduped
