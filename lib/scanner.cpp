#include "scanner.hpp"

#include "fs_walk.hpp"

#include <spdlog/spdlog.h>

namespace deduped {

void scan_files(const ScanOptions& opts, const std::function<void(const std::filesystem::path&)>& visitor)
{
	namespace fs = std::filesystem;

	for (const auto& root : opts.roots) {
		for_each_descendant(root, [&](const fs::directory_entry& entry) {
			std::error_code ec;
			if (!entry.is_regular_file(ec) || ec) {
				if (ec) {
					spdlog::warn("Ignoring unreadable subtree {}: {}", entry.path().string(), ec.message());
				}
				return;
			}
			const auto canonical = fs::canonical(entry.path(), ec);
			if (ec) {
				spdlog::warn("Ignoring unreadable subtree {}: {}", entry.path().string(), ec.message());
				return;
			}
			visitor(canonical);
		});
	}
}

} // namespace deduped
