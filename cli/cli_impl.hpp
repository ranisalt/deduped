#pragma once

#include <string>
#include <vector>

namespace deduped {

/// Run the CLI deduplication logic with parsed arguments.
/// Returns 0 on success, non-zero on failure.
int run_cli_impl(const std::string& db_dir, const std::vector<std::string>& roots, const std::string& log_level,
                 bool apply_flag);

} // namespace deduped
