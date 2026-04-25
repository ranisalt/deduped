#pragma once

#include <string>
#include <vector>

namespace deduped {

/// Run the daemon deduplication logic with parsed arguments.
/// Returns 0 on success, non-zero on failure.
int run_daemon_impl(const std::string& config_dir, const std::vector<std::string>& data_dirs,
                    const std::string& log_level, bool apply_flag);

/// Initialize the daemon without running the watcher.
/// This is exposed for testing purposes to verify initialization, recovery, and reconciliation
/// without blocking on the watcher event loop.
/// Returns 0 on success, non-zero on failure.
int init_daemon_without_watcher(const std::string& config_dir, const std::vector<std::string>& data_dirs,
                                 const std::string& log_level, bool apply_flag);
} // namespace deduped
