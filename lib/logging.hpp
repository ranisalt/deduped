#pragma once

#include <algorithm>
#include <cctype>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>

namespace deduped {

// Parse and apply a log level string. Returns false and logs an error if the
// string is non-empty but not a recognised spdlog level name.
[[nodiscard]] inline bool configure_log_level(std::string_view log_level_arg)
{
	// Flush every info-or-above record so that long-running services do not
	// lose context if they exit abruptly (silent crashes, signals).
	spdlog::flush_on(spdlog::level::info);

	if (log_level_arg.empty()) {
		return true;
	}

	std::string lower{log_level_arg};
	std::ranges::transform(lower, lower.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

	const auto parsed = spdlog::level::from_str(lower);
	if (parsed == spdlog::level::off && lower != "off") {
		spdlog::error("Invalid log level '{}'. Valid values: trace, debug, info, warn, error, critical, off", lower);
		return false;
	}
	spdlog::set_level(parsed);
	return true;
}

} // namespace deduped
