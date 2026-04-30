#include "daemon_impl.hpp"

#include <CLI/CLI.hpp>
#include <cstdlib>
#include <exception>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>

int main(int argc, char** argv)
{
	CLI::App app{"deduped, inotify-based deduplication daemon"};
	app.set_version_flag("--version", "0.1.0");

	std::string config_dir;
	app.add_option("--config", config_dir, "Directory where deduped.db is stored")->required();

	std::vector<std::string> data_dirs;
	app.add_option("data", data_dirs, "Directory to watch")->required();

	std::string log_level_arg;
	app.add_option("--log-level", log_level_arg, "Set spdlog level (trace|debug|info|warn|error|critical|off).");

	bool apply_flag{false};
	app.add_flag("--apply", apply_flag, "Apply deduplication (create hardlinks). Default is dry-run/report-only.");

	CLI11_PARSE(app, argc, argv);

	try {
		return deduped::run_daemon_impl(config_dir, data_dirs, log_level_arg, apply_flag);
	} catch (const std::exception& ex) {
		spdlog::critical("Fatal: {}", ex.what());
		spdlog::default_logger()->flush();
		return EXIT_FAILURE;
	} catch (...) {
		spdlog::critical("Fatal: aborted by unknown exception");
		spdlog::default_logger()->flush();
		return EXIT_FAILURE;
	}
}
