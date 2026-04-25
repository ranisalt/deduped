#include "cli_impl.hpp"

#include <CLI/CLI.hpp>
#include <string>
#include <vector>

int main(int argc, char** argv)
{
	CLI::App app{"deduped-cli, file deduplication tool"};
	app.set_version_flag("--version", "0.1.0");

	std::string db_dir{"."};
	app.add_option("--db", db_dir, "Directory where deduped.db is stored (default: current directory)");

	std::vector<std::string> roots_arg;
	app.add_option("roots", roots_arg, "Directories to scan")->required();

	std::string log_level_arg;
	app.add_option("--log-level", log_level_arg, "Set spdlog level (trace|debug|info|warn|error|critical|off).");

	bool apply_flag{false};
	app.add_flag("--apply", apply_flag, "Apply deduplication (create hardlinks). Default is dry-run/report-only.");

	CLI11_PARSE(app, argc, argv);

	return deduped::run_cli_impl(db_dir, roots_arg, log_level_arg, apply_flag);
}
