#include "../helpers/temp_dir.hpp"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <string_view>
#include <sys/wait.h>

using namespace deduped::test;
namespace fs = std::filesystem;

namespace {

std::string shell_quote(const std::string_view raw)
{
	std::string quoted{"'"};
	for (const char ch : raw) {
		if (ch == '\'') {
			quoted += "'\"'\"'";
		} else {
			quoted += ch;
		}
	}
	quoted += '\'';
	return quoted;
}

std::string read_text(const fs::path& path)
{
	std::ifstream in(path);
	return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void make_executable(const fs::path& path)
{
	fs::permissions(path, fs::perms::owner_all | fs::perms::group_read | fs::perms::group_exec |
	                          fs::perms::others_read | fs::perms::others_exec,
	                fs::perm_options::replace);
}

int system_exit_code(const int raw_exit_code)
{
	if (raw_exit_code != -1 && WIFEXITED(raw_exit_code)) {
		return WEXITSTATUS(raw_exit_code);
	}
	if (raw_exit_code != -1 && WIFSIGNALED(raw_exit_code)) {
		return 128 + WTERMSIG(raw_exit_code);
	}
	return raw_exit_code;
}

int run_docker_entrypoint(const TempDir& td, const std::string_view data_roots,
	                      const std::optional<std::string_view> mode = std::nullopt,
	                      const std::optional<std::string_view> log_level = std::nullopt)
{
	const auto fake_bin = td.path() / "fake-bin";
	fs::create_directories(fake_bin);
	make_executable(td.write_file(
	    "fake-bin/deduped",
	    "#!/usr/bin/env bash\n"
	    "printf '%s\\n' \"$@\" > \"${DEDUPED_TEST_ARGS_FILE}\"\n"));
	make_executable(td.write_file(
	    "fake-bin/sleep",
	    "#!/usr/bin/env bash\n"
	    "printf '%s\\n' \"$@\" > \"${DEDUPED_TEST_SLEEP_ARGS_FILE}\"\n"));

	std::string command =
	    "env PATH=" + shell_quote((fake_bin.string() + ":/usr/bin:/bin")) +
	    " DEDUPED_TEST_ARGS_FILE=" + shell_quote((td.path() / "deduped-args.txt").string()) +
	    " DEDUPED_TEST_SLEEP_ARGS_FILE=" + shell_quote((td.path() / "sleep-args.txt").string()) +
	    " DEDUPED_CONFIG='/config'" +
	    " DEDUPED_DATA=" + shell_quote(data_roots);
	if (mode.has_value()) {
		command += " DEDUPED_MODE=" + shell_quote(*mode);
	}
	if (log_level.has_value()) {
		command += " LOG_LEVEL=" + shell_quote(*log_level);
	}
	command += " bash " + shell_quote(DEDUPED_RUN_SCRIPT_PATH) + " >" +
	           shell_quote((td.path() / "script-output.txt").string()) + " 2>&1";

	return system_exit_code(std::system(command.c_str()));
}

} // namespace

TEST(DockerRunScriptTest, DefaultsToApplyMode)
{
	TempDir td;
	EXPECT_EQ(run_docker_entrypoint(td, "/data,/another", std::nullopt, std::string_view{"info"}), 0);
	EXPECT_EQ(read_text(td.path() / "deduped-args.txt"),
	          "--config\n/config\n--apply\n--log-level\ninfo\n/data\n/another\n");
	EXPECT_TRUE(read_text(td.path() / "sleep-args.txt").empty());
}

TEST(DockerRunScriptTest, DryRunModeOmitsApplyFlag)
{
	TempDir td;
	EXPECT_EQ(run_docker_entrypoint(td, "/data", std::string_view{"dry-run"}), 0);
	EXPECT_EQ(read_text(td.path() / "deduped-args.txt"), "--config\n/config\n/data\n");
	EXPECT_TRUE(read_text(td.path() / "sleep-args.txt").empty());
}

TEST(DockerRunScriptTest, InvalidModeLogsErrorAndSleeps)
{
	TempDir td;
	EXPECT_EQ(run_docker_entrypoint(td, "/data", std::string_view{"banana"}), 0);
	EXPECT_TRUE(read_text(td.path() / "deduped-args.txt").empty());
	EXPECT_EQ(read_text(td.path() / "sleep-args.txt"), "infinity\n");
	EXPECT_NE(read_text(td.path() / "script-output.txt").find("DEDUPED_MODE must be 'apply' or 'dry-run'. Sleeping."),
	          std::string::npos);
}
