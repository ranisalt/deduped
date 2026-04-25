#pragma once

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace deduped::test {

// Creates a uniquely-named temporary directory scoped to the test.
// Destroyed (recursively) in the destructor.
class TempDir
{
public:
	TempDir()
	{
		namespace fs = std::filesystem;
		const auto base = fs::temp_directory_path() / "deduped_test";
		fs::create_directories(base);
		const auto pattern = (base / "XXXXXX").string();
		std::vector<char> tmpl(pattern.begin(), pattern.end());
		tmpl.push_back('\0');
		if (!::mkdtemp(tmpl.data())) {
			throw std::runtime_error("TempDir: mkdtemp failed");
		}
		path_ = tmpl.data();
	}

	~TempDir() { std::filesystem::remove_all(path_); }

	TempDir(const TempDir&) = delete;
	TempDir& operator=(const TempDir&) = delete;

	const std::filesystem::path& path() const noexcept { return path_; }

	// Write `content` to a file at path()/name and return its path.
	std::filesystem::path write_file(const std::string& name, const std::string& content) const
	{
		auto p = path_ / name;
		std::filesystem::create_directories(p.parent_path());
		std::ofstream f(p, std::ios::binary);
		if (!f) throw std::runtime_error("TempDir::write_file: " + p.string());
		f.write(content.data(), static_cast<std::streamsize>(content.size()));
		return p;
	}

private:
	std::filesystem::path path_;
};

} // namespace deduped::test
