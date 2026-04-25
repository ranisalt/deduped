#include "watcher.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <sys/inotify.h>
#include <system_error>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace deduped {

struct Watcher::Impl
{
	std::vector<std::filesystem::path> roots;
	Callback cb;
	int inotify_fd{-1};
	int pipe_rd{-1};
	int pipe_wr{-1};

	// Maps inotify watch descriptor to directory path.
	std::unordered_map<int, std::filesystem::path> wd_to_dir;

	// Events we care about.
	static constexpr uint32_t kWatchMask =
	    IN_CLOSE_WRITE | IN_CREATE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO | IN_DELETE_SELF | IN_MOVE_SELF;

	explicit Impl(std::vector<std::filesystem::path> roots_arg, Callback c) :
	    roots(std::move(roots_arg)), cb(std::move(c))
	{
		inotify_fd = inotify_init1(IN_CLOEXEC | IN_NONBLOCK);
		if (inotify_fd < 0) throw std::system_error(errno, std::generic_category(), "inotify_init1");

		int pipefd[2];
		if (pipe2(pipefd, O_CLOEXEC) < 0) {
			::close(inotify_fd);
			throw std::system_error(errno, std::generic_category(), "pipe2");
		}
		pipe_rd = pipefd[0];
		pipe_wr = pipefd[1];

		for (const auto& root : roots) {
			add_watch_recursive(root, true);
		}
	}

	~Impl()
	{
		if (inotify_fd >= 0) ::close(inotify_fd);
		if (pipe_rd >= 0) ::close(pipe_rd);
		if (pipe_wr >= 0) ::close(pipe_wr);
	}

	void add_watch(const std::filesystem::path& dir, const bool required)
	{
		const int wd = inotify_add_watch(inotify_fd, dir.c_str(), kWatchMask);
		if (wd < 0) {
			if (required) {
				throw std::system_error(errno, std::generic_category(), "inotify_add_watch");
			}
			spdlog::warn("Ignoring unreadable subtree while watching {}: {}", dir.string(), std::strerror(errno));
			return;
		}
		wd_to_dir[wd] = dir;
	}

	void add_watch_recursive(const std::filesystem::path& dir, const bool required_root)
	{
		namespace fs = std::filesystem;
		std::error_code ec;
		if (required_root) {
			fs::directory_iterator root_probe{dir, ec};
			if (ec) {
				throw fs::filesystem_error("watch root is unreadable", dir, ec);
			}
		}

		add_watch(dir, required_root);

		fs::recursive_directory_iterator it{dir, fs::directory_options::skip_permission_denied, ec};
		if (ec) {
			if (required_root) {
				throw fs::filesystem_error("watch root is unreadable", dir, ec);
			}
			spdlog::warn("Ignoring unreadable subtree while watching {}: {}", dir.string(), ec.message());
			return;
		}

		const auto end = fs::recursive_directory_iterator{};
		while (it != end) {
			const auto entry = *it;
			std::error_code entry_ec;
			const bool is_symlink = entry.is_symlink(entry_ec);
			if (!entry_ec && is_symlink) {
				if (entry.is_directory(entry_ec) && !entry_ec) {
					it.disable_recursion_pending();
				}
			} else if (!entry_ec && entry.is_directory(entry_ec) && !entry_ec) {
				add_watch(entry.path(), false);
			} else if (entry_ec) {
				spdlog::warn("Ignoring unreadable subtree while watching {}: {}", entry.path().string(),
				             entry_ec.message());
			}

			it.increment(ec);
			if (ec) {
				spdlog::warn("Ignoring unreadable subtree while watching {}: {}", entry.path().string(), ec.message());
				ec.clear();
			}
		}
	}

	void run()
	{
		// Buffer large enough for several events.
		constexpr std::size_t kBufLen = 64 * (sizeof(inotify_event) + NAME_MAX + 1);
		std::vector<char> buf(kBufLen);

		while (true) {
			struct pollfd fds[2];
			fds[0] = {inotify_fd, POLLIN, 0};
			fds[1] = {pipe_rd, POLLIN, 0};

			const int n = poll(fds, 2, -1);
			if (n < 0) {
				if (errno == EINTR) continue;
				throw std::system_error(errno, std::generic_category(), "poll");
			}

			// Stop pipe signalled.
			if (fds[1].revents & POLLIN) break;

			if (!(fds[0].revents & POLLIN)) continue;

			// Drain all queued inotify events.
			while (true) {
				const ssize_t len = ::read(inotify_fd, buf.data(), buf.size());
				if (len < 0) {
					if (errno == EAGAIN || errno == EWOULDBLOCK) break;
					if (errno == EINTR) continue;
					throw std::system_error(errno, std::generic_category(), "inotify read");
				}
				if (len == 0) break;

				for (const char* p = buf.data(); p < buf.data() + len;) {
					// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
					const auto* ev = reinterpret_cast<const inotify_event*>(p);
					handle_event(*ev);
					p += sizeof(inotify_event) + ev->len;
				}
			}
		}
	}

	void handle_event(const inotify_event& ev)
	{
		namespace fs = std::filesystem;

		auto it = wd_to_dir.find(ev.wd);
		if (it == wd_to_dir.end()) return; // stale descriptor

		const auto& dir = it->second;
		const bool is_dir = (ev.mask & IN_ISDIR) != 0;

		// Directory-self events: remove from map (kernel removes the wd).
		if (ev.mask & (IN_DELETE_SELF | IN_MOVE_SELF)) {
			wd_to_dir.erase(it);
			return;
		}

		// Need a name for all other events.
		if (ev.len == 0) return;
		const auto path = dir / ev.name;

		if (is_dir) {
			// New subdirectory: watch it (and any files already inside it).
			if (ev.mask & (IN_CREATE | IN_MOVED_TO)) {
				add_watch_recursive(path, false);
			}
			// Deleted subdirectory: kernel auto-removes the wd; our map will
			// become stale, which is harmless (the guard above handles it).
			return;
		}

		// Regular-file events.
		if (ev.mask & (IN_CLOSE_WRITE | IN_MOVED_TO)) {
			// File written to completion or moved in.
			std::error_code ec;
			if (fs::is_regular_file(path, ec)) cb({path, FileEvent::Modified});
		} else if (ev.mask & (IN_DELETE | IN_MOVED_FROM)) {
			cb({path, FileEvent::Deleted});
		}
	}
};

Watcher::Watcher(std::filesystem::path root, Callback cb) :
    Watcher(std::vector<std::filesystem::path>{std::move(root)}, std::move(cb))
{}

Watcher::Watcher(std::vector<std::filesystem::path> roots, Callback cb) :
    impl_(std::make_unique<Impl>(std::move(roots), std::move(cb)))
{}

Watcher::~Watcher() = default;

void Watcher::run() { impl_->run(); }
void Watcher::stop() noexcept
{
	const char byte = 1;
	static_cast<void>(::write(impl_->pipe_wr, &byte, 1));
}

} // namespace deduped
