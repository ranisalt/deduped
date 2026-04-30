#include "watcher.hpp"

#include "fs_walk.hpp"

#include <algorithm>
#include <atomic>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <cerrno>
#include <cstring>
#include <exception>
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
	boost::asio::io_context io_context;
	boost::asio::posix::stream_descriptor inotify_stream;
	std::atomic<bool> stop_requested{false};
	std::exception_ptr async_error;

	// Maps inotify watch descriptor to directory path.
	std::unordered_map<int, std::filesystem::path> wd_to_dir;

	// Events we care about.
	static constexpr uint32_t kWatchMask =
	    IN_CLOSE_WRITE | IN_CREATE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO | IN_DELETE_SELF | IN_MOVE_SELF;

	explicit Impl(std::vector<std::filesystem::path> roots_arg, Callback c) :
	    roots(std::move(roots_arg)), cb(std::move(c)), inotify_stream(io_context)
	{
		const int inotify_fd = inotify_init1(IN_CLOEXEC | IN_NONBLOCK);
		if (inotify_fd < 0) throw std::system_error(errno, std::generic_category(), "inotify_init1");
		inotify_stream.assign(inotify_fd);

		for (const auto& root : roots) {
			add_watch_recursive(root, true);
		}

		arm_inotify_wait();
	}

	~Impl() noexcept { stop(); }

	void add_watch(const std::filesystem::path& dir, const bool required)
	{
		const int wd = inotify_add_watch(inotify_stream.native_handle(), dir.c_str(), kWatchMask);
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

		if (required_root) {
			std::error_code ec;
			fs::directory_iterator probe{dir, ec};
			if (ec) {
				throw fs::filesystem_error("watch root is unreadable", dir, ec);
			}
		}

		add_watch(dir, required_root);

		try {
			for_each_descendant(dir, [&](const fs::directory_entry& entry) {
				std::error_code ec;
				if (entry.is_directory(ec) && !ec) {
					add_watch(entry.path(), false);
				}
			});
		} catch (const fs::filesystem_error&) {
			if (required_root) {
				throw;
			}
		}
	}

	void arm_inotify_wait()
	{
		if (stop_requested.load()) {
			return;
		}

		inotify_stream.async_wait(
		    boost::asio::posix::stream_descriptor::wait_read, [this](const boost::system::error_code& ec) {
			    if (ec) {
				    if (ec == boost::asio::error::operation_aborted && stop_requested.load()) {
					    return;
				    }
				    async_error = std::make_exception_ptr(
				        std::system_error(std::error_code(ec.value(), std::system_category()), "inotify wait"));
				    io_context.stop();
				    return;
			    }

			    drain_inotify_events();
			    if (async_error || stop_requested.load()) {
				    return;
			    }

			    arm_inotify_wait();
		    });
	}

	void drain_inotify_events()
	{
		constexpr std::size_t kBufLen = 64 * (sizeof(inotify_event) + NAME_MAX + 1);
		std::vector<char> buf(kBufLen);

		while (true) {
			const ssize_t len = ::read(inotify_stream.native_handle(), buf.data(), buf.size());
			if (len < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) break;
				if (errno == EINTR) continue;
				async_error =
				    std::make_exception_ptr(std::system_error(errno, std::generic_category(), "inotify read"));
				io_context.stop();
				return;
			}
			if (len == 0) {
				break;
			}

			for (const char* p = buf.data(); p < buf.data() + len;) {
				// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
				const auto* ev = reinterpret_cast<const inotify_event*>(p);
				handle_event(*ev);
				p += sizeof(inotify_event) + ev->len;
			}
		}
	}

	void run()
	{
		if (stop_requested.load()) {
			return;
		}

		io_context.run();
		if (async_error) {
			std::rethrow_exception(async_error);
		}
	}

	void stop() noexcept
	{
		stop_requested = true;
		boost::system::error_code ec;
		inotify_stream.cancel(ec);
		io_context.stop();
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
void Watcher::stop() noexcept { impl_->stop(); }

} // namespace deduped
