#include "signal_watcher.hpp"

#include <csignal>
#include <utility>

namespace deduped {

SignalWatcher::SignalWatcher(Handler on_signal)
    : signals_(io_context_, SIGINT, SIGTERM), on_signal_(std::move(on_signal))
{
	signals_.async_wait([this](const boost::system::error_code& ec, int) {
		if (!ec) {
			termination_requested_.store(true);
			if (on_signal_) {
				on_signal_();
			}
		}
		io_context_.stop();
	});

	thread_ = std::jthread([this](std::stop_token) { io_context_.run(); });
}

SignalWatcher::~SignalWatcher()
{
	boost::system::error_code ec;
	signals_.cancel(ec);
	io_context_.stop();
}

} // namespace deduped
