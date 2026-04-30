#pragma once

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <functional>
#include <thread>

namespace deduped {

// Cross-binary SIGINT/SIGTERM watcher. Runs an internal io_context on a jthread
// that fires `on_signal` once when SIGINT or SIGTERM is received and flips
// `termination_requested()` to true. Safe to ignore the callback and just poll
// the flag (cli pattern) or pass a callback for prompt shutdown (daemon pattern).
class SignalWatcher
{
public:
	using Handler = std::function<void()>;

	explicit SignalWatcher(Handler on_signal = {});
	~SignalWatcher();

	SignalWatcher(const SignalWatcher&) = delete;
	SignalWatcher& operator=(const SignalWatcher&) = delete;
	SignalWatcher(SignalWatcher&&) = delete;
	SignalWatcher& operator=(SignalWatcher&&) = delete;

	[[nodiscard]] bool termination_requested() const noexcept { return termination_requested_.load(); }

private:
	std::atomic<bool> termination_requested_{false};
	boost::asio::io_context io_context_;
	boost::asio::signal_set signals_;
	Handler on_signal_;
	std::jthread thread_;
};

} // namespace deduped
