#pragma once

#include <utility>

namespace deduped {

// Executes a callable when the scope_exit object is destroyed.
// Useful as a lightweight RAII guard without writing a dedicated struct.
template <class Fn>
class scope_exit
{
public:
	explicit scope_exit(Fn&& fn) : fn_{std::move(fn)} {}
	~scope_exit() { fn_(); }

	scope_exit(const scope_exit&) = delete;
	scope_exit& operator=(const scope_exit&) = delete;

private:
	Fn fn_;
};

} // namespace deduped
