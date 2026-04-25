#pragma once

#include <utility>

namespace deduped::test {

template <class T>
class scope_exit
{
public:
	explicit scope_exit(T&& func) : func_{std::move(func)} {}
	~scope_exit() { func_(); }

	scope_exit(const scope_exit&) = delete;
	scope_exit& operator=(const scope_exit&) = delete;

private:
	T func_;
};

} // namespace deduped::test
