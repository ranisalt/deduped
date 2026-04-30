#include "dedup_session.hpp"

#include <spdlog/spdlog.h>
#include <utility>

namespace deduped {

namespace {

// Wrap a user-supplied EngineCallbacks set so that recovery-time apply events
// are logged with the standard "RECOVERED LINKED / RECOVERY FAILED" prefix and
// then forwarded to the user's on_apply if any.
EngineCallbacks decorate_for_recovery(const EngineCallbacks& user)
{
	EngineCallbacks cbs = user;
	auto chain = user.on_apply;
	cbs.on_apply = [chain = std::move(chain)](const ApplyResult& r) {
		if (r.status == ApplyStatus::Linked) {
			spdlog::warn("RECOVERED LINKED {}", r.pair.duplicate_path);
		} else if (r.status == ApplyStatus::Failed) {
			spdlog::warn("RECOVERY FAILED {}: {}", r.pair.duplicate_path, r.message);
		}
		if (chain) {
			chain(r);
		}
	};
	return cbs;
}

} // namespace

DedupSession::DedupSession(Config cfg) : cfg_(std::move(cfg)), repo_(std::make_unique<Repository>(cfg_.db_path)) {}

DedupSession::~DedupSession() = default;

void DedupSession::recover(const EngineCallbacks& cbs)
{
	recover_pending_operations(*repo_, decorate_for_recovery(cbs));
}

std::vector<ApplyResult> DedupSession::run_full_scan(const EngineCallbacks& cbs)
{
	ScanOptions scan_opts;
	scan_opts.roots = cfg_.data_roots;
	return run_engine(*repo_, scan_opts, cfg_.engine_opts, cbs);
}

std::optional<ApplyResult> DedupSession::handle_change(const std::filesystem::path& path, const EngineCallbacks& cbs)
{
	return handle_file_change(*repo_, path, cfg_.engine_opts, cbs);
}

void DedupSession::handle_removed(const std::filesystem::path& path) { handle_file_removed(*repo_, path); }

IRepository& DedupSession::repo() noexcept { return *repo_; }

} // namespace deduped
