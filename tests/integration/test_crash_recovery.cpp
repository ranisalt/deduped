#include "../../lib/engine.hpp"
#include "../../lib/repository.hpp"
#include "../../lib/types.hpp"
#include "../helpers/temp_dir.hpp"

#include <fstream>
#include <gtest/gtest.h>

using namespace deduped;
using namespace deduped::test;

namespace fs = std::filesystem;

TEST(CrashRecovery, UnrecoverablePlannedOperationIsMarkedFailedOnRecovery)
{
	TempDir td;
	const auto db = td.path() / "crash.db";
	const auto canonical = td.write_file("canonical.txt", "crash content");
	const auto duplicate = td.write_file("duplicate.txt", "crash content");

	{
		Repository repo{db};
		const auto op_id = repo.log_op_planned(canonical.string(), duplicate.string());
		(void)op_id;
	}

	Repository repo{db};
	std::vector<ApplyResult> recovered;
	EngineCallbacks cbs;
	cbs.on_apply = [&](const ApplyResult& r) { recovered.push_back(r); };
	recover_pending_operations(repo, cbs);

	ASSERT_EQ(recovered.size(), 1u);
	EXPECT_EQ(recovered[0].status, ApplyStatus::Failed);
	EXPECT_EQ(recovered[0].pair.canonical_path, canonical);
	EXPECT_EQ(recovered[0].pair.duplicate_path, duplicate);
	EXPECT_EQ(recovered[0].message, "interrupted apply could not be automatically recovered");

	ASSERT_TRUE(repo.list_ops(Repository::OpStatus::Planned).empty());
	const auto failed_ops = repo.list_ops(Repository::OpStatus::Failed);
	ASSERT_EQ(failed_ops.size(), 1u);
	EXPECT_EQ(failed_ops[0].canonical_path, canonical);
	EXPECT_EQ(failed_ops[0].duplicate_path, duplicate);
	EXPECT_EQ(failed_ops[0].message, "interrupted apply could not be automatically recovered");
	EXPECT_TRUE(fs::exists(canonical));
	EXPECT_TRUE(fs::exists(duplicate));
	EXPECT_NE(meta_from_path(canonical).inode, meta_from_path(duplicate).inode);
}

TEST(CrashRecovery, RestoresRenamedDuplicateAfterInterruptedApply)
{
	TempDir td;
	const auto db = td.path() / "recover-restore.db";
	const auto canonical = td.write_file("canonical.txt", "same");
	const auto duplicate = td.write_file("duplicate.txt", "same");
	const auto backup = td.path() / "duplicate.txt.deduped_tmp";

	Repository repo{db};
	const auto op_id = repo.log_op_planned(canonical.string(), duplicate.string(), backup.string());
	(void)op_id;
	fs::rename(duplicate, backup);

	std::vector<ApplyResult> recovered;
	EngineCallbacks cbs;
	cbs.on_apply = [&](const ApplyResult& r) { recovered.push_back(r); };
	recover_pending_operations(repo, cbs);

	ASSERT_EQ(recovered.size(), 1u);
	EXPECT_EQ(recovered[0].status, ApplyStatus::Failed);
	EXPECT_EQ(recovered[0].message, "recovered original after interrupted apply");
	EXPECT_TRUE(fs::exists(duplicate));
	EXPECT_FALSE(fs::exists(backup));
	EXPECT_NE(meta_from_path(canonical).inode, meta_from_path(duplicate).inode);
	EXPECT_TRUE(repo.list_ops(Repository::OpStatus::Planned).empty());
	ASSERT_EQ(repo.list_ops(Repository::OpStatus::Failed).size(), 1u);
}

TEST(CrashRecovery, CleansBackupAfterInterruptedSuccessfulLink)
{
	TempDir td;
	const auto db = td.path() / "recover-cleanup.db";
	const auto canonical = td.write_file("canonical.txt", "same");
	const auto duplicate = td.write_file("duplicate.txt", "same");
	const auto backup = td.path() / "duplicate.txt.deduped_tmp";

	Repository repo{db};
	const auto op_id = repo.log_op_planned(canonical.string(), duplicate.string(), backup.string());
	(void)op_id;
	fs::rename(duplicate, backup);
	fs::create_hard_link(canonical, duplicate);

	std::vector<ApplyResult> recovered;
	EngineCallbacks cbs;
	cbs.on_apply = [&](const ApplyResult& r) { recovered.push_back(r); };
	recover_pending_operations(repo, cbs);

	ASSERT_EQ(recovered.size(), 1u);
	EXPECT_EQ(recovered[0].status, ApplyStatus::Linked);
	EXPECT_TRUE(fs::exists(duplicate));
	EXPECT_FALSE(fs::exists(backup));
	EXPECT_EQ(meta_from_path(canonical).inode, meta_from_path(duplicate).inode);
	EXPECT_TRUE(repo.list_ops(Repository::OpStatus::Planned).empty());
	EXPECT_EQ(repo.list_ops(Repository::OpStatus::Done).size(), 1u);
}

// If the canonical file was deleted between the crash and the recovery run,
// the planned operation cannot be completed and must be marked Failed.
TEST(CrashRecovery, MarksFailedWhenCanonicalDeletedBeforeRecovery)
{
	TempDir td;
	const auto db = td.path() / "recover-no-canonical.db";
	const auto canonical = td.write_file("canonical.txt", "same");
	const auto duplicate = td.write_file("duplicate.txt", "same");

	{
		Repository repo{db};
		const auto op_id = repo.log_op_planned(canonical.string(), duplicate.string());
		(void)op_id;
		// Simulate canonical disappearing before recovery runs.
		fs::remove(canonical);
	}

	Repository repo{db};
	std::vector<ApplyResult> recovered;
	EngineCallbacks cbs;
	cbs.on_apply = [&](const ApplyResult& r) { recovered.push_back(r); };
	recover_pending_operations(repo, cbs);

	ASSERT_EQ(recovered.size(), 1u);
	EXPECT_EQ(recovered[0].status, ApplyStatus::Failed);
	EXPECT_FALSE(fs::exists(canonical));
	EXPECT_TRUE(fs::exists(duplicate));
	EXPECT_TRUE(repo.list_ops(Repository::OpStatus::Planned).empty());
	ASSERT_EQ(repo.list_ops(Repository::OpStatus::Failed).size(), 1u);
}
