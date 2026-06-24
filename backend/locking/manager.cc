//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "backend/locking/manager.h"

#include <functional>
#include <map>
#include <memory>

#include "absl/memory/memory.h"
#include "absl/random/random.h"
#include "absl/random/uniform_int_distribution.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "common/config.h"
#include "common/errors.h"
#include "googlesql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

std::unique_ptr<LockHandle> LockManager::CreateHandle(
    TransactionID tid, const std::function<absl::Status()>& abort_fn,
    TransactionPriority priority) {
  return absl::WrapUnique(new LockHandle(this, tid, abort_fn, priority));
}

void LockManager::ReleaseLocksForHandleLocked(LockHandle* handle) {
  if (database_lock_holder_ != nullptr &&
      database_lock_holder_->tid() == handle->tid()) {
    database_lock_holder_ = nullptr;
  }
  for (auto it = table_lock_holders_.begin();
       it != table_lock_holders_.end();) {
    if (it->second->tid() == handle->tid()) {
      it = table_lock_holders_.erase(it);
    } else {
      ++it;
    }
  }
  if (pending_commit_handle_ != nullptr &&
      pending_commit_handle_->tid() == handle->tid()) {
    pending_commit_handle_ = nullptr;
  }
}

bool LockManager::TryAbortHolderLocked(LockHandle* holder,
                                       LockHandle* requester) {
  absl::BitGen gen;
  if (absl::uniform_int_distribution<int>(1, 100)(gen) >
      config::abort_current_transaction_probability()) {
    return false;
  }
  auto could_be_aborted = holder->TryAbortTransaction(
      error::AbortCurrentTransaction(holder->tid(), requester->tid()));
  if (!could_be_aborted.ok()) {
    return false;
  }
  ReleaseLocksForHandleLocked(holder);
  return true;
}

void LockManager::AbortRequester(LockHandle* requester, LockHandle* holder) {
  requester->Abort(
      error::AbortConcurrentTransaction(requester->tid(), holder->tid()));
}

void LockManager::EnqueueLock(LockHandle* handle, const LockRequest& request) {
  absl::MutexLock lock(mu_);

  // Don't hand out locks to aborted handles.
  if (handle->IsAborted()) {
    return;
  }

  const bool is_database_lock = request.table_id().empty();

  if (is_database_lock) {
    if (pending_commit_handle_ != nullptr &&
        pending_commit_handle_->tid() != handle->tid()) {
      AbortRequester(handle, pending_commit_handle_);
      return;
    }

    if (database_lock_holder_ != nullptr) {
      if (database_lock_holder_->tid() == handle->tid()) {
        return;
      }
      AbortRequester(handle, database_lock_holder_);
      return;
    }

    for (const auto& [_, holder] : table_lock_holders_) {
      if (holder->tid() == handle->tid()) {
        continue;
      }
      AbortRequester(handle, holder);
      return;
    }

    database_lock_holder_ = handle;
    return;
  }

  if (database_lock_holder_ != nullptr &&
      database_lock_holder_->tid() != handle->tid()) {
    if (!TryAbortHolderLocked(database_lock_holder_, handle)) {
      AbortRequester(handle, database_lock_holder_);
      return;
    }
  }

  auto holder_it = table_lock_holders_.find(request.table_id());
  if (holder_it == table_lock_holders_.end()) {
    table_lock_holders_[request.table_id()] = handle;
    return;
  }

  LockHandle* holder = holder_it->second;
  if (holder->tid() == handle->tid()) {
    return;
  }

  if (TryAbortHolderLocked(holder, handle)) {
    table_lock_holders_[request.table_id()] = handle;
    return;
  }

  AbortRequester(handle, holder);
}

void LockManager::UnlockAll(LockHandle* handle) {
  absl::MutexLock lock(mu_);

  ReleaseLocksForHandleLocked(handle);
  handle->Reset();
}

absl::StatusOr<absl::Time> LockManager::ReserveCommitTimestamp(
    LockHandle* handle) {
  absl::MutexLock lock(mu_);

  if (pending_commit_handle_ != nullptr &&
      pending_commit_handle_->tid() != handle->tid()) {
    return error::AbortConcurrentTransaction(handle->tid(),
                                             pending_commit_handle_->tid());
  }

  pending_commit_handle_ = handle;
  pending_commit_timestamp_ = clock_->Now();
  return pending_commit_timestamp_;
}

absl::Status LockManager::MarkCommitted(LockHandle* handle) {
  absl::MutexLock lock(mu_);

  // This transaction should have reserved the in-progress commit timestamp.
  GOOGLESQL_RET_CHECK_NE(pending_commit_handle_, nullptr);
  GOOGLESQL_RET_CHECK_EQ(pending_commit_handle_->tid(), handle->tid())
      << absl::Substitute("Transaction $0 does not own pending commit.",
                          handle->tid());

  last_commit_timestamp_ = pending_commit_timestamp_;
  pending_commit_timestamp_ = absl::InfiniteFuture();
  pending_commit_handle_ = nullptr;
  pending_commit_cvar_.SignalAll();
  return absl::OkStatus();
}

void LockManager::WaitForSafeRead(absl::Time read_time) {
  absl::MutexLock lock(mu_);

  // Wait for read time to become current if passed a future timestamp  for the
  // case of exact timestamp bound for snapshot read.
  // https://cloud.google.com/spanner/docs/timestamp-bounds#introduction
  bool f = false;
  mu_.AwaitWithDeadline(absl::Condition(&f), read_time);

  while (pending_commit_timestamp_ < read_time) {
    pending_commit_cvar_.Wait(&mu_);
  }
}

absl::Time LockManager::LastCommitTimestamp() {
  absl::ReaderMutexLock lock(mu_);
  return last_commit_timestamp_;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
