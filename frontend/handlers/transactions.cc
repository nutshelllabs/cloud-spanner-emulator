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

#include <memory>

#include "google/protobuf/empty.pb.h"
#include "google/spanner/v1/commit_response.pb.h"
#include "google/spanner/v1/mutation.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/access/write.h"
#include <set>
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/time/clock.h"
#include "common/request_stats.h"
#include "common/errors.h"
#include "frontend/converters/mutations.h"
#include "frontend/converters/time.h"
#include "frontend/entities/session.h"
#include "frontend/entities/transaction.h"
#include "frontend/server/handler.h"
#include "googlesql/base/status_macros.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "absl/status/status.h"

namespace protobuf_api = ::google::protobuf;

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Begins a new transaction.
absl::Status BeginTransaction(
    RequestContext* ctx, const spanner_api::BeginTransactionRequest* request,
    spanner_api::Transaction* response) {
  // Get session information.
  SessionManager* session_manager = ctx->env()->session_manager();
  GOOGLESQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   session_manager->GetSession(request->session()));

  // Create a new transaction.
  GOOGLESQL_ASSIGN_OR_RETURN(
      std::shared_ptr<Transaction> txn,
      session->CreateMultiUseTransaction(
          request->options(), Session::TransactionActivation::kInitializeOnly));

  // Populate transaction proto in response.
  GOOGLESQL_ASSIGN_OR_RETURN(*response, txn->ToProto());

  if (txn->IsReadWrite() && session->multiplexed() &&
      request->has_mutation_key()) {
    backend::Mutation mutation;
    google::protobuf::RepeatedPtrField<spanner_api::Mutation> mutations;
    *mutations.Add() = request->mutation_key();
    absl::Status status =
        MutationFromProto(*txn->schema(), mutations, &mutation);
    // MutationFromProto returns FailedPrecondition if the proto does not
    // contain a syntactically valid proto. Spanner however returns
    // InvalidArgument in the same scenario, so we convert that here to
    // InvalidArgument. All other errors are returned as-is.
    if (status.code() == absl::StatusCode::kFailedPrecondition) {
      status =
          absl::Status(absl::StatusCode::kInvalidArgument, status.message());
    }
    GOOGLESQL_RETURN_IF_ERROR(status);
    // Set a precommit token in the begin transaction response.
    response->mutable_precommit_token();
  }

  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(Spanner, BeginTransaction);

// Commits a transaction.
// The tables a commit touches, as the shape request stats aggregate by.
std::string CommitShape(const spanner_api::CommitRequest& request) {
  std::set<std::string> tables;
  for (const spanner_api::Mutation& mutation : request.mutations()) {
    switch (mutation.operation_case()) {
      case spanner_api::Mutation::kInsert:
        tables.insert(mutation.insert().table());
        break;
      case spanner_api::Mutation::kUpdate:
        tables.insert(mutation.update().table());
        break;
      case spanner_api::Mutation::kInsertOrUpdate:
        tables.insert(mutation.insert_or_update().table());
        break;
      case spanner_api::Mutation::kReplace:
        tables.insert(mutation.replace().table());
        break;
      case spanner_api::Mutation::kDelete:
        tables.insert(mutation.delete_().table());
        break;
      default:
        break;
    }
  }
  return absl::StrCat("commit ", absl::StrJoin(tables, ","));
}

absl::Status Commit(RequestContext* ctx,
                    const spanner_api::CommitRequest* request,
                    spanner_api::CommitResponse* response) {
  const absl::Time start_time = absl::Now();
  absl::Duration convert_time, write_time, commit_time;

  // Get session information.
  GOOGLESQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get transaction object to commit.
  absl::StatusOr<std::shared_ptr<Transaction>> maybe_txn;
  bool is_single_use = false;
  switch (request->transaction_case()) {
    case spanner_api::CommitRequest::kSingleUseTransaction:
      maybe_txn = session->CreateSingleUseTransaction(
          request->single_use_transaction());
      is_single_use = true;
      break;
    case spanner_api::CommitRequest::kTransactionId:
      maybe_txn = session->FindAndUseTransaction(request->transaction_id());
      break;
    case spanner_api::CommitRequest::TRANSACTION_NOT_SET:
      return error::MissingRequiredFieldError("CommitRequest.transaction");
  }

  GOOGLESQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn, maybe_txn);

  // Wrap all operations on this transaction so they are atomic .
  absl::Status status = txn->GuardedCall(
      Transaction::OpType::kCommit, [&]() -> absl::Status {
        // Cannot commit a ReadOnlyTransaction.
        if (txn->IsReadOnly()) {
          return error::
              CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction();
        }

        // Cannot commit after transaction has been rolled back or encountered
        // a non-recoverable error.
        if (txn->IsInvalid()) {
          return error::CannotUseTransactionAfterConstraintError();
        }
        if (txn->IsRolledback()) {
          return error::CannotCommitAfterRollback();
        }

        // Commit should be indempotent.
        if (txn->IsCommitted()) {
          GOOGLESQL_ASSIGN_OR_RETURN(absl::Time commit_timestamp,
                           txn->GetCommitTimestamp());
          GOOGLESQL_ASSIGN_OR_RETURN(*response->mutable_commit_timestamp(),
                           TimestampToProto(commit_timestamp));
          return absl::OkStatus();
        }

        // Process mutations and write to transaction store.
        absl::Time phase_start = absl::Now();
        backend::Mutation mutation;
        GOOGLESQL_RETURN_IF_ERROR(
            MutationFromProto(*txn->schema(), request->mutations(), &mutation));
        convert_time = absl::Now() - phase_start;
        phase_start = absl::Now();
        GOOGLESQL_RETURN_IF_ERROR(txn->Write(mutation));
        write_time = absl::Now() - phase_start;

        if (txn->IsReadWrite() && session->multiplexed() && !is_single_use &&
            !request->has_precommit_token()) {
          // A lightweight commit retry protocol.
          response->mutable_precommit_token();
          return absl::OkStatus();
        }

        // Actually commit the request.
        phase_start = absl::Now();
        GOOGLESQL_RETURN_IF_ERROR(txn->Commit());
        commit_time = absl::Now() - phase_start;

        // Return commit timestamp to user.
        GOOGLESQL_ASSIGN_OR_RETURN(absl::Time commit_timestamp,
                         txn->GetCommitTimestamp());
        GOOGLESQL_ASSIGN_OR_RETURN(*response->mutable_commit_timestamp(),
                         TimestampToProto(commit_timestamp));
        return absl::OkStatus();
      });
  RequestStats::Instance().Record(RequestSample{
      .kind = "commit",
      .text = CommitShape(*request),
      .elapsed = absl::Now() - start_time,
      .phases = {{"convert", convert_time},
                 {"write", write_time},
                 {"commit", commit_time}},
      .rows = request->mutations_size(),
  });
  return status;
}
REGISTER_GRPC_HANDLER(Spanner, Commit);

// Rolls back a transaction, releasing any locks it holds.
absl::Status Rollback(RequestContext* ctx,
                      const spanner_api::RollbackRequest* request,
                      protobuf_api::Empty* response) {
  // Get session information.
  GOOGLESQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get transaction object to rollback.
  GOOGLESQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindAndUseTransaction(request->transaction_id()));

  // Wrap all operations on this transaction so they are atomic.
  return txn->GuardedCall(
      Transaction::OpType::kRollback, [&]() -> absl::Status {
        // Can not rollback a ReadOnlyTransaction.
        if (txn->IsReadOnly()) {
          return error::
              CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction();
        }

        // Committed transaction can not be rolled back.
        if (txn->IsCommitted()) {
          return error::CannotRollbackAfterCommit();
        }

        // Rollback should be idempotent.
        if (txn->IsRolledback()) {
          return absl::OkStatus();
        }

        // Rollback the transaction.
        return txn->Rollback();
      });
}
REGISTER_GRPC_HANDLER(Spanner, Rollback);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
