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

#include "tests/conformance/common/database_test_base.h"

#include <chrono>  // NOLINT(build/c++11)
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "google/cloud/spanner/admin/database_admin_client.h"
#include "google/cloud/spanner/backoff_policy.h"
#include "google/cloud/spanner/batch_dml_result.h"
#include "google/cloud/spanner/commit_result.h"
#include "google/cloud/spanner/create_instance_request_builder.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/spanner/polling_policy.h"
#include "google/cloud/spanner/retry_policy.h"
#include "google/cloud/spanner/transaction.h"
#include "google/cloud/status_or.h"
#include "common/constants.h"
#include "tests/common/file_based_schema_reader.h"
#include "tests/conformance/common/environment.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

void DatabaseTest::SetUp() {
  // Get the global environment in which the test runs.
  const ConformanceTestGlobals& globals = GetConformanceTestGlobals();

  // Specify custom retry, backoff, and polling policies. The default policies
  // are on the order of 10's of seconds. We want the tests to complete much
  // faster than that, so we set the policies on the order of milliseconds. By
  // default TestEnv conformance tests wait quite a while for admin operations
  // like create instance/database/schema.
  auto retry_policy = std::make_unique<cloud::spanner::LimitedTimeRetryPolicy>(
      std::chrono::seconds(300));
  auto backoff_policy =
      std::make_unique<cloud::spanner::ExponentialBackoffPolicy>(
          std::chrono::milliseconds(1), std::chrono::milliseconds(4), 1.01);
  auto polling_policy =
      std::make_unique<cloud::spanner::GenericPollingPolicy<>>(*retry_policy,
                                                               *backoff_policy);

  // Pick a unique database name for every test (reuse the instance).
  database_ = std::make_unique<google::cloud::spanner::Database>(
      google::cloud::spanner::Instance(globals.project_id, globals.instance_id),
      absl::StrCat("test-database-", absl::ToUnixMicros(absl::Now())));

  // Setup the database client.
  auto connection_options = *globals.connection_options;
  connection_options.set<cloud::spanner::SpannerRetryPolicyOption>(
      retry_policy->clone());
  connection_options.set<cloud::spanner::SpannerBackoffPolicyOption>(
      backoff_policy->clone());
  connection_options.set<cloud::spanner::SpannerPollingPolicyOption>(
      polling_policy->clone());
  database_client_ =
      std::make_unique<cloud::spanner_admin::DatabaseAdminClient>(
          cloud::spanner_admin::MakeDatabaseAdminConnection(
              std::move(connection_options)));
  google::spanner::admin::database::v1::CreateDatabaseRequest request;
  request.set_parent(database_->instance().FullName());
  std::string quote = kGSQLQuote;
  if (dialect_ == admin::database::v1::DatabaseDialect::POSTGRESQL) {
    quote = kPGQuote;
  }
  request.set_create_statement("CREATE DATABASE " + quote +
                               database_->database_id() + quote);
  request.set_database_dialect(dialect_);
  ZETASQL_ASSERT_OK(ToUtilStatusOr(database_client_->CreateDatabase(request).get()));

  // Setup a client to interact with the database.
  client_ = std::make_unique<cloud::spanner::Client>(
      google::cloud::spanner::MakeConnection(*database_,
                                             *globals.connection_options));

  // Setup stubs to access low-level API features not exposed by the C++ client.
  std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
      globals.connection_options->get<google::cloud::EndpointOption>(),
      globals.connection_options->get<google::cloud::GrpcCredentialOption>());
  spanner_stub_ = v1::Spanner::NewStub(channel);
  database_admin_stub_ =
      admin::database::v1::DatabaseAdmin::NewStub(channel);
  operations_stub_ = longrunning::Operations::NewStub(channel);

  // Allow test suites to customize the database at SetUp time.
  ZETASQL_ASSERT_OK(SetUpDatabase());
}

void DatabaseTest::TearDown() {
  database_client_->DropDatabase(database_->FullName());
}

absl::Status DatabaseTest::ResetDatabase() {
  const ConformanceTestGlobals& globals = GetConformanceTestGlobals();
  ZETASQL_RETURN_IF_ERROR(
      ToUtilStatus(database_client_->DropDatabase(database_->FullName())));
  database_ = std::make_unique<google::cloud::spanner::Database>(
      google::cloud::spanner::Instance(globals.project_id, globals.instance_id),
      absl::StrCat("test-database-", absl::ToUnixMicros(absl::Now())));
  google::spanner::admin::database::v1::CreateDatabaseRequest request;
  request.set_parent(database_->instance().FullName());
  std::string quote = kGSQLQuote;
  if (dialect_ == admin::database::v1::DatabaseDialect::POSTGRESQL) {
    quote = kPGQuote;
  }
  request.set_create_statement("CREATE DATABASE " + quote +
                               database_->database_id() + quote);
  request.set_database_dialect(dialect_);
  return ToUtilStatus(database_client_->CreateDatabase(request).get().status());
}

absl::Status DatabaseTest::SetSchema(const std::vector<std::string>& schema) {
  auto status_or =
      database_client_->UpdateDatabaseDdl(database_->FullName(), schema).get();
  if (!status_or.ok()) {
    return ToUtilStatus(status_or.status());
  }
  return absl::OkStatus();
}

absl::Status DatabaseTest::SetSchemaFromFile(const std::string& file_name) {
  const std::string root_dir = GetRunfilesDir(kSchemaTestDataDir);
  const std::string file_path = absl::StrCat(root_dir, "/", file_name);

  ZETASQL_ASSIGN_OR_RETURN(
      auto schema_set,
      ReadSchemaSetFromFile(file_path, FileBasedSchemaSetOptions{}));

  // Split the input into individual DDL statements.
  std::string text = schema_set.schemas[dialect_];
  absl::StripAsciiWhitespace(&text);
  std::vector<std::string> input_statements =
      absl::StrSplit(text, ';', absl::SkipEmpty());
  return SetSchema(input_statements);
}

absl::StatusOr<DatabaseTest::UpdateDatabaseDdlMetadata>
DatabaseTest::UpdateSchema(const std::vector<std::string>& schema) {
  auto status_or =
      database_client_->UpdateDatabaseDdl(database_->FullName(), schema).get();
  if (!status_or.ok()) {
    return ToUtilStatus(status_or.status());
  }
  return status_or.value();
}

absl::StatusOr<std::vector<std::string>> DatabaseTest::GetDatabaseDdl() const {
  auto status_or = database_client_->GetDatabaseDdl(database_->FullName());
  if (!status_or.ok()) {
    return ToUtilStatus(status_or.status());
  }
  GetDatabaseDdlResponse response = status_or.value();
  std::vector<std::string> ddl_statements;
  ddl_statements.reserve(response.statements_size());
  for (int i = 0; i < response.statements_size(); ++i) {
    ddl_statements.push_back(response.statements(i));
  }
  return ddl_statements;
}

// Helper typedefs to make code below a bit more readable.
using cloud::spanner::CommitResult;
using InsertMutationBuilder = cloud::spanner::InsertMutationBuilder;
using UpdateMutationBuilder = cloud::spanner::UpdateMutationBuilder;
using ReplaceMutationBuilder = cloud::spanner::ReplaceMutationBuilder;
using InsertOrUpdateMutationBuilder =
    cloud::spanner::InsertOrUpdateMutationBuilder;
using DeleteMutationBuilder = cloud::spanner::DeleteMutationBuilder;
using Transaction = cloud::spanner::Transaction;
using BatchDmlResult = cloud::spanner::BatchDmlResult;
using PartitionedDmlResult = cloud::spanner::PartitionedDmlResult;

absl::StatusOr<PartitionedDmlResult> DatabaseTest::ExecutePartitionedDml(
    const SqlStatement& sql_statement) {
  return ToUtilStatusOr(client().ExecutePartitionedDml(sql_statement));
}

absl::StatusOr<CommitResult> DatabaseTest::Commit(Mutations mutations) {
  return ToUtilStatusOr(client().Commit(
      [&](Transaction) -> cloud::StatusOr<Mutations> { return mutations; }));
}

absl::StatusOr<CommitResult> DatabaseTest::CommitTransaction(
    Transaction txn, Mutations mutations) {
  return ToUtilStatusOr(client().Commit(txn, mutations));
}

absl::StatusOr<CommitResult> DatabaseTest::CommitDml(
    const std::vector<SqlStatement>& sql_statements) {
  return ToUtilStatusOr(client().Commit(
      [&](Transaction const& txn) -> cloud::StatusOr<Mutations> {
        for (const auto& sql_statement : sql_statements) {
          auto result = client().ExecuteDml(txn, sql_statement);
          if (!result) return result.status();
        }
        return Mutations{};
      }));
}

absl::StatusOr<CommitResult> DatabaseTest::CommitDmlReturning(
    const SqlStatement sql_statement, std::vector<ValueRow>& result) {
  return ToUtilStatusOr(client().Commit(
      [&](Transaction const& txn) -> cloud::StatusOr<Mutations> {
        auto rows = client().ExecuteQuery(txn, std::move(sql_statement));
        for (auto& row : rows) {
          if (!row.ok()) {
            return row.status();
          }
          result.push_back(row.value());
        }
        return Mutations{};
      }));
}

absl::StatusOr<CommitResult> DatabaseTest::CommitDmlTransaction(
    Transaction txn, const std::vector<SqlStatement>& sql_statements) {
  for (const auto& sql_statement : sql_statements) {
    auto result = client().ExecuteDml(txn, sql_statement);
    if (!result) return ToUtilStatus(result.status());
  }
  auto result = client().Commit(txn, Mutations{});
  if (!result) return ToUtilStatus(result.status());
  return result.value();
}

absl::StatusOr<CommitResult> DatabaseTest::CommitBatchDml(
    const std::vector<SqlStatement>& sql_statements) {
  return ToUtilStatusOr(client().Commit(
      [&](Transaction const& txn) -> cloud::StatusOr<Mutations> {
        auto result = client().ExecuteBatchDml(txn, sql_statements);
        if (!result) return result.status();
        if (!result.value().status.ok()) return result.value().status;
        return Mutations{};
      }));
}

absl::StatusOr<BatchDmlResult> DatabaseTest::BatchDmlTransaction(
    Transaction txn, const std::vector<SqlStatement>& sql_statements) {
  auto result = client().ExecuteBatchDml(txn, sql_statements);
  if (!result) return ToUtilStatus(result.status());
  return result.value();
}

cloud::spanner::BatchedCommitResultStream DatabaseTest::BatchWrite(
    const std::vector<Mutations>& mutations) {
  cloud::spanner::BatchedCommitResultStream result =
      client().CommitAtLeastOnce(mutations);
  return result;
}

absl::Status DatabaseTest::Rollback(Transaction txn) {
  auto status = client().Rollback(txn);
  return absl::Status(absl::StatusCode(status.code()), status.message());
}

absl::StatusOr<CommitResult> DatabaseTest::Insert(
    std::string table, std::vector<std::string> columns, ValueRow row) {
  return Commit({InsertMutationBuilder(table, columns).AddRow(row).Build()});
}

absl::StatusOr<CommitResult> DatabaseTest::MultiInsert(
    std::string table, std::vector<std::string> columns,
    std::vector<ValueRow> rows) {
  auto mutation_builder = InsertMutationBuilder(table, columns);
  for (const auto& row : rows) {
    mutation_builder.AddRow(row);
  }
  return Commit({mutation_builder.Build()});
}

absl::StatusOr<CommitResult> DatabaseTest::Update(
    std::string table, std::vector<std::string> columns, ValueRow row) {
  return Commit({UpdateMutationBuilder(table, columns).AddRow(row).Build()});
}

absl::StatusOr<CommitResult> DatabaseTest::MultiUpdate(
    std::string table, std::vector<std::string> columns,
    std::vector<ValueRow> rows) {
  auto mutation_builder = UpdateMutationBuilder(table, columns);
  for (const auto& row : rows) {
    mutation_builder.AddRow(row);
  }
  return Commit({mutation_builder.Build()});
}

absl::StatusOr<CommitResult> DatabaseTest::Delete(std::string table,
                                                  cloud::spanner::Key key) {
  KeySet key_set;
  key_set.AddKey(key);
  return Commit({cloud::spanner::MakeDeleteMutation(table, key_set)});
}

absl::StatusOr<CommitResult> DatabaseTest::Delete(
    std::string table, std::vector<cloud::spanner::Key> keys) {
  KeySet key_set;
  for (const auto& key : keys) {
    key_set.AddKey(key);
  }
  return Commit({cloud::spanner::MakeDeleteMutation(table, key_set)});
}

absl::StatusOr<CommitResult> DatabaseTest::Delete(
    std::string table, cloud::spanner::KeySet key_set) {
  return Commit({cloud::spanner::MakeDeleteMutation(table, key_set)});
}

absl::StatusOr<CommitResult> DatabaseTest::InsertOrUpdate(
    std::string table, std::vector<std::string> columns, ValueRow row) {
  return Commit(
      {InsertOrUpdateMutationBuilder(table, columns).AddRow(row).Build()});
}

absl::StatusOr<CommitResult> DatabaseTest::MultiInsertOrUpdate(
    std::string table, std::vector<std::string> columns,
    std::vector<ValueRow> rows) {
  auto mutation_builder = InsertOrUpdateMutationBuilder(table, columns);
  for (const auto& row : rows) {
    mutation_builder.AddRow(row);
  }
  return Commit({mutation_builder.Build()});
}

absl::StatusOr<CommitResult> DatabaseTest::Replace(
    std::string table, std::vector<std::string> columns, ValueRow row) {
  return Commit({ReplaceMutationBuilder(table, columns).AddRow(row).Build()});
}

absl::StatusOr<CommitResult> DatabaseTest::MultiReplace(
    std::string table, std::vector<std::string> columns,
    std::vector<ValueRow> rows) {
  auto mutation_builder = ReplaceMutationBuilder(table, columns);
  for (const auto& row : rows) {
    mutation_builder.AddRow(row);
  }
  return Commit({mutation_builder.Build()});
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
