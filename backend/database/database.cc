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

#include "backend/database/database.h"

#include <memory>
#include <thread>  // NOLINT
#include <utility>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/functional/bind_front.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/variant.h"
#include "backend/actions/manager.h"
#include "backend/common/ids.h"
#include "backend/database/change_stream/change_stream_partition_churner.h"
#include "backend/database/pg_oid_assigner/pg_oid_assigner.h"
#include "backend/locking/manager.h"
#include "backend/query/query_engine.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/schema/graph/schema_graph.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/schema/updater/scoped_schema_change_lock.h"
#include "backend/storage/in_memory_storage.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_only_transaction.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"
#include "common/errors.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// TransactionIDGenerator is initialized to 1 because 0 is used as a sentinel
// value for an invalid transaction.
Database::Database() : transaction_id_generator_(1) {}

absl::StatusOr<std::unique_ptr<Database>> Database::Create(
    Clock* clock, std::string_view database_id,
    const SchemaChangeOperation& schema_change_operation) {
  auto database = absl::WrapUnique(new Database());
  database->clock_ = clock;
  database->database_id_ = database_id;
  database->storage_ = std::make_unique<InMemoryStorage>();
  database->lock_manager_ = std::make_unique<LockManager>(clock);
  database->type_factory_ = std::make_unique<zetasql::TypeFactory>();
  database->query_engine_ =
      std::make_unique<QueryEngine>(database->type_factory_.get());
  database->action_manager_ = std::make_unique<ActionManager>();
  database->dialect_ = schema_change_operation.database_dialect;
  database->pg_oid_assigner_ = std::make_unique<PgOidAssigner>(
      schema_change_operation.database_dialect ==
      database_api::DatabaseDialect::POSTGRESQL);

  if (schema_change_operation.statements.empty()) {
    if (database->dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
      // Create an empty schema with the dialect set.
      database->versioned_catalog_ =
          std::make_unique<VersionedCatalog>(std::make_unique<const Schema>(
              SchemaGraph::CreateEmpty(), ProtoBundle::CreateEmpty(),
              database->dialect_, database_id));
    } else {
      database->versioned_catalog_ = std::make_unique<VersionedCatalog>();
    }
  } else {
    SchemaUpdater updater;
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const Schema> schema,
        updater.CreateSchemaFromDDL(schema_change_operation,
                                    database->GetSchemaChangeContext()));
    database->versioned_catalog_ =
        std::make_unique<VersionedCatalog>(std::move(schema));
  }

  database->action_manager_->AddActionsForSchema(
      database->versioned_catalog_->GetLatestSchema(),
      database->query_engine_->function_catalog(),
      database->query_engine_->type_factory());

  database->change_stream_partition_churner_ =
      std::make_unique<ChangeStreamPartitionChurner>(
          absl::bind_front(&Database::CreateReadWriteTransaction,
                           database.get()),
          database->clock_);

  database->change_stream_partition_churner_->Update(
      database->versioned_catalog_->GetLatestSchema());

  // Some functions need to access the schema (e.g. sequence functions), so
  // set the latest schema to the function catalog here.
  database->query_engine_->SetLatestSchemaForFunctionCatalog(
      database->versioned_catalog_->GetLatestSchema());

  return database;
}
absl::StatusOr<std::unique_ptr<ReadOnlyTransaction>>
Database::CreateReadOnlyTransaction(const ReadOnlyOptions& options) {
  return std::make_unique<ReadOnlyTransaction>(
      options, transaction_id_generator_.NextId(), clock_, storage_.get(),
      lock_manager_.get(), versioned_catalog_.get());
}

absl::StatusOr<std::unique_ptr<ReadWriteTransaction>>
Database::CreateReadWriteTransaction(const ReadWriteOptions& options,
                                     const RetryState& retry_state) {
  return std::make_unique<ReadWriteTransaction>(
      options, retry_state, transaction_id_generator_.NextId(), clock_,
      storage_.get(), lock_manager_.get(), versioned_catalog_.get(),
      action_manager_.get());
}

SchemaChangeContext Database::GetSchemaChangeContext() {
  return SchemaChangeContext{
      .type_factory = type_factory_.get(),
      .table_id_generator = &table_id_generator_,
      .column_id_generator = &column_id_generator_,
      .storage = storage_.get(),
      .pg_oid_assigner = pg_oid_assigner_.get(),
      .database_id = database_id_,
  };
}

absl::Status Database::UpdateSchema(
    const SchemaChangeOperation& schema_change_operation,
    int* num_succesful_statements, absl::Time* commit_timestamp,
    absl::Status* backfill_status) {
  if (schema_change_operation.statements.empty()) {
    return error::UpdateDatabaseMissingStatements();
  }

  // Make an exclusive lock request for the database. If there are any
  // concurrent transactions it will be denied and the operation aborted.
  ScopedSchemaChangeLock lock{transaction_id_generator_.NextId(),
                              lock_manager_.get()};
  ZETASQL_RETURN_IF_ERROR(lock.Wait());

  // Reserve a commit timestamp for the schema changes. Even if the
  // schema change fails, it will result in a no-op commit that will
  // be invisible to other read-only/read-write transactions.
  ZETASQL_ASSIGN_OR_RETURN(auto update_timestamp, lock.ReserveCommitTimestamp());

  auto context = GetSchemaChangeContext();
  context.schema_change_timestamp = update_timestamp;
  const Schema* existing_schema = versioned_catalog_->GetLatestSchema();
  SchemaUpdater updater;
  ZETASQL_ASSIGN_OR_RETURN(auto result,
                   updater.UpdateSchemaFromDDL(
                       existing_schema, schema_change_operation, context));
  *commit_timestamp = update_timestamp;
  *num_succesful_statements = result.num_successful_statements;
  *backfill_status = result.backfill_status;

  // We update the schema even if the backfill status was not OK, the returned
  // schema will be the schema for the last valid statement before the statement
  // for which the backfill/verification failed.
  if (result.updated_schema != nullptr) {
    ZETASQL_RETURN_IF_ERROR(versioned_catalog_->AddSchema(
        update_timestamp, std::move(result.updated_schema)));
    action_manager_->AddActionsForSchema(versioned_catalog_->GetLatestSchema(),
                                         query_engine_->function_catalog(),
                                         query_engine_->type_factory());
  }
  change_stream_partition_churner_->Update(
      versioned_catalog_->GetLatestSchema());

  // Some functions need to access the schema (e.g. sequence functions), so
  // set the latest schema to the function catalog here.
  query_engine_->SetLatestSchemaForFunctionCatalog(
      versioned_catalog_->GetLatestSchema());

  return absl::OkStatus();
}

const Schema* Database::GetLatestSchema() const {
  return versioned_catalog_->GetLatestSchema();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
