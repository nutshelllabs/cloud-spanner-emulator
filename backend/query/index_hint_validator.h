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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INDEX_HINT_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INDEX_HINT_VALIDATOR_H_

#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_ast_visitor.h"
#include "googlesql/resolved_ast/resolved_node_kind.pb.h"
#include <string>
#include <vector>

#include "googlesql/public/catalog.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Checks if an index hint specified on a table scan is valid.
class IndexHintValidator : public googlesql::ResolvedASTVisitor {
 public:
  // `catalog` resolves the functions of expression index keys so that a
  // query's `expression IS NOT NULL` predicate can be matched against them.
  // Without it an expression key is never proven non-null.
  IndexHintValidator(const Schema* schema,
                     bool disable_null_filtered_index_check = false,
                     bool allow_search_indexes_in_transaction = false,
                     bool in_partition_query = false,
                     bool in_select_for_update_query = false,
                     googlesql::Catalog* catalog = nullptr)
      : schema_(schema),
        disable_null_filtered_index_check_(disable_null_filtered_index_check),
        allow_search_indexes_in_transaction_(
            allow_search_indexes_in_transaction),
        in_partition_query_(in_partition_query),
        in_select_for_update_query_(in_select_for_update_query),
        catalog_(catalog) {}

 private:
  absl::Status VisitResolvedQueryStmt(
      const googlesql::ResolvedQueryStmt* stmt) final;

  absl::Status VisitResolvedInsertStmt(
      const googlesql::ResolvedInsertStmt* stmt) final;

  absl::Status VisitResolvedUpdateStmt(
      const googlesql::ResolvedUpdateStmt* stmt) final;

  absl::Status VisitResolvedDeleteStmt(
      const googlesql::ResolvedDeleteStmt* stmt) final;

  // Validates that all the index hints used in the query can be applied to
  // serving the tables.
  absl::Status ValidateIndexesForTables();

  // To collect the 'force_index' hints from all table scans.
  absl::Status VisitResolvedTableScan(
      const googlesql::ResolvedTableScan* scan) final;

  // To collect the `expr IS NOT NULL` conjuncts that apply to the table scans
  // beneath each filter.
  absl::Status VisitResolvedFilterScan(
      const googlesql::ResolvedFilterScan* scan) final;

  // To collect the 'force_index' hints on graph element patterns, which apply
  // to the element tables the pattern matches.
  absl::Status VisitResolvedGraphNodeScan(
      const googlesql::ResolvedGraphNodeScan* scan) final;
  absl::Status VisitResolvedGraphEdgeScan(
      const googlesql::ResolvedGraphEdgeScan* scan) final;
  absl::Status CollectGraphElementIndexHint(
      const googlesql::ResolvedGraphElementScan* scan);

  // Validates one 'force_index' hint naming `index_name` on `schema_table`.
  // `table_scan` is the hinted scan, or null for a graph element pattern.
  absl::Status ValidateIndexHint(const Table* schema_table,
                                 const std::string& index_name,
                                 const googlesql::ResolvedTableScan* table_scan);

  // Whether the filters above `table_scan` require the value of `key`, a key
  // column of a null-filtered index on the scanned table, to be non-null.
  absl::StatusOr<bool> IsKeyProvenNotNull(
      const googlesql::ResolvedTableScan* table_scan, const Index* index,
      const Column* key);

  // Expressions the filters above each table scan require to be non-null.
  absl::flat_hash_map<const googlesql::ResolvedTableScan*,
                      std::vector<const googlesql::ResolvedExpr*>>
      not_null_exprs_;

  // Mapping of table scans to the index hints specified on each.
  absl::flat_hash_map<const googlesql::ResolvedTableScan*, std::string>
      index_hints_map_;

  // Index hints on graph element patterns, with the element tables each
  // pattern may match.
  struct GraphElementIndexHint {
    std::vector<const Table*> tables;
    std::string index_name;
  };
  std::vector<GraphElementIndexHint> graph_index_hints_;

  // The database schema.
  const Schema* schema_;

  // Whether to disable checks around using null-filtered indexes in SQL
  // queries.
  const bool disable_null_filtered_index_check_;

  // Whether to allow using search index in transactions.
  const bool allow_search_indexes_in_transaction_;

  // Whether to validate indexes in partition query.
  const bool in_partition_query_;

  // Whether to validate index hints based on SELECT FOR UPDATE.
  const bool in_select_for_update_query_;

  // Resolves functions when analyzing expression index keys. May be null.
  googlesql::Catalog* catalog_;

  // Owns the types of analyzed expression index keys.
  googlesql::TypeFactory type_factory_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INDEX_HINT_VALIDATOR_H_
