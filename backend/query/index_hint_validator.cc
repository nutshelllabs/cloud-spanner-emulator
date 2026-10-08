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

#include "backend/query/index_hint_validator.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/analyzer.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/analyzer_output.h"
#include "googlesql/public/property_graph.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "googlesql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/updater/global_schema_names.h"
#include "common/errors.h"
#include "re2/re2.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Query hints using generated names of managed indexes are special-cased. The
// emulator and production name generators use different fingerprint algorithms,
// so the generated names have different suffixes. A hint using a managed
// index's name that was generated in production never matches the name of the
// emulator's index, and vice versa. In order to permit the use of production
// queries in the emulator, names generated in production are permitted in the
// emulator. However, in order to avoid the unintentional and unsupported use of
// emulator generated names in production, names generated in the emulator are
// not allowed in query hints, neither in production nor in the emulator.

// Returns true if an index name matches the signature of a generated managed
// index name. See also GlobalSchemaNames::GenerateManagedIndexName.
bool MatchesManagedIndexName(absl::string_view table_name,
                             absl::string_view index_name) {
  return RE2::FullMatch(index_name, absl::StrCat("IDX_", table_name, "_",
                                                 "\\w+", "_", "[0-9A-F]{16}"));
}

// Returns true if an index's name matches the name of an index created in the
// emulator with GlobalSchemaNames::GenerateManagedIndexName.
bool HasGeneratedEmulatorName(const Index* index) {
  if (!index->is_managed()) {
    return false;  // Only managed indexes have generated names.
  }
  auto columns = index->key_columns();
  std::vector<std::string> column_names;
  std::transform(
      columns.begin(), columns.end(), std::back_inserter(column_names),
      [](const KeyColumn* column) { return column->column()->Name(); });
  return index->Name() == GlobalSchemaNames::GenerateManagedIndexName(
                              index->indexed_table()->Name(), column_names,
                              index->is_null_filtered(), index->is_unique())
                              .value();
}

// Comparisons that are NULL, hence not satisfied, when an operand is NULL.
bool IsNullRejectingComparison(absl::string_view function_name) {
  static const auto* const kFunctions = new absl::flat_hash_set<std::string>{
      "$equal",   "$not_equal",        "$less", "$less_or_equal", "$greater",
      "$greater_or_equal", "$in", "$between", "$like"};
  return kFunctions->contains(function_name);
}

// Appends to `exprs` every expression that `filter_expr` requires to be
// non-null: the `x IS NOT NULL` conjuncts, which resolve to $not($is_null(x)),
// and the operands of comparison conjuncts such as `x = 1` or `x > 'a'`.
void CollectNotNullExprs(const googlesql::ResolvedExpr* filter_expr,
                         std::vector<const googlesql::ResolvedExpr*>* exprs) {
  if (filter_expr == nullptr ||
      filter_expr->node_kind() != googlesql::RESOLVED_FUNCTION_CALL) {
    return;
  }
  const auto* call = filter_expr->GetAs<googlesql::ResolvedFunctionCall>();
  const std::string& name = call->function()->Name();
  if (name == "$and") {
    for (const auto& arg : call->argument_list()) {
      CollectNotNullExprs(arg.get(), exprs);
    }
    return;
  }
  if (IsNullRejectingComparison(name)) {
    for (const auto& arg : call->argument_list()) {
      exprs->push_back(arg.get());
    }
    return;
  }
  if (name == "$not" && call->argument_list_size() == 1 &&
      call->argument_list(0)->node_kind() ==
          googlesql::RESOLVED_FUNCTION_CALL) {
    const auto* inner =
        call->argument_list(0)->GetAs<googlesql::ResolvedFunctionCall>();
    if (inner->function()->Name() == "$is_null" &&
        inner->argument_list_size() == 1) {
      exprs->push_back(inner->argument_list(0));
    }
  }
}

// Collects the table scans in a resolved subtree.
class TableScanCollector : public googlesql::ResolvedASTVisitor {
 public:
  absl::Status VisitResolvedTableScan(
      const googlesql::ResolvedTableScan* scan) override {
    scans_.push_back(scan);
    return DefaultVisit(scan);
  }
  const std::vector<const googlesql::ResolvedTableScan*>& scans() const {
    return scans_;
  }

 private:
  std::vector<const googlesql::ResolvedTableScan*> scans_;
};

bool ScanProducesColumn(const googlesql::ResolvedTableScan* scan,
                        const googlesql::ResolvedColumn& column) {
  for (const googlesql::ResolvedColumn& scan_column : scan->column_list()) {
    if (scan_column == column) {
      return true;
    }
  }
  return false;
}

// Whether `query_expr`, taken from a filter above `scan`, computes the same
// value as `key_expr`, an index key expression analyzed over the scanned
// table's columns. Column references match by name; the query side refers to
// the scan's columns, the index side to expression columns.
bool SameExpression(const googlesql::ResolvedExpr* query_expr,
                    const googlesql::ResolvedExpr* key_expr,
                    const googlesql::ResolvedTableScan* scan) {
  if (!query_expr->type()->Equals(key_expr->type())) {
    return false;
  }
  if (query_expr->node_kind() == googlesql::RESOLVED_COLUMN_REF) {
    if (key_expr->node_kind() != googlesql::RESOLVED_EXPRESSION_COLUMN) {
      return false;
    }
    const googlesql::ResolvedColumn& column =
        query_expr->GetAs<googlesql::ResolvedColumnRef>()->column();
    return ScanProducesColumn(scan, column) &&
           absl::EqualsIgnoreCase(
               column.name(),
               key_expr->GetAs<googlesql::ResolvedExpressionColumn>()->name());
  }
  if (query_expr->node_kind() != key_expr->node_kind()) {
    return false;
  }
  switch (query_expr->node_kind()) {
    case googlesql::RESOLVED_LITERAL:
      return query_expr->GetAs<googlesql::ResolvedLiteral>()->value().Equals(
          key_expr->GetAs<googlesql::ResolvedLiteral>()->value());
    case googlesql::RESOLVED_FUNCTION_CALL: {
      const auto* a = query_expr->GetAs<googlesql::ResolvedFunctionCall>();
      const auto* b = key_expr->GetAs<googlesql::ResolvedFunctionCall>();
      if (a->function()->FullName(/*include_group=*/false) !=
              b->function()->FullName(/*include_group=*/false) ||
          a->argument_list_size() != b->argument_list_size() ||
          !a->generic_argument_list().empty() ||
          !b->generic_argument_list().empty()) {
        return false;
      }
      for (int i = 0; i < a->argument_list_size(); ++i) {
        if (!SameExpression(a->argument_list(i), b->argument_list(i), scan)) {
          return false;
        }
      }
      return true;
    }
    case googlesql::RESOLVED_CAST:
      return SameExpression(query_expr->GetAs<googlesql::ResolvedCast>()->expr(),
                            key_expr->GetAs<googlesql::ResolvedCast>()->expr(),
                            scan);
    case googlesql::RESOLVED_GET_PROTO_FIELD: {
      const auto* a = query_expr->GetAs<googlesql::ResolvedGetProtoField>();
      const auto* b = key_expr->GetAs<googlesql::ResolvedGetProtoField>();
      return a->field_descriptor() == b->field_descriptor() &&
             SameExpression(a->expr(), b->expr(), scan);
    }
    case googlesql::RESOLVED_GET_STRUCT_FIELD: {
      const auto* a = query_expr->GetAs<googlesql::ResolvedGetStructField>();
      const auto* b = key_expr->GetAs<googlesql::ResolvedGetStructField>();
      return a->field_idx() == b->field_idx() &&
             SameExpression(a->expr(), b->expr(), scan);
    }
    case googlesql::RESOLVED_GET_JSON_FIELD: {
      const auto* a = query_expr->GetAs<googlesql::ResolvedGetJsonField>();
      const auto* b = key_expr->GetAs<googlesql::ResolvedGetJsonField>();
      return a->field_name() == b->field_name() &&
             SameExpression(a->expr(), b->expr(), scan);
    }
    default:
      return false;
  }
}

}  // namespace

absl::Status IndexHintValidator::VisitResolvedFilterScan(
    const googlesql::ResolvedFilterScan* scan) {
  GOOGLESQL_RETURN_IF_ERROR(googlesql::ResolvedASTVisitor::DefaultVisit(scan));
  std::vector<const googlesql::ResolvedExpr*> exprs;
  CollectNotNullExprs(scan->filter_expr(), &exprs);
  if (exprs.empty()) {
    return absl::OkStatus();
  }
  TableScanCollector collector;
  GOOGLESQL_RETURN_IF_ERROR(scan->input_scan()->Accept(&collector));
  for (const googlesql::ResolvedTableScan* table_scan : collector.scans()) {
    auto& known = not_null_exprs_[table_scan];
    known.insert(known.end(), exprs.begin(), exprs.end());
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> IndexHintValidator::IsKeyProvenNotNull(
    const googlesql::ResolvedTableScan* table_scan, const Index* index,
    const Column* key) {
  auto it = not_null_exprs_.find(table_scan);
  if (it == not_null_exprs_.end()) {
    return false;
  }
  const std::vector<const googlesql::ResolvedExpr*>& exprs = it->second;

  const Column* source_column = key->source_column();
  if (source_column != nullptr) {
    for (const googlesql::ResolvedExpr* expr : exprs) {
      if (expr->node_kind() != googlesql::RESOLVED_COLUMN_REF) {
        continue;
      }
      const googlesql::ResolvedColumn& column =
          expr->GetAs<googlesql::ResolvedColumnRef>()->column();
      if (ScanProducesColumn(table_scan, column) &&
          absl::EqualsIgnoreCase(column.name(), source_column->Name())) {
        return true;
      }
    }
    return false;
  }

  if (catalog_ == nullptr || !key->expression().has_value()) {
    return false;
  }
  googlesql::AnalyzerOptions options =
      MakeGoogleSqlAnalyzerOptions(schema_->default_time_zone());
  for (const Column* column : index->indexed_table()->columns()) {
    GOOGLESQL_RETURN_IF_ERROR(
        options.AddExpressionColumn(column->Name(), column->GetType()));
  }
  std::unique_ptr<const googlesql::AnalyzerOutput> output;
  GOOGLESQL_RETURN_IF_ERROR(googlesql::AnalyzeExpression(
      *key->expression(), options, catalog_, &type_factory_, &output));
  for (const googlesql::ResolvedExpr* expr : exprs) {
    if (SameExpression(expr, output->resolved_expr(), table_scan)) {
      return true;
    }
  }
  return false;
}

absl::Status IndexHintValidator::VisitResolvedGraphNodeScan(
    const googlesql::ResolvedGraphNodeScan* scan) {
  GOOGLESQL_RETURN_IF_ERROR(CollectGraphElementIndexHint(scan));
  return googlesql::ResolvedASTVisitor::DefaultVisit(scan);
}

absl::Status IndexHintValidator::VisitResolvedGraphEdgeScan(
    const googlesql::ResolvedGraphEdgeScan* scan) {
  GOOGLESQL_RETURN_IF_ERROR(CollectGraphElementIndexHint(scan));
  return googlesql::ResolvedASTVisitor::DefaultVisit(scan);
}

absl::Status IndexHintValidator::CollectGraphElementIndexHint(
    const googlesql::ResolvedGraphElementScan* scan) {
  for (const auto& hint : scan->hint_list()) {
    if (!(absl::EqualsIgnoreCase(hint->qualifier(), "spanner") ||
          hint->qualifier().empty()) ||
        !absl::EqualsIgnoreCase(hint->name(), "force_index")) {
      continue;
    }
    GOOGLESQL_RET_CHECK_EQ(hint->value()->node_kind(), googlesql::RESOLVED_LITERAL);
    const googlesql::Value& value =
        hint->value()->GetAs<googlesql::ResolvedLiteral>()->value();
    GOOGLESQL_RET_CHECK(value.type()->IsString());
    GraphElementIndexHint index_hint;
    index_hint.index_name = value.string_value();
    for (const googlesql::GraphElementTable* element_table :
         scan->target_element_table_list()) {
      index_hint.tables.push_back(
          element_table->GetTable()->GetAs<QueryableTable>()->wrapped_table());
    }
    graph_index_hints_.push_back(std::move(index_hint));
    break;
  }
  return absl::OkStatus();
}

absl::Status IndexHintValidator::VisitResolvedTableScan(
    const googlesql::ResolvedTableScan* table_scan) {
  // Visit child nodes first.
  GOOGLESQL_RETURN_IF_ERROR(googlesql::ResolvedASTVisitor::DefaultVisit(table_scan));

  std::vector<const googlesql::ResolvedNode*> child_nodes;
  table_scan->GetChildNodes(&child_nodes);

  for (const googlesql::ResolvedNode* child_node : child_nodes) {
    if (child_node->node_kind() == googlesql::RESOLVED_OPTION) {
      const googlesql::ResolvedOption* hint =
          child_node->GetAs<googlesql::ResolvedOption>();
      if ((absl::EqualsIgnoreCase(hint->qualifier(), "spanner") ||
           hint->qualifier().empty()) &&
          absl::EqualsIgnoreCase(hint->name(), "force_index")) {
        // We should expect only one hint per table scan as multiple hints per
        // node is not allowed and would've been rejected by the HintValidator.
        GOOGLESQL_RET_CHECK_EQ(hint->value()->node_kind(), googlesql::RESOLVED_LITERAL);
        const googlesql::Value& value =
            hint->value()->GetAs<googlesql::ResolvedLiteral>()->value();
        GOOGLESQL_RET_CHECK(value.type()->IsString());
        index_hints_map_[table_scan] = value.string_value();
        break;
      }
    }
  }

  return absl::OkStatus();
}

absl::Status IndexHintValidator::ValidateIndexesForTables() {
  for (auto [table_scan, index_name] : index_hints_map_) {
    auto table = table_scan->table();
    auto query_table = table->GetAs<QueryableTable>();
    GOOGLESQL_RETURN_IF_ERROR(ValidateIndexHint(query_table->wrapped_table(),
                                                index_name, table_scan));
  }
  for (const GraphElementIndexHint& hint : graph_index_hints_) {
    // The pattern matches one element table per label; the index must belong
    // to one of them.
    const Table* indexed_table = nullptr;
    for (const Table* table : hint.tables) {
      if (table->FindIndex(table->FindIndexQualifiedName(hint.index_name)) !=
          nullptr) {
        indexed_table = table;
        break;
      }
    }
    if (indexed_table == nullptr && !hint.tables.empty()) {
      indexed_table = hint.tables.front();
    }
    GOOGLESQL_RET_CHECK_NE(indexed_table, nullptr) << hint.index_name;
    GOOGLESQL_RETURN_IF_ERROR(
        ValidateIndexHint(indexed_table, hint.index_name, /*table_scan=*/nullptr));
  }
  return absl::OkStatus();
}

absl::Status IndexHintValidator::ValidateIndexHint(
    const Table* schema_table, const std::string& index_name,
    const googlesql::ResolvedTableScan* table_scan) {
  if (absl::EqualsIgnoreCase(index_name, "_base_table")) {
    return absl::OkStatus();
  }
  const auto* index =
      schema_table->FindIndex(schema_table->FindIndexQualifiedName(index_name));

  // See comments above regarding special-casing of managed indexes.
  if (index == nullptr) {
    if (MatchesManagedIndexName(schema_table->Name(), index_name)) {
      return absl::OkStatus();
    }
    return error::QueryHintIndexNotFound(schema_table->Name(), index_name);
  } else if (HasGeneratedEmulatorName(index)) {
    return error::QueryHintManagedIndexNotSupported(index_name);
  }

  if (index->is_search_index()) {
    if (!allow_search_indexes_in_transaction_) {
      return error::SearchIndexNotUsable(
          index_name,
          "is a SEARCH index type which is not supported for transactional "
          "queries by default");
    }

    // FOR UPDATE queries are not supported on search indexes.
    if (in_select_for_update_query_) {
      return error::ForUpdateUnsupportedInSearchQueries();
    }

    if (in_partition_query_) {
      // Not allowed in batch query.
      return error::SearchIndexNotUsable(
          index_name,
          "is a SEARCH index type which is not supported for partitioned "
          "queries");
    }
  }
  if (index->is_null_filtered() && !disable_null_filtered_index_check_) {
    for (const auto* key_column : index->key_columns()) {
      const Column* key = key_column->column();
      const Column* source_column = key->source_column();
      // A nullable key column, or an expression key, means the index does
      // not cover the full table. The index is usable only when a filter
      // above the scan requires that key to be non-null, since the emulator
      // does not otherwise analyze predicates.
      if (source_column != nullptr && !source_column->is_nullable()) {
        continue;
      }
      bool proven = false;
      if (table_scan != nullptr) {
        GOOGLESQL_ASSIGN_OR_RETURN(proven,
                                   IsKeyProvenNotNull(table_scan, index, key));
      }
      if (!proven) {
        return error::NullFilteredIndexUnusable(index_name);
      }
    }
  }
  return absl::OkStatus();
}

absl::Status IndexHintValidator::VisitResolvedQueryStmt(
    const googlesql::ResolvedQueryStmt* stmt) {
  // Visit children first to collect all hints.
  GOOGLESQL_RETURN_IF_ERROR(googlesql::ResolvedASTVisitor::DefaultVisit(stmt));
  // Validate all index hints.
  return ValidateIndexesForTables();
}

absl::Status IndexHintValidator::VisitResolvedInsertStmt(
    const googlesql::ResolvedInsertStmt* stmt) {
  // Visit children first to collect all hints.
  GOOGLESQL_RETURN_IF_ERROR(googlesql::ResolvedASTVisitor::DefaultVisit(stmt));
  // The target table should not have any hints (not allowed by GoogleSQL).
  GOOGLESQL_RET_CHECK(!index_hints_map_.contains(stmt->table_scan()));
  return ValidateIndexesForTables();
}

absl::Status IndexHintValidator::VisitResolvedUpdateStmt(
    const googlesql::ResolvedUpdateStmt* stmt) {
  // Visit children first to collect all hints.
  GOOGLESQL_RETURN_IF_ERROR(googlesql::ResolvedASTVisitor::DefaultVisit(stmt));
  return ValidateIndexesForTables();
}

absl::Status IndexHintValidator::VisitResolvedDeleteStmt(
    const googlesql::ResolvedDeleteStmt* stmt) {
  // Visit children first to collect all hints.
  GOOGLESQL_RETURN_IF_ERROR(googlesql::ResolvedASTVisitor::DefaultVisit(stmt));
  return ValidateIndexesForTables();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
