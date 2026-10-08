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

#include "backend/common/indexing.h"

#include <memory>
#include <string>
#include <utility>

#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/evaluator.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "backend/common/ids.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "common/errors.h"
#include "common/limits.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Looks up the value of an index data table column in a row of the indexed
// table. Expression columns have no source column; their values are stored
// under the index column itself by IndexExpressionEvaluator.
googlesql::Value GetIndexColumnValue(const Row& base_row,
                                     const Column* index_column) {
  const Column* source_column = index_column->source_column();
  return GetColumnValueOrNull(
      base_row, source_column != nullptr ? source_column : index_column);
}

absl::Status ValidateKeySizeForIndex(const Index* index, const Key& key) {
  int64_t key_size = key.LogicalSizeInBytes();
  if (key_size > limits::kMaxKeySizeBytes) {
    return error::IndexKeyTooLarge(index->Name(), key_size,
                                   limits::kMaxKeySizeBytes);
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<std::unique_ptr<IndexExpressionEvaluator>>
IndexExpressionEvaluator::Create(
    const Index* index, const googlesql::AnalyzerOptions& analyzer_options,
    googlesql::Catalog* catalog) {
  auto evaluator = absl::WrapUnique(new IndexExpressionEvaluator());
  absl::flat_hash_set<ColumnID> seen_dependencies;
  for (const Column* column : index->index_data_table()->columns()) {
    if (column->source_column() != nullptr || !column->expression()) {
      continue;
    }
    std::string sql = absl::Substitute(
        "CAST(($0) AS $1)", *column->expression(),
        column->GetType()->TypeName(googlesql::PRODUCT_EXTERNAL,
                                    /*use_external_float32=*/true));
    auto expression = std::make_unique<googlesql::PreparedExpression>(sql);
    googlesql::AnalyzerOptions options = analyzer_options;
    for (const Column* dependency : column->dependent_columns()) {
      GOOGLESQL_RETURN_IF_ERROR(options.AddExpressionColumn(
          dependency->Name(), dependency->GetType()));
      if (seen_dependencies.insert(dependency->id()).second) {
        evaluator->dependent_columns_.push_back(dependency);
      }
    }
    GOOGLESQL_RETURN_IF_ERROR(expression->Prepare(options, catalog));
    GOOGLESQL_RET_CHECK(column->GetType()->Equals(expression->output_type()))
        << column->FullName();
    evaluator->expressions_.push_back({column, std::move(expression)});
  }
  return evaluator;
}

absl::Status IndexExpressionEvaluator::Evaluate(Row* base_row) const {
  for (const KeyExpression& key_expression : expressions_) {
    googlesql::ParameterValueMap column_values;
    for (const Column* dependency : key_expression.column->dependent_columns()) {
      column_values[dependency->Name()] =
          GetColumnValueOrNull(*base_row, dependency);
    }
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value value,
                     key_expression.expression->Execute(column_values));
    (*base_row)[key_expression.column] = std::move(value);
  }
  return absl::OkStatus();
}

absl::StatusOr<Key> ComputeIndexKey(const Row& base_row, const Index* index) {
  // Columns must be added to the key for each column in index data table
  // primary key.
  Key key;
  for (const auto& key_column : index->index_data_table()->primary_key()) {
    key.AddColumn(GetIndexColumnValue(base_row, key_column->column()),
                  key_column->is_descending(), key_column->is_nulls_last());
  }
  GOOGLESQL_RETURN_IF_ERROR(ValidateKeySizeForIndex(index, key));
  return key;
}

ValueList ComputeIndexValues(const Row& base_row, const Index* index) {
  ValueList values;
  for (const Column* column : index->index_data_table()->columns()) {
    values.push_back(GetIndexColumnValue(base_row, column));
  }
  return values;
}

bool ShouldFilterIndexKeyOrValue(const Index* index, const Key& key,
                                 const Row& base_row) {
  if (index->is_null_filtered()) {
    // NULL_FILTERED index should filter the row if any of the key columns is
    // NULL.
    for (int i = 0; i < index->key_columns().size(); ++i) {
      if (key.ColumnValue(i).is_null()) {
        return true;
      }
    }
  } else {
    // Check for null filtered columns.
    for (const Column* column : index->null_filtered_columns()) {
      if (GetIndexColumnValue(base_row, column).is_null()) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
