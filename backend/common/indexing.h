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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_INDEXING_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_INDEXING_H_

#include <memory>
#include <vector>

#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/evaluator.h"
#include "googlesql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Evaluates the key expressions of an expression index.
//
// Expression keys are stored as columns of the index data table that carry
// the expression and have no source column. Before an index entry can be
// computed from an indexed table row, Evaluate() adds the value of every such
// column to the row, keyed by the index data table column itself, which is
// where ComputeIndexKey and ComputeIndexValues look for it.
class IndexExpressionEvaluator {
 public:
  // `catalog` resolves the functions used by the expressions. A null catalog
  // resolves only GoogleSQL built-in functions.
  static absl::StatusOr<std::unique_ptr<IndexExpressionEvaluator>> Create(
      const Index* index, const googlesql::AnalyzerOptions& analyzer_options,
      googlesql::Catalog* catalog);

  // Indexed table columns read by the key expressions.
  absl::Span<const Column* const> dependent_columns() const {
    return dependent_columns_;
  }

  absl::Status Evaluate(Row* base_row) const;

 private:
  IndexExpressionEvaluator() = default;

  struct KeyExpression {
    const Column* column;
    std::unique_ptr<googlesql::PreparedExpression> expression;
  };
  std::vector<KeyExpression> expressions_;
  std::vector<const Column*> dependent_columns_;
};

// Computes the index key from the given row.
absl::StatusOr<Key> ComputeIndexKey(const Row& base_row, const Index* index);

// Computes an the ordered list of index row values using the given base row.
ValueList ComputeIndexValues(const Row& base_row, const Index* index);

// Returns true if no index entry should be added for the given base table key
// or value.
bool ShouldFilterIndexKeyOrValue(const Index* index, const Key& key,
                                 const Row& base_row);
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_INDEXING_H_
