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

#include "backend/actions/check_constraint.h"

#include <memory>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/query/analyzer_options.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status CheckConstraintVerifier::PrepareExpression(
    const CheckConstraint* check_constraint,
    const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::Catalog* function_catalog) {
  // Prepare an execuatable expression given a check constraint expression.
  auto expr = std::make_unique<zetasql::PreparedExpression>(
      check_constraint->expression());
  zetasql::AnalyzerOptions options = analyzer_options;
  for (const Column* dep : check_constraint->dependent_columns()) {
    ZETASQL_RETURN_IF_ERROR(options.AddExpressionColumn(dep->Name(), dep->GetType()));
  }
  ZETASQL_RETURN_IF_ERROR(expr->Prepare(options, function_catalog));
  ZETASQL_RET_CHECK(expr->output_type()->Equals(zetasql::types::BoolType()));
  expression_ = std::move(expr);
  return absl::OkStatus();
}

absl::Status CheckConstraintVerifier::VerifyRow(
    const zetasql::ParameterValueMap& column_values, const Key& key) const {
  ZETASQL_ASSIGN_OR_RETURN(zetasql::Value value, expression_->Execute(column_values));
  // The value could be True, False, or Null. The check constraint is violated
  // if the value is False.
  if (value.Equals(zetasql::values::False())) {
    return error::CheckConstraintViolated(check_constraint_->Name(),
                                          check_constraint_->table()->Name(),
                                          key.DebugString());
  }
  return absl::OkStatus();
}

CheckConstraintVerifier::CheckConstraintVerifier(
    const CheckConstraint* check_constraint,
    const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::Catalog* function_catalog)
    : check_constraint_(check_constraint) {
  absl::Status s =
      PrepareExpression(check_constraint, analyzer_options, function_catalog);
  ABSL_DCHECK(s.ok()) << "Failed to initialize CheckConstraintVerifier: " << s;
}

absl::Status CheckConstraintVerifier::VerifyInsertUpdateOp(
    const ActionContext* ctx, const Table* table,
    const std::vector<const Column*>& columns,
    const std::vector<zetasql::Value>& values, const Key& key) const {
  absl::Span<const Column* const> dependent_columns =
      check_constraint_->dependent_columns();
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<StorageIterator> itr,
      ctx->store()->Read(table, KeyRange::Point(key), dependent_columns));
  ZETASQL_RET_CHECK(itr->Next());
  ZETASQL_RETURN_IF_ERROR(itr->Status());
  ZETASQL_RET_CHECK_EQ(columns.size(), values.size());
  ZETASQL_RET_CHECK_EQ(dependent_columns.size(), itr->NumColumns());

  zetasql::ParameterValueMap column_values;
  for (int i = 0; i < dependent_columns.size(); ++i) {
    column_values[dependent_columns[i]->Name()] = itr->ColumnValue(i);
  }
  // The generated column values have been updated in
  // GeneratedColumnEffector::Effect before the CheckConstraintVerifier is
  // executed, so that we are able to evaluate the check constraints depending
  // on the generated values.
  for (int i = 0; i < columns.size(); ++i) {
    column_values[columns[i]->Name()] = values[i];
  }

  ZETASQL_RETURN_IF_ERROR(VerifyRow(column_values, key));
  return absl::OkStatus();
}

absl::Status CheckConstraintVerifier::Verify(const ActionContext* ctx,
                                             const InsertOp& op) const {
  return VerifyInsertUpdateOp(ctx, op.table, op.columns, op.values, op.key);
}

absl::Status CheckConstraintVerifier::Verify(const ActionContext* ctx,
                                             const UpdateOp& op) const {
  return VerifyInsertUpdateOp(ctx, op.table, op.columns, op.values, op.key);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
