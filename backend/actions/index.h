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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_INDEX_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_INDEX_H_

#include "backend/actions/action.h"
#include "backend/actions/ops.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/catalog.h"
#include "backend/common/indexing.h"
#include "backend/common/rows.h"
#include "backend/schema/catalog/table.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// IndexEffector triggers on mutations to an indexed table.
//
// Insert, Update & Delete operations to a row in an indexed table are added as
// operations to an index data table. The following operations are applied to
// the index data table based on operations to the indexed table:
// - Insert: Index entry is computed from the indexed row & buffered to the
//           index.
// - Update: Old index entry is buffered to be deleted and a new index entry is
//           buffered to be added.
// - Delete: Index entry is buffered to be deleted.
//
// NULL_FILTERED index entries are omitted from all operations above.
// UNIQUE index checks are handled by UniqueIndexVerifier.
class IndexEffector : public Effector {
 public:
  // Suitable for indexes whose keys are all column references.
  explicit IndexEffector(const Index* index);

  // `analyzer_options` and `catalog` prepare the key expressions of an
  // expression index; see IndexExpressionEvaluator.
  IndexEffector(const Index* index,
                const googlesql::AnalyzerOptions& analyzer_options,
                googlesql::Catalog* catalog);

 private:
  absl::Status Effect(const ActionContext* ctx,
                      const InsertOp& op) const override;
  absl::Status Effect(const ActionContext* ctx,
                      const UpdateOp& op) const override;
  absl::Status Effect(const ActionContext* ctx,
                      const DeleteOp& op) const override;

  absl::Status EvaluateKeyExpressions(Row* base_row) const;

  const Index* index_;

  // List of indexed table columns relevant to the index.
  std::vector<const Column*> base_columns_;

  // Null unless the index has expression keys.
  std::unique_ptr<IndexExpressionEvaluator> expression_evaluator_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_INDEX_H_
