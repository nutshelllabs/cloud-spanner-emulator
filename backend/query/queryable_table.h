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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_TABLE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_TABLE_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/query/queryable_column.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A wrapper over Table class which implements zetasql::Table.
// QueryableTable builds instances of EvalutorTableIterator by reading data of
// the table through a RowReader.
class QueryableTable : public zetasql::Table {
 public:
  // 'options' , 'catalog' , 'type_factory' must be non-null when specifying a
  // QueryableTable with default value columns.
  QueryableTable(
      const backend::Table* table, RowReader* reader,
      std::optional<const zetasql::AnalyzerOptions> options = std::nullopt,
      zetasql::Catalog* catalog = nullptr,
      zetasql::TypeFactory* type_factory = nullptr, bool is_synonym = false);

  std::string Name() const override {
    return std::string(SDLObjectName::GetInSchemaName(SynonymOrName()));
  }

  // FullName includes schema if present.
  std::string FullName() const override { return SynonymOrName(); }

  int NumColumns() const override { return columns_.size(); }

  const zetasql::Column* GetColumn(int i) const override {
    return columns_[i].get();
  }

  const zetasql::Column* FindColumnByName(
      const std::string& name) const override;

  std::optional<std::vector<int>> PrimaryKey() const override {
    return primary_key_column_indexes_.empty()
               ? std::nullopt
               : std::make_optional(primary_key_column_indexes_);
  }

  const backend::Table* wrapped_table() const { return wrapped_table_; }

  // Override CreateEvaluatorTableIterator.
  absl::StatusOr<std::unique_ptr<zetasql::EvaluatorTableIterator>>
  CreateEvaluatorTableIterator(
      absl::Span<const int> column_idxs) const override;

 private:
  absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
  AnalyzeColumnExpression(
      const Column* column, zetasql::TypeFactory* type_factory,
      zetasql::Catalog* catalog,
      std::optional<const zetasql::AnalyzerOptions> opt_options) const;

  // Whether the table should be treated as a synonym.
  bool is_synonym_;

  // Returns the name of the table or the synonym if the table is a synonym.
  std::string SynonymOrName() const {
    return is_synonym_ ? wrapped_table_->synonym() : wrapped_table_->Name();
  }

  // The underlying Table object which backes the QueryableTable.
  const backend::Table* wrapped_table_;

  // A RowReader which data of the table can be read from to build a
  // EvalutorTableIterator when CreateEvaluatorTableIterator is called.
  RowReader* reader_;

  // The columns in the table.
  std::vector<std::unique_ptr<const QueryableColumn>> columns_;

  // A list of ordinal indexes of the primary key columns of the table.
  std::vector<int> primary_key_column_indexes_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_TABLE_H_
