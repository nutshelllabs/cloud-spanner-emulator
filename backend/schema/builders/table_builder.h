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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_TABLE_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_TABLE_BUILDER_H_

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/log/check.h"
#include "absl/memory/memory.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/locality_group.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/validators/table_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Table::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(new Table(
            TableValidator::Validate, TableValidator::ValidateUpdate))) {}

  std::unique_ptr<const Table> build() { return std::move(instance_); }

  const Table* get() const { return instance_.get(); }

  Builder& set_id(const std::string& id) {
    instance_->id_ = id;
    return *this;
  }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& set_owner_index(const Index* index) {
    instance_->owner_index_ = index;
    return *this;
  }

  Builder& set_owner_change_stream(const ChangeStream* change_stream) {
    instance_->owner_change_stream_ = change_stream;
    return *this;
  }

  Builder& add_change_stream(const ChangeStream* change_stream) {
    instance_->change_streams_.push_back(change_stream);
    return *this;
  }

  Builder& add_column(const Column* column) {
    instance_->columns_.push_back(column);
    instance_->columns_map_[column->Name()] = column;
    return *this;
  }

  Builder& add_key_column(const KeyColumn* key_col) {
    instance_->primary_key_.push_back(key_col);
    return *this;
  }

  Builder& set_parent_table(const Table* table) {
    instance_->parent_table_ = table;
    return *this;
  }

  Builder& set_on_delete(OnDeleteAction action) {
    instance_->on_delete_action_ = action;
    return *this;
  }

  Builder& add_foreign_key(const ForeignKey* foreign_key) {
    instance_->foreign_keys_.push_back(foreign_key);
    return *this;
  }

  Builder& add_referencing_foreign_key(const ForeignKey* foreign_key) {
    instance_->referencing_foreign_keys_.push_back(foreign_key);
    return *this;
  }

  Builder& set_row_deletion_policy(
      std::optional<ddl::RowDeletionPolicy> policy) {
    instance_->row_deletion_policy_ = policy;
    return *this;
  }

  Builder& set_synonym(const std::string& synonym) {
    instance_->synonym_ = synonym;
    return *this;
  }

  Builder& set_postgresql_oid(std::optional<uint32_t> postgresql_oid) {
    if (postgresql_oid.has_value()) {
      instance_->set_postgresql_oid(postgresql_oid.value());
    }
    return *this;
  }

  Builder& set_interleave_in_parent_postgresql_oid(
      std::optional<uint32_t> postgresql_oid) {
    if (postgresql_oid.has_value()) {
      instance_->set_interleave_in_parent_postgresql_oid(
          postgresql_oid.value());
    }
    return *this;
  }

  Builder& set_primary_key_index_postgresql_oid(
      std::optional<uint32_t> postgresql_oid) {
    if (postgresql_oid.has_value()) {
      instance_->set_primary_key_index_postgresql_oid(postgresql_oid.value());
    }
    return *this;
  }

 private:
  std::unique_ptr<Table> instance_;
};

class Table::Editor {
 public:
  explicit Editor(Table* instance) : instance_(instance) {}

  const Table* get() const { return instance_; }

  Editor& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Editor& add_column(const Column* column) {
    instance_->columns_.push_back(column);
    instance_->columns_map_[column->Name()] = column;
    return *this;
  }

  Editor& add_index(const Index* index) {
    instance_->indexes_.push_back(index);
    return *this;
  }

  Editor& add_child_table(const Table* table) {
    instance_->child_tables_.push_back(table);
    return *this;
  }

  Editor& set_on_delete(OnDeleteAction action) {
    instance_->on_delete_action_ = action;
    return *this;
  }

  Editor& add_check_constraint(const CheckConstraint* check_constraint) {
    instance_->check_constraints_.push_back(check_constraint);
    return *this;
  }

  Editor& add_foreign_key(const ForeignKey* foreign_key) {
    instance_->foreign_keys_.push_back(foreign_key);
    return *this;
  }

  Editor& add_referencing_foreign_key(const ForeignKey* foreign_key) {
    instance_->referencing_foreign_keys_.push_back(foreign_key);
    return *this;
  }

  Editor& set_row_deletion_policy(
      std::optional<ddl::RowDeletionPolicy> policy) {
    instance_->row_deletion_policy_ = policy;
    return *this;
  }

  Editor& add_change_stream(const ChangeStream* change_stream) {
    instance_->change_streams_.push_back(change_stream);
    return *this;
  }

  Editor& add_change_stream_explicitly_tracking_table(
      const ChangeStream* change_stream) {
    instance_->change_streams_explicitly_tracking_table_.push_back(
        change_stream);
    return *this;
  }

  Editor& remove_change_stream(const ChangeStream* change_stream) {
    auto itr = std::find_if(
        instance_->change_streams_.begin(), instance_->change_streams_.end(),
        [change_stream](const auto& change_stream_element) {
          return absl::EqualsIgnoreCase(change_stream->Name(),
                                        change_stream_element->Name());
        });
    if (itr != instance_->change_streams_.end()) {
      instance_->change_streams_.erase(itr);
    }
    auto itr_2 = std::find_if(
        instance_->change_streams_explicitly_tracking_table_.begin(),
        instance_->change_streams_explicitly_tracking_table_.end(),
        [change_stream](const auto& change_stream_element) {
          return absl::EqualsIgnoreCase(change_stream->Name(),
                                        change_stream_element->Name());
        });
    if (itr_2 != instance_->change_streams_explicitly_tracking_table_.end()) {
      instance_->change_streams_explicitly_tracking_table_.erase(itr_2);
    }
    return *this;
  }

  Editor& set_synonym(const std::string& synonym) {
    instance_->synonym_ = synonym;
    return *this;
  }

  Editor& drop_synonym(const std::string& synonym) {
    if (instance_->synonym_ == synonym) {
      instance_->synonym_ = "";
    }
    return *this;
  }

  Editor& set_locality_group(const LocalityGroup* locality_group) {
    instance_->locality_group_ = locality_group;
    return *this;
  }

  Editor& clear_locality_group() {
    instance_->locality_group_ = nullptr;
    return *this;
  }

 private:
  // Not owned.
  Table* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_TABLE_BUILDER_H_
