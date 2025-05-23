#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Test cases for schema_updater_test.cc.

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "base",
    testonly = 1,
    srcs = ["base.cc"],
    hdrs = ["base.h"],
    deps = [
        "//backend/common:ids",
        "//backend/database/pg_oid_assigner",
        "//backend/schema/catalog:schema",
        "//backend/schema/graph:schema_graph",
        "//backend/schema/graph:schema_graph_editor",
        "//backend/schema/graph:schema_node",
        "//backend/schema/parser:ddl_parser",
        "//backend/schema/updater:schema_updater",
        "//backend/storage:in_memory_storage",
        "//common:errors",
        "//common:limits",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//third_party/spanner_pg/ddl:spangres_direct_schema_printer_impl",
        "//third_party/spanner_pg/ddl:spangres_schema_printer",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "schema_updater_tests",
    testonly = 1,
    srcs = [
        "change_stream_test.cc",
        "check_constraint.cc",
        "column_default_values.cc",
        "common.cc",
        "database_option.cc",
        "foreign_key.cc",
        "generated_column.cc",
        "identity_column.cc",
        "index.cc",
        "locality_group_test.cc",
        "model_test.cc",
        "named_schema.cc",
        "oid_assignment.cc",
        "property_graph_test.cc",
        "sequence.cc",
        "table.cc",
        "udf.cc",
        "views.cc",
    ],
    deps = [
        ":base",
        "//backend/schema/catalog:schema",
        "//backend/schema/graph:schema_node",
        "//backend/schema/printer:print_ddl",
        "//backend/schema/updater:global_schema_names",
        "//backend/schema/updater:schema_updater",
        "//common:errors",
        "//common:feature_flags",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:spanner_extended_type",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/public:json_value",
        "@com_google_zetasql//zetasql/public:numeric_value",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
    ],
    alwayslink = 1,
)

cc_library(
    name = "proto_bundle_tests",
    testonly = 1,
    srcs = ["proto_bundle.cc"],
    deps = [
        ":base",
        "//tests/common:test_2_cc_proto",
        "//tests/common:test_cc_proto",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
    ],
    alwayslink = 1,
)
