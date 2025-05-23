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

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "database",
    srcs = [
        "database.cc",
    ],
    hdrs = [
        "database.h",
    ],
    deps = [
        "//backend/actions:manager",
        "//backend/common:ids",
        "//backend/database/change_stream:change_stream_partition_churner",
        "//backend/database/pg_oid_assigner",
        "//backend/locking:manager",
        "//backend/query:query_engine",
        "//backend/schema/catalog:proto_bundle",
        "//backend/schema/catalog:schema",
        "//backend/schema/catalog:versioned_catalog",
        "//backend/schema/graph:schema_graph",
        "//backend/schema/updater:schema_updater",
        "//backend/schema/updater:scoped_schema_change_lock",
        "//backend/storage",
        "//backend/storage:in_memory_storage",
        "//backend/transaction:read_only_transaction",
        "//backend/transaction:read_write_transaction",
        "//common:clock",
        "//common:errors",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:variant",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "database_test",
    srcs = [
        "database_test.cc",
    ],
    deps = [
        ":database",
        "//backend/access:read",
        "//backend/datamodel:key_set",
        "//backend/schema/updater:schema_updater",
        "//backend/transaction:read_only_transaction",
        "//common:clock",
        "//common:config",
        "//common:errors",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
