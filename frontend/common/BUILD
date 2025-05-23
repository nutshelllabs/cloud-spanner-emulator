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
    name = "uris",
    srcs = [
        "uris.cc",
    ],
    hdrs = [
        "uris.h",
    ],
    deps = [
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "uris_test",
    srcs = [
        "uris_test.cc",
    ],
    deps = [
        ":uris",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "protos",
    srcs = [
        "protos.cc",
    ],
    hdrs = [
        "protos.h",
    ],
    deps = [
        "//backend/common:ids",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/rpc:status_cc_proto",
        "@com_google_protobuf//:cc_wkt_protos",
    ],
)

cc_library(
    name = "labels",
    srcs = ["labels.cc"],
    hdrs = ["labels.h"],
    deps = [
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/status",
        "@com_google_protobuf//:protobuf_headers",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "labels_test",
    srcs = [
        "labels_test.cc",
    ],
    deps = [
        ":labels",
        "//common:limits",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "validations",
    srcs = ["validations.cc"],
    hdrs = ["validations.h"],
    deps = [
        "//common:errors",
        "//frontend/entities:transaction",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "status",
    srcs = [
        "status.cc",
    ],
    hdrs = [
        "status.h",
    ],
    deps = [
        "//common:limits",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:cord",
        "@com_google_googleapis//google/rpc:status_cc_proto",
        "@com_google_protobuf//:cc_wkt_protos",
    ],
)

cc_test(
    name = "status_test",
    srcs = [
        "status_test.cc",
    ],
    deps = [
        ":status",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
