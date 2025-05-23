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

cc_test(
    name = "emulator_conformance_test",
    size = "large",
    srcs = ["emulator_conformance_test.cc"],
    shard_count = 20,
    deps = [
        "//common:feature_flags",
        "//frontend/server",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/conformance/cases",
        "//tests/conformance/common:environment",
        "@com_github_googleapis_google_cloud_cpp//:common",
        "@com_github_googleapis_google_cloud_cpp//:grpc_utils",
        "@com_github_googleapis_google_cloud_cpp//:spanner",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
