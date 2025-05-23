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

py_library(
    name = "emulator",
    srcs = ["emulator.py"],
    data = [
        "//binaries:emulator_main",
        "//binaries:gateway_main",
    ],
    deps = [
        "@bazel_tools//tools/python/runfiles",
        "@org_python_pypi_portpicker//:portpicker",
    ],
)

py_test(
    name = "instance_admin_test",
    srcs = ["instance_admin_test.py"],
    deps = [
        ":emulator",
    ],
)

py_test(
    name = "database_admin_test",
    srcs = ["database_admin_test.py"],
    data = [
        "//tests/common:testproto",
    ],
    deps = [
        ":emulator",
        "@bazel_tools//tools/python/runfiles",
    ],
)

py_test(
    name = "session_test",
    srcs = ["session_test.py"],
    deps = [
        ":emulator",
    ],
)

py_test(
    name = "read_write_test",
    srcs = ["read_write_test.py"],
    deps = [
        ":emulator",
    ],
)

py_test(
    name = "database_ddl_test",
    srcs = ["database_ddl_test.py"],
    deps = [
        ":emulator",
    ],
)

py_test(
    name = "operation_test",
    srcs = ["operation_test.py"],
    deps = [
        ":emulator",
    ],
)
