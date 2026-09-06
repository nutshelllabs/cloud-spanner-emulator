load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

# Candidate-selection queries over a synthetic versioned artifact graph. Excluded from wildcard test runs; run it explicitly:
#   bazel test //tests/benchmark:graph_scanner_benchmark --test_output=all
cc_test(
    name = "graph_scanner_benchmark",
    size = "large",
    timeout = "eternal",
    srcs = ["graph_scanner_benchmark.cc"],
    data = [
        "aggregate_scanner.sql",
        "common_scanner_suffix.sql",
        "dispatch_scanner.sql",
        "evaluation_scanner.sql",
        "pipeline_schema.sql",
        "validation_scanner.sql",
    ],
    tags = ["manual"],
    deps = [
        "//common:feature_flags",
        "//frontend/server",
        "//tests/common:file_based_schema_reader",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/conformance/common:database_test_base",
        "//tests/conformance/common:environment",
        "@com_github_googleapis_google_cloud_cpp//:common",
        "@com_github_googleapis_google_cloud_cpp//:grpc_utils",
        "@com_github_googleapis_google_cloud_cpp//:spanner",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/log:absl_log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_googlesql//googlesql/base",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_googletest//:gtest",
    ],
)

