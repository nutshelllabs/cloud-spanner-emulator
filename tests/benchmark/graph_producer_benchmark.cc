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

// Times executable-job selection over a synthetic artifact pipeline.
// The fixture grows through three stages, each retaining earlier artifacts:
//   A: evaluation jobs pending, nothing produced yet;
//   B: evaluations produced, validation jobs pending;
//   C: validations produced, aggregate jobs pending.
// Stage C selects a small set of aggregate jobs from a large graph, exposing
// work proportional to the whole workspace instead of the pending inputs.
//
// Every stage checks the exact (job, input artifact) pairs, versions, strong
// flags, and types. A job with any invalid input must be excluded entirely.
// Three variants run against the same state and expected results:
//   current: graph traversal with inline payload selection;
//   ids_then_hydrate: graph IDs and versions followed by relational hydration;
//   no_graph: equivalent base-table joins starting from pending jobs.
//
// Scale with GRAPH_BENCH_ITEMS (default 5000), GRAPH_BENCH_ITERATIONS (default
// 1), and GRAPH_BENCH_PAYLOAD_BYTES (default 1200). GRAPH_BENCH_STAGES selects
// a,b,c; GRAPH_BENCH_VARIANTS selects a comma-separated subset of the variants.
// Peak RSS includes the embedded emulator and the benchmark client.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/log/absl_log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "common/config.h"
#include "common/feature_flags.h"
#include "frontend/server/server.h"
#include "gmock/gmock.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/spanner/admin/instance_admin_client.h"
#include "google/cloud/spanner/create_instance_request_builder.h"
#include "google/cloud/spanner/json.h"
#include "googlesql/base/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "tests/common/file_based_schema_reader.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"
#include "tests/conformance/common/environment.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace {

constexpr char kDataDir[] = "tests/benchmark";
constexpr char kWorkspaceId[] = "pipeline-producer-benchmark";
constexpr char kTypePrefix[] = "type.googleapis.com/benchmark.pipeline.";
constexpr char kInvalidFlagType[] =
    "type.googleapis.com/benchmark.pipeline.flag.Invalid";
constexpr char kUuidMin[] = "00000000-0000-0000-0000-000000000000";
constexpr char kUuidMax[] = "ffffffff-ffff-ffff-ffff-ffffffffffff";

constexpr int kGroups = 50;
// Share of items whose check contributes inputs to a group's aggregate job.
constexpr int kIncludedPercent = 18;
// One item in this many carries an Invalid flag, which must exclude every
// job that takes it as an input.
constexpr int kInvalidStride = 997;

std::string TypeUrl(absl::string_view suffix) {
  return absl::StrCat(kTypePrefix, suffix);
}

int EnvInt(const char* name, int value) {
  const char* text = std::getenv(name);
  return text == nullptr ? value : std::atoi(text);
}

absl::StatusOr<std::string> ReadDataFile(absl::string_view name) {
  std::string path = absl::StrCat(GetRunfilesDir(kDataDir), "/", name);
  std::ifstream in(path);
  if (!in) return absl::NotFoundError(absl::StrCat("Cannot read ", path));
  std::stringstream buffer;
  buffer << in.rdbuf();
  return buffer.str();
}

std::vector<std::string> SplitDdl(absl::string_view ddl) {
  std::string stripped;
  for (absl::string_view line : absl::StrSplit(ddl, '\n')) {
    size_t comment = line.find("--");
    if (comment != absl::string_view::npos) line = line.substr(0, comment);
    absl::StrAppend(&stripped, line, "\n");
  }
  std::vector<std::string> statements;
  for (absl::string_view statement : absl::StrSplit(stripped, ';')) {
    std::string trimmed(absl::StripAsciiWhitespace(statement));
    if (!trimmed.empty()) statements.push_back(std::move(trimmed));
  }
  return statements;
}

int64_t PeakRssKb() {
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    if (absl::StartsWith(line, "VmHWM:")) {
      std::vector<std::string> parts =
          absl::StrSplit(line, ' ', absl::SkipEmpty());
      if (parts.size() >= 2) return std::atoll(parts[1].c_str());
    }
  }
  return -1;
}

std::string Filler(absl::string_view seed, int bytes) {
  std::string out;
  out.reserve(bytes + 40);
  uint32_t state = 2166136261u;
  for (char c : seed) state = (state ^ static_cast<uint8_t>(c)) * 16777619u;
  static constexpr char kAlphabet[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
  while (static_cast<int>(out.size()) < bytes) {
    state = state * 1664525u + 1013904223u;
    out.push_back(kAlphabet[(state >> 24) % (sizeof(kAlphabet) - 1)]);
  }
  return out;
}

// One (job, input artifact) pair the producer query must return.
struct ExpectedInput {
  std::string job_id;
  int64_t job_revision_version;
  std::string artifact_id;
  int64_t revision_version;
  bool strong;
  std::string artifact_type;

  bool operator<(const ExpectedInput& other) const {
    return std::tie(job_id, job_revision_version, artifact_id, revision_version,
                    strong, artifact_type) <
           std::tie(other.job_id, other.job_revision_version, other.artifact_id,
                    other.revision_version, other.strong, other.artifact_type);
  }
  bool operator==(const ExpectedInput& other) const {
    return !(*this < other) && !(other < *this);
  }
};

class GraphProducerBenchmarkEnvironment : public testing::Environment {
 public:
  GraphProducerBenchmarkEnvironment()
      : feature_flags_({
            .enable_check_constraint = true,
            .enable_column_default_values = true,
            .enable_views = true,
            .enable_generated_pk = true,
            .enable_fk_delete_cascade_action = true,
            .enable_batch_query_with_no_table_scan = true,
            .enable_fk_enforcement_option = true,
            .enable_interleave_in = true,
        }) {}

  void SetUp() override {
    const int probability =
        EnvInt("GRAPH_BENCH_ABORT_PROBABILITY",
               config::abort_current_transaction_probability());
    ASSERT_GE(probability, 0);
    ASSERT_LE(probability, 100);
    config::set_abort_current_transaction_probability(probability);
    ABSL_LOG(INFO) << "GRAPH_PRODUCER abort_probability=" << probability;
    frontend::Server::Options options;
    options.server_address = "localhost:0";
    server_ = frontend::Server::Create(options);
    ASSERT_NE(server_, nullptr);

    auto connection_options = std::make_unique<google::cloud::Options>();
    connection_options->set<google::cloud::GrpcCredentialOption>(
        grpc::InsecureChannelCredentials());
    connection_options->set<google::cloud::EndpointOption>(
        absl::StrCat(server_->host(), ":", server_->port()));

    google::cloud::spanner::Instance instance("bench-project",
                                              "bench-instance");
    auto instance_client =
        std::make_unique<google::cloud::spanner_admin::InstanceAdminClient>(
            google::cloud::spanner_admin::MakeInstanceAdminConnection(
                *connection_options));
    GOOGLESQL_ASSERT_OK(ToUtilStatusOr(
        instance_client
            ->CreateInstance(
                google::cloud::spanner::CreateInstanceRequestBuilder(
                    instance, "bench-config")
                    .SetDisplayName("bench-config")
                    .SetNodeCount(1)
                    .Build())
            .get()));

    globals_ = std::make_unique<ConformanceTestGlobals>();
    globals_->project_id = instance.project_id();
    globals_->instance_id = instance.instance_id();
    globals_->connection_options = std::move(connection_options);
    globals_->in_prod_env = false;
    SetConformanceTestGlobals(globals_.get());
  }

 private:
  std::unique_ptr<frontend::Server> server_;
  std::unique_ptr<ConformanceTestGlobals> globals_;
  ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

class GraphProducerBenchmark : public DatabaseTest {
 protected:
  absl::Status SetUpDatabase() override {
    items_ = EnvInt("GRAPH_BENCH_ITEMS", 5000);
    iterations_ = EnvInt("GRAPH_BENCH_ITERATIONS", 1);
    payload_bytes_ = EnvInt("GRAPH_BENCH_PAYLOAD_BYTES", 1200);
    GOOGLESQL_ASSIGN_OR_RETURN(std::string ddl,
                               ReadDataFile("pipeline_schema.sql"));
    GOOGLESQL_RETURN_IF_ERROR(SetSchema(SplitDdl(ddl)));
    GOOGLESQL_ASSIGN_OR_RETURN(variants_["current"],
                               ReadDataFile("producer_query.sql"));
    GOOGLESQL_ASSIGN_OR_RETURN(
        variants_["ids_then_hydrate"],
        ReadDataFile("producer_query_ids_then_hydrate.sql"));
    GOOGLESQL_ASSIGN_OR_RETURN(variants_["no_graph"],
                               ReadDataFile("producer_query_no_graph.sql"));
    return SeedStageA();
  }

  // The database drop after a workspace this size has aborted the emulator;
  // main leaves with _Exit instead.
  void TearDown() override {}

  bool Invalid(int j) const { return j % kInvalidStride == 0; }
  // Indexed within the group, so every aggregate job takes about the same
  // number of members instead of the share landing on the first few.
  bool Included(int j) const {
    return !Invalid(j) && ((j / kGroups) % 100) < kIncludedPercent;
  }
  int GroupOf(int j) const { return j % kGroups; }

  // Runs every selected variant against the same state, so their timings are
  // comparable and their results are checked against the same expectation.
  void RunStage(absl::string_view name, const std::string& job_type,
                const std::vector<ExpectedInput>& expected) {
    const SqlStatement::ParamType params{
        {"workspace_id", Value(std::string(kWorkspaceId))},
        {"job_type", Value(job_type)},
        {"job_id_start", Value(std::string(kUuidMin))},
        {"job_id_end", Value(std::string(kUuidMax))},
    };
    for (const std::string& variant : selected_variants_) {
      auto it = variants_.find(variant);
      ASSERT_NE(it, variants_.end()) << "unknown variant " << variant;
      absl::Duration best = absl::InfiniteDuration();
      for (int i = 0; i < iterations_; ++i) {
        absl::Time start = absl::Now();
        auto rows = QueryWithParams(it->second, params);
        absl::Duration elapsed = absl::Now() - start;
        GOOGLESQL_ASSERT_OK(rows.status()) << name << " " << variant;
        best = std::min(best, elapsed);
        ABSL_LOG(INFO) << absl::StrFormat(
            "GRAPH_PRODUCER stage=%s variant=%s items=%d iteration=%d "
            "rows=%d ms=%d peak_rss_kb=%d",
            name, variant, items_, i, rows->size(),
            absl::ToInt64Milliseconds(elapsed), PeakRssKb());
        Verify(absl::StrCat(name, "/", variant), *rows, expected);
      }
      ABSL_LOG(INFO) << absl::StrFormat(
          "GRAPH_PRODUCER stage=%s variant=%s items=%d best_ms=%d rows=%d "
          "peak_rss_kb=%d",
          name, variant, items_, absl::ToInt64Milliseconds(best),
          expected.size(), PeakRssKb());
    }
  }

  // The producer query returns one row per (job, input artifact). Every field
  // the producer reads is compared, and a job with an invalid input must be
  // absent entirely rather than partly.
  void Verify(absl::string_view name, const std::vector<ValueRow>& rows,
              const std::vector<ExpectedInput>& expected) {
    std::vector<ExpectedInput> seen;
    seen.reserve(rows.size());
    std::set<std::string> job_types;
    for (const ValueRow& row : rows) {
      absl::Span<const Value> v = row.values();
      ASSERT_EQ(v.size(), 10) << name << " column count";
      auto job_id = v[0].get<std::string>();
      auto job_version = v[1].get<int64_t>();
      auto job_type = v[2].get<std::string>();
      auto workspace_id = v[3].get<std::string>();
      auto artifact_id = v[5].get<std::string>();
      auto artifact_version = v[6].get<int64_t>();
      auto strong = v[7].get<bool>();
      auto artifact_type = v[8].get<std::string>();
      ASSERT_TRUE(job_id.ok() && job_version.ok() && job_type.ok() &&
                  workspace_id.ok() && artifact_id.ok() &&
                  artifact_version.ok() && strong.ok() && artifact_type.ok())
          << name << " row has an unexpected shape";
      EXPECT_EQ(*workspace_id, kWorkspaceId);
      EXPECT_TRUE(v[4].get<Json>().ok()) << name << " job payload";
      EXPECT_TRUE(v[9].get<Json>().ok()) << name << " artifact payload";
      job_types.insert(*job_type);
      seen.push_back({*job_id, *job_version, *artifact_id, *artifact_version,
                      *strong, *artifact_type});
    }
    EXPECT_THAT(job_types, testing::SizeIs(testing::Le(1)))
        << name << " returned more than one job type";
    std::vector<ExpectedInput> want = expected;
    std::sort(seen.begin(), seen.end());
    std::sort(want.begin(), want.end());
    EXPECT_EQ(seen.size(), want.size()) << name << " row count";
    if (seen == want) return;
    // Aggregate the first few differences rather than dumping thousands of
    // rows.
    std::vector<ExpectedInput> missing, extra;
    std::set_difference(want.begin(), want.end(), seen.begin(), seen.end(),
                        std::back_inserter(missing));
    std::set_difference(seen.begin(), seen.end(), want.begin(), want.end(),
                        std::back_inserter(extra));
    ADD_FAILURE() << name << ": " << missing.size() << " missing, "
                  << extra.size() << " unexpected rows";
    for (int i = 0; i < std::min<int>(5, missing.size()); ++i) {
      ADD_FAILURE() << "  missing " << missing[i].job_id << " <- "
                    << missing[i].artifact_id << " v"
                    << missing[i].revision_version;
    }
    for (int i = 0; i < std::min<int>(5, extra.size()); ++i) {
      ADD_FAILURE() << "  unexpected " << extra[i].job_id << " <- "
                    << extra[i].artifact_id << " v"
                    << extra[i].revision_version;
    }
  }

  int items_ = 0;
  int iterations_ = 0;
  int payload_bytes_ = 0;
  std::map<std::string, std::string> variants_;
  std::vector<std::string> selected_variants_{"current", "ids_then_hydrate",
                                              "no_graph"};

  // ---- expectations -------------------------------------------------------

  std::vector<ExpectedInput> ExpectedEvaluationJobs() const {
    std::vector<ExpectedInput> want;
    for (int j = 0; j < items_; ++j) {
      if (Invalid(j)) continue;  // whole job excluded
      const int i = GroupOf(j);
      const std::string job = EvaluationJobId(j);
      want.push_back({job, kEvaluationJobVersion, ItemId(j), 1, true,
                      TypeUrl("artifact.Item")});
      want.push_back({job, kEvaluationJobVersion, GroupId(i), 1, false,
                      TypeUrl("artifact.Group")});
      want.push_back({job, kEvaluationJobVersion, ConfigurationId(i), 1, false,
                      TypeUrl("artifact.Configuration")});
      want.push_back({job, kEvaluationJobVersion, BatchId(i), 1, false,
                      TypeUrl("artifact.Batch")});
    }
    return want;
  }

  std::vector<ExpectedInput> ExpectedValidationJobs() const {
    std::vector<ExpectedInput> want;
    for (int j = 0; j < items_; ++j) {
      if (Invalid(j)) continue;
      const int i = GroupOf(j);
      const std::string job = ValidationJobId(j);
      want.push_back({job, kValidationJobVersion, EvaluationId(j), 1, true,
                      TypeUrl("artifact.Evaluation")});
      want.push_back({job, kValidationJobVersion, ItemId(j), 1, false,
                      TypeUrl("artifact.Item")});
      want.push_back({job, kValidationJobVersion, ConfigurationId(i), 1, false,
                      TypeUrl("artifact.Configuration")});
      want.push_back({job, kValidationJobVersion, PrimaryRuleId(i), 1, false,
                      TypeUrl("artifact.PrimaryRule")});
      want.push_back({job, kValidationJobVersion, SecondaryRuleId(i), 1, false,
                      TypeUrl("artifact.SecondaryRule")});
    }
    return want;
  }

  std::vector<ExpectedInput> ExpectedAggregateJobs() const {
    std::vector<ExpectedInput> want;
    for (int i = 0; i < kGroups; ++i) {
      const std::string job = AggregateJobId(i);
      const int64_t version = AggregateJobVersion(i);
      want.push_back(
          {job, version, BatchId(i), 1, true, TypeUrl("artifact.Batch")});
      want.push_back(
          {job, version, GroupId(i), 1, false, TypeUrl("artifact.Group")});
      want.push_back({job, version, ConfigurationId(i), 1, false,
                      TypeUrl("artifact.Configuration")});
      for (int j = i; j < items_; j += kGroups) {
        if (!Included(j)) continue;
        want.push_back({job, version, EvaluationId(j), 1, false,
                        TypeUrl("artifact.Evaluation")});
        want.push_back({job, version, ValidationId(j), 1, false,
                        TypeUrl("artifact.Validation")});
      }
    }
    return want;
  }

 private:
  static constexpr int64_t kEvaluationJobVersion = 4;
  static constexpr int64_t kValidationJobVersion = 5;
  int64_t AggregateJobVersion(int i) const {
    int64_t version = 3;
    for (int j = i; j < items_; j += kGroups) {
      if (Included(j)) version += 2;
    }
    return version;
  }

  static std::string Uuid(int kind, int n) {
    return absl::StrFormat("%08x-0000-4000-8000-%012d", kind, n);
  }
  static std::string ItemId(int n) { return Uuid(0xa, n); }
  static std::string GroupId(int n) { return Uuid(0xb, n); }
  static std::string ConfigurationId(int n) { return Uuid(0xc, n); }
  static std::string BatchId(int n) { return Uuid(0xd, n); }
  static std::string PrimaryRuleId(int n) { return Uuid(0x11, n); }
  static std::string SecondaryRuleId(int n) { return Uuid(0x12, n); }
  static std::string EvaluationId(int n) { return Uuid(0xe, n); }
  static std::string EvaluationJobId(int n) { return Uuid(0xf, n); }
  static std::string ValidationId(int n) { return Uuid(0x2f, n); }
  static std::string ValidationJobId(int n) { return Uuid(0x2e, n); }
  static std::string AggregateJobId(int n) { return Uuid(0x3e, n); }

  Timestamp WriteTime() { return MakePastTimestamp(std::chrono::minutes(1)); }

  void AddArtifact(const std::string& id, const std::string& type,
                   const std::string& job_id = "", int64_t job_version = 0) {
    heads_.push_back(MakeInsert(
        "benchmark.artifact_head",
        {"artifact_id", "workspace_id", "type", "modified_timestamp"}, id,
        kWorkspaceId, type, WriteTime()));
    std::string producer =
        job_id.empty() ? ""
                       : absl::StrFormat(R"(,"job_id":"%s","job_version":%d)",
                                         job_id, job_version);
    artifacts_.push_back(MakeInsert(
        "benchmark.artifact", {"payload_data", "modified_timestamp"},
        Json(absl::StrFormat(
            R"({"id":"%s","version":1,"workspace_id":"%s"%s,"data":{"@type":"%s"},"padding":"%s"})",
            id, kWorkspaceId, producer, type, Filler(id, payload_bytes_))),
        WriteTime()));
    latest_.push_back(
        MakeInsert("benchmark.latest_artifact",
                   {"artifact_id", "revision_version", "modified_timestamp"},
                   id, int64_t{1}, WriteTime()));
  }

  void AddJob(const std::string& id, int64_t version, const std::string& type) {
    jobs_.push_back(MakeInsert(
        "benchmark.job", {"payload_data", "modified_timestamp"},
        Json(absl::StrFormat(
            R"({"id":"%s","version":%d,"workspace_id":"%s","data":{"@type":"%s"},"padding":"%s"})",
            id, version, kWorkspaceId, type,
            Filler(absl::StrCat(id, version), payload_bytes_ / 4))),
        WriteTime()));
  }

  void AddInput(const std::string& artifact_id, const std::string& job_id,
                int64_t job_version, bool strong, const std::string& job_type) {
    inputs_.push_back(MakeInsert(
        "benchmark.artifact_job",
        {"artifact_id", "revision_version", "job_id", "job_revision_version",
         "strong", "modified_timestamp", "job_type"},
        artifact_id, int64_t{1}, job_id, job_version, strong, WriteTime(),
        job_type));
  }

  void AddInvalidFlag(const std::string& artifact_id) {
    flags_.push_back(MakeInsert(
        "benchmark.flag", {"payload_data", "modified_timestamp"},
        Json(absl::StrFormat(
            R"({"artifact_id":"%s","version":1,"flag_id":"invalid","workspace_id":"%s","data":{"@type":"%s"}})",
            artifact_id, kWorkspaceId, kInvalidFlagType)),
        WriteTime()));
  }

  absl::Status Flush() {
    for (Mutations* batch :
         {&heads_, &artifacts_, &latest_, &jobs_, &inputs_, &flags_}) {
      constexpr size_t kChunk = 1000;
      for (size_t i = 0; i < batch->size(); i += kChunk) {
        Mutations chunk(batch->begin() + i,
                        batch->begin() + std::min(i + kChunk, batch->size()));
        GOOGLESQL_RETURN_IF_ERROR(Commit(std::move(chunk)).status());
      }
      batch->clear();
    }
    return absl::OkStatus();
  }

 protected:
  // Stage A: the workspace's fixed artifacts, its items, and one pending
  // evaluation job per item.
  absl::Status SeedStageA() {
    const std::string evaluation_job_type = TypeUrl("job.EvaluationJob");
    for (int i = 0; i < kGroups; ++i) {
      AddArtifact(GroupId(i), TypeUrl("artifact.Group"));
      AddArtifact(ConfigurationId(i), TypeUrl("artifact.Configuration"));
      AddArtifact(BatchId(i), TypeUrl("artifact.Batch"));
      AddArtifact(PrimaryRuleId(i), TypeUrl("artifact.PrimaryRule"));
      AddArtifact(SecondaryRuleId(i), TypeUrl("artifact.SecondaryRule"));
    }
    for (int j = 0; j < items_; ++j) {
      const int i = GroupOf(j);
      AddArtifact(ItemId(j), TypeUrl("artifact.Item"));
      if (Invalid(j)) AddInvalidFlag(ItemId(j));
      AddJob(EvaluationJobId(j), kEvaluationJobVersion, evaluation_job_type);
      AddInput(ItemId(j), EvaluationJobId(j), kEvaluationJobVersion, true,
               evaluation_job_type);
      AddInput(GroupId(i), EvaluationJobId(j), kEvaluationJobVersion, false,
               evaluation_job_type);
      AddInput(ConfigurationId(i), EvaluationJobId(j), kEvaluationJobVersion,
               false, evaluation_job_type);
      AddInput(BatchId(i), EvaluationJobId(j), kEvaluationJobVersion, false,
               evaluation_job_type);
    }
    return Flush();
  }

  // Stage B: the evaluations those jobs produced, and a pending validation
  // job per evaluation.
  absl::Status SeedStageB() {
    const std::string job_type = TypeUrl("job.ValidationJob");
    for (int j = 0; j < items_; ++j) {
      const int i = GroupOf(j);
      AddArtifact(EvaluationId(j), TypeUrl("artifact.Evaluation"),
                  EvaluationJobId(j), kEvaluationJobVersion);
      AddJob(ValidationJobId(j), kValidationJobVersion, job_type);
      AddInput(EvaluationId(j), ValidationJobId(j), kValidationJobVersion, true,
               job_type);
      AddInput(ItemId(j), ValidationJobId(j), kValidationJobVersion, false,
               job_type);
      AddInput(ConfigurationId(i), ValidationJobId(j), kValidationJobVersion,
               false, job_type);
      AddInput(PrimaryRuleId(i), ValidationJobId(j), kValidationJobVersion,
               false, job_type);
      AddInput(SecondaryRuleId(i), ValidationJobId(j), kValidationJobVersion,
               false, job_type);
    }
    return Flush();
  }

  // Stage C: the checks those jobs produced, and one pending aggregate job per
  // group over its included items.
  absl::Status SeedStageC() {
    const std::string job_type = TypeUrl("job.AggregateJob");
    for (int j = 0; j < items_; ++j) {
      AddArtifact(ValidationId(j), TypeUrl("artifact.Validation"),
                  ValidationJobId(j), kValidationJobVersion);
    }
    for (int i = 0; i < kGroups; ++i) {
      const int64_t version = AggregateJobVersion(i);
      AddJob(AggregateJobId(i), version, job_type);
      AddInput(BatchId(i), AggregateJobId(i), version, true, job_type);
      AddInput(GroupId(i), AggregateJobId(i), version, false, job_type);
      AddInput(ConfigurationId(i), AggregateJobId(i), version, false, job_type);
      for (int j = i; j < items_; j += kGroups) {
        if (!Included(j)) continue;
        AddInput(EvaluationId(j), AggregateJobId(i), version, false, job_type);
        AddInput(ValidationId(j), AggregateJobId(i), version, false, job_type);
      }
    }
    return Flush();
  }

 private:
  Mutations heads_, artifacts_, latest_, jobs_, inputs_, flags_;
};

// The three producer stages against the database each one would see. Stages
// are cumulative, so they run in one test against one database.
TEST_F(GraphProducerBenchmark, ProducerStages) {
  std::set<std::string> stages = {"a", "b", "c"};
  if (const char* env = std::getenv("GRAPH_BENCH_STAGES"); env != nullptr) {
    stages = absl::StrSplit(env, ',', absl::SkipEmpty());
  }
  if (const char* env = std::getenv("GRAPH_BENCH_VARIANTS"); env != nullptr) {
    selected_variants_ = absl::StrSplit(env, ',', absl::SkipEmpty());
  }
  if (stages.contains("a")) {
    RunStage("evaluation", TypeUrl("job.EvaluationJob"),
             ExpectedEvaluationJobs());
  }
  GOOGLESQL_ASSERT_OK(SeedStageB());
  if (stages.contains("b")) {
    RunStage("validation", TypeUrl("job.ValidationJob"),
             ExpectedValidationJobs());
  }
  GOOGLESQL_ASSERT_OK(SeedStageC());
  if (stages.contains("c")) {
    RunStage("aggregate", TypeUrl("job.AggregateJob"), ExpectedAggregateJobs());
  }
}

class ContendedCommitBenchmark : public DatabaseTest {
 protected:
  absl::Status SetUpDatabase() override {
    return SetSchema(
        {"CREATE TABLE RetryCounter (Id INT64 NOT NULL, "
         "CounterValue INT64 NOT NULL) PRIMARY KEY (Id)"});
  }
};

// Unlike the graph fixtures' serial setup writes, these transactions contend
// on the same row. The client must retry aborted transactions without losing
// or duplicating committed increments.
TEST_F(ContendedCommitBenchmark, RetriesPreserveEveryIncrement) {
  GOOGLESQL_ASSERT_OK(Insert("RetryCounter", {"Id", "CounterValue"}, {1, 0}));
  constexpr int kWriters = 4;
  constexpr int kIncrements = 50;
  std::atomic<bool> start{false};
  std::atomic<int> attempts{0};
  std::atomic<int> committed{0};
  std::vector<std::thread> writers;
  for (int i = 0; i < kWriters; ++i) {
    writers.emplace_back([&] {
      while (!start.load()) std::this_thread::yield();
      for (int j = 0; j < kIncrements; ++j) {
        auto result = client().Commit(
            [&](Transaction const& txn) -> google::cloud::StatusOr<Mutations> {
              ++attempts;
              auto updated = client().ExecuteDml(
                  txn,
                  SqlStatement("UPDATE RetryCounter SET "
                               "CounterValue = CounterValue + 1 WHERE Id = 1"));
              if (!updated) return updated.status();
              // Leave time for another transaction to request the held lock.
              std::this_thread::sleep_for(std::chrono::milliseconds(2));
              return Mutations{};
            });
        if (!result) {
          ADD_FAILURE() << result.status();
          return;
        }
        ++committed;
      }
    });
  }
  start.store(true);
  for (auto& writer : writers) writer.join();
  EXPECT_EQ(committed.load(), kWriters * kIncrements);
  EXPECT_GT(attempts.load(), committed.load());
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto rows, Query("SELECT CounterValue FROM RetryCounter WHERE Id = 1"));
  ASSERT_EQ(rows.size(), 1);
  auto value = rows[0].values()[0].get<int64_t>();
  ASSERT_TRUE(value.ok()) << value.status();
  EXPECT_EQ(*value, kWriters * kIncrements);
  ABSL_LOG(INFO) << "GRAPH_RETRIES abort_probability="
                 << config::abort_current_transaction_probability()
                 << " attempts=" << attempts.load()
                 << " committed=" << committed.load();
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  testing::AddGlobalTestEnvironment(
      new google::spanner::emulator::test::GraphProducerBenchmarkEnvironment());
  const int result = RUN_ALL_TESTS();
  // Orderly destruction after a workspace this size has aborted the emulator in
  // teardown; every result is already aggregateed.
  std::fflush(stdout);
  std::fflush(stderr);
  std::_Exit(result);
}
