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

// Times four candidate-selection queries over a synthetic artifact pipeline.
// Each scanner combines stage-specific graph CTEs with a shared suffix that
// suppresses existing jobs and hydrates the remaining candidates.
//
// The fixture exercises version history, shared configuration, optional flags,
// prior outputs, pending-job suppression, and aggregate membership. Every row
// is checked against exact artifact IDs, versions, types, flags, input roles,
// and revision sums. Payloads contain deterministic varied filler.
//
// Scale with GRAPH_BENCH_ITEMS (default 310), GRAPH_BENCH_ITERATIONS (default
// 3), GRAPH_BENCH_PAYLOAD_BYTES (default 1200), and GRAPH_BENCH_VERSIONS
// (default 2). GRAPH_BENCH_SCANNERS selects evaluation, validation, dispatch,
// or aggregate. GRAPH_BENCH_DROP_DATABASE=1 and GRAPH_BENCH_EXIT_NORMALLY=1
// exercise teardown. Peak RSS includes the embedded emulator and the benchmark
// client.

#include <algorithm>
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
constexpr char kWorkspaceId[] = "pipeline-scan-benchmark";
constexpr char kTypePrefix[] = "type.googleapis.com/benchmark.pipeline.";
constexpr char kUuidMin[] = "00000000-0000-0000-0000-000000000000";
constexpr char kUuidMax[] = "ffffffff-ffff-ffff-ffff-ffffffffffff";

constexpr int kGroups = 50;
constexpr int kPrimaryRulesPerConfiguration = 10;
constexpr int kSecondaryRulesPerConfiguration = 2;
// Items at the tail of the range have no evaluation yet, so the evaluation
// scanner has candidates to emit.
constexpr int kItemsWithoutEvaluation = 10;
// Every kVersionedItemStride-th item carries historical versions.
constexpr int kVersionedItemStride = 3;

std::string TypeUrl(absl::string_view suffix) {
  return absl::StrCat(kTypePrefix, suffix);
}

int EnvInt(const char* name, int default_value) {
  const char* value = std::getenv(name);
  return value == nullptr ? default_value : std::atoi(value);
}

absl::StatusOr<std::string> ReadDataFile(absl::string_view name) {
  std::string path = absl::StrCat(GetRunfilesDir(kDataDir), "/", name);
  std::ifstream in(path);
  if (!in) {
    return absl::NotFoundError(absl::StrCat("Cannot read ", path));
  }
  std::stringstream buffer;
  buffer << in.rdbuf();
  return buffer.str();
}

// Drop line comments and split the fixture DDL on semicolons.
std::vector<std::string> SplitDdl(absl::string_view ddl) {
  std::string stripped;
  for (absl::string_view line : absl::StrSplit(ddl, '\n')) {
    size_t comment = line.find("--");
    if (comment != absl::string_view::npos) {
      line = line.substr(0, comment);
    }
    absl::StrAppend(&stripped, line, "\n");
  }
  std::vector<std::string> statements;
  for (absl::string_view statement : absl::StrSplit(stripped, ';')) {
    std::string trimmed(absl::StripAsciiWhitespace(statement));
    if (!trimmed.empty()) {
      statements.push_back(std::move(trimmed));
    }
  }
  return statements;
}

// Parameter names referenced as @name in `sql`, excluding @{hints}.
std::vector<std::string> ReferencedParams(absl::string_view sql) {
  std::vector<std::string> names;
  for (size_t i = 0; i < sql.size(); ++i) {
    if (sql[i] != '@' || i + 1 >= sql.size() ||
        !(absl::ascii_isalpha(sql[i + 1]) || sql[i + 1] == '_')) {
      continue;
    }
    size_t end = i + 1;
    while (end < sql.size() &&
           (absl::ascii_isalnum(sql[end]) || sql[end] == '_')) {
      ++end;
    }
    names.emplace_back(sql.substr(i + 1, end - i - 1));
    i = end;
  }
  return names;
}

// Peak resident set size of this process in kB, from /proc. This process
// hosts the emulator, so this is the emulator's peak. -1 where unavailable.
int64_t PeakRssKb() {
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    if (absl::StartsWith(line, "VmHWM:")) {
      std::vector<std::string> parts =
          absl::StrSplit(line, ' ', absl::SkipEmpty());
      if (parts.size() >= 2) {
        return std::atoll(parts[1].c_str());
      }
    }
  }
  return -1;
}

// Deterministic varied filler exercises payload compression and hashing.
std::string Filler(absl::string_view seed, int bytes) {
  std::string out;
  out.reserve(bytes + 40);
  uint32_t state = 2166136261u;
  for (char c : seed) {
    state = (state ^ static_cast<uint8_t>(c)) * 16777619u;
  }
  static constexpr char kAlphabet[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
  while (static_cast<int>(out.size()) < bytes) {
    state = state * 1664525u + 1013904223u;
    out.push_back(kAlphabet[(state >> 24) % (sizeof(kAlphabet) - 1)]);
  }
  return out;
}

class GraphScannerBenchmarkEnvironment : public testing::Environment {
 public:
  GraphScannerBenchmarkEnvironment()
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

  // No TearDown: main() leaves with _Exit after the tests, because stopping
  // the server and destroying its state has aborted the process.

 private:
  std::unique_ptr<frontend::Server> server_;
  std::unique_ptr<ConformanceTestGlobals> globals_;
  ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

// One artifact the fixture expects in a candidate: the exact row the common
// suffix hydrates, plus the flag row it carries, if any.
struct ExpectedArtifact {
  std::string artifact_id;
  int64_t revision_version;
  std::string type;
  std::string flag_id;  // empty when the artifact has no flag row
};

// What the fixture expects a scanner to emit for one candidate.
struct ExpectedCandidate {
  // role -> the artifacts in that role, in any order.
  std::map<std::string, std::vector<ExpectedArtifact>> roles;
  std::string strong_role;
  // Version contributed by revision-only rows, which hydrate nothing.
  int64_t revision_only_version = 0;

  int64_t rows() const {
    int64_t n = 0;
    for (const auto& [role, artifacts] : roles) {
      for (const ExpectedArtifact& artifact : artifacts) {
        n += artifact.flag_id.empty() ? 1 : 2;
      }
    }
    return n;
  }
};

class GraphScannerBenchmark : public DatabaseTest {
 protected:
  absl::Status SetUpDatabase() override {
    items_ = EnvInt("GRAPH_BENCH_ITEMS", 310);
    iterations_ = EnvInt("GRAPH_BENCH_ITERATIONS", 3);
    payload_bytes_ = EnvInt("GRAPH_BENCH_PAYLOAD_BYTES", 1200);
    versions_ = std::max(1, EnvInt("GRAPH_BENCH_VERSIONS", 2));
    GOOGLESQL_ASSIGN_OR_RETURN(std::string ddl,
                               ReadDataFile("pipeline_schema.sql"));
    GOOGLESQL_RETURN_IF_ERROR(SetSchema(SplitDdl(ddl)));
    GOOGLESQL_ASSIGN_OR_RETURN(suffix_,
                               ReadDataFile("common_scanner_suffix.sql"));
    return PopulateWorkspace();
  }

  // Runs the scanner `iterations_` times, verifies every result against
  // `expected`, and logs each duration and the peak RSS so far.
  void RunScanner(absl::string_view name,
                  const std::map<std::string, ExpectedCandidate>& expected) {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        std::string prefix, ReadDataFile(absl::StrCat(name, "_scanner.sql")));
    std::string sql = absl::StrCat(prefix, suffix_);
    SqlStatement::ParamType params;
    for (const std::string& param : ReferencedParams(sql)) {
      auto it = all_params_.find(param);
      ASSERT_NE(it, all_params_.end()) << "No value for @" << param;
      params[param] = it->second;
    }
    int64_t expected_rows = 0;
    for (const auto& [key, candidate] : expected) {
      expected_rows += candidate.rows();
    }
    absl::Duration best = absl::InfiniteDuration();
    for (int i = 0; i < iterations_; ++i) {
      absl::Time start = absl::Now();
      auto rows = QueryWithParams(sql, params);
      absl::Duration elapsed = absl::Now() - start;
      GOOGLESQL_ASSERT_OK(rows.status()) << name << " scanner";
      EXPECT_EQ(static_cast<int64_t>(rows->size()), expected_rows)
          << name << " scanner rows";
      VerifyRows(name, *rows, expected);
      best = std::min(best, elapsed);
      ABSL_LOG(INFO) << absl::StrFormat(
          "GRAPH_BENCH scanner=%s items=%d iteration=%d rows=%d ms=%d "
          "peak_rss_kb=%d",
          name, items_, i, rows->size(), absl::ToInt64Milliseconds(elapsed),
          PeakRssKb());
    }
    ABSL_LOG(INFO) << absl::StrFormat(
        "GRAPH_BENCH scanner=%s items=%d best_ms=%d peak_rss_kb=%d", name,
        items_, absl::ToInt64Milliseconds(best), PeakRssKb());
  }

  // Dropping a database this size used to abort the emulator in absl::Mutex
  // after the test had passed (see main). The process leaves right after, so
  // the database is only dropped when GRAPH_BENCH_DROP_DATABASE=1 asks for the
  // teardown path to be exercised.
  void TearDown() override {
    if (EnvInt("GRAPH_BENCH_DROP_DATABASE", 0) != 0) {
      DatabaseTest::TearDown();
    }
  }

  int items_with_evaluation() const { return items_ - kItemsWithoutEvaluation; }

  // Number of versions an item (and its evaluation) carries.
  int VersionsOf(int j) const {
    return j % kVersionedItemStride == 0 ? versions_ : 1;
  }

  bool InDispatchGroup(int j) const { return j % kGroups == 0; }

  // Historical dispatch: produced for the old evaluation version. The item is
  // a candidate again and carries a previous_dispatch input.
  bool HasHistoricalDispatch(int j) const {
    return j < items_with_evaluation() && InDispatchGroup(j) &&
           VersionsOf(j) > 1;
  }

  // Every other evaluated item has a validation check, produced for the
  // first evaluation version. On a single-version item that job matches
  // the current version and suppresses the validation candidate.
  bool HasValidation(int j) const {
    return j < items_with_evaluation() && j % 2 == 0;
  }
  bool HasCurrentValidation(int j) const {
    return HasValidation(j) && VersionsOf(j) == 1;
  }
  static bool CheckComplete(int j) { return j % 4 != 0; }
  static bool EvaluationSelected(int j) { return j % 5 != 0; }
  bool HasIncompleteFlag(int j) const { return HasValidation(j) && j % 6 == 0; }
  // A check the aggregate scanner records as membership rather than version.
  bool Included(int j) const {
    return HasValidation(j) && CheckComplete(j) && EvaluationSelected(j) &&
           !HasIncompleteFlag(j);
  }

  // Pending dispatch: job written at the current version, nothing produced yet.
  // The existing-job check suppresses the candidate.
  bool HasPendingDispatchJob(int j) const {
    return j < items_with_evaluation() && InDispatchGroup(j) &&
           VersionsOf(j) == 1 && (j / kGroups) % 2 == 1;
  }

  // The fixture's artifacts by role, at the versions the scanners see.
  ExpectedArtifact Item(int j) const {
    return {ItemId(j), VersionsOf(j), TypeUrl("artifact.Item"),
            absl::StrCat("flag-", j)};
  }
  ExpectedArtifact Evaluation(int j) const {
    return {EvaluationId(j), VersionsOf(j), TypeUrl("artifact.Evaluation"), ""};
  }
  static ExpectedArtifact Group(int i) {
    return {GroupId(i), 1, TypeUrl("artifact.Group"), ""};
  }
  static ExpectedArtifact Configuration(int i) {
    return {ConfigurationId(i), 1, TypeUrl("artifact.Configuration"), ""};
  }
  static ExpectedArtifact Batch(int i) {
    return {BatchId(i), 1, TypeUrl("artifact.Batch"), ""};
  }
  static std::vector<ExpectedArtifact> PrimaryRules(int i) {
    std::vector<ExpectedArtifact> rules;
    for (int k = 0; k < kPrimaryRulesPerConfiguration; ++k) {
      rules.push_back(
          {PrimaryRuleId(i, k), 1, TypeUrl("artifact.PrimaryRule"), ""});
    }
    return rules;
  }
  static std::vector<ExpectedArtifact> SecondaryRules(int i) {
    std::vector<ExpectedArtifact> rules;
    for (int k = 0; k < kSecondaryRulesPerConfiguration; ++k) {
      rules.push_back(
          {SecondaryRuleId(i, k), 1, TypeUrl("artifact.SecondaryRule"), ""});
    }
    return rules;
  }
  // The dispatch produced for item j's first evaluation version.
  static ExpectedArtifact HistoricalDispatch(int j) {
    return {DispatchId(j), 1, TypeUrl("artifact.Dispatch"), ""};
  }
  ExpectedArtifact Validation(int j) const {
    return {ValidationId(j), 1, TypeUrl("artifact.Validation"), ""};
  }

  std::map<std::string, ExpectedCandidate> ExpectedEvaluation() const {
    std::map<std::string, ExpectedCandidate> expected;
    for (int j = items_with_evaluation(); j < items_; ++j) {
      const int group = j % kGroups;
      ExpectedCandidate c;
      c.roles["item"] = {Item(j)};
      c.roles["group"] = {Group(group)};
      c.roles["configuration"] = {Configuration(group)};
      c.roles["batch"] = {Batch(group)};
      c.strong_role = "item";
      expected[absl::StrCat(ItemId(j), "|", BatchId(group))] = c;
    }
    return expected;
  }

  // One candidate per batch. Included checks contribute their
  // evaluation and check as members; every other check moves the version
  // through the revision-only row.
  std::map<std::string, ExpectedCandidate> ExpectedAggregate() const {
    std::map<std::string, ExpectedCandidate> expected;
    for (int i = 0; i < kGroups; ++i) {
      ExpectedCandidate c;
      c.roles["batch"] = {Batch(i)};
      c.roles["group"] = {Group(i)};
      c.roles["configuration"] = {Configuration(i)};
      c.strong_role = "batch";
      for (int j = i; j < items_with_evaluation(); j += kGroups) {
        if (!HasValidation(j)) continue;
        if (Included(j)) {
          c.roles["evaluation"].push_back(Evaluation(j));
          c.roles["validation"].push_back(Validation(j));
        } else {
          c.revision_only_version += VersionsOf(j) + 1;
        }
      }
      expected[BatchId(i)] = c;
    }
    return expected;
  }

  std::map<std::string, ExpectedCandidate> ExpectedValidation() const {
    std::map<std::string, ExpectedCandidate> expected;
    for (int j = 0; j < items_with_evaluation(); ++j) {
      if (HasCurrentValidation(j)) continue;
      const int group = j % kGroups;
      ExpectedCandidate c;
      c.roles["evaluation"] = {Evaluation(j)};
      c.roles["item"] = {Item(j)};
      c.roles["configuration"] = {Configuration(group)};
      c.roles["primary_rule"] = PrimaryRules(group);
      c.roles["secondary_rule"] = SecondaryRules(group);
      if (HasHistoricalDispatch(j)) {
        c.roles["dispatch"] = {HistoricalDispatch(j)};
      }
      c.strong_role = "evaluation";
      expected[EvaluationId(j)] = c;
    }
    return expected;
  }

  std::map<std::string, ExpectedCandidate> ExpectedDispatch() const {
    std::map<std::string, ExpectedCandidate> expected;
    for (int j = 0; j < items_with_evaluation(); ++j) {
      if (!InDispatchGroup(j) || HasPendingDispatchJob(j)) continue;
      const int group = j % kGroups;
      ExpectedCandidate c;
      c.roles["evaluation"] = {Evaluation(j)};
      c.roles["item"] = {Item(j)};
      c.roles["configuration"] = {Configuration(group)};
      if (HasHistoricalDispatch(j)) {
        c.roles["previous_dispatch"] = {HistoricalDispatch(j)};
      }
      c.strong_role = "evaluation";
      expected[EvaluationId(j)] = c;
    }
    return expected;
  }

  // Checks every candidate against the fixture: the exact artifacts in each
  // role with their versions, types and flag rows, exactly one strong input,
  // the version sum including revision-only rows, and the per-candidate
  // counts the common suffix aggregates.
  void VerifyRows(absl::string_view name, const std::vector<ValueRow>& rows,
                  const std::map<std::string, ExpectedCandidate>& expected) {
    // (artifact_id, revision_version) -> type or flag id, per role.
    using ArtifactKey = std::pair<std::string, int64_t>;
    struct Seen {
      std::map<std::string, std::map<ArtifactKey, std::string>> artifacts;
      std::map<std::string, std::map<ArtifactKey, std::string>> flags;
      std::set<std::string> strong_roles;
      int64_t version_sum = 0;
      int64_t input_artifact_count = -1;
      int64_t job_revision_version = -1;
      int64_t strong_input_count = -1;
      int artifact_rows = 0;
    };
    std::map<std::string, Seen> seen;
    for (const ValueRow& row : rows) {
      absl::Span<const Value> v = row.values();
      ASSERT_EQ(v.size(), 14) << name << " column count";
      auto key = v[0].get<std::string>();
      auto row_kind = v[1].get<std::string>();
      auto input_artifact_count = v[2].get<int64_t>();
      auto job_revision_version = v[3].get<int64_t>();
      auto strong_input_count = v[4].get<int64_t>();
      auto role = v[5].get<std::string>();
      auto artifact_id = v[6].get<std::string>();
      auto revision_version = v[7].get<int64_t>();
      auto strong = v[8].get<bool>();
      ASSERT_TRUE(key.ok() && row_kind.ok() && input_artifact_count.ok() &&
                  job_revision_version.ok() && strong_input_count.ok() &&
                  role.ok() && artifact_id.ok() && revision_version.ok() &&
                  strong.ok())
          << name << " row has an unexpected shape";
      Seen& s = seen[*key];
      if (s.input_artifact_count < 0) {
        s.input_artifact_count = *input_artifact_count;
        s.job_revision_version = *job_revision_version;
        s.strong_input_count = *strong_input_count;
      } else {
        EXPECT_EQ(s.input_artifact_count, *input_artifact_count) << *key;
        EXPECT_EQ(s.job_revision_version, *job_revision_version) << *key;
        EXPECT_EQ(s.strong_input_count, *strong_input_count) << *key;
      }
      const ArtifactKey artifact_key(*artifact_id, *revision_version);
      if (*row_kind == "artifact") {
        auto artifact_type = v[9].get<std::string>();
        ASSERT_TRUE(artifact_type.ok()) << *key << " " << *role;
        EXPECT_TRUE(v[10].get<Json>().ok()) << *key << " " << *role;
        EXPECT_TRUE(
            s.artifacts[*role].emplace(artifact_key, *artifact_type).second)
            << name << " duplicate artifact row " << *key << " " << *role << " "
            << *artifact_id;
        s.version_sum += *revision_version;
        s.artifact_rows++;
        if (*strong) s.strong_roles.insert(*role);
      } else {
        EXPECT_EQ(*row_kind, "flag") << *key;
        auto flag_id = v[12].get<std::string>();
        ASSERT_TRUE(flag_id.ok()) << *key << " flag_id";
        EXPECT_TRUE(v[13].get<Json>().ok()) << *key << " flag_payload";
        EXPECT_TRUE(s.flags[*role].emplace(artifact_key, *flag_id).second)
            << name << " duplicate flag row " << *key << " " << *artifact_id;
      }
    }
    EXPECT_EQ(seen.size(), expected.size()) << name << " candidates";
    for (const auto& [key, s] : seen) {
      auto it = expected.find(key);
      if (it == expected.end()) {
        ADD_FAILURE() << name << " unexpected candidate " << key;
        continue;
      }
      const ExpectedCandidate& e = it->second;
      std::map<std::string, std::map<ArtifactKey, std::string>> want_artifacts;
      std::map<std::string, std::map<ArtifactKey, std::string>> want_flags;
      int64_t want_version_sum = e.revision_only_version;
      for (const auto& [role, artifacts] : e.roles) {
        for (const ExpectedArtifact& p : artifacts) {
          want_artifacts[role][{p.artifact_id, p.revision_version}] = p.type;
          want_version_sum += p.revision_version;
          if (!p.flag_id.empty()) {
            want_flags[role][{p.artifact_id, p.revision_version}] = p.flag_id;
          }
        }
      }
      EXPECT_EQ(s.artifacts, want_artifacts)
          << name << " artifacts for " << key;
      EXPECT_EQ(s.flags, want_flags) << name << " flags for " << key;
      EXPECT_EQ(s.strong_roles, std::set<std::string>{e.strong_role})
          << name << " strong input for " << key;
      EXPECT_EQ(s.strong_input_count, 1) << key;
      EXPECT_EQ(s.input_artifact_count, s.artifact_rows) << key;
      EXPECT_EQ(s.job_revision_version, want_version_sum) << key;
    }
    for (const auto& [key, e] : expected) {
      EXPECT_TRUE(seen.contains(key)) << name << " missing candidate " << key;
    }
  }

  int items_ = 0;
  int iterations_ = 0;
  int payload_bytes_ = 0;
  int versions_ = 1;

 private:
  using Params = std::map<std::string, Value>;

  static std::string Uuid(int kind, int n) {
    return absl::StrFormat("%08x-0000-4000-8000-%012d", kind, n);
  }
  static std::string ItemId(int n) { return Uuid(0xa, n); }
  static std::string GroupId(int n) { return Uuid(0xb, n); }
  static std::string ConfigurationId(int n) { return Uuid(0xc, n); }
  static std::string BatchId(int n) { return Uuid(0xd, n); }
  static std::string EvaluationId(int n) { return Uuid(0xe, n); }
  static std::string EvaluationJobId(int n) { return Uuid(0xf, n); }
  static std::string DispatchJobId(int n) { return Uuid(0x1e, n); }
  static std::string DispatchId(int n) { return Uuid(0x1f, n); }
  static std::string ValidationJobId(int n) { return Uuid(0x2e, n); }
  static std::string ValidationId(int n) { return Uuid(0x2f, n); }
  static std::string SecondaryRuleId(int configuration, int k) {
    return absl::StrFormat("group-%02d:secondary-rule-%d", configuration, k);
  }
  static std::string PrimaryRuleId(int configuration, int k) {
    return absl::StrFormat("group-%02d:primary-rule-%d", configuration, k);
  }

  Timestamp WriteTime() { return MakePastTimestamp(std::chrono::minutes(1)); }

  // A head plus `versions` artifact versions; latest_artifact points at the
  // newest. Version v is produced by (job_id, job_version(v)) when given.
  // `any_fields` are extra JSON members of the packed artifact message, such as
  // the flags the aggregate scanner filters on.
  void AddArtifact(const std::string& id, const std::string& type,
                   int versions = 1, const std::string& job_id = "",
                   std::function<int64_t(int)> job_version = nullptr,
                   const std::string& any_fields = "") {
    heads_.push_back(MakeInsert(
        "benchmark.artifact_head",
        {"artifact_id", "workspace_id", "type", "modified_timestamp"}, id,
        kWorkspaceId, type, WriteTime()));
    for (int v = 1; v <= versions; ++v) {
      std::string producer;
      if (!job_id.empty()) {
        producer = absl::StrFormat(R"(,"job_id":"%s","job_version":%d)", job_id,
                                   job_version(v));
      }
      std::string payload = absl::StrFormat(
          R"({"id":"%s","version":%d,"workspace_id":"%s"%s,"data":{"@type":"%s"%s},"padding":"%s"})",
          id, v, kWorkspaceId, producer, type, any_fields,
          Filler(absl::StrCat(id, v), payload_bytes_));
      artifacts_.push_back(MakeInsert("benchmark.artifact",
                                      {"payload_data", "modified_timestamp"},
                                      Json(payload), WriteTime()));
    }
    latest_.push_back(
        MakeInsert("benchmark.latest_artifact",
                   {"artifact_id", "revision_version", "modified_timestamp"},
                   id, int64_t{versions}, WriteTime()));
  }

  void AddJob(const std::string& id, int64_t revision_version,
              const std::string& type, const std::string& binding_id = "") {
    std::string binding =
        binding_id.empty()
            ? ""
            : absl::StrFormat(R"(,"binding_id":"%s")", binding_id);
    std::string payload = absl::StrFormat(
        R"({"id":"%s","version":%d,"workspace_id":"%s","data":{"@type":"%s"}%s,"padding":"%s"})",
        id, revision_version, kWorkspaceId, type, binding,
        Filler(absl::StrCat(id, revision_version), payload_bytes_ / 4));
    jobs_.push_back(MakeInsert("benchmark.job",
                               {"payload_data", "modified_timestamp"},
                               Json(payload), WriteTime()));
  }

  void AddInput(const std::string& artifact_id, int64_t revision_version,
                const std::string& job_id, int64_t job_revision_version,
                bool strong, const std::string& job_type) {
    inputs_.push_back(MakeInsert(
        "benchmark.artifact_job",
        {"artifact_id", "revision_version", "job_id", "job_revision_version",
         "strong", "modified_timestamp", "job_type"},
        artifact_id, revision_version, job_id, job_revision_version, strong,
        WriteTime(), job_type));
  }

  void AddRelation(const std::string& source, const std::string& relation,
                   const std::string& dest) {
    relations_.push_back(
        MakeInsert("benchmark.artifact_relation",
                   {"source_artifact_id", "relation_type", "dest_artifact_id",
                    "workspace_id", "modified_timestamp"},
                   source, relation, dest, kWorkspaceId, WriteTime()));
  }

  void AddFlag(const std::string& artifact_id, int64_t revision_version,
               const std::string& flag_id,
               const std::string& type = TypeUrl("artifact.Flag")) {
    std::string payload = absl::StrFormat(
        R"({"artifact_id":"%s","version":%d,"flag_id":"%s","workspace_id":"%s","data":{"@type":"%s"},"padding":"%s"})",
        artifact_id, revision_version, flag_id, kWorkspaceId, type,
        Filler(absl::StrCat(artifact_id, flag_id), payload_bytes_ / 6));
    flags_.push_back(MakeInsert("benchmark.flag",
                                {"payload_data", "modified_timestamp"},
                                Json(payload), WriteTime()));
  }

  absl::Status CommitInChunks(Mutations& mutations) {
    constexpr size_t kChunk = 1000;
    for (size_t i = 0; i < mutations.size(); i += kChunk) {
      Mutations chunk(
          mutations.begin() + i,
          mutations.begin() + std::min(i + kChunk, mutations.size()));
      GOOGLESQL_RETURN_IF_ERROR(Commit(std::move(chunk)).status());
    }
    mutations.clear();
    return absl::OkStatus();
  }

  absl::Status PopulateWorkspace() {
    const std::string item_type = TypeUrl("artifact.Item");
    const std::string evaluation_type = TypeUrl("artifact.Evaluation");
    const std::string group_type = TypeUrl("artifact.Group");
    const std::string configuration_type = TypeUrl("artifact.Configuration");
    const std::string batch_type = TypeUrl("artifact.Batch");
    const std::string primary_rule_type = TypeUrl("artifact.PrimaryRule");
    const std::string secondary_rule_type = TypeUrl("artifact.SecondaryRule");
    const std::string dispatch_type = TypeUrl("artifact.Dispatch");
    const std::string evaluation_job_type = TypeUrl("job.EvaluationJob");
    const std::string dispatch_job_type = TypeUrl("job.DispatchJob");
    const std::string validation_type = TypeUrl("artifact.Validation");
    const std::string validation_job_type = TypeUrl("job.ValidationJob");
    const std::string incomplete_flag_type =
        "type.googleapis.com/benchmark.pipeline.flag.Incomplete";

    for (int i = 0; i < kGroups; ++i) {
      AddArtifact(GroupId(i), group_type);
      AddArtifact(ConfigurationId(i), configuration_type);
      AddArtifact(BatchId(i), batch_type);
      AddRelation(GroupId(i), "HAS", ConfigurationId(i));
      AddRelation(ConfigurationId(i), "HAS_BATCH", BatchId(i));
      for (int k = 0; k < kPrimaryRulesPerConfiguration; ++k) {
        AddArtifact(PrimaryRuleId(i, k), primary_rule_type);
        AddRelation(ConfigurationId(i), "HAS_PRIMARY_RULE",
                    PrimaryRuleId(i, k));
      }
      for (int k = 0; k < kSecondaryRulesPerConfiguration; ++k) {
        AddArtifact(SecondaryRuleId(i, k), secondary_rule_type);
        AddRelation(ConfigurationId(i), "HAS_SECONDARY_RULE",
                    SecondaryRuleId(i, k));
      }
    }

    for (int j = 0; j < items_; ++j) {
      const int group = j % kGroups;
      const int versions = VersionsOf(j);
      AddArtifact(ItemId(j), item_type, versions);
      AddRelation(ItemId(j), "MEMBER_OF", GroupId(group));
      for (int v = 1; v <= versions; ++v) {
        AddFlag(ItemId(j), v, absl::StrCat("flag-", j));
      }
      if (j >= items_with_evaluation()) continue;

      // An evaluation job's revision version is the sum of its inputs': the
      // item at version v plus group, configuration and batch at 1.
      // One job version per item version, each producing the evaluation
      // at that version.
      auto evaluation_job_version = [](int v) -> int64_t { return v + 3; };
      for (int v = 1; v <= versions; ++v) {
        const int64_t job_version = evaluation_job_version(v);
        AddJob(EvaluationJobId(j), job_version, evaluation_job_type);
        AddInput(ItemId(j), v, EvaluationJobId(j), job_version,
                 /*strong=*/true, evaluation_job_type);
        AddInput(GroupId(group), 1, EvaluationJobId(j), job_version,
                 /*strong=*/false, evaluation_job_type);
        AddInput(ConfigurationId(group), 1, EvaluationJobId(j), job_version,
                 /*strong=*/false, evaluation_job_type);
        AddInput(BatchId(group), 1, EvaluationJobId(j), job_version,
                 /*strong=*/false, evaluation_job_type);
      }
      AddArtifact(EvaluationId(j), evaluation_type, versions,
                  EvaluationJobId(j), evaluation_job_version,
                  EvaluationSelected(j) ? R"(,"selected":true)"
                                        : R"(,"selected":false)");

      // A validation check produced for the first evaluation version. Its
      // job's revision version is evaluation, item and configuration at version
      // one plus every rule of the configuration.
      if (HasValidation(j)) {
        const int64_t check_job_version = 1 + 1 + 1 +
                                          kPrimaryRulesPerConfiguration +
                                          kSecondaryRulesPerConfiguration;
        AddJob(ValidationJobId(j), check_job_version, validation_job_type);
        AddInput(EvaluationId(j), 1, ValidationJobId(j), check_job_version,
                 /*strong=*/true, validation_job_type);
        AddInput(ItemId(j), 1, ValidationJobId(j), check_job_version,
                 /*strong=*/false, validation_job_type);
        AddInput(ConfigurationId(group), 1, ValidationJobId(j),
                 check_job_version, /*strong=*/false, validation_job_type);
        AddArtifact(
            ValidationId(j), validation_type, 1, ValidationJobId(j),
            [check_job_version](int) { return check_job_version; },
            CheckComplete(j) ? R"(,"complete":true)" : R"(,"complete":false)");
        if (HasIncompleteFlag(j)) {
          AddFlag(ValidationId(j), 1, "incomplete-validation",
                  incomplete_flag_type);
        }
      }

      // An dispatch job's revision version is evaluation + item +
      // configuration, all at the evaluation's version except the configuration
      // at 1.
      if (HasHistoricalDispatch(j)) {
        const int64_t dispatch_job_version = 1 + 1 + 1;
        AddJob(DispatchJobId(j), dispatch_job_version, dispatch_job_type,
               DispatchBindingId());
        AddInput(EvaluationId(j), 1, DispatchJobId(j), dispatch_job_version,
                 /*strong=*/true, dispatch_job_type);
        AddInput(ItemId(j), 1, DispatchJobId(j), dispatch_job_version,
                 /*strong=*/false, dispatch_job_type);
        AddInput(ConfigurationId(group), 1, DispatchJobId(j),
                 dispatch_job_version,
                 /*strong=*/false, dispatch_job_type);
        AddArtifact(
            DispatchId(j), dispatch_type, 1, DispatchJobId(j),
            [dispatch_job_version](int) { return dispatch_job_version; });
      } else if (HasPendingDispatchJob(j)) {
        const int64_t dispatch_job_version = 1 + 1 + 1;
        AddJob(DispatchJobId(j), dispatch_job_version, dispatch_job_type,
               DispatchBindingId());
        AddInput(EvaluationId(j), 1, DispatchJobId(j), dispatch_job_version,
                 /*strong=*/true, dispatch_job_type);
        AddInput(ItemId(j), 1, DispatchJobId(j), dispatch_job_version,
                 /*strong=*/false, dispatch_job_type);
        AddInput(ConfigurationId(group), 1, DispatchJobId(j),
                 dispatch_job_version,
                 /*strong=*/false, dispatch_job_type);
      }
    }

    GOOGLESQL_RETURN_IF_ERROR(CommitInChunks(heads_));
    GOOGLESQL_RETURN_IF_ERROR(CommitInChunks(artifacts_));
    GOOGLESQL_RETURN_IF_ERROR(CommitInChunks(latest_));
    GOOGLESQL_RETURN_IF_ERROR(CommitInChunks(jobs_));
    GOOGLESQL_RETURN_IF_ERROR(CommitInChunks(inputs_));
    GOOGLESQL_RETURN_IF_ERROR(CommitInChunks(relations_));
    GOOGLESQL_RETURN_IF_ERROR(CommitInChunks(flags_));

    all_params_ = {
        {"workspace_id", Value(std::string(kWorkspaceId))},
        {"artifact_id_start", Value(std::string(kUuidMin))},
        {"artifact_id_end", Value(std::string(kUuidMax))},
        {"job_id_start", Value(std::string(kUuidMin))},
        {"job_id_end", Value(std::string(kUuidMax))},
        {"item_type", Value(item_type)},
        {"evaluation_type", Value(evaluation_type)},
        {"group_type", Value(group_type)},
        {"configuration_type", Value(configuration_type)},
        {"batch_type", Value(batch_type)},
        {"primary_rule_type", Value(primary_rule_type)},
        {"secondary_rule_type", Value(secondary_rule_type)},
        {"dispatch_type", Value(dispatch_type)},
        {"delivery_type", Value(TypeUrl("artifact.Delivery"))},
        {"receipt_type", Value(TypeUrl("artifact.ReceiptResult"))},
        {"evaluation_job_type", Value(evaluation_job_type)},
        {"dispatch_job_type", Value(dispatch_job_type)},
        {"delivery_job_type", Value(TypeUrl("job.DeliveryJob"))},
        {"receipt_job_type", Value(TypeUrl("job.ReceiptJob"))},
        {"primary_rule_artifact_id", Value(PrimaryRuleId(0, 0))},
        {"validation_type", Value(validation_type)},
        {"validation_job_type", Value(validation_job_type)},
        {"incomplete_validation_flag_type", Value(incomplete_flag_type)},
    };
    return absl::OkStatus();
  }

 protected:
  // Per-scanner values for @job_type and @task_binding_id.
  void SetScannerParams(const std::string& job_type, Value task_binding_id) {
    all_params_["job_type"] = Value(job_type);
    all_params_["task_binding_id"] = std::move(task_binding_id);
  }

  static std::string DispatchBindingId() { return PrimaryRuleId(0, 0); }

 private:
  std::string suffix_;
  Params all_params_;
  Mutations heads_, artifacts_, latest_, jobs_, inputs_, relations_, flags_;
};

// All four scanners against one populated database, so the database is
// created and dropped once. Dropping it between scanners, and the process
// teardown after, have both aborted the emulator in absl::Mutex mid-run.
//
// Evaluation: every item without an evaluation is a candidate keyed by
// item|batch; evaluated items are suppressed by their current job.
// Validation: every evaluation is a candidate with the evaluation, item,
// configuration, every dispatch and delivery rule of the configuration, any
// dispatch already sent for an earlier evaluation version, and the item's flag.
// Dispatch: one binding, the first dispatch rule of group 0's configuration.
// Candidates are that group's evaluated items minus those with a
// dispatch job pending at the current version; items dispatched at an older
// version carry that dispatch as previous_dispatch.
//
// GRAPH_BENCH_SCANNERS, a comma-separated subset of evaluation, validation,
// dispatch and aggregate, restricts which scanners run; the default is all
// four. Aggregate: one candidate per batch whose members are the complete
// checks of selected, unflagged evaluations; every other check only moves the
// version.
TEST_F(GraphScannerBenchmark, Scanners) {
  std::set<std::string> selected = {"evaluation", "validation", "dispatch",
                                    "aggregate"};
  if (const char* env = std::getenv("GRAPH_BENCH_SCANNERS"); env != nullptr) {
    selected = absl::StrSplit(env, ',', absl::SkipEmpty());
  }
  if (selected.contains("evaluation")) {
    SetScannerParams(TypeUrl("job.EvaluationJob"), Null<std::string>());
    RunScanner("evaluation", ExpectedEvaluation());
  }
  if (selected.contains("validation")) {
    SetScannerParams(TypeUrl("job.ValidationJob"), Null<std::string>());
    RunScanner("validation", ExpectedValidation());
  }
  if (selected.contains("dispatch")) {
    SetScannerParams(TypeUrl("job.DispatchJob"), Value(DispatchBindingId()));
    RunScanner("dispatch", ExpectedDispatch());
  }
  if (selected.contains("aggregate")) {
    SetScannerParams(TypeUrl("job.AggregateJob"), Null<std::string>());
    RunScanner("aggregate", ExpectedAggregate());
  }
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  testing::AddGlobalTestEnvironment(
      new google::spanner::emulator::test::GraphScannerBenchmarkEnvironment());
  const int result = RUN_ALL_TESTS();
  if (std::getenv("GRAPH_BENCH_EXIT_NORMALLY") != nullptr) {
    // Exercise orderly server shutdown and static destruction.
    return result;
  }
  // The emulator's server threads and static state did not survive orderly
  // destruction after a run this size (heap corruption and absl::Mutex aborts
  // in teardown on otherwise passing runs). Every result has been aggregateed,
  // so leave without running destructors.
  std::fflush(stdout);
  std::fflush(stderr);
  std::_Exit(result);
}
