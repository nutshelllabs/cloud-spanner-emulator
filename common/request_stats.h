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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_REQUEST_STATS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_REQUEST_STATS_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"

namespace google {
namespace spanner {
namespace emulator {

// One timed request: a query, a nested view query, or a commit.
struct RequestSample {
  // "query" for a client SQL statement, "view" for a view body evaluated
  // inside another query, "commit" for a Commit RPC.
  std::string kind;
  // Text that identifies the request shape: the SQL, or the tables a commit
  // touches. Aggregation is by a hash of this text with whitespace collapsed.
  std::string text;
  absl::Duration elapsed;
  // Named parts of `elapsed`. Queries report catalog, analyze and evaluate;
  // commits report convert, write and commit.
  std::vector<std::pair<std::string, absl::Duration>> phases;
  int64_t rows = 0;
};

// Aggregates request timings by request shape and logs them periodically,
// and logs individual requests slower than a threshold. Both are off unless
// --request_stats_log_interval_seconds or --log_slow_requests_ms is set, and
// Record() returns immediately then.
//
// Nested view queries are recorded under their own kind so their time is
// not attributed twice: the enclosing query's elapsed time already includes
// them.
class RequestStats {
 public:
  static RequestStats& Instance();

  void Record(const RequestSample& sample) ABSL_LOCKS_EXCLUDED(mu_);

  // Logs the aggregate table now, sorted by cumulative time.
  void LogSummary() ABSL_LOCKS_EXCLUDED(mu_);

  // Stable identifier of a request shape: hex hash of `text` with whitespace
  // collapsed, plus the collapsed prefix as a readable sample.
  static std::string Fingerprint(absl::string_view text);
  static std::string Sample(absl::string_view text, size_t max_chars = 96);

 private:
  RequestStats();

  struct Aggregate {
    std::string kind;
    std::string sample;
    int64_t calls = 0;
    int64_t rows = 0;
    absl::Duration total;
    absl::Duration max;
    std::map<std::string, absl::Duration> phases;
  };

  void MaybeStartLogger() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Mutex mu_;
  std::map<std::string, Aggregate> aggregates_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<std::thread> logger_ ABSL_GUARDED_BY(mu_);
};

}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_REQUEST_STATS_H_
