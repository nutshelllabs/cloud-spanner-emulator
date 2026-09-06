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

#include "common/config.h"

#include <cstdint>
#include <string>

#include "absl/flags/flag.h"

ABSL_FLAG(std::string, host_port, "localhost:10007",
          "Emulator host IP and port that serves Cloud Spanner gRPC requests.");

ABSL_FLAG(bool, log_requests, false,
          "If true, gRPC request and response messages are streamed to the "
          "INFO log. This switch is intended for emulator debugging.");

ABSL_FLAG(
    bool, enable_fault_injection, false,
    "If true, the emulator will inject faults to allow testing application "
    "error handling behavior. For instance, transaction Commits may be aborted "
    "to facilitate application abort-retry testing.");

ABSL_FLAG(bool, disable_query_null_filtered_index_check, false,
          "If true, then queries that use NULL_FILTERED indexes will be "
          "answered. Please test all queries using null filtered indexes "
          "against production Cloud Spanner before disabling this check."
          "\n"
          "Please consider using the query hint "
          "`@{spanner_emulator.disable_query_null_filtered_index_check=true}` "
          "to disable this check per query, instead of disabling this check "
          "for all the queries at once.");

ABSL_FLAG(
    int, abort_current_transaction_probability, 20,
    "The probability that the emulator will try to abort the current "
    "transaction if a new transaction is requested. A higher value gives "
    "higher priority to new transactions. A lower value gives higher priority "
    "to the current transaction. A value of zero means that the emulator will "
    "never abort the current transaction.");

ABSL_FLAG(int, request_stats_log_interval_seconds, 0,
          "If positive, log a table of request timings aggregated by request "
          "shape (queries, nested view queries and commits) every this many "
          "seconds. Zero disables aggregation.");

ABSL_FLAG(int, log_slow_requests_ms, 0,
          "If positive, log every query and commit that takes at least this "
          "many milliseconds, with its phase breakdown. Zero disables it.");

ABSL_FLAG(int64_t, max_intermediate_byte_size, int64_t{4} << 30,
          "Upper bound on the bytes a single query may hold in intermediate "
          "results such as join, sort and WITH materializations. The "
          "reference evaluator counts every copy of a value at its full "
          "logical size, so this runs well ahead of process memory.");

namespace google {
namespace spanner {
namespace emulator {
namespace config {

std::string grpc_host_port() { return absl::GetFlag(FLAGS_host_port); }

bool should_log_requests() { return absl::GetFlag(FLAGS_log_requests); }

bool fault_injection_enabled() {
  return absl::GetFlag(FLAGS_enable_fault_injection);
}

bool disable_query_null_filtered_index_check() {
  return absl::GetFlag(FLAGS_disable_query_null_filtered_index_check);
}

int abort_current_transaction_probability() {
  return absl::GetFlag(FLAGS_abort_current_transaction_probability);
}

void set_abort_current_transaction_probability(int probability) {
  absl::SetFlag(&FLAGS_abort_current_transaction_probability, probability);
}

int request_stats_log_interval_seconds() {
  return absl::GetFlag(FLAGS_request_stats_log_interval_seconds);
}

int log_slow_requests_ms() { return absl::GetFlag(FLAGS_log_slow_requests_ms); }

int64_t max_intermediate_byte_size() {
  return absl::GetFlag(FLAGS_max_intermediate_byte_size);
}

}  // namespace config
}  // namespace emulator
}  // namespace spanner
}  // namespace google
