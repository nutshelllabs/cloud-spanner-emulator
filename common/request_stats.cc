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

#include "common/request_stats.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/hash/hash.h"
#include "absl/log/log.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "common/config.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

std::string CollapseWhitespace(absl::string_view text) {
  std::string out;
  out.reserve(text.size());
  bool pending_space = false;
  for (char c : text) {
    if (absl::ascii_isspace(c)) {
      pending_space = !out.empty();
      continue;
    }
    if (pending_space) {
      out.push_back(' ');
      pending_space = false;
    }
    out.push_back(c);
  }
  return out;
}

std::string Ms(absl::Duration d) {
  return absl::StrFormat("%.1f", absl::ToDoubleMilliseconds(d));
}

}  // namespace

RequestStats& RequestStats::Instance() {
  static RequestStats* instance = new RequestStats();
  return *instance;
}

RequestStats::RequestStats() = default;

std::string RequestStats::Fingerprint(absl::string_view text) {
  return absl::StrFormat("%016x",
                         absl::HashOf(CollapseWhitespace(text)));
}

std::string RequestStats::Sample(absl::string_view text, size_t max_chars) {
  std::string collapsed = CollapseWhitespace(text);
  if (collapsed.size() > max_chars) {
    collapsed.resize(max_chars);
    collapsed.append("...");
  }
  return collapsed;
}

void RequestStats::Record(const RequestSample& sample) {
  const int interval_seconds = config::request_stats_log_interval_seconds();
  const int slow_ms = config::log_slow_requests_ms();
  if (interval_seconds <= 0 && slow_ms <= 0) {
    return;
  }
  const std::string fingerprint = Fingerprint(sample.text);

  if (slow_ms > 0 && sample.elapsed >= absl::Milliseconds(slow_ms)) {
    std::vector<std::string> phases;
    for (const auto& [name, duration] : sample.phases) {
      phases.push_back(absl::StrCat(name, "_ms=", Ms(duration)));
    }
    LOG(INFO) << "SLOW_REQUEST kind=" << sample.kind << " fp=" << fingerprint
              << " ms=" << Ms(sample.elapsed) << " rows=" << sample.rows
              << " " << absl::StrJoin(phases, " ")
              << " sample=\"" << Sample(sample.text) << "\"";
  }

  if (interval_seconds <= 0) {
    return;
  }
  absl::MutexLock lock(&mu_);
  MaybeStartLogger();
  Aggregate& aggregate = aggregates_[absl::StrCat(sample.kind, ":", fingerprint)];
  if (aggregate.calls == 0) {
    aggregate.kind = sample.kind;
    aggregate.sample = Sample(sample.text);
  }
  aggregate.calls++;
  aggregate.rows += sample.rows;
  aggregate.total += sample.elapsed;
  aggregate.max = std::max(aggregate.max, sample.elapsed);
  for (const auto& [name, duration] : sample.phases) {
    aggregate.phases[name] += duration;
  }
}

void RequestStats::MaybeStartLogger() {
  if (logger_ != nullptr) {
    return;
  }
  const int interval_seconds = config::request_stats_log_interval_seconds();
  logger_ = std::make_unique<std::thread>([this, interval_seconds] {
    while (true) {
      absl::SleepFor(absl::Seconds(interval_seconds));
      LogSummary();
    }
  });
  logger_->detach();
}

void RequestStats::LogSummary() {
  std::vector<std::pair<std::string, Aggregate>> rows;
  {
    absl::MutexLock lock(&mu_);
    rows.assign(aggregates_.begin(), aggregates_.end());
  }
  if (rows.empty()) {
    return;
  }
  std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) {
    return a.second.total > b.second.total;
  });
  absl::Duration total;
  int64_t calls = 0;
  for (const auto& [key, aggregate] : rows) {
    if (aggregate.kind != "view") {
      total += aggregate.total;
      calls += aggregate.calls;
    }
  }
  LOG(INFO) << "REQUEST_STATS summary shapes=" << rows.size()
            << " calls=" << calls << " total_ms=" << Ms(total)
            << " (view queries excluded from totals)";
  for (const auto& [key, aggregate] : rows) {
    std::vector<std::string> phases;
    for (const auto& [name, duration] : aggregate.phases) {
      phases.push_back(absl::StrCat(name, "_ms=", Ms(duration)));
    }
    LOG(INFO) << "REQUEST_STATS kind=" << aggregate.kind << " fp="
              << key.substr(key.find(':') + 1) << " calls=" << aggregate.calls
              << " total_ms=" << Ms(aggregate.total)
              << " avg_ms=" << Ms(aggregate.total / aggregate.calls)
              << " max_ms=" << Ms(aggregate.max) << " rows=" << aggregate.rows
              << " " << absl::StrJoin(phases, " ") << " sample=\""
              << aggregate.sample << "\"";
  }
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
