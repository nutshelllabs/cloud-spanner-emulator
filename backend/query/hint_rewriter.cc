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

#include "backend/query/hint_rewriter.h"

#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

constexpr absl::string_view kSpannerQueryEngineHintPrefix = "spanner";

bool IsSpannerHint(const googlesql::ResolvedOption* node) {
  return node->qualifier().empty() ||
         absl::EqualsIgnoreCase(node->qualifier(),
                                kSpannerQueryEngineHintPrefix);
}

bool IsNoopQueryPlannerHint(const googlesql::ResolvedOption* node) {
  return IsSpannerHint(node) &&
         (absl::EqualsIgnoreCase(node->name(), "join_method") ||
          absl::EqualsIgnoreCase(node->name(), "force_join_order") ||
          absl::EqualsIgnoreCase(node->name(), "optimizer_version"));
}

}  // namespace

absl::Status HintRewriter::VisitResolvedOption(
    const googlesql::ResolvedOption* node) {
  GOOGLESQL_RETURN_IF_ERROR(CopyVisitResolvedOption(node));
  googlesql::ResolvedOption* option =
      GetUnownedTopOfStack<googlesql::ResolvedOption>();
  if (IsNoopQueryPlannerHint(node)) {
    option->set_qualifier("spanner_emulator_noop");
  } else if (option->qualifier().empty()) {
    option->set_qualifier("spanner");
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
