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

#include "backend/query/search/search_evaluator_helpers.h"

#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "backend/query/search/tokenizer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

// The test suite focuses on verifying the code path that cannot easily
// triggered by end to end tests, or code paths with outputs not easily
// verifiable by end to end tests. Normal test scenarios (including invalid
// customer inputs) are covered in search_test.cc.

namespace {

TokenMap BuildTokenMap(std::string doc) {
  TokenMap result;
  std::vector<absl::string_view> tokens = absl::StrSplit(doc, ' ');
  for (int i = 0; i < tokens.size(); i++) {
    result[tokens[i]].push_back(i);
  }

  return result;
}

}  // namespace

TEST(SearchQueryCacheTest, GetParsedQuery) {
  auto result =
      SearchQueryCache::GetInstance()->GetParsedQuery("google spanner");
  ZETASQL_EXPECT_OK(result.status());
  EXPECT_NE(result.value(), nullptr);
}

TEST(SearchQueryCacheTest, FailedToParseQuery) {
  EXPECT_THAT(
      SearchQueryCache::GetInstance()->GetParsedQuery("google| |spanner"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Error(s) parsing search query")));
}

TEST(MatchResultTest, SortedUniqueHits) {
  std::vector<Hit> hits = {{3, 1}, {2, 1}, {2, 2}, {4, 2},
                           {3, 1}, {5, 3}, {1, 1}};
  auto result = MatchResult::Match(hits);
  EXPECT_TRUE(result.is_match());
  EXPECT_THAT(result.hits(),
              testing::ElementsAreArray<Hit>(
                  {{1, 1}, {2, 1}, {2, 2}, {3, 1}, {4, 2}, {5, 3}}));
}

TEST(PharaseMatcherTest, UnNearMatchNotOverlapping) {
  TokenMap token_map = BuildTokenMap("foo bar foo baz goo foo bar");
  std::vector<std::vector<Hit>> hits;

  // Hits for "foo *"
  std::vector<Hit> foo_wildcard = {{0, 2}, {2, 2}, {5, 2}};
  hits.push_back(std::move(foo_wildcard));

  // Hits for "bar"
  std::vector<Hit> bar = {{1, 1}, {6, 1}};
  hits.push_back(std::move(bar));

  // Find match for ["foo *" AROUND(1) bar]. "foo bar" overlaps with "bar", so
  // it's not a match.
  PhraseMatcher matcher(hits, 1);
  EXPECT_THAT(matcher.FindMatchingHits(),
              testing::ElementsAreArray<Hit>({{1, 3}}));
}

TEST(PharaseMatcherTest, ChainUnNearMatchNotOverlapping) {
  TokenMap token_map = BuildTokenMap("foo baz bar foo baz goo foo bar foo bar");
  std::vector<std::vector<Hit>> hits;

  // Hit for "foo"
  std::vector<Hit> foo = {{0, 1}, {3, 1}, {6, 1}, {8, 1}};
  hits.push_back(std::move(foo));

  // Hits for "baz * foo"
  std::vector<Hit> baz_wildcard = {{1, 3}, {4, 3}};
  hits.push_back(std::move(baz_wildcard));

  // Hits for "bar"
  std::vector<Hit> bar = {{2, 1}, {7, 1}, {9, 1}};
  hits.push_back(std::move(bar));

  // Find match for [foo AROUND(1) "baz * foo" AROUND(1) bar]. "foo baz bar foo"
  // overlaps with "bar", so it's not a match.
  PhraseMatcher matcher(hits, 1);
  EXPECT_THAT(matcher.FindMatchingHits(),
              testing::ElementsAreArray<Hit>({{2, 5}, {4, 5}}));
}

TEST(PharaseMatcherTest, CompositeUnNearMatch) {
  TokenMap token_map = BuildTokenMap("bar baz bar foo baz goo foo bar");
  std::vector<std::vector<Hit>> hits;

  // Hit for "foo *"
  std::vector<Hit> foo = {{6, 2}, {3, 2}};
  hits.push_back(std::move(foo));

  // Hits for "bar | baz-goo"
  std::vector<Hit> or_clause = {{0, 1}, {2, 1}, {4, 2}, {7, 1}};
  hits.push_back(std::move(or_clause));

  // Find match for ["foo *" AROUND(1) "bar | baz-goo"].
  PhraseMatcher matcher(hits, 1);
  EXPECT_THAT(matcher.FindMatchingHits(),
              testing::ElementsAreArray<Hit>({{2, 3}, {4, 4}}));
}

TEST(SearchHelperTest, TestBuildTokenMap) {
  const zetasql::Value tokenlist = TokenListFromStrings({"substring"});

  bool is_source_null;
  EXPECT_THAT(
      SearchHelper::BuildTokenMap(tokenlist, "SEARCH", is_source_null),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("SEARCH function's first argument must be a "
                         "TOKENLIST column generated by TOKENIZE_FULLTEXT")));
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google