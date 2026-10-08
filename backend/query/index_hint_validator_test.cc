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

#include "backend/query/index_hint_validator.h"

#include <memory>
#include <string>

#include "googlesql/public/analyzer.h"
#include "googlesql/public/analyzer_options.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/memory/memory.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/schema.h"
#include "common/constants.h"
#include "common/errors.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

class IndexHintValidatorTest : public testing::Test {
 public:
  IndexHintValidatorTest()
      : analyzer_options_(MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone)) {}

  void SetUp() override {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema_, test::CreateSchemaFromDDL(
                                      {
                                          R"(
      CREATE TABLE T1 (
        k1 INT64,
        col1 STRING(MAX)
      ) PRIMARY KEY (k1))",
                                          R"(
      CREATE TABLE T2 (
        k2 INT64,
        col2 STRING(MAX)
      ) PRIMARY KEY (k2))",
                                          R"(
      CREATE TABLE T3 (
        k3 INT64 NOT NULL,
        col3 STRING(MAX) NOT NULL
      ) PRIMARY KEY (k3))",
                                          R"(
      CREATE TABLE T4 (
        k4 INT64 NOT NULL,
        type STRING(MAX),
        data JSON
      ) PRIMARY KEY (k4))",
                                          R"(
      CREATE INDEX I1 ON T1(col1))",
                                          R"(
      CREATE INDEX I2 ON T2(col2))",
                                          R"(
      CREATE NULL_FILTERED INDEX NF_I1 ON T1(col1))",
                                          R"(
      CREATE NULL_FILTERED INDEX I3 ON T3(col3))",
                                          R"(
      CREATE NULL_FILTERED INDEX NF_EXPR ON T4(type, (CASE
        WHEN type = 'a' THEN JSON_VALUE(data, '$.id') ELSE NULL END)))",
                                          R"(
      ALTER TABLE T2 ADD FOREIGN KEY(col2) REFERENCES T1(col1))"},
                                      &type_factory_));

    fn_catalog_ = std::make_unique<FunctionCatalog>(
        &type_factory_,
        /*catalog_name=*/kCloudSpannerEmulatorFunctionCatalogName,
        /*latest_schema=*/schema_.get());

    catalog_ = std::make_unique<Catalog>(schema_.get(), fn_catalog_.get(),
                                         &type_factory_, analyzer_options_);
  }

  std::unique_ptr<const googlesql::AnalyzerOutput> AnalyzeQuery(
      const std::string& sql) {
    std::unique_ptr<const googlesql::AnalyzerOutput> output;
    GOOGLESQL_EXPECT_OK(googlesql::AnalyzeStatement(
        sql, analyzer_options_, catalog_.get(), &type_factory_, &output));
    return output;
  }

  const Schema* schema() const { return schema_.get(); }

  googlesql::Catalog* catalog() { return catalog_.get(); }

 private:
  googlesql::TypeFactory type_factory_;

  const googlesql::AnalyzerOptions analyzer_options_;

  std::unique_ptr<const FunctionCatalog> fn_catalog_;

  std::unique_ptr<const Schema> schema_;

  std::unique_ptr<Catalog> catalog_;
};

TEST_F(IndexHintValidatorTest, ValidateTableIndexHint) {
  auto output = AnalyzeQuery("SELECT k1 FROM T1@{force_index=I1}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  GOOGLESQL_EXPECT_OK(stmt->Accept(&validator));
}

TEST_F(IndexHintValidatorTest, InvalidIndexHintReturnsError) {
  auto output = AnalyzeQuery("SELECT k1 FROM T1@{force_index=I2}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::QueryHintIndexNotFound("T1", "I2"));
}

TEST_F(IndexHintValidatorTest, InvalidIndexHintInInsertReturnsError) {
  auto output =
      AnalyzeQuery("INSERT INTO T1(k1) SELECT k1 FROM T1@{force_index=I2}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::QueryHintIndexNotFound("T1", "I2"));
}

TEST_F(IndexHintValidatorTest, InvalidIndexHintInUpdateReturnsError) {
  auto output = AnalyzeQuery(
      "UPDATE T1 SET k1 = 1 WHERE k1 IN "
      "(SELECT k1 FROM T1@{force_index=I2})");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::QueryHintIndexNotFound("T1", "I2"));
}

TEST_F(IndexHintValidatorTest, InvalidIndexHintInDeleteReturnsError) {
  auto output = AnalyzeQuery(
      "DELETE FROM T1 WHERE k1 IN "
      "(SELECT k1 FROM T1@{force_index=I2})");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::QueryHintIndexNotFound("T1", "I2"));
}

TEST_F(IndexHintValidatorTest, ValidIndexHintOnDeleteTargetSucceeds) {
  auto output =
      AnalyzeQuery("DELETE FROM T1@{force_index=I1} WHERE col1 = 'value'");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  GOOGLESQL_EXPECT_OK(stmt->Accept(&validator));
}

TEST_F(IndexHintValidatorTest,
       NullFilteredIndexCannotBeUsedForNullableColumns) {
  auto output = AnalyzeQuery("SELECT col1 FROM T1@{force_index=NF_I1}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator),
            error::NullFilteredIndexUnusable("NF_I1"));
}

TEST_F(IndexHintValidatorTest, DisableNullFilteredIndexCheck) {
  auto stmt1 = AnalyzeQuery(
      "SELECT col1 FROM T1@{force_index=NF_I1} "
      "WHERE k1 > 1");
  // Statement is rejected without the disabling index hint.
  {
    IndexHintValidator validator{schema()};
    EXPECT_EQ(stmt1->resolved_statement()->Accept(&validator),
              error::NullFilteredIndexUnusable("NF_I1"));
  }

  // Statement is accepted after disabling the check through a hint.
  auto stmt2 = AnalyzeQuery(
      "SELECT col1 FROM T1"
      "@{force_index=NF_I1, "
      "spanner_emulator.disable_query_null_filtered_index_check=true} "
      "WHERE k1 > 1");
  {
    IndexHintValidator validator{schema(),
                                 /*disable_null_filtered_index_check=*/true};
    GOOGLESQL_EXPECT_OK(stmt2->resolved_statement()->Accept(&validator));
  }
}

TEST_F(IndexHintValidatorTest, NullFilteredIndexUsableWithIsNotNullPredicate) {
  for (const char* sql : {
           "SELECT col1 FROM T1@{force_index=NF_I1} WHERE col1 IS NOT NULL",
           "SELECT col1 FROM T1@{force_index=NF_I1} "
           "WHERE k1 > 1 AND col1 IS NOT NULL AND k1 < 10",
           "SELECT col1 FROM T1@{force_index=NF_I1} "
           "WHERE NOT (col1 IS NULL)",
           // Comparisons reject NULL operands.
           "SELECT col1 FROM T1@{force_index=NF_I1} WHERE col1 = 'a'",
           "SELECT col1 FROM T1@{force_index=NF_I1} "
           "WHERE k1 > 1 AND col1 >= 'a' AND col1 < 'z'",
           "SELECT col1 FROM T1@{force_index=NF_I1} "
           "WHERE col1 IN ('a', 'b')",
           // The filter sits above a join over the hinted scan.
           "SELECT t1.col1 FROM T1@{force_index=NF_I1} t1 JOIN T2 t2 "
           "ON t1.k1 = t2.k2 WHERE t1.col1 IS NOT NULL",
       }) {
    SCOPED_TRACE(sql);
    auto output = AnalyzeQuery(sql);
    IndexHintValidator validator{schema()};
    GOOGLESQL_EXPECT_OK(output->resolved_statement()->Accept(&validator));
  }

  for (const char* sql : {
           // The predicate is on another column.
           "SELECT col1 FROM T1@{force_index=NF_I1} WHERE k1 IS NOT NULL",
           // The predicate is disjunctive.
           "SELECT col1 FROM T1@{force_index=NF_I1} "
           "WHERE col1 IS NOT NULL OR k1 > 1",
           // The predicate is on another scan's column of the same name.
           "SELECT t1.col1 FROM T1@{force_index=NF_I1} t1 JOIN T1 t3 "
           "ON t1.k1 = t3.k1 WHERE t3.col1 IS NOT NULL",
       }) {
    SCOPED_TRACE(sql);
    auto output = AnalyzeQuery(sql);
    IndexHintValidator validator{schema()};
    EXPECT_EQ(output->resolved_statement()->Accept(&validator),
              error::NullFilteredIndexUnusable("NF_I1"));
  }
}

TEST_F(IndexHintValidatorTest, NullFilteredExpressionIndexNeedsMatchingPredicate) {
  constexpr char kKey[] =
      "CASE WHEN type = 'a' THEN JSON_VALUE(data, '$.id') ELSE NULL END";
  auto accepted = AnalyzeQuery(absl::StrCat(
      "SELECT k4 FROM T4@{force_index=NF_EXPR} WHERE type IS NOT NULL AND (",
      kKey, ") IS NOT NULL AND ", kKey, " >= 'x'"));
  {
    IndexHintValidator validator{schema(), false, false, false, false,
                                 catalog()};
    GOOGLESQL_EXPECT_OK(accepted->resolved_statement()->Accept(&validator));
  }

  // The shape Iris uses: an equality on the column key and a range on the
  // expression key, both of which reject NULLs.
  auto compared = AnalyzeQuery(absl::StrCat(
      "SELECT k4 FROM T4@{force_index=NF_EXPR} WHERE type = 'a' AND ", kKey,
      " >= 'x' AND ", kKey, " < 'z'"));
  {
    IndexHintValidator validator{schema(), false, false, false, false,
                                 catalog()};
    GOOGLESQL_EXPECT_OK(compared->resolved_statement()->Accept(&validator));
  }

  // Without a catalog the expression key cannot be analyzed.
  {
    IndexHintValidator validator{schema()};
    EXPECT_EQ(accepted->resolved_statement()->Accept(&validator),
              error::NullFilteredIndexUnusable("NF_EXPR"));
  }

  for (const std::string& sql : {
           // No predicate on the expression.
           std::string("SELECT k4 FROM T4@{force_index=NF_EXPR} "
                       "WHERE type IS NOT NULL"),
           // A different expression.
           absl::StrCat("SELECT k4 FROM T4@{force_index=NF_EXPR} WHERE type IS "
                        "NOT NULL AND (CASE WHEN type = 'b' THEN "
                        "JSON_VALUE(data, '$.id') ELSE NULL END) IS NOT NULL"),
           // The nullable column key has no predicate.
           absl::StrCat("SELECT k4 FROM T4@{force_index=NF_EXPR} WHERE (", kKey,
                        ") IS NOT NULL"),
       }) {
    SCOPED_TRACE(sql);
    auto output = AnalyzeQuery(sql);
    IndexHintValidator validator{schema(), false, false, false, false,
                                 catalog()};
    EXPECT_EQ(output->resolved_statement()->Accept(&validator),
              error::NullFilteredIndexUnusable("NF_EXPR"));
  }
}

TEST_F(IndexHintValidatorTest,
       NullFilteredIndexCanOnlyBeUsedForNotNullColumns) {
  auto output = AnalyzeQuery("SELECT col3 FROM T3@{force_index=I3}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  GOOGLESQL_EXPECT_OK(stmt->Accept(&validator));
}

TEST_F(IndexHintValidatorTest, EmulatorManagedIndexName) {
  auto output = AnalyzeQuery(
      "SELECT k1 FROM T1@{force_index=IDX_T1_col1_U_EA26CF5871E82344}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  EXPECT_EQ(stmt->Accept(&validator), error::QueryHintManagedIndexNotSupported(
                                          "IDX_T1_col1_U_EA26CF5871E82344"));
}

TEST_F(IndexHintValidatorTest, NonEmulatorManagedIndexName) {
  auto output = AnalyzeQuery(
      "SELECT k1 FROM T1@{force_index=IDX_T1_col1_U_EA26CF5871E82340}");
  auto stmt = output->resolved_statement();
  IndexHintValidator validator{schema()};
  GOOGLESQL_EXPECT_OK(stmt->Accept(&validator));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
