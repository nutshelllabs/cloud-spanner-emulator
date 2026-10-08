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

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using googlesql_base::testing::StatusIs;

// Indexes whose keys are SQL expressions over the indexed table's columns.
class ExpressionIndexTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(CREATE TABLE Products (
          ScopeId INT64 NOT NULL,
          ProductId INT64 NOT NULL,
          Type STRING(MAX),
          Name STRING(MAX),
          ProtoData JSON,
          SemanticVersion INT64,
        ) PRIMARY KEY (ScopeId, ProductId))",
        R"(CREATE NULL_FILTERED INDEX ProductsByTransmission
             ON Products(ScopeId, Type, JSON_VALUE(ProtoData, '$.7.10'))
             STORING (SemanticVersion))",
        R"(CREATE INDEX ProductsByLowerName
             ON Products(LOWER(Name) DESC, ProductId))",
    });
  }

  absl::Status InsertProduct(int64_t scope_id, int64_t product_id,
                             const std::string& type, const std::string& name,
                             const std::string& proto_data, int64_t version) {
    return Insert("Products",
                  {"ScopeId", "ProductId", "Type", "Name", "ProtoData",
                   "SemanticVersion"},
                  {scope_id, product_id, type, name, Json(proto_data),
                   version})
        .status();
  }
};

TEST_F(ExpressionIndexTest, OrdersEntriesByExpressionValue) {
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 1, "pfi", "Alpha", R"({"7":{"10":"t3"}})", 1));
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 2, "pfi", "beta", R"({"7":{"10":"t1"}})", 2));
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 3, "pfi", "Gamma", R"({"7":{"10":"t2"}})", 3));

  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByTransmission",
                               {"ProductId", "SemanticVersion"}),
              IsOkAndHoldsRows({{2, 2}, {3, 3}, {1, 1}}));

  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByLowerName",
                               {"ProductId"}),
              IsOkAndHoldsRows({{3}, {2}, {1}}));

  EXPECT_THAT(Query(R"(SELECT ProductId
                       FROM Products@{FORCE_INDEX=ProductsByLowerName}
                       WHERE LOWER(Name) = 'beta')"),
              IsOkAndHoldsRows({{2}}));
}

TEST_F(ExpressionIndexTest, NullFilteredIndexSkipsNullExpressionValues) {
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 1, "pfi", "Alpha", R"({"7":{"10":"t1"}})", 1));
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 2, "pfi", "Beta", R"({"7":{"11":"x"}})", 2));
  GOOGLESQL_ASSERT_OK(Insert("Products", {"ScopeId", "ProductId", "Type", "Name"},
                             {1, 3, "pfi", "Gamma"}));

  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByTransmission",
                               {"ProductId"}),
              IsOkAndHoldsRows({{1}}));
  // Rows with a NULL expression value stay readable through the other index.
  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByLowerName",
                               {"ProductId"}),
              IsOkAndHoldsRows({{3}, {2}, {1}}));
}

TEST_F(ExpressionIndexTest, UpdatesAndDeletesMaintainEntries) {
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 1, "pfi", "Alpha", R"({"7":{"10":"t1"}})", 1));
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 2, "pfi", "Beta", R"({"7":{"10":"t2"}})", 2));

  // Moving the expression value moves the entry.
  GOOGLESQL_ASSERT_OK(Update("Products", {"ScopeId", "ProductId", "ProtoData"},
                             {1, 1, Json(R"({"7":{"10":"t9"}})")}));
  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByTransmission",
                               {"ProductId"}),
              IsOkAndHoldsRows({{2}, {1}}));

  // Updating an unrelated column keeps the entry in place.
  GOOGLESQL_ASSERT_OK(Update("Products", {"ScopeId", "ProductId", "SemanticVersion"},
                             {1, 2, 7}));
  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByTransmission",
                               {"ProductId", "SemanticVersion"}),
              IsOkAndHoldsRows({{2, 7}, {1, 1}}));

  // Nulling the expression value removes the entry from the filtered index.
  GOOGLESQL_ASSERT_OK(Update("Products", {"ScopeId", "ProductId", "ProtoData"},
                             {1, 2, Json("{}")}));
  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByTransmission",
                               {"ProductId"}),
              IsOkAndHoldsRows({{1}}));

  GOOGLESQL_ASSERT_OK(Delete("Products", Key(1, 1)));
  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByTransmission",
                               {"ProductId"}),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByLowerName",
                               {"ProductId"}),
              IsOkAndHoldsRows({{2}}));
}

TEST_F(ExpressionIndexTest, DmlMaintainsEntries) {
  GOOGLESQL_ASSERT_OK(CommitDml({SqlStatement(
      R"(INSERT INTO Products (ScopeId, ProductId, Type, Name, ProtoData)
         VALUES (1, 1, 'pfi', 'Alpha', JSON '{"7":{"10":"t1"}}'),
                (1, 2, 'pfi', 'Beta', JSON '{"7":{"10":"t0"}}'))")}));
  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByTransmission",
                               {"ProductId"}),
              IsOkAndHoldsRows({{2}, {1}}));

  GOOGLESQL_ASSERT_OK(CommitDml({SqlStatement(
      R"(UPDATE Products SET ProtoData = JSON '{"7":{"10":"t5"}}'
         WHERE ProductId = 2)")}));
  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByTransmission",
                               {"ProductId"}),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(ExpressionIndexTest, BackfillsExistingRows) {
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 1, "pfi", "Alpha", R"({"7":{"10":"t2"}})", 1));
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 2, "pfi", "Beta", R"({"7":{"10":"t1"}})", 2));
  GOOGLESQL_ASSERT_OK(InsertProduct(2, 3, "pfi", "alpha", R"({"7":{"10":"t1"}})", 3));

  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"(CREATE INDEX ProductsByTransmissionOnly
           ON Products(JSON_VALUE(ProtoData, '$.7.10'), ScopeId))",
  }));
  EXPECT_THAT(ReadAllWithIndex("Products", "ProductsByTransmissionOnly",
                               {"ProductId"}),
              IsOkAndHoldsRows({{2}, {3}, {1}}));

  // Backfill enforces uniqueness over the expression value.
  EXPECT_THAT(UpdateSchema({
                  R"(CREATE UNIQUE INDEX ProductsByUniqueLowerName
                       ON Products(LOWER(Name)))",
              }),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  GOOGLESQL_ASSERT_OK(Delete("Products", Key(2, 3)));
  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"(CREATE UNIQUE INDEX ProductsByUniqueLowerName
           ON Products(LOWER(Name)))",
  }));
  EXPECT_THAT(InsertProduct(3, 4, "pfi", "ALPHA", "{}", 4),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(ExpressionIndexTest, DdlRoundTrips) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto ddl, GetDatabaseDdl());
  EXPECT_THAT(ddl, testing::Contains(
                       "CREATE NULL_FILTERED INDEX ProductsByTransmission ON "
                       "Products(ScopeId, Type, JSON_VALUE(ProtoData, "
                       "'$.7.10')) STORING (SemanticVersion)"));
  EXPECT_THAT(ddl, testing::Contains(
                       "CREATE INDEX ProductsByLowerName ON "
                       "Products(LOWER(Name) DESC, ProductId)"));
}

// The index Iris uses to look up PFI products by IRS transmission id: a
// NULL_FILTERED index over a CASE expression that only yields a key for one
// product type.
class TransmissionIdIndexTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        "CREATE SCHEMA pfi",
        R"(CREATE TABLE pfi.product (
          scope_id INT64 NOT NULL,
          product_id INT64 NOT NULL,
          type STRING(MAX),
          proto_data JSON,
          semantic_version INT64,
        ) PRIMARY KEY (scope_id, product_id))",
        R"(CREATE NULL_FILTERED INDEX pfi.iris_pfi_transmission_id_index
            ON pfi.product (scope_id, type, (CASE
                WHEN type = 'type.googleapis.com/nutshell.iris.proto.pfi.ustax.product.IrsFormSubmission'
                THEN JSON_VALUE(proto_data, '$.7.10')
                ELSE NULL
            END)))",
    });
  }

  static constexpr char kSubmission[] =
      "type.googleapis.com/nutshell.iris.proto.pfi.ustax.product."
      "IrsFormSubmission";
  static constexpr char kOther[] =
      "type.googleapis.com/nutshell.iris.proto.pfi.ustax.product.Other";

  absl::Status InsertProduct(int64_t scope_id, int64_t product_id,
                             const std::string& type,
                             const std::string& proto_data) {
    return Insert("pfi.product",
                  {"scope_id", "product_id", "type", "proto_data"},
                  {scope_id, product_id, type, Json(proto_data)})
        .status();
  }
};

TEST_F(TransmissionIdIndexTest, IndexesOnlySubmissionsWithTransmissionId) {
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 1, kSubmission, R"({"7":{"10":"t2"}})"));
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 2, kSubmission, R"({"7":{"10":"t1"}})"));
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 3, kOther, R"({"7":{"10":"t0"}})"));
  GOOGLESQL_ASSERT_OK(InsertProduct(1, 4, kSubmission, R"({"7":{"11":"x"}})"));
  GOOGLESQL_ASSERT_OK(InsertProduct(2, 5, kSubmission, R"({"7":{"10":"t1"}})"));

  EXPECT_THAT(ReadAllWithIndex("pfi.product", "pfi.iris_pfi_transmission_id_index",
                               {"scope_id", "product_id"}),
              IsOkAndHoldsRows({{1, 2}, {1, 1}, {2, 5}}));

  // Changing the type of a product moves it in or out of the index.
  GOOGLESQL_ASSERT_OK(Update("pfi.product", {"scope_id", "product_id", "type"},
                             {1, 3, kSubmission}));
  GOOGLESQL_ASSERT_OK(Update("pfi.product", {"scope_id", "product_id", "type"},
                             {1, 1, kOther}));
  EXPECT_THAT(ReadAllWithIndex("pfi.product", "pfi.iris_pfi_transmission_id_index",
                               {"scope_id", "product_id"}),
              IsOkAndHoldsRows({{1, 3}, {1, 2}, {2, 5}}));

  EXPECT_THAT(Query(R"(SELECT product_id FROM pfi.product
                       WHERE scope_id = 1
                         AND type = 'type.googleapis.com/nutshell.iris.proto.pfi.ustax.product.IrsFormSubmission'
                         AND JSON_VALUE(proto_data, '$.7.10') = 't1')"),
              IsOkAndHoldsRows({{2}}));

  // Iris drives its scan from the index. The emulator accepts the hint on the
  // null-filtered index because the predicate repeats the key expression
  // with IS NOT NULL.
  constexpr char kTransmissionId[] = R"(CASE
        WHEN type = 'type.googleapis.com/nutshell.iris.proto.pfi.ustax.product.IrsFormSubmission'
        THEN JSON_VALUE(proto_data, '$.7.10')
        ELSE NULL
      END)";
  const std::string indexed_query = absl::StrCat(
      "SELECT product_id, ", kTransmissionId, " AS transmission_id\n",
      "FROM pfi.product@{FORCE_INDEX=`pfi.iris_pfi_transmission_id_index`}\n",
      "WHERE scope_id = 1\n",
      "  AND type = 'type.googleapis.com/nutshell.iris.proto.pfi.ustax.product.IrsFormSubmission'\n",
      "  AND ", kTransmissionId, " IS NOT NULL\n",
      "  AND ", kTransmissionId, " >= 't1'\n",
      "  AND ", kTransmissionId, " < 't3'\n",
      "ORDER BY transmission_id");
  EXPECT_THAT(Query(indexed_query), IsOkAndHoldsRows({{2, "t1"}}));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto ddl, GetDatabaseDdl());
  EXPECT_THAT(ddl, testing::Contains(testing::AllOf(
                       testing::HasSubstr("CREATE NULL_FILTERED INDEX "
                                          "pfi.iris_pfi_transmission_id_index "
                                          "ON pfi.product(scope_id, type, (CASE"),
                       testing::HasSubstr("ELSE NULL"))));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
