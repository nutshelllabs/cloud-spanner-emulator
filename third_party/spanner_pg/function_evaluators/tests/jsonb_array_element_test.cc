//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/jsonb_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using spangres::datatypes::CreatePgJsonbValue;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

static zetasql::Value CreatePgJsonBNullValue() {
  static const zetasql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  return zetasql::values::Null(gsql_pg_jsonb);
}

class JsonBArrayElementTest : public PgEvaluatorTest {};

TEST_F(JsonBArrayElementTest, ReturnsJsonbValue) {
  static const zetasql::Value null_jsonb = zetasql::values::Null(
      spangres::types::PgJsonbArrayMapping()->mapped_type());
  EXPECT_THAT(JsonBArrayElement(R"([null, "string val"])", 0),
              IsOkAndHolds(*CreatePgJsonbValue("null")));
  EXPECT_THAT(JsonBArrayElement(R"([1.00, "string val"])", 1),
              IsOkAndHolds(*CreatePgJsonbValue(R"("string val")")));
  EXPECT_THAT(JsonBArrayElement(R"([null, "string val"])", 2),
              IsOkAndHolds(CreatePgJsonBNullValue()));

  zetasql::Value result = CreatePgJsonBNullValue();
  EXPECT_THAT(JsonBArrayElement(R"([null, "string val"])", -1),
              IsOkAndHolds(result));

  EXPECT_THAT(JsonBArrayElement(R"({"a": "string val"})", 0),
              IsOkAndHolds(CreatePgJsonBNullValue()));

  EXPECT_THAT(JsonBArrayElement("1", 0),
              IsOkAndHolds(*CreatePgJsonbValue("1")));
  EXPECT_THAT(JsonBArrayElement("1.500", 0),
              IsOkAndHolds(*CreatePgJsonbValue("1.500")));
  EXPECT_THAT(JsonBArrayElement("null", 0),
              IsOkAndHolds(*CreatePgJsonbValue("null")));
  EXPECT_THAT(JsonBArrayElement(R"("a string")", 0),
              IsOkAndHolds(*CreatePgJsonbValue(R"("a string")")));
  EXPECT_THAT(JsonBArrayElement("true", 0),
              IsOkAndHolds(*CreatePgJsonbValue("true")));
  EXPECT_THAT(JsonBArrayElement("false", 0),
              IsOkAndHolds(*CreatePgJsonbValue("false")));
}

TEST_F(JsonBArrayElementTest, ReturnsErrorWhenInvalidArgumentIsGiven) {
  EXPECT_THAT(JsonBArrayElement("a", 0),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(JsonBArrayElement("", 0),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}