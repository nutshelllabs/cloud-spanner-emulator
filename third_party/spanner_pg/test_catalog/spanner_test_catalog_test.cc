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

#include "third_party/spanner_pg/test_catalog/spanner_test_catalog.h"

#include "zetasql/public/catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/test_catalog/emulator_catalog.h"

namespace postgres_translator::spangres::test {
namespace {

TEST(SpannerTestCatalogTest, SchemaTables) {
  // Check that the full names of the tables in the SqlCatalog include the
  // tables schemas.
  zetasql::EnumerableCatalog* sql_catalog =
      GetSpangresTestSpannerUserCatalog();
  const zetasql::Table* sql_table;
  ZETASQL_ASSERT_OK(
      sql_catalog->FindTable({"INFORMATION_SCHEMA", "TABLES"}, &sql_table));
  std::string full_name = "INFORMATION_SCHEMA.TABLES";
  EXPECT_EQ(sql_table->FullName(), full_name);
  EXPECT_EQ(sql_table->Name(), "TABLES");

  ZETASQL_ASSERT_OK(sql_catalog->FindTable(
      {"SPANNER_SYS", "SUPPORTED_OPTIMIZER_VERSIONS"}, &sql_table));
  full_name = "SPANNER_SYS.SUPPORTED_OPTIMIZER_VERSIONS";
  EXPECT_EQ(sql_table->FullName(), full_name);
  EXPECT_EQ(sql_table->Name(), "SUPPORTED_OPTIMIZER_VERSIONS");
}

}  // namespace
}  // namespace postgres_translator::spangres::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
