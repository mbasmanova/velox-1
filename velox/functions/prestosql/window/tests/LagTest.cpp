/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/functions/lib/window/tests/WindowTestBase.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {
namespace {

class LagTest : public WindowTestBase {
 protected:
  void SetUp() override {
    WindowTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
  }
};

TEST_F(LagTest, offset) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Offsets.
      makeFlatVector<int64_t>({1, 2, 3, 1, 2}),
      // Offsets with nulls.
      makeNullableFlatVector<int64_t>({1, 2, 3, std::nullopt, 2}),
  });

  createDuckDbTable({data});

  // Default offset.
  auto queryInfo = buildWindowQuery({data}, "lag(c0)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  // Constant offset.
  queryInfo = buildWindowQuery({data}, "lag(c0, 2)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  // Constant null offset. DuckDB returns incorrect results for this case. It
  // treats null offset as 0.
  queryInfo =
      buildWindowQuery({data}, "lag(c0, null::bigint)", "order by c0", "");

  auto expected = makeRowVector({
      data->childAt(0),
      data->childAt(1),
      data->childAt(2),
      makeAllNullFlatVector<int64_t>(data->size()),
  });
  assertQuery(queryInfo.planNode, expected);

  // Variable offsets.
  queryInfo = buildWindowQuery({data}, "lag(c0, c1)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  // Variable offsets with nulls.
  queryInfo = buildWindowQuery({data}, "lag(c0, c2)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);
}

TEST_F(LagTest, defaultValue) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Default values.
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      // Default values with nulls.
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30, std::nullopt, 50}),
  });

  createDuckDbTable({data});

  // Constant non-null default value.
  auto queryInfo =
      buildWindowQuery({data}, "lag(c0, 2, 100)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery({data}, "lag(c0, 22, 100)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  // Constant null default value.
  queryInfo =
      buildWindowQuery({data}, "lag(c0, 2, null::bigint)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  // Variable default values.
  queryInfo = buildWindowQuery({data}, "lag(c0, 2, c1)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  buildWindowQuery({data}, "lag(c0, 22, c1)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  // Variable default values with nulls.
  queryInfo = buildWindowQuery({data}, "lag(c0, 2, c2)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery({data}, "lag(c0, 22, c2)", "order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);
}

// Make sure resultOffset passed to LagFunction::apply is handled correctly.
TEST_F(LagTest, smallPartitions) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
      // Small partitions. 5 rows each.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row / 5; }),
      // Default values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row * 10; }),
  });

  createDuckDbTable({data});

  // Single-row partitions.
  auto queryInfo = buildWindowQuery({data}, "lag(c0)", "partition by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo =
      buildWindowQuery({data}, "lag(c0, 1, 100)", "partition by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery({data}, "lag(c0, 2, c2)", "partition by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  // Small partitions.
  queryInfo = buildWindowQuery({data}, "lag(c0)", "partition by c1", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo =
      buildWindowQuery({data}, "lag(c0, 1, 100)", "partition by c1", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery({data}, "lag(c0, 2, c2)", "partition by c1", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);
}

// Make sure partitionOffset logic in LagFunction::apply works correctly.
TEST_F(LagTest, largePartitions) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
      // Offsets with nulls.
      makeFlatVector<int64_t>(
          10'000, [](auto row) { return 1 + row % 5; }, nullEvery(7)),
      // Default values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row * 10; }),
  });

  createDuckDbTable({data});

  auto queryInfo = buildWindowQuery({data}, "lag(c0)", "order by c0", "");

  auto assertResults = [&]() {
    AssertQueryBuilder(queryInfo.planNode, duckDbQueryRunner_)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(queryInfo.querySql);
  };

  assertResults();

  queryInfo = buildWindowQuery({data}, "lag(c0, 5)", "order by c0", "");
  assertResults();

  queryInfo = buildWindowQuery({data}, "lag(c0, 5, 100)", "order by c0", "");
  assertResults();

  queryInfo =
      buildWindowQuery({data}, "lag(c0, 50000, 100)", "order by c0", "");
  assertResults();

  queryInfo = buildWindowQuery({data}, "lag(c0, c1)", "order by c0", "");
  assertResults();

  queryInfo = buildWindowQuery({data}, "lag(c0, 50000, c2)", "order by c0", "");
  assertResults();

  queryInfo = buildWindowQuery({data}, "lag(c0, c1, c2)", "order by c0", "");
  assertResults();
}

TEST_F(LagTest, invalidOffset) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Offsets.
      makeFlatVector<int64_t>({1, 0, -2, 2, 4}),
  });

  auto copyResults = [&](const std::string& sql) {
    auto queryInfo = buildWindowQuery({data}, sql, "", "");
    AssertQueryBuilder(queryInfo.planNode).copyResults(pool());
  };

  VELOX_ASSERT_THROW(
      copyResults("lag(c0, -1)"), "(-1 vs. 0) Offset must be at least 0");
  VELOX_ASSERT_THROW(
      copyResults("lag(c0, c1)"), "(-2 vs. 0) Offset must be at least 0");
}

// Verify that lag function doesn't take frames into account. It operates on the
// whole partition instead.
TEST_F(LagTest, emptyFrames) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  createDuckDbTable({data});

  static const std::string kEmptyFrame =
      "rows between 100 preceding AND 90 preceding";

  auto queryInfo = buildWindowQuery({data}, "lag(c0)", "", kEmptyFrame);

  // DuckDB results are incorrect. It returns NULL for empty frames.
  auto expected = makeRowVector({
      data->childAt(0),
      makeNullableFlatVector<int64_t>({std::nullopt, 1, 2, 3, 4}),
  });
  assertQuery(queryInfo.planNode, expected);

  queryInfo = buildWindowQuery({data}, "lag(c0, 2)", "", kEmptyFrame);
  expected = makeRowVector({
      data->childAt(0),
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt, 1, 2, 3}),
  });
  assertQuery(queryInfo.planNode, expected);

  queryInfo = buildWindowQuery({data}, "lag(c0, 2, 100)", "", kEmptyFrame);
  expected = makeRowVector({
      data->childAt(0),
      makeFlatVector<int64_t>({100, 100, 1, 2, 3}),
  });
  assertQuery(queryInfo.planNode, expected);
}

} // namespace
} // namespace facebook::velox::window::test
