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

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::aggregate::test;

namespace facebook::velox::functions::sparksql::aggregate::test {

namespace {

using namespace facebook::velox::aggregate::test;

class FirstAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    aggregate::registerAggregateFunctions("spark_");
    AggregationTestBase::disableTestStreaming();
  }

  template <typename T>
  void testGroupBy() {
    auto vectors = {makeRowVector({
        makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
        makeFlatVector<T>(
            98, // size
            [](auto row) { return row; }, // valueAt
            [](auto row) { return row % 3 == 0; }), // nullAt
    })};

    createDuckDbTable(vectors);

    // Verify when ignoreNull is true.
    testAggregations(
        vectors,
        {"c0"},
        {"spark_first_ignore_null(c1)"},
        "SELECT c0, first(c1 ORDER BY c1 NULLS LAST) FROM tmp GROUP BY c0");

    // Verify when ignoreNull is false.
    // Expected result should have first 7 rows [91..98) including nulls.
    auto expected = {makeRowVector({
        makeFlatVector<int32_t>(7, [](auto row) { return row; }),
        makeFlatVector<T>(
            7, // size
            [](auto row) { return row; }, // valueAt
            [](auto row) { return row % 3 == 0; }), // nullAt
    })};
    testAggregations(vectors, {"c0"}, {"spark_first(c1)"}, expected);
  }

  template <typename T>
  void testGlobalAggregation() {
    auto vectors = {makeRowVector({
        makeNullableFlatVector<T>({std::nullopt, 1, 2, std::nullopt}),
    })};

    // Verify when ignoreNull is true.
    auto expectedTrue = {makeRowVector({makeNullableFlatVector<T>({1})})};
    testAggregations(vectors, {}, {"spark_first_ignore_null(c0)"}, expectedTrue);

    // Verify when ignoreNull is false.
//    auto expectedFalse = {
//        makeRowVector({makeNullableFlatVector<T>({std::nullopt})})};
//    testAggregations(vectors, {}, {"spark_first(c0)"}, expectedFalse);
  }
};

// Verify aggregation with group by keys for TINYINT.
TEST_F(FirstAggregateTest, tinyIntGroupBy) {
  testGroupBy<int8_t>();
}

// Verify global aggregation for TINYINT.
TEST_F(FirstAggregateTest, tinyIntGlobal) {
  testGlobalAggregation<int8_t>();
}

// Verify aggregation with group by keys for SMALLINT.
TEST_F(FirstAggregateTest, smallIntGroupBy) {
  testGroupBy<int16_t>();
}

// Verify global aggregation for SMALLINT.
TEST_F(FirstAggregateTest, smallIntGlobal) {
  testGlobalAggregation<int16_t>();
}

// Verify aggregation with group by keys for INTEGER.
TEST_F(FirstAggregateTest, integerGroupBy) {
  testGroupBy<int32_t>();
}

// Verify global aggregation for INTEGER.
TEST_F(FirstAggregateTest, integerGlobal) {
  testGlobalAggregation<int32_t>();
}

// Verify aggregation with group by keys for BIGINT.
TEST_F(FirstAggregateTest, bigintGroupBy) {
  testGroupBy<int64_t>();
}

// Verify global aggregation for BIGINT.
TEST_F(FirstAggregateTest, bigintGlobal) {
  testGlobalAggregation<int64_t>();
}

// Verify aggregation with group by keys for REAL.
TEST_F(FirstAggregateTest, realGroupBy) {
  testGroupBy<float>();
}

// Verify global aggregation for REAL.
TEST_F(FirstAggregateTest, realGlobal) {
  testGlobalAggregation<float>();
}

// Verify aggregation with group by keys for DOUBLE.
TEST_F(FirstAggregateTest, doubleGroupBy) {
  testGroupBy<double>();
}

// Verify global aggregation for DOUBLE.
TEST_F(FirstAggregateTest, doubleGlobal) {
  testGlobalAggregation<double>();
}

// Verify aggregation with group by keys for VARCHAR.
TEST_F(FirstAggregateTest, varcharGroupBy) {
  std::vector<std::string> data(98);
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeFlatVector<StringView>(
          98, // size
          [&data](auto row) {
            data[row] = std::to_string(row);
            return StringView(data[row]);
          }, // valueAt
          [](auto row) { return row % 3 == 0; }), // nullAt
  })};

  createDuckDbTable(vectors);

  // Verify when ignoreNull is true.
  testAggregations(
      vectors,
      {"c0"},
      {"spark_first_ignore_null(c1)"},
      "SELECT c0, first(c1) FROM tmp WHERE c1 IS NOT NULL GROUP BY c0");

  // Verify when ignoreNull is false.
  // Expected result should have first 7 rows [91..98) including nulls.
  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeFlatVector<StringView>(
          7, // size
          [&data](auto row) { return StringView(data[row]); }, // valueAt
          [](auto row) { return row % 3 == 0; }), // nullAt
  })};
  testAggregations(vectors, {"c0"}, {"spark_first(c1)"}, expected);
}

// Verify global aggregation for VARCHAR.
TEST_F(FirstAggregateTest, varcharGlobal) {
  auto vectors = {makeRowVector({
      makeNullableFlatVector<std::string>(
          {std::nullopt, "a", "b", std::nullopt}),
  })};

  // Verify when ignoreNull is true.
  auto expectedTrue = {
      makeRowVector({makeNullableFlatVector<std::string>({"a"})})};
  testAggregations(vectors, {}, {"spark_first_ignore_null(c0)"}, expectedTrue);

  // Verify when ignoreNull is false.
  auto expectedFalse = {
      makeRowVector({makeNullableFlatVector<std::string>({std::nullopt})})};
  testAggregations(vectors, {}, {"spark_first(c0)"}, expectedFalse);
}

// Verify aggregation with group by keys for ARRAY.
TEST_F(FirstAggregateTest, arrayGroupBy) {
  std::vector<RowVectorPtr> vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeArrayVector<int64_t>(
          98, // size
          [](auto row) { return row % 3; }, // sizeAt
          [](auto row, auto idx) { return row * 100 + idx; }, // valueAt
          // Even rows are null.
          [](auto row) { return row % 2 == 0; }), // nullAt
  })};
  LOG(INFO) << vectors[0]->toString(0, vectors[0]->size());

  auto expectedTrue = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeArrayVector<int64_t>(
          7,
          [](auto row) {
            // Even rows are null, for these return values for (row - 7)
            return (row % 2) ? row % 3 : (row + 7) % 3;
          },
          [](auto row, auto idx) {
            // Even rows are null, for these return values for (row - 7)
            return (row % 2) ? row * 100 + idx
                                    : (row + 7) * 100 + idx;
          }),
  })};

  // Verify when ignoreNull is true.
  testAggregations(
      vectors, {"c0"}, {"spark_first_ignore_null(c1)"}, expectedTrue);

  // Verify when ignoreNull is false.
  // Expected result should have first 7 rows [91..98) of input |vectors|
  auto expectedFalse = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeArrayVector<int64_t>(
          7, // size
          [](auto row) { return row % 3; }, // sizeAt
          [](auto row, auto idx) { return row * 100 + idx; }, // valueAt
          [](auto row) { return row % 2 == 0; }), // nullAt
  })};

  testAggregations(vectors, {"c0"}, {"spark_first(c1)"}, expectedFalse);
}

// Verify global aggregation for ARRAY.
TEST_F(FirstAggregateTest, arrayGlobal) {
  auto vectors = {makeRowVector({
      makeNullableArrayVector<int64_t>(
          {std::nullopt, {{1, 2}}, {{3, 4}}, std::nullopt}),
  })};

  auto expectedTrue = {makeRowVector({
      makeArrayVector<int64_t>({{1, 2}}),
  })};

  // Verify when ignoreNull is true.
  testAggregations(vectors, {}, {"spark_first_ignore_null(c0)"}, expectedTrue);

  // Verify when ignoreNull is false.
  auto expectedFalse = {makeRowVector({
      makeNullableArrayVector<int64_t>({std::nullopt}),
  })};
  testAggregations(vectors, {}, {"spark_first(c0)"}, expectedFalse);
}

// Verify aggregation with group by keys for MAP column.
TEST_F(FirstAggregateTest, mapGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeMapVector<int64_t, float>(
          98, // size
          [](auto row) { return row % 2 ? 0 : 2; }, // sizeAt
          [](auto idx) { return idx; }, // keyAt
          [](auto idx) { return idx * 0.1; }), // valueAt
  })};

  // Expected result should have first 7 rows of input |vectors|
  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeMapVector<int64_t, float>(
          7, // size
          [](auto row) { return row % 2 ? 0 : 2; }, // sizeAt
          [](auto idx) { return idx; }, // keyAt
          [](auto idx) { return idx * 0.1; }), // valueAt
  })};

  testAggregations(vectors, {"c0"}, {"spark_first(c1)"}, expected);
}

// Verify global aggregation for MAP column.
TEST_F(FirstAggregateTest, mapGlobal) {
  auto O = [](const std::vector<std::pair<int64_t, std::optional<float>>>& m) {
    return std::make_optional(m);
  };
  auto vectors = {makeRowVector({
      makeNullableMapVector<int64_t, float>(
          {std::nullopt, O({{1, 2.0}}), O({{2, 4.0}}), std::nullopt}),
  })};

  auto expectedTrue = {makeRowVector({
      makeNullableMapVector<int64_t, float>({O({{1, 2.0}})}),
  })};

  // Verify when ignoreNull is true.
  testAggregations(vectors, {}, {"spark_first_ignore_null(c0)"}, expectedTrue);

  // Verify when ignoreNull is false.
  auto expectedFalse = {makeRowVector({
      makeNullableMapVector<int64_t, float>({std::nullopt}),
  })};
  testAggregations(vectors, {}, {"spark_first(c0)"}, expectedFalse);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::aggregate::test
