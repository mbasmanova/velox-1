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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions {
namespace {

class FindFirstTest : public functions::test::FunctionBaseTest {
 protected:
  void findFirst(
      const VectorPtr& input,
      const std::string& lambda,
      const VectorPtr& expected) {
    const std::string expr = fmt::format("find_first(c0, x -> ({}))", lambda);

    SCOPED_TRACE(expr);
    auto result = evaluate(expr, makeRowVector({input}));
    velox::test::assertEqualVectors(expected, result);
  }

  void tryFindFirst(
      const VectorPtr& input,
      const std::string& lambda,
      const VectorPtr& expected) {
    const std::string expr =
        fmt::format("try(find_first(c0, x -> ({})))", lambda);

    SCOPED_TRACE(expr);
    auto result = evaluate(expr, makeRowVector({input}));
    velox::test::assertEqualVectors(expected, result);
  }

  void findFirstFails(
      const VectorPtr& input,
      const std::string& lambda,
      const std::string& errorMessage) {
    const std::string expr = fmt::format("find_first(c0, x -> ({}))", lambda);

    SCOPED_TRACE(expr);
    VELOX_ASSERT_THROW(evaluate(expr, makeRowVector({input})), errorMessage);
  }
};

TEST_F(FindFirstTest, basic) {
  auto data = makeArrayVectorFromJson<int32_t>({
      "null",
      "[1, 2, 3]",
      "[-1, 0, 1, 3, 2]",
      "[10, 11, 12]",
      "[-5, -4, -3, 0]",
      "[null, 1, null, 2, null, 3]",
      "[null, null, null]",
  });

  auto expected = makeNullableFlatVector<int32_t>({
      std::nullopt,
      2,
      3,
      10,
      std::nullopt,
      2,
      std::nullopt,
  });

  findFirst(data, "x > 1", expected);
  findFirstFails(data, "x is null", "find_first found NULL as the first match");
}

TEST_F(FindFirstTest, errors) {
  auto data = makeArrayVectorFromJson<int32_t>({
      "[1, 2, 3, 0]",
      "[-1, 3, 0, 5]",
      "[5, 6, 7, 0]",
  });

  auto expected = makeNullableFlatVector<int32_t>({
      1,
      3,
      std::nullopt,
  });

  findFirstFails(data, "10 / x > 2", "division by zero");
  findFirst(data, "try(10 / x) > 2", expected);
  tryFindFirst(data, "10 / x > 2", expected);
}

} // namespace
} // namespace facebook::velox::functions
