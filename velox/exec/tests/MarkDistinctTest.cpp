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

#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

class MarkDistinctTest : public OperatorTestBase {
 public:
  template <class NativeType>
  std::function<VectorPtr()> baseFunctorTemplate() {
    return [&]() {
      return makeFlatVector<NativeType>(
          2, [&](vector_size_t row) { return (NativeType)(row % 2); }, nullptr);
    };
  }

  template <class NativeType>
  void markDistinctTest(
      std::function<VectorPtr()> baseFunctor,
      std::function<TypePtr()> exprTypeFunctor = []() {
        using implType = typename CppToType<NativeType>::ImplType;
        return std::make_shared<implType>();
      }) {
    std::vector<RowVectorPtr> vectors;

    VectorPtr base = baseFunctor();

    const vector_size_t baseSize = base->size();
    const vector_size_t size = baseSize * 2;
    auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (auto i = 0; i < size; ++i) {
      rawIndices[i] = i % (baseSize);
    }
    auto baseEncoded =
        BaseVector::wrapInDictionary(nullptr, indices, size, base);

    vectors.push_back(makeRowVector({baseEncoded}));

    auto field1 = std::make_shared<const core::FieldAccessTypedExpr>(
        exprTypeFunctor(), "c0");

    auto markerVariable = std::make_shared<const core::FieldAccessTypedExpr>(
        BOOLEAN(), "c0$Distinct");

    auto distinctCol = makeFlatVector<bool>(
        size, [&](vector_size_t row) { return row < baseSize; }, nullptr);

    RowVectorPtr expectedResults = makeRowVector({baseEncoded, distinctCol});

    auto op = PlanBuilder()
                  .values(vectors)
                  .markDistinct(markerVariable, {field1}, std::nullopt)
                  .planNode();

    CursorParameters params;
    params.planNode = op;

    auto result = readCursor(params, [](auto) {});
    auto res = result.second[0]->childAt(1);

    assertEqualVectors(distinctCol, res);
  }
};

template <typename T>
class MarkDistinctPODTest : public MarkDistinctTest {};

using MyTypes =
    ::testing::Types<int8_t, int16_t, int32_t, int64_t, float, double, bool>;
TYPED_TEST_SUITE(MarkDistinctPODTest, MyTypes);

TYPED_TEST(MarkDistinctPODTest, basic) {
  using cppColType = TypeParam;
  this->template markDistinctTest<cppColType>(
      this->template baseFunctorTemplate<cppColType>());
}

TEST_F(MarkDistinctTest, basicArrayTest) {
  using cppColType = Array<int64_t>;
  auto base = makeArrayVector<int64_t>({
      {1, 2, 3, 4, 5},
      {1, 2, 3},
  });
  markDistinctTest<cppColType>(
      [&]() { return base; }, []() { return CppToType<cppColType>::create(); });
}

TEST_F(MarkDistinctTest, basicMapTest) {
  using cppColType = Map<int8_t, int32_t>;
  auto base = makeMapVector<int8_t, int32_t>(
      {{{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}}, {{1, 1}, {1, 1}, {1, 1}}});
  markDistinctTest<cppColType>(
      [&]() { return base; }, []() { return CppToType<cppColType>::create(); });
}

TEST_F(MarkDistinctTest, basicVarcharTest) {
  using cppColType = StringView;
  auto base = makeFlatVector<StringView>({
      "{1, 2, 3, 4, 5}",
      "{1, 2, 3}",
  });
  markDistinctTest<cppColType>(
      [&]() { return base; }, []() { return CppToType<cppColType>::create(); });
}

TEST_F(MarkDistinctTest, basicRowTest) {
  using cppColType = Row<Array<int64_t>, Map<int8_t, int32_t>>;
  auto base = makeRowVector(
      {makeArrayVector<int64_t>({
           {1, 2, 3, 4, 5},
           {1, 2, 3},
       }),
       makeMapVector<int8_t, int32_t>(
           {{{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
            {{1, 1}, {1, 1}, {1, 1}}})});
  markDistinctTest<cppColType>(
      [&]() { return base; }, []() { return CppToType<cppColType>::create(); });
}

// This test verifies this query:
// WITH sample AS ( SELECT * FROM (VALUES (1,1,1), (1,1,2), (1,1,1), (2,1,2),
// (2,2,1), (2,3,2)) AS account (c1, c2, c3) ) SELECT c1,sum(distinct
// c2),sum(distinct c3) from sample group by c1;
TEST_F(MarkDistinctTest, distinctAggregationTest) {
  std::vector<RowVectorPtr> vectors;
  // Simulate the input over 3 splits.
  vectors.push_back(makeRowVector(
      {makeFlatVector<int32_t>({1, 1}),
       makeFlatVector<int32_t>({1, 1}),
       makeFlatVector<int32_t>({1, 2})}));
  vectors.push_back(makeRowVector(
      {makeFlatVector<int32_t>({1, 2}),
       makeFlatVector<int32_t>({1, 1}),
       makeFlatVector<int32_t>({1, 2})}));
  vectors.push_back(makeRowVector(
      {makeFlatVector<int32_t>({2, 2}),
       makeFlatVector<int32_t>({2, 3}),
       makeFlatVector<int32_t>({1, 2})}));

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      distinctVariablesC1;
  distinctVariablesC1.push_back(
      std::make_shared<const core::FieldAccessTypedExpr>(INTEGER(), "c0"));
  distinctVariablesC1.push_back(
      std::make_shared<const core::FieldAccessTypedExpr>(INTEGER(), "c1"));

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      distinctVariablesC2;
  distinctVariablesC2.push_back(
      std::make_shared<const core::FieldAccessTypedExpr>(INTEGER(), "c0"));
  distinctVariablesC2.push_back(
      std::make_shared<const core::FieldAccessTypedExpr>(INTEGER(), "c2"));

  auto markerVariableC1 = std::make_shared<const core::FieldAccessTypedExpr>(
      BOOLEAN(), "c1$Distinct");
  auto markerVariableC2 = std::make_shared<const core::FieldAccessTypedExpr>(
      BOOLEAN(), "c2$Distinct");
  auto op =
      PlanBuilder()
          .values(vectors)
          .markDistinct(markerVariableC1, distinctVariablesC1, std::nullopt)
          .markDistinct(markerVariableC2, distinctVariablesC2, std::nullopt)
          .singleAggregation(
              {"c0"},
              {"sum(c1)", "sum(c2)"},
              {markerVariableC1->name(), markerVariableC2->name()})
          .orderBy({"c0"}, false)
          .planNode();

  CursorParameters params;
  params.planNode = op;

  auto result = readCursor(params, [](auto) {});
  auto actual = result.second;

  RowVectorPtr expected = makeRowVector(
      {makeFlatVector<int32_t>({1, 2}),
       makeFlatVector<int64_t>({1, 6}),
       makeFlatVector<int64_t>({3, 3})});
  assertEqualVectors(expected, actual[0]);
}