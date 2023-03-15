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
#include "velox/exec/WindowFunction.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::test {

class PlanNodeSerdeTest : public testing::Test,
                          public velox::test::VectorTestBase {
 protected:
  PlanNodeSerdeTest() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();

    Type::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();

    data_ = {makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3}),
        makeFlatVector<int32_t>({10, 20, 30}),
        makeConstant(true, 3),
    })};
  }

  void testSerde(const core::PlanNodePtr& plan) {
    auto serialized = plan->serialize();

    auto copy =
        velox::ISerializable::deserialize<core::PlanNode>(serialized, pool());

    ASSERT_EQ(plan->toString(true, true), copy->toString(true, true));
  }

  std::vector<RowVectorPtr> data_;
};

TEST_F(PlanNodeSerdeTest, values) {
  auto plan = PlanBuilder().values({data_}).planNode();
  testSerde(plan);

  plan = PlanBuilder()
             .values({data_}, true /*parallelizable*/, 5 /*repeatTimes*/)
             .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, assignUniqueId) {
  auto plan = PlanBuilder().values({data_}).assignUniqueId().planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, filter) {
  auto plan = PlanBuilder().values({data_}).filter("c0 > 100").planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, project) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .project({"c0 * 10", "c0 + c1", "c1 > 0"})
                  .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, aggregation) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .partialAggregation({"c0"}, {"count(1)", "sum(c1)"})
                  .finalAggregation()
                  .planNode();

  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, hashJoin) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values({data_})
                  .hashJoin(
                      {"c0"},
                      {"c0"},
                      PlanBuilder(planNodeIdGenerator).values(data_).planNode(),
                      "",
                      {})
                  .planNode();

  testSerde(plan);
}
} // namespace facebook::velox::exec::test
