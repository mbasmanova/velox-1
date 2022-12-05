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

#include "velox/connectors/hive/PartitionIdGenerator.h"
#include "gtest/gtest.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::connector::hive {

class PartitionIdGeneratorTest : public ::testing::Test,
                                 public test::VectorTestBase {};

TEST_F(PartitionIdGeneratorTest, numDistinctIds) {
  int32_t numPartitions = 1000;

  PartitionIdGenerator idGenerator(
      ROW({VARCHAR(), INTEGER(), BIGINT()}), {1, 2}, pool_.get());

  RowVectorPtr input = makeRowVector({
      makeFlatVector<StringView>(
          numPartitions * 3,
          [&](auto row) {
            return StringView(fmt::format("str_{}", row % numPartitions));
          }),
      makeFlatVector<int32_t>(
          numPartitions * 3, [&](auto row) { return row % numPartitions; }),
      makeFlatVector<int64_t>(
          numPartitions * 3,
          [&](auto row) { return (row * 10) % numPartitions; }),
  });

  raw_vector<uint64_t> ids;
  EXPECT_EQ(idGenerator.run(input, ids).size(), 0);
  EXPECT_EQ(
      std::unordered_set<uint64_t>(ids.begin(), ids.end()).size(),
      numPartitions);
}

TEST_F(PartitionIdGeneratorTest, rehash) {
  int32_t numPartitions = 1000;

  PartitionIdGenerator idGenerator(
      ROW({VARCHAR(), INTEGER(), BIGINT()}), {1, 2}, pool());

  RowVectorPtr smallInput = makeRowVector({
      makeFlatVector<StringView>(
          numPartitions,
          [&](auto row) { return StringView(fmt::format("str_{}", row)); }),
      makeFlatVector<int32_t>(numPartitions, [&](auto row) { return row; }),
      makeFlatVector<int64_t>(numPartitions, [&](auto row) { return row; }),
  });
  RowVectorPtr largeInput = makeRowVector({
      makeFlatVector<StringView>(
          numPartitions,
          [&](auto row) {
            return StringView(fmt::format("str_{}", row * 100));
          }),
      makeFlatVector<int32_t>(
          numPartitions, [&](auto row) { return row * 100; }),
      makeFlatVector<int64_t>(
          numPartitions, [&](auto row) { return row * 100; }),
  });

  raw_vector<uint64_t> smallIdsOld;
  EXPECT_EQ(idGenerator.run(smallInput, smallIdsOld).size(), 0);

  std::map<uint64_t, uint64_t> idMap;
  raw_vector<uint64_t> largeIds;
  EXPECT_EQ(
      (idMap = idGenerator.run(largeInput, largeIds)).size(), numPartitions);

  raw_vector<uint64_t> smallIdsNew;
  EXPECT_EQ(idGenerator.run(smallInput, smallIdsNew).size(), 0);

  for (auto i = 0; i < numPartitions; i++) {
    EXPECT_TRUE(idMap[smallIdsOld[i]] = smallIdsNew[i]);
  }
}

} // namespace facebook::velox::connector::hive
