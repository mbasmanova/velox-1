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

#include <optional>

#include "velox/row/UnsafeRowDeserializers.h"

#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace facebook::velox::row {
class UnsafeRowSerdeTest : public testing::Test, public test::VectorTestBase {
 public:
  void testSerde(const RowVectorPtr& data) {
    std::vector<std::optional<std::string_view>> serializedRows;
    serializedRows.reserve(data->size());

    std::vector<BufferPtr> buffers;
    buffers.reserve(data->size());

    auto fixedRowSize = UnsafeRowFast::fixedRowSize(asRowType(data->type()));

    UnsafeRowFast fast(data);
    for (auto i = 0; i < data->size(); ++i) {
      const auto expectedRowSize = fast.rowSize(i);
      if (fixedRowSize) {
        EXPECT_EQ(fixedRowSize.value(), expectedRowSize);
      }

      buffers.push_back(
          AlignedBuffer::allocate<char>(expectedRowSize, pool_.get()));

      const auto rowSize = fast.serialize(i, buffers[i]->asMutable<char>());

      EXPECT_EQ(expectedRowSize, rowSize);

      serializedRows.push_back(
          std::string_view(buffers[i]->asMutable<char>(), rowSize));
    }

    const auto copy = UnsafeRowDeserializer::deserialize(
        serializedRows, data->type(), pool());
    test::assertEqualVectors(data, copy);
  }
};

TEST_F(UnsafeRowSerdeTest, unknown) {
  const vector_size_t batchSize = 10;
  auto flatVector = makeAllNullFlatVector<UnknownValue>(batchSize);
  testSerde(makeRowVector({flatVector}));

  auto arrayVector = makeAllNullArrayVector(batchSize, UNKNOWN());
  testSerde(makeRowVector({arrayVector}));

  auto mapVector = makeAllNullMapVector(batchSize, UNKNOWN(), UNKNOWN());
  testSerde(makeRowVector({mapVector}));

  auto rowVector = makeRowVector({flatVector, arrayVector, mapVector});
  testSerde(makeRowVector({rowVector}));

  testSerde(makeRowVector({arrayVector, mapVector, flatVector, rowVector}));
}

} // namespace facebook::velox::row
