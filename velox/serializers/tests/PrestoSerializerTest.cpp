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
#include "velox/serializers/PrestoSerializer.h"
#include <folly/Random.h>
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class PrestoSerializerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = memory::getDefaultScopedMemoryPool();
    serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
    vectorMaker_ = std::make_unique<test::VectorMaker>(pool_.get());
  }

  void sanityCheckEstimateSerializedSize(
      RowVectorPtr rowVector,
      const folly::Range<const IndexRange*>& ranges) {
    auto numRows = rowVector->size();
    std::vector<vector_size_t> rowSizes(numRows, 0);
    std::vector<vector_size_t*> rawRowSizes(numRows);
    for (auto i = 0; i < numRows; i++) {
      rawRowSizes[i] = &rowSizes[i];
    }
    serde_->estimateSerializedSize(rowVector, ranges, rawRowSizes.data());
  }

  void serialize(
      RowVectorPtr rowVector,
      std::ostream* output,
      const VectorSerde::Options* serdeOptions) {
    auto numRows = rowVector->size();

    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }

    sanityCheckEstimateSerializedSize(
        rowVector, folly::Range(rows.data(), numRows));

    auto arena =
        std::make_unique<StreamArena>(memory::MappedMemory::getInstance());
    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto serializer =
        serde_->createSerializer(rowType, numRows, arena.get(), serdeOptions);

    serializer->append(rowVector, folly::Range(rows.data(), numRows));
    facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
    OStreamOutputStream out(output, &listener);
    serializer->flush(&out);
  }

  std::unique_ptr<ByteStream> toByteStream(const std::string& input) {
    auto byteStream = std::make_unique<ByteStream>();
    ByteRange byteRange{
        reinterpret_cast<uint8_t*>(const_cast<char*>(input.data())),
        (int32_t)input.length(),
        0};
    byteStream->resetInput({byteRange});
    return byteStream;
  }

  RowVectorPtr deserialize(
      std::shared_ptr<const RowType> rowType,
      const std::string& input,
      const VectorSerde::Options* serdeOptions) {
    auto byteStream = toByteStream(input);

    RowVectorPtr result;
    serde_->deserialize(
        byteStream.get(), pool_.get(), rowType, &result, serdeOptions);
    return result;
  }

  RowVectorPtr makeTestVector(vector_size_t size) {
    auto a = vectorMaker_->flatVector<int64_t>(
        size, [](vector_size_t row) { return row; });
    auto b = vectorMaker_->flatVector<double>(
        size, [](vector_size_t row) { return row * 0.1; });

    std::vector<VectorPtr> childVectors = {a, b};

    return vectorMaker_->rowVector(childVectors);
  }

  void writeInt32(OutputStream* out, int32_t value) {
    out->write(reinterpret_cast<char*>(&value), sizeof(value));
  }

  void writeInt64(OutputStream* out, int64_t value) {
    out->write(reinterpret_cast<char*>(&value), sizeof(value));
  }

  // Take a base vector and serialize it as an RLE vector
  void serializeAsRle(
      int32_t numValues,
      RowVectorPtr rowValueVector,
      std::ostream* output) {
    // row must have a single vector
    EXPECT_EQ(rowValueVector->size(), 1);

    // value vector must have a single value
    EXPECT_EQ(rowValueVector->childAt(0)->size(), 1);

    const char codec = 0;
    const int32_t sizeInBytesOffset = 4 + 1;
    const int32_t headerSize = sizeInBytesOffset + 4 + 4 + 8;
    const int32_t numRowsSize = 4;
    const int32_t dataOffset = headerSize + numRowsSize;

    // serialize value vector
    std::ostringstream outBase;
    serialize(rowValueVector, &outBase, nullptr);
    auto bytes = outBase.str();
    auto sourcePage = toByteStream(bytes);
    int32_t sourceOffset = sourcePage->tellp();

    // read number of streams
    sourcePage->seekp(sourceOffset + headerSize);
    int32_t numStreams = sourcePage->read<int32_t>();

    // read size of serialized value vector
    sourcePage->seekp(sourceOffset + sizeInBytesOffset);
    int32_t sourceDataSize = sourcePage->read<int32_t>() - numRowsSize;

    // read bytes for serialized value vector
    std::string sourceData;
    sourceData.resize(sourceDataSize);
    sourcePage->seekp(sourceOffset + dataOffset);
    sourcePage->readBytes(&sourceData[0], sourceDataSize);

    OStreamOutputStream out(output);
    int32_t offset = out.tellp();

    /*
    Presto Page forrmat:
    Size in bytes, Section name
    4 - num rows
    1 - codec marker
    4 - uncompressed size in bytes
    4 - compressed size in bytes (compression is not supported atm)
    8 - checksum
    4 - num streams
    x - serialized streams:
      4 - stream name length
      ... - stream name
      ... - stream data
    */

    // start creating the new serialized page
    writeInt32(&out, numValues);

    out.write(&codec, 1); // write codec w/o checksum
    writeInt32(&out, 0); // make space for uncompressedSizeInBytes
    writeInt32(&out, 0); // make space for sizeInBytes
    writeInt64(&out, 0); // write zero checksum

    // write stream count
    writeInt32(&out, numStreams + 1); // write number of strmeas + RLE stream

    // write RLE encoding name length + name
    writeInt32(&out, 3);
    out.write("RLE", 3);
    writeInt32(&out, numValues); // write number of RLE encoded values

    // copy serialized value vector into the RLE page
    out.write(&sourceData[0], sourceDataSize);

    // fill in uncompressedSizeInBytes & sizeInBytes
    int32_t size = (int32_t)out.tellp() - offset;
    int32_t uncompressedSize = size - headerSize;

    out.seekp(offset + sizeInBytesOffset);
    writeInt32(&out, uncompressedSize);
    writeInt32(&out, uncompressedSize);
    out.seekp(offset + size);
  }

  void testRoundTrip(
      VectorPtr vector,
      const VectorSerde::Options* serdeOptions = nullptr) {
    auto rowVector = vectorMaker_->rowVector({vector});
    std::ostringstream out;
    serialize(rowVector, &out, serdeOptions);

    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto deserialized = deserialize(rowType, out.str(), serdeOptions);
    assertEqualVectors(deserialized, rowVector);
  }

  void testRleRoundTrip(int32_t numValues, VectorPtr valueVector) {
    EXPECT_EQ(valueVector->size(), 1); // value vector must have a single value
    EXPECT_GT(numValues, 0);

    auto rowVector = vectorMaker_->rowVector({valueVector});
    std::ostringstream out;
    serializeAsRle(numValues, rowVector, &out);

    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto deserialized = deserialize(rowType, out.str(), nullptr);

    auto expectedRleVector =
        BaseVector::wrapInConstant(numValues, 0, valueVector);
    auto expectedRleRowVector = vectorMaker_->rowVector({expectedRleVector});
    assertEqualVectors(deserialized, expectedRleRowVector);
  }

  std::unique_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<VectorSerde> serde_;
  std::unique_ptr<test::VectorMaker> vectorMaker_;
};

TEST_F(PrestoSerializerTest, basic) {
  vector_size_t numRows = 1'000;
  auto rowVector = makeTestVector(numRows);
  testRoundTrip(rowVector);
}

/// Test serialization of a dictionary vector that adds nulls to the base
/// vector.
TEST_F(PrestoSerializerTest, dictionaryWithExtraNulls) {
  vector_size_t size = 1'000;

  auto base =
      vectorMaker_->flatVector<int64_t>(10, [](auto row) { return row; });

  BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool_.get());
  auto rawNulls = nulls->asMutable<uint64_t>();
  for (auto i = 0; i < size; i++) {
    bits::setNull(rawNulls, i, i % 5 == 0);
  }

  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; i++) {
    if (i % 5 != 0) {
      rawIndices[i] = i % 10;
    }
  }

  auto dictionary = BaseVector::wrapInDictionary(nulls, indices, size, base);
  testRoundTrip(dictionary);
}

TEST_F(PrestoSerializerTest, emptyPage) {
  auto rowVector = vectorMaker_->rowVector(ROW({"a"}, {BIGINT()}), 0);

  std::ostringstream out;
  serialize(rowVector, &out, nullptr);

  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
  auto deserialized = deserialize(rowType, out.str(), nullptr);
  assertEqualVectors(deserialized, rowVector);
}

TEST_F(PrestoSerializerTest, emptyArray) {
  auto arrayVector = vectorMaker_->arrayVector<int32_t>(
      1'000,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t row) { return row; });

  testRoundTrip(arrayVector);
}

TEST_F(PrestoSerializerTest, emptyMap) {
  auto mapVector = vectorMaker_->mapVector<int32_t, int32_t>(
      1'000,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row * 2; });

  testRoundTrip(mapVector);
}

TEST_F(PrestoSerializerTest, timestampWithTimeZone) {
  auto timestamp = vectorMaker_->flatVector<int64_t>(
      100, [](auto row) { return 10'000 + row; });
  auto timezone =
      vectorMaker_->flatVector<int16_t>(100, [](auto row) { return row % 37; });

  auto vector = std::make_shared<RowVector>(
      pool_.get(),
      TIMESTAMP_WITH_TIME_ZONE(),
      BufferPtr(nullptr),
      100,
      std::vector<VectorPtr>{timestamp, timezone});

  testRoundTrip(vector);

  // Add some nulls.
  for (auto i = 0; i < 100; i += 7) {
    vector->setNull(i, true);
  }
  testRoundTrip(vector);
}

TEST_F(PrestoSerializerTest, intervalDayTime) {
  auto vector = vectorMaker_->flatVector<IntervalDayTime>(100, [](auto row) {
    return IntervalDayTime(row + folly::Random::rand32());
  });

  testRoundTrip(vector);

  // Add some nulls.
  for (auto i = 0; i < 100; i += 7) {
    vector->setNull(i, true);
  }
  testRoundTrip(vector);
}

TEST_F(PrestoSerializerTest, unknown) {
  const vector_size_t size = 123;
  auto constantVector =
      BaseVector::createConstant(variant(TypeKind::UNKNOWN), 123, pool_.get());
  testRoundTrip(constantVector);

  auto flatVector = BaseVector::create(UNKNOWN(), size, pool_.get());
  for (auto i = 0; i < size; i++) {
    flatVector->setNull(i, true);
  }
  testRoundTrip(flatVector);
}

TEST_F(PrestoSerializerTest, multiPage) {
  std::ostringstream out;

  // page 1
  auto a = makeTestVector(1'234);
  serialize(a, &out, nullptr);

  // page 2
  auto b = makeTestVector(538);
  serialize(b, &out, nullptr);

  // page 3
  auto c = makeTestVector(2'048);
  serialize(c, &out, nullptr);

  auto bytes = out.str();

  auto rowType = std::dynamic_pointer_cast<const RowType>(a->type());
  auto byteStream = toByteStream(bytes);

  RowVectorPtr deserialized;
  serde_->deserialize(
      byteStream.get(), pool_.get(), rowType, &deserialized, nullptr);
  ASSERT_FALSE(byteStream->atEnd());
  assertEqualVectors(deserialized, a);

  serde_->deserialize(
      byteStream.get(), pool_.get(), rowType, &deserialized, nullptr);
  assertEqualVectors(deserialized, b);
  ASSERT_FALSE(byteStream->atEnd());

  serde_->deserialize(
      byteStream.get(), pool_.get(), rowType, &deserialized, nullptr);
  assertEqualVectors(deserialized, c);
  ASSERT_TRUE(byteStream->atEnd());
}

TEST_F(PrestoSerializerTest, timestampWithNanosecondPrecision) {
  // Verify that nanosecond precision is preserved when the right options are
  // passed to the serde.
  const serializer::presto::PrestoVectorSerde::PrestoOptions
      kUseLosslessTimestampOptions(true);
  auto timestamp = vectorMaker_->flatVector<Timestamp>(
      {Timestamp{0, 0},
       Timestamp{12, 0},
       Timestamp{0, 17'123'456},
       Timestamp{1, 17'123'456},
       Timestamp{-1, 17'123'456}});
  testRoundTrip(timestamp, &kUseLosslessTimestampOptions);

  // Verify that precision is lost when no option is passed to the serde.
  auto timestampMillis = vectorMaker_->flatVector<Timestamp>(
      {Timestamp{0, 0},
       Timestamp{12, 0},
       Timestamp{0, 17'000'000},
       Timestamp{1, 17'000'000},
       Timestamp{-1, 17'000'000}});
  auto inputRowVector = vectorMaker_->rowVector({timestamp});
  auto expectedOutputWithLostPrecision =
      vectorMaker_->rowVector({timestampMillis});
  std::ostringstream out;
  serialize(inputRowVector, &out, {});
  auto rowType =
      std::dynamic_pointer_cast<const RowType>(inputRowVector->type());
  auto deserialized = deserialize(rowType, out.str(), {});
  assertEqualVectors(deserialized, expectedOutputWithLostPrecision);
}

TEST_F(PrestoSerializerTest, unscaledLongDecimal) {
  std::vector<int128_t> decimalValues(102);
  decimalValues[0] = UnscaledLongDecimal::min().unscaledValue();
  for (int row = 1; row < 101; row++) {
    decimalValues[row] = row - 50;
  }
  decimalValues[101] = UnscaledLongDecimal::max().unscaledValue();
  auto vector =
      vectorMaker_->longDecimalFlatVector(decimalValues, DECIMAL(20, 5));

  testRoundTrip(vector);

  // Add some nulls.
  for (auto i = 0; i < 102; i += 7) {
    vector->setNull(i, true);
  }
  testRoundTrip(vector);
}

TEST_F(PrestoSerializerTest, rlePrimitiveValue) {
  int32_t numRows = 5;

  auto booleanVector =
      vectorMaker_->flatVector<bool>(1, [](vector_size_t row) { return true; });
  testRleRoundTrip(5, booleanVector);

  auto longVector = vectorMaker_->flatVector<int64_t>(
      1, [](vector_size_t row) { return row; });
  testRleRoundTrip(5, longVector);

  auto doubleVector = vectorMaker_->flatVector<double>(
      1, [](vector_size_t row) { return row * 0.1; });
  testRleRoundTrip(5, doubleVector);

  std::vector<std::string> strings = {"hello"};
  auto stringVector = vectorMaker_->flatVector(strings);
  testRleRoundTrip(5, stringVector);

  std::vector<std::vector<int64_t>> arrayData = {{3, 25}};
  auto arrayVector = vectorMaker_->arrayVector<int64_t>(arrayData);
  testRleRoundTrip(5, arrayVector);

  auto mapKeys = vectorMaker_->flatVector<int32_t>({1});
  auto mapValues = vectorMaker_->flatVector<int64_t>({7});
  auto mapVector = vectorMaker_->mapVector({0}, mapKeys, mapValues);
  testRleRoundTrip(5, arrayVector);
}

TEST_F(PrestoSerializerTest, rleNulls) {
  int32_t numRows = 5;

  auto booleanVector = vectorMaker_->allNullFlatVector<bool>(1);
  testRleRoundTrip(5, booleanVector);

  auto longVector = vectorMaker_->allNullFlatVector<int64_t>(1);
  testRleRoundTrip(5, longVector);

  auto doubleVector = vectorMaker_->allNullFlatVector<double>(1);
  testRleRoundTrip(5, doubleVector);

  auto stringVector =
      vectorMaker_->flatVectorNullable<const char*>({std::nullopt});
  testRleRoundTrip(5, stringVector);

  auto arrayVector = vectorMaker_->allNullArrayVector(1, BIGINT());
  testRleRoundTrip(numRows, arrayVector);

  auto mapVector = vectorMaker_->allNullMapVector(1, VARCHAR(), BIGINT());
  testRleRoundTrip(numRows, mapVector);
}
