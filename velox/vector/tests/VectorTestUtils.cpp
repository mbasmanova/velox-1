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

#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

void checkVectorFlagsSet(const BaseVector& vector);

FlatVectorPtr<StringView> makeFlatVectorWithFlags(
    vector_size_t kSize,
    const BufferPtr& nulls,
    memory::MemoryPool* pool) {
  auto values = AlignedBuffer::allocate<StringView>(kSize, pool, "a"_sv);
  auto vector = std::make_shared<FlatVector<StringView>>(
      pool,
      VARCHAR(),
      nulls,
      kSize,
      std::move(values),
      std::vector<BufferPtr>(),
      /*stats*/ SimpleVectorStats<StringView>{"a"_sv, "a"_sv},
      /*distinctValueCount*/ 1,
      /*nullCount*/ 1,
      /*isSorted*/ true,
      /*representedBytes*/ 0,
      /*storageByteCount*/ 0);
  vector->computeAndSetIsAscii(SelectivityVector(kSize - 1));
  return vector;
}

ConstantVectorPtr<StringView> makeConstantVectorWithFlags(
    vector_size_t kSize,
    memory::MemoryPool* pool) {
  auto vector = std::make_shared<ConstantVector<StringView>>(
      pool,
      kSize,
      false,
      VARCHAR(),
      "a"_sv,
      /*stats*/ SimpleVectorStats<StringView>{"a"_sv, "a"_sv},
      /*representedBytes*/ 0,
      /*storageByteCount*/ 0);
  vector->computeAndSetIsAscii(SelectivityVector(kSize));
  return vector;
}

DictionaryVectorPtr<StringView> makeDictionaryVectorWithFlags(
    vector_size_t size,
    const BufferPtr& nulls,
    memory::MemoryPool* pool) {
  auto base = BaseVector::createConstant(VARCHAR(), "a", size, pool);
  auto vector = std::make_shared<DictionaryVector<StringView>>(
      pool,
      nulls,
      size,
      base,
      test::makeIndices(
          size, [](auto row) { return row; }, pool),
      /*stats*/ SimpleVectorStats<StringView>{"a"_sv, "a"_sv},
      /*distinctValueCount*/ 1,
      /*nullCount*/ 1,
      /*isSorted*/ true,
      /*representedBytes*/ 0,
      /*storageByteCount*/ 0);
  vector->computeAndSetIsAscii(SelectivityVector(size - 1));
  return vector;
}

MapVectorPtr makeMapVectorWithFlags(
    vector_size_t size,
    const BufferPtr& nulls,
    memory::MemoryPool* pool) {
  auto mapVector =
      BaseVector::create<MapVector>(MAP(VARCHAR(), VARCHAR()), size, pool);
  MapVector::canonicalize(mapVector);
  mapVector->setNullCount(BaseVector::countNulls(nulls, size));
  return mapVector;
}

namespace {

void checkBaseVectorFlagsSet(const BaseVector& vector) {
  EXPECT_TRUE(vector.getNullCount().has_value());
  EXPECT_TRUE(vector.getDistinctValueCount().has_value());
  EXPECT_TRUE(vector.representedBytes().has_value());
  EXPECT_TRUE(vector.storageBytes().has_value());
}

void checkBaseVectorFlagsCleared(const BaseVector& vector) {
  EXPECT_FALSE(vector.getNullCount().has_value());
  EXPECT_FALSE(vector.getDistinctValueCount().has_value());
  EXPECT_FALSE(vector.representedBytes().has_value());
  EXPECT_FALSE(vector.storageBytes().has_value());
}

template <TypeKind kind>
void checkVectorFlagsClearedTyped(const BaseVector& vector) {
  using T = typename TypeTraits<kind>::NativeType;

  auto simpleVector = vector.as<SimpleVector<T>>();
  EXPECT_FALSE(simpleVector->getStats().min.has_value());
  EXPECT_FALSE(simpleVector->getStats().max.has_value());
  EXPECT_FALSE(simpleVector->isSorted().has_value());

  if constexpr (std::is_same_v<T, StringView>) {
    EXPECT_FALSE(simpleVector->isAscii(0).has_value());
  }
}

template <>
void checkVectorFlagsClearedTyped<TypeKind::MAP>(const BaseVector& vector) {
  auto mapVector = vector.as<MapVector>();
  EXPECT_FALSE(mapVector->hasSortedKeys());
}

template <>
void checkVectorFlagsClearedTyped<TypeKind::ARRAY>(const BaseVector& vector) {}

template <>
void checkVectorFlagsClearedTyped<TypeKind::ROW>(const BaseVector& vector) {}

template <TypeKind kind>
void checkVectorFlagsSetTyped(const BaseVector& vector) {
  using T = typename TypeTraits<kind>::NativeType;

  checkBaseVectorFlagsSet(vector);

  auto simpleVector = vector.as<SimpleVector<T>>();

  EXPECT_TRUE(simpleVector->getStats().min.has_value());
  EXPECT_TRUE(simpleVector->getStats().max.has_value());
  EXPECT_TRUE(simpleVector->isSorted().has_value());

  if constexpr (std::is_same_v<T, StringView>) {
    EXPECT_TRUE(simpleVector->isAscii(0).has_value());
  }
}

template <>
void checkVectorFlagsSetTyped<TypeKind::MAP>(const BaseVector& vector) {
  EXPECT_TRUE(vector.getNullCount().has_value());

  auto mapVector = vector.as<MapVector>();
  EXPECT_TRUE(mapVector->hasSortedKeys());
}

template <>
void checkVectorFlagsSetTyped<TypeKind::ARRAY>(const BaseVector& vector) {
  EXPECT_TRUE(vector.getNullCount().has_value());
}

template <>
void checkVectorFlagsSetTyped<TypeKind::ROW>(const BaseVector& vector) {
  EXPECT_TRUE(vector.getNullCount().has_value());
}
} // namespace

void checkVectorFlagsCleared(const BaseVector& vector) {
  checkBaseVectorFlagsCleared(vector);
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      checkVectorFlagsClearedTyped, vector.typeKind(), vector);
}

void checkVectorFlagsSet(const BaseVector& vector) {
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      checkVectorFlagsSetTyped, vector.typeKind(), vector);
}

void checkVectorFlagsReset(
    memory::MemoryPool* pool,
    const std::function<VectorPtr(vector_size_t size, const BufferPtr& nulls)>&
        createVector,
    const std::function<void(VectorPtr& vector)>& makeMutable) {
  auto kSize = 10;
  auto nulls = allocateNulls(kSize, pool);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  bits::setNull(rawNulls, kSize - 1, true);

  auto vector = createVector(kSize, nulls);
  checkVectorFlagsSet(*vector);
  auto another = vector;
  makeMutable(vector);
  checkVectorFlagsCleared(*vector);

  vector = createVector(kSize, nulls);
  if (vector->isFlatEncoding()) {
    checkVectorFlagsSet(*vector);
    auto values = vector->values();
    makeMutable(vector);
    checkVectorFlagsCleared(*vector);
  }

  vector = createVector(kSize, nulls);
  checkVectorFlagsSet(*vector);
  makeMutable(vector);
  checkVectorFlagsCleared(*vector);
}

} // namespace facebook::velox::test
