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

#include <iostream>
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/array.html
class SequenceFunction : public exec::VectorFunction {
 public:
  static constexpr int32_t kMaxResultEntries = 10'000;

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VectorPtr localResult = applyNumber(rows, args, outputType, context);
    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  static VectorPtr applyNumber(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context) {
    const auto numRows = rows.size();
    auto pool = context.pool();
    vector_size_t numElements = 0;

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto startVector = decodedArgs.at(0);
    auto stopVector = decodedArgs.at(1);
    DecodedVector* stepVector = nullptr;
    if (args.size() == 3) {
      stepVector = decodedArgs.at(2);
    }

    BufferPtr sizes = allocateSizes(numRows, pool);
    BufferPtr offsets = allocateOffsets(numRows, pool);

    auto rawSizes = sizes->asMutable<vector_size_t>();
    auto rawOffsets = offsets->asMutable<vector_size_t>();

    SelectivityVector remainingRows = rows;
    context.applyToSelectedNoThrow(remainingRows, [&](auto row) {
      auto start = startVector->valueAt<int64_t>(row);
      auto stop = stopVector->valueAt<int64_t>(row);
      const int64_t step = (stepVector == nullptr)
          ? (stop >= start ? 1 : -1)
          : stepVector->valueAt<int64_t>(row);
      checkArguments(start, stop, step);
      rawSizes[row] = (stop - start) / step + 1;
      numElements += rawSizes[row];
    });

    context.deselectErrors(remainingRows);

    VectorPtr elements = BaseVector::create(BIGINT(), numElements, pool);
    auto rawElements = elements->asFlatVector<int64_t>()->mutableRawValues();

    vector_size_t elementsOffset = 0;
    context.applyToSelectedNoThrow(remainingRows, [&](auto row) {
      const auto numElements = rawSizes[row];
      if (numElements) {
        auto start = startVector->valueAt<int64_t>(row);

        rawOffsets[row] = elementsOffset;
        rawSizes[row] = numElements;
        std::iota(
            rawElements + elementsOffset,
            rawElements + elementsOffset + numElements,
            start);

        elementsOffset += numElements;
      }
    });

    return std::make_shared<ArrayVector>(
        pool, outputType, nullptr, numRows, offsets, sizes, elements);
  }

  static void checkArguments(int64_t start, int64_t stop, int64_t step) {
    VELOX_USER_CHECK_NE(step, 0, "step must not be zero");
    VELOX_USER_CHECK(
        step > 0 ? stop >= start : stop <= start,
        "sequence stop value should be greater than or equal to start value if "
        "step is greater than zero otherwise stop should be less than or equal to start")
    VELOX_USER_CHECK_LE(
        (stop - start) / step + 1,
        kMaxResultEntries,
        "result of sequence function must not have more than 10000 entries");
  }
};

} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  signatures.reserve(2);
  signatures.emplace_back(exec::FunctionSignatureBuilder()
                              .returnType("array(bigint)")
                              .argumentType("bigint")
                              .argumentType("bigint")
                              .build());
  signatures.emplace_back(exec::FunctionSignatureBuilder()
                              .returnType("array(bigint)")
                              .argumentType("bigint")
                              .argumentType("bigint")
                              .argumentType("bigint")
                              .build());
  return signatures;
}

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_sequence,
    signatures(),
    std::make_unique<SequenceFunction>());
} // namespace facebook::velox::functions
