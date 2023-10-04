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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"

namespace facebook::velox::functions {
namespace {

class FindFirstFunctionBase : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // find_first function is null preserving for the array argument, but
    // predicate expression may use other fields and may not preserve nulls in
    // these.
    // For example: find_first(array[1, 2, 3], x -> x > coalesce(a, 0)) should
    // not return null when 'a' is null.
    return false;
  }

 protected:
  ArrayVectorPtr prepareInputArray(
      const VectorPtr& input,
      const SelectivityVector& rows,
      exec::EvalCtx& context) const {
    exec::LocalDecodedVector arrayDecoder(context, *input, rows);
    auto& decodedArray = *arrayDecoder.get();

    return flattenArray(rows, input, decodedArray);
  }

  // Evaluates predicate on all elements of the array and identifies the first
  // matching element.
  //
  // @tparam THit Called when first matching element is found. Receives array
  // index in the flatArray vector and an index of the first matching element in
  // flatArray->elements() vector.
  // @tparam TMiss Called when no matching element is found. Receives array
  // index in the flatArray vector.
  //
  // @param rows Rows in 'flatArray' vector to process.
  // @param flatArray Flat array vector with possibly encoded elements.
  // @param predicates FunctionVector holding the predicate expressions.
  template <typename THit, typename TMiss>
  void doApply(
      const SelectivityVector& rows,
      const ArrayVectorPtr& flatArray,
      FunctionVector& predicates,
      exec::EvalCtx& context,
      THit onHit,
      TMiss onMiss) const {
    const auto* rawOffsets = flatArray->rawOffsets();
    const auto* rawSizes = flatArray->rawSizes();

    std::vector<VectorPtr> lambdaArgs = {flatArray->elements()};
    const auto numElements = flatArray->elements()->size();

    VectorPtr matchBits;
    exec::LocalDecodedVector bitsDecoder(context);

    // Loop over lambda functions and apply these to elements of the base array,
    // in most cases there will be only one function and the loop will run once.
    auto it = predicates.iterator(&rows);
    while (auto entry = it.next()) {
      auto elementRows =
          toElementRows<ArrayVector>(numElements, *entry.rows, flatArray.get());
      auto wrapCapture = toWrapCapture<ArrayVector>(
          numElements, entry.callable, *entry.rows, flatArray);

      ErrorVectorPtr elementErrors;
      entry.callable->applyNoThrow(
          elementRows,
          nullptr, // No need to preserve any values in 'matchBits'.
          wrapCapture,
          &context,
          lambdaArgs,
          elementErrors,
          &matchBits);

      bitsDecoder.get()->decode(*matchBits, elementRows);
      entry.rows->applyToSelected([&](vector_size_t row) {
        if (auto firstMatchingIndex = findFirstMatch(
                context,
                row,
                rawOffsets[row],
                rawSizes[row],
                elementErrors,
                bitsDecoder)) {
          onHit(row, firstMatchingIndex.value());
        } else {
          onMiss(row);
        }
      });
    }
  }

 private:
  static FOLLY_ALWAYS_INLINE std::optional<std::exception_ptr> getOptionalError(
      const ErrorVectorPtr& errors,
      vector_size_t row) {
    if (errors && row < errors->size() && !errors->isNullAt(row)) {
      return *std::static_pointer_cast<std::exception_ptr>(
          errors->valueAt(row));
    }

    return std::nullopt;
  }

  // Returns an index of the first matching element or std::nullopt if no
  // element matches or there was an error evaluating the predicate.
  std::optional<vector_size_t> findFirstMatch(
      exec::EvalCtx& context,
      vector_size_t arrayRow,
      vector_size_t offset,
      vector_size_t size,
      const ErrorVectorPtr& elementErrors,
      const exec::LocalDecodedVector& matchDecoder) const {
    for (auto i = 0; i < size; ++i) {
      auto index = offset + i;
      if (auto error = getOptionalError(elementErrors, index)) {
        // Report first error to match Presto's implementation.
        context.setError(arrayRow, error.value());
        return std::nullopt;
      }

      if (!matchDecoder->isNullAt(index) &&
          matchDecoder->valueAt<bool>(index)) {
        return index;
      }
    }

    return std::nullopt;
  }
};

class FindFirstFunction : public FindFirstFunctionBase {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto flatArray = prepareInputArray(args[0], rows, context);

    // Collect indices of the first matching elements or NULLs if no match or
    // error.
    BufferPtr resultIndices = allocateIndices(rows.end(), context.pool());
    auto* rawResultIndices = resultIndices->asMutable<vector_size_t>();

    BufferPtr resultNulls = nullptr;
    uint64_t* rawResultNulls = nullptr;

    doApply(
        rows,
        flatArray,
        *args[1]->asUnchecked<FunctionVector>(),
        context,
        [&](vector_size_t row, vector_size_t firstMatchingIndex) {
          if (flatArray->elements()->isNullAt(firstMatchingIndex)) {
            try {
              VELOX_USER_FAIL("find_first found NULL as the first match");
            } catch (const VeloxUserError& exception) {
              context.setVeloxExceptionError(row, std::current_exception());
            }
          } else {
            rawResultIndices[row] = firstMatchingIndex;
          }
        },
        [&](vector_size_t row) {
          if (rawResultNulls == nullptr) {
            resultNulls = allocateNulls(rows.end(), context.pool());
            rawResultNulls = resultNulls->asMutable<uint64_t>();
          }
          bits::setNull(rawResultNulls, row);
        });

    auto localResult = BaseVector::wrapInDictionary(
        resultNulls, resultIndices, rows.end(), flatArray->elements());
    context.moveOrCopyResult(localResult, rows, result);
  }
};

class FindFirstIndexFunction : public FindFirstFunctionBase {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto flatArray = prepareInputArray(args[0], rows, context);
    const auto* rawOffsets = flatArray->rawOffsets();

    context.ensureWritable(rows, BIGINT(), result);
    auto flatResult = result->asFlatVector<int64_t>();

    doApply(
        rows,
        flatArray,
        *args[1]->asUnchecked<FunctionVector>(),
        context,
        [&](vector_size_t row, vector_size_t firstMatchingIndex) {
          // Convert zero-based index of a row in the elements vector into a
          // 1-based index of the element in the array.
          flatResult->set(row, 1 + firstMatchingIndex - rawOffsets[row]);
        },
        [&](vector_size_t row) { flatResult->setNull(row, true); });
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> valueSignatures() {
  // array(T), function(T, boolean) -> T
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("T")
              .argumentType("array(T)")
              .argumentType("function(T, boolean)")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> indexSignatures() {
  // array(T), function(T, boolean) -> bigint
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("bigint")
              .argumentType("array(T)")
              .argumentType("function(T, boolean)")
              .build()};
}

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_find_first,
    valueSignatures(),
    std::make_unique<FindFirstFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_find_first_index,
    indexSignatures(),
    std::make_unique<FindFirstIndexFunction>());

} // namespace facebook::velox::functions
