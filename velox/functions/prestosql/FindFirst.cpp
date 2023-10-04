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

class FindFirstFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // find_first function is null preserving for the array argument, but
    // predicate expression may use other fields and may not preserve nulls in
    // these.
    // For example: find_first(array[1, 2, 3], x -> x > coalesce(a, 0)) should
    // not return null when 'a' is null.
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::LocalDecodedVector arrayDecoder(context, *args[0], rows);
    auto& decodedArray = *arrayDecoder.get();

    auto flatArray = flattenArray(rows, args[0], decodedArray);
    auto* rawOffsets = flatArray->rawOffsets();
    auto* rawSizes = flatArray->rawSizes();

    std::vector<VectorPtr> lambdaArgs = {flatArray->elements()};
    const auto numElements = flatArray->elements()->size();

    // Collect indices of the first matching elements or NULLs if no match or
    // error.
    BufferPtr resultIndices = allocateIndices(rows.end(), context.pool());
    auto* rawResultIndices = resultIndices->asMutable<vector_size_t>();

    BufferPtr resultNulls = nullptr;
    uint64_t* rawResultNulls = nullptr;

    VectorPtr matchBits;
    exec::LocalDecodedVector bitsDecoder(context);

    // Loop over lambda functions and apply these to elements of the base array,
    // in most cases there will be only one function and the loop will run once.
    auto it = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);
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
                flatArray->elements(),
                elementErrors,
                bitsDecoder)) {
          rawResultIndices[row] = firstMatchingIndex.value();
        } else {
          if (rawResultNulls == nullptr) {
            resultNulls = allocateNulls(rows.end(), context.pool());
            rawResultNulls = resultNulls->asMutable<uint64_t>();
          }
          bits::setNull(rawResultNulls, row);
        }
      });
    }

    auto localResult = BaseVector::wrapInDictionary(
        resultNulls, resultIndices, rows.end(), flatArray->elements());
    context.moveOrCopyResult(localResult, rows, result);
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
  // element matches or first matching element is null or there was an error
  // evaluating the predicate.
  std::optional<vector_size_t> findFirstMatch(
      exec::EvalCtx& context,
      vector_size_t arrayRow,
      vector_size_t offset,
      vector_size_t size,
      const VectorPtr& elements,
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
          matchDecoder->valueAt<bool>(index) == true) {
        if (elements->isNullAt(index)) {
          try {
            VELOX_USER_FAIL("find_first found NULL as the first match");
          } catch (const VeloxUserError& exception) {
            context.setVeloxExceptionError(arrayRow, std::current_exception());
          }
          return std::nullopt;
        }

        return index;
      }
    }

    return std::nullopt;
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // array(T), function(T, boolean) -> T
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("T")
              .argumentType("array(T)")
              .argumentType("function(T, boolean)")
              .build()};
}

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_find_first,
    signatures(),
    std::make_unique<FindFirstFunction>());

} // namespace facebook::velox::functions
