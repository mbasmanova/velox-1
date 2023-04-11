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

#pragma once

#include <fmt/format.h>
#include <string>

#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/SimpleNumericAggregate.h"
#include "velox/functions/prestosql/aggregates/SingleValueAccumulator.h"

namespace facebook::velox::functions::sparksql::aggregate {

namespace {

using namespace facebook::velox::aggregate;

/// FirstAggregate returns the first value of |expr| for a group of rows.
/// If |ignoreNull| is true, returns only non-null values.
///
/// The function is non-deterministic because its results depends on the order
/// of the rows which may be non-deterministic after a shuffle.  This can be
/// made deterministic by providing explicit ordering by adding order by or sort
/// by in query.
///
/// Supported types: TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, VARCHAR,
/// ARRAY, MAP
template <bool ignoreNull, bool numeric, typename TDataType = int>
class FirstAggregateBase
    : public SimpleNumericAggregate<TDataType, TDataType, TDataType> {
  using BaseAggregate = SimpleNumericAggregate<TDataType, TDataType, TDataType>;

 public:
  explicit FirstAggregateBase(TypePtr resultType) : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    if (numeric) {
      return sizeof(TDataType);
    } else {
      return sizeof(SingleValueAccumulator);
    }
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    if (!numeric) {
      for (auto i : indices) {
        new (groups[i] + exec::Aggregate::offset_) SingleValueAccumulator();
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    std::vector<vector_size_t> selectedIndexes;
    selectedIndexes.reserve(rows.countSelected());
    rows.applyToSelected(
        [&selectedIndexes](vector_size_t i) { selectedIndexes.push_back(i); });

    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        rows.applyToSelected(
            [&](vector_size_t i) { updateValue(i, groups[i], decoded); });
      }
    } else if (decoded.mayHaveNulls()) {
      for (auto it = selectedIndexes.crbegin(); it < selectedIndexes.crend();
           it++) {
        if (!decoded.isNullAt(*it)) {
          updateValue(*it, groups[*it], decoded);
        } else if (!ignoreNull) {
          exec::Aggregate::setNull(groups[*it]);
        }
      }
    } else {
      for (auto it = selectedIndexes.crbegin(); it < selectedIndexes.crend();
           it++) {
        updateValue(*it, groups[*it], decoded);
      }
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    this->addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    auto i = rows.begin();
    while (i < rows.end()) {
      if (!rows.isValid(i)) {
        ++i;
        continue;
      }
      if (!decoded.isNullAt(i)) {
        updateValue(i, group, decoded);
        return;
      }
      if (!ignoreNull) {
        exec::Aggregate::setNull(group);
        return;
      }
      ++i;
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    this->addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    if (numeric) {
      BaseAggregate::doExtractValues(
          groups, numGroups, result, [&](char* group) {
            return *exec::Aggregate::value<TDataType>(group);
          });
    } else {
      VELOX_CHECK(result);
      (*result)->resize(numGroups);

      auto* rawNulls = exec::Aggregate::getRawNulls(result->get());

      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        if (exec::Aggregate::isNull(group)) {
          (*result)->setNull(i, true);
        } else {
          exec::Aggregate::clearNull(rawNulls, i);
          auto accumulator =
              exec::Aggregate::value<SingleValueAccumulator>(group);
          accumulator->read(*result, i);
        }
      }
    }
  }

 private:
  void updateValue(vector_size_t i, char* group, DecodedVector& decoded) {
    exec::Aggregate::clearNull(group);

    if (numeric) {
      auto value = decoded.valueAt<TDataType>(i);
      *exec::Aggregate::value<TDataType>(group) = value;
    } else {
      const auto* indices = decoded.indices();
      const auto* baseVector = decoded.base();
      auto* accumulator = exec::Aggregate::value<SingleValueAccumulator>(group);
      accumulator->write(baseVector, indices[i], exec::Aggregate::allocator_);
    }
  }
};

} // namespace

template <bool ignoreNull>
bool registerFirst(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType :
       {"tinyint",
        "smallint",
        "integer",
        "bigint",
        "real",
        "double",
        "varchar"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .argumentType(inputType)
                             .intermediateType(inputType)
                             .returnType(inputType)
                             .build());
  }
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("T")
                           .argumentType("array(T)")
                           .intermediateType("array(T)")
                           .returnType("array(T)")
                           .build());

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("T")
                           .typeVariable("M")
                           .argumentType("map(T, M)")
                           .intermediateType("map(T, M)")
                           .returnType("map(T, M)")
                           .build());

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only 1 arguments", name);
        const auto& inputType = argTypes[0];
        TypeKind dataKind = inputType->kind();
        switch (dataKind) {
          case TypeKind::TINYINT:
            return std::make_unique<
                FirstAggregateBase<ignoreNull, true, int8_t>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<
                FirstAggregateBase<ignoreNull, true, int16_t>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<
                FirstAggregateBase<ignoreNull, true, int32_t>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<
                FirstAggregateBase<ignoreNull, true, int64_t>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<
                FirstAggregateBase<ignoreNull, true, float>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<
                FirstAggregateBase<ignoreNull, true, double>>(resultType);
          case TypeKind::VARCHAR:
          case TypeKind::ARRAY:
          case TypeKind::MAP:
            return std::make_unique<FirstAggregateBase<ignoreNull, false>>(
                resultType);
          default:
            VELOX_FAIL(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->toString());
        }
      });
}

} // namespace facebook::velox::functions::sparksql::aggregate
