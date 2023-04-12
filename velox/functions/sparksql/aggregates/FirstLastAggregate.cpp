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

#include <fmt/format.h>
#include <string>

#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/SimpleNumericAggregate.h"
#include "velox/functions/prestosql/aggregates/SingleValueAccumulator.h"

namespace facebook::velox::functions::sparksql::aggregates {

namespace {

using namespace facebook::velox::aggregate;

template <typename T>
struct SimpleAccumulator {
  bool valid_ = false;
  bool isNumeric = false;
  T value_;
};

/// FirstLastAggregate returns the first or last value of |expr| for a group of
/// rows. If |ignoreNull| is true, returns only non-null values.
///
/// The function is non-deterministic because its results depends on the order
/// of the rows which may be non-deterministic after a shuffle.  This can be
/// made deterministic by providing explicit ordering by adding order by or sort
/// by in query.
template <
    bool numeric,
    template <typename T>
    class TAccumulator,
    typename TDataType>
class FirstLastAggregateBase
    : public SimpleNumericAggregate<TDataType, TDataType, TDataType> {
  using BaseAggregate = SimpleNumericAggregate<TDataType, TDataType, TDataType>;

 public:
  explicit FirstLastAggregateBase(TypePtr resultType)
      : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    if (numeric) {
      return sizeof(TAccumulator<TDataType>);
    } else {
      return sizeof(TAccumulator<SingleValueAccumulator>);
    }
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);

    if (numeric) {
      for (auto i : indices) {
        new (groups[i] + exec::Aggregate::offset_) TAccumulator<TDataType>();
        exec::Aggregate::value<TAccumulator<TDataType>>(groups[i])->isNumeric =
            true;
      }
    } else {
      for (auto i : indices) {
        new (groups[i] + exec::Aggregate::offset_)
            TAccumulator<SingleValueAccumulator>();
      }
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    if (numeric) {
      BaseAggregate::doExtractValues(
          groups, numGroups, result, [&](char* group) {
            auto accumulator =
                exec::Aggregate::value<TAccumulator<TDataType>>(group);
            return accumulator->value_;
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
              exec::Aggregate::value<TAccumulator<SingleValueAccumulator>>(
                  group);
          accumulator->value_.read(*result, i);
        }
      }
    }

    LOG(INFO) << "==== extractValues";
    LOG(INFO) << result->get()->toString(0, result->get()->size());
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void destroy(folly::Range<char**> groups) override {
    if (!numeric) {
      for (auto group : groups) {
        auto accumulator =
            exec::Aggregate::value<TAccumulator<SingleValueAccumulator>>(group);
        accumulator->value_.destroy(exec::Aggregate::allocator_);
      }
    }
  }

 protected:
  //  inline TAccumulator* accumulator() {
  //    return
  //  }
  //  void updateValue(vector_size_t i, char* group, DecodedVector& decoded) {
  //    exec::Aggregate::clearNull(group);
  //
  //    if (numeric) {
  //      auto value = decoded.valueAt<TNumeric>(i);
  //      *exec::Aggregate::value<TNumeric>(group) = value;
  //    } else {
  //      const auto* indices = decoded.indices();
  //      const auto* baseVector = decoded.base();
  //      auto* accumulator =
  //      exec::Aggregate::value<SingleValueAccumulator>(group);
  //      accumulator->write(baseVector, indices[i],
  //      exec::Aggregate::allocator_);
  //    }
  //  }
};

template <bool ignoreNull, typename TDataType, bool numeric>
class FirstAggregate
    : public FirstLastAggregateBase<numeric, SimpleAccumulator, TDataType> {
 public:
  explicit FirstAggregate(TypePtr resultType)
      : FirstLastAggregateBase<numeric, SimpleAccumulator, TDataType>(
            resultType) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected(
        [&](vector_size_t i) { return updateValue(i, groups[i], decoded); });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    LOG(INFO) << "==== addSingleGroupRawInput";
    LOG(INFO) << args[0]->toString(0, args[0]->size());
    DecodedVector decoded(*args[0], rows);

    rows.testSelected(
        [&](vector_size_t i) { return updateValue(i, group, decoded); });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    LOG(INFO) << "==== addSingleGroupIntermediateResults";
    LOG(INFO) << args[0]->toString(0, args[0]->size());
    DecodedVector decoded(*args[0], rows);

    rows.testSelected(
        [&](vector_size_t i) { return updateValue(i, group, decoded); });
  }

 private:
  bool updateValue(vector_size_t i, char* group, DecodedVector& decoded) {
    if (!numeric) {
      return updateNonNumeric(i, group, decoded);
    }

    auto accumulator =
        exec::Aggregate::value<SimpleAccumulator<TDataType>>(group);
    if (accumulator->valid_) {
      return false;
    }
    exec::Aggregate::clearNull(group);

    if (!ignoreNull) {
      if (!decoded.isNullAt(i)) {
        auto value = decoded.valueAt<TDataType>(i);
        accumulator->value_ = value;
      } else {
        exec::Aggregate::setNull(group);
      }
      accumulator->valid_ = true;
      return false;
    }
    if (!decoded.isNullAt(i)) {
      auto value = decoded.valueAt<TDataType>(i);
      accumulator->value_ = value;
      accumulator->valid_ = true;
      return false;
    }
    return true;
  }

  bool updateNonNumeric(vector_size_t i, char* group, DecodedVector& decoded) {
    auto accumulator =
        exec::Aggregate::value<SimpleAccumulator<SingleValueAccumulator>>(
            group);
    if (accumulator->valid_) {
      return false;
    }
    exec::Aggregate::clearNull(group);

    const auto* indices = decoded.indices();
    const auto* baseVector = decoded.base();
    if (!ignoreNull) {
      if (!decoded.isNullAt(i)) {
        accumulator->value_.write(
            baseVector, indices[i], exec::Aggregate::allocator_);
      } else {
        exec::Aggregate::setNull(group);
      }
      accumulator->valid_ = true;
      return false;
    }
    if (!decoded.isNullAt(i)) {
      accumulator->value_.write(
          baseVector, indices[i], exec::Aggregate::allocator_);
      accumulator->valid_ = true;
      return false;
    }
    return true;
  }
};

template <bool ignoreNull, typename TDataType, bool numeric>
class LastAggregate
    : public FirstLastAggregateBase<numeric, SimpleAccumulator, TDataType> {
 public:
  explicit LastAggregate(TypePtr resultType)
      : FirstLastAggregateBase<numeric, SimpleAccumulator, TDataType>(
            resultType) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected(
        [&](vector_size_t i) { this->updateValue(i, groups[i], decoded); });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected(
        [&](vector_size_t i) { this->updateValue(i, group, decoded); });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

 private:
  void updateValue(vector_size_t i, char* group, DecodedVector& decoded) {
    auto accumulator =
        exec::Aggregate::value<SimpleAccumulator<TDataType>>(group);
    exec::Aggregate::clearNull(group);

    if (!ignoreNull) {
      if (!decoded.isNullAt(i)) {
        auto value = decoded.valueAt<TDataType>(i);
        accumulator->value_ = value;
      } else {
        exec::Aggregate::setNull(group);
      }
    } else if (!decoded.isNullAt(i)) {
      auto value = decoded.valueAt<TDataType>(i);
      accumulator->value_ = value;
    }
  }
};

} // namespace

template <template <bool B1, typename T, bool B2> class TClass, bool ignoreNull>
bool registerFirstLast(const std::string& name) {
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
            return std::make_unique<TClass<ignoreNull, int8_t, true>>(
                resultType);
          case TypeKind::SMALLINT:
            //            return std::make_unique<TClass<ignoreNull,
            //            NumericAccumulator<int16_t>>>(resultType);
          case TypeKind::INTEGER:
            //            return std::make_unique<TClass<ignoreNull,
            //            int32_t>>(resultType);
          case TypeKind::BIGINT:
            //            return std::make_unique<TClass<ignoreNull,
            //            int64_t>>(resultType);
          case TypeKind::REAL:
            //            return std::make_unique<TClass<ignoreNull,
            //            float>>(resultType);
          case TypeKind::DOUBLE:
            //            return std::make_unique<TClass<ignoreNull,
            //            double>>(resultType);
          case TypeKind::VARCHAR:
          case TypeKind::ARRAY:
          case TypeKind::MAP:
            return std::make_unique<TClass<ignoreNull, int8_t, false>>(
                resultType);
          default:
            VELOX_FAIL(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->toString());
        }
      });
}

void registerFirstLastAggregate(const std::string& prefix) {
  registerFirstLast<FirstAggregate, false>(prefix + "first");
  registerFirstLast<FirstAggregate, true>(prefix + "first_ignore_null");
  registerFirstLast<LastAggregate, false>(prefix + "last");
  registerFirstLast<LastAggregate, true>(prefix + "last_ignore_null");
}

} // namespace facebook::velox::functions::sparksql::aggregates
