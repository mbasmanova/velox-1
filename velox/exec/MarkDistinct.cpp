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

#include "MarkDistinct.h"
#include "velox/common/base/Range.h"
#include "velox/vector/FlatVector.h"

#include <algorithm>
#include <utility>

namespace facebook::velox::exec {

MarkDistinct::MarkDistinct(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::MarkDistinctNode>& planNode)
    : Operator(
          driverCtx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "MarkDistinct") {
  auto inputType = planNode->sources()[0]->outputType();

  // Set all input columns as identity projection.
  for (auto i = 0; i < inputType->size(); ++i) {
    identityProjections_.emplace_back(i, i);
  }

  // Set up result projection
  resultProjections_.emplace_back(0, inputType->size());

  // Initialize groupingset
  auto numHashers = planNode.get()->distinctVariables().size();
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.reserve(numHashers);

  for (const auto& distinctVariable : planNode.get()->distinctVariables()) {
    auto channel = exprToChannel(distinctVariable.get(), inputType);
    hashers.push_back(VectorHasher::create(distinctVariable->type(), channel));
  }

  // We hijack groupingSet to do most of the heavy lifting for us for distinct.
  // Most aggregation related arguments are empty as they are not needed by
  // groupingSet for our DISTINCT use case.
  // The advantage of groupingSet (vs std::unordered_map) is that:
  // 1. It handles hash collisions. (must for correctness).
  // 2. It is faster. (According to
  // https://github.com/facebookincubator/velox/pull/2321/commits/225ab35c5834cb68acee9199fa4f7fd0513e7715#r951849869)
  groupingSet_ = std::make_unique<GroupingSet>(
      std::move(hashers),
      std::vector<column_index_t>{},
      std::vector<std::unique_ptr<Aggregate>>{},
      std::vector<std::optional<column_index_t>>{},
      std::vector<std::vector<column_index_t>>{},
      std::vector<std::vector<VectorPtr>>{},
      std::vector<TypePtr>{},
      true,
      false,
      false,
      nullptr,
      operatorCtx_.get());

  // Set up result
  results_.resize(1);
}

void MarkDistinct::addInput(RowVectorPtr input) {
  // Add input to groupingset
  groupingSet_->addInput(input, false);

  // Save input
  input_ = input;
}

RowVectorPtr MarkDistinct::getOutput() {
  if (finished_) {
    input_ = nullptr;
    return nullptr;
  }

  if (!input_) {
    if (noMoreInput_) {
      finished_ = true;
    }
    return nullptr;
  }

  auto outputSize = input_->size();
  // Each input gets a newly allocated mask column.
  results_[0] = BaseVector::create(BOOLEAN(), outputSize, operatorCtx_->pool());

  // newGroups contains the indices of distinct rows.
  // For each index in newGroups, we mark the index'th bit true in the result
  // vector.
  auto resultBits =
      results_[0]->as<FlatVector<bool>>()->mutableRawValues<uint64_t>();

  bits::fillBits(resultBits, 0, outputSize, false);
  for (const auto i : groupingSet_->hashLookup().newGroups) {
    bits::setBit(resultBits, i, true);
  }
  auto output = fillOutput(outputSize, nullptr);

  // Drop reference to input_ to make it singly-referenced at the producer and
  // allow for memory reuse.
  input_ = nullptr;
  // Drop reference to output mask channel vector.
  results_[0].reset();

  return output;
}

bool MarkDistinct::isFinished() {
  return finished_;
}

} // namespace facebook::velox::exec