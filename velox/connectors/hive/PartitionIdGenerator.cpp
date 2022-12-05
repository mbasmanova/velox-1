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

#include "velox/common/base/Portability.h"

namespace facebook::velox::connector::hive {

PartitionIdGenerator::PartitionIdGenerator(
    const RowTypePtr& inputType,
    std::vector<column_index_t> partitionChannels,
    memory::MemoryPool* FOLLY_NONNULL pool)
    : partitionChannels_(std::move(partitionChannels)),
      allPartitionsVector_(BaseVector::create<RowVector>(inputType, 0, pool)) {
  hashers_.reserve(partitionChannels_.size());
  for (column_index_t i = 0; i < partitionChannels_.size(); i++) {
    hashers_.push_back(exec::VectorHasher::create(
        inputType->childAt(partitionChannels_[i]), partitionChannels_[i]));
  }
}

std::map<uint64_t, uint64_t> PartitionIdGenerator::run(
    const RowVectorPtr& input,
    raw_vector<uint64_t>& result) {
  result.resize(input->size());
  allRows_.resize(input->size());
  allRows_.setAll();
  bool rehashed = computeIds(input, allRows_, result);

  std::map<uint64_t, uint64_t> idMap;
  if (rehashed && allPartitionsVector_->size() > 0) {
    raw_vector<uint64_t> newIds(allPartitionsVector_->size());

    VELOX_CHECK(
        !computeIds(allPartitionsVector_, activeIds_, newIds),
        "Partition ID generator should not rehash when computing for the existing partitions.");

    activeIds_.applyToSelected([&](auto id) { idMap[id] = newIds[id]; });

    movePartitions(idMap);
  }

  addPartitions(input, result);

  return idMap;
}

bool PartitionIdGenerator::computeIds(
    const RowVectorPtr& input,
    const SelectivityVector& rows,
    raw_vector<uint64_t>& result) {
  result.resize(input->size());

  for (auto& hasher : hashers_) {
    auto partitionVector = input->childAt(hasher->channel())->loadedVector();
    hasher->decode(*partitionVector, rows);
  }

  bool rehashed = false;
  bool toRehash = false;
  do {
    toRehash = false;
    for (auto& hasher : hashers_) {
      if (!hasher->computeValueIds(rows, result)) {
        toRehash = true;
      }
    }
    if (toRehash) {
      rehashed = true;

      uint64_t multiplier = 1;
      for (auto i = 0; i < hashers_.size(); i++) {
        multiplier = hashers_[i]->enableValueIds(multiplier, kHasherReservePct);
        VELOX_CHECK_NE(
            multiplier,
            exec::VectorHasher::kRangeTooLarge,
            "Exceeded limit of distinct partitions.");
      }
    }
  } while (toRehash);

  return rehashed;
}

void PartitionIdGenerator::movePartitions(
    const std::map<uint64_t, uint64_t>& indexMap) {
  vector_size_t maxNewId = indexMap.rbegin()->second;

  allPartitionsVector_->resize(maxNewId + 1, false);
  activeIds_.resize(maxNewId + 1, false);

  for (auto entry = indexMap.rbegin(); entry != indexMap.rend(); entry++) {
    if (entry->first == entry->second) {
      continue;
    }
    allPartitionsVector_->copy(
        allPartitionsVector_.get(), entry->second, entry->first, 1);
    allPartitionsVector_->setNull(entry->first, true);
    activeIds_.setValid(entry->first, false);
    activeIds_.setValid(entry->second, true);
  }
  activeIds_.updateBounds();
}

void PartitionIdGenerator::addPartitions(
    const RowVectorPtr& input,
    const raw_vector<uint64_t>& inputIds) {
  maxId_ = *std::max_element(inputIds.begin(), inputIds.end());
  if (((vector_size_t)maxId_) > allPartitionsVector_->size() - 1) {
    allPartitionsVector_->ensureWritable(SelectivityVector::empty(maxId_ + 1));
    activeIds_.resize(maxId_ + 1, false);
  }

  for (vector_size_t row = 0; row < inputIds.size(); row++) {
    uint64_t id = inputIds[row];
    if (!activeIds_.isValid(id)) {
      for (vector_size_t channel : partitionChannels_) {
        allPartitionsVector_->childAt(channel)->copy(
            input->childAt(channel).get(), id, row, 1);
      }
      activeIds_.setValid(id, true);
    }
  }
  activeIds_.updateBounds();
}

} // namespace facebook::velox::connector::hive
