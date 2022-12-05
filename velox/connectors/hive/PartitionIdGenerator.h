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

#include "velox/exec/VectorHasher.h"

namespace facebook::velox::connector::hive {
/// Generate sequential integer IDs for distinct multi-partition
/// values, which could be used as vector index. As it is based on VectorHasher
/// internally, rehash could happen when applied to a new input. run()
/// returns a map, if rehash happens, from the old partition IDs to the new
/// partition IDs. run() returns an empty map if no rehash happens.
///
/// Note that the IDs generated are sequential but not consecutive. Also note
/// that, even after rehash, the order of generated IDs before and after for
/// the same input are preserved.
class PartitionIdGenerator {
 public:
  /// @param inputType RowType of the input.
  /// @param partitionChannels Channels of partition keys in the input
  /// RowVector.
  PartitionIdGenerator(
      const RowTypePtr& inputType,
      std::vector<column_index_t> partitionChannels,
      memory::MemoryPool* FOLLY_NONNULL pool);

  /// Generate sequential IDs for multi-partition input vector. Rehash could
  /// happen, which means the IDs for existing partition values could change.
  /// @param input Input RowVector.
  /// @param result Generated integer IDs indexed by input row number.
  /// @return a map from the old partition IDs to the new partition IDs if
  /// rehash happens. Map is empty if no rehash happens.
  std::map<uint64_t, uint64_t> run(
      const RowVectorPtr& input,
      raw_vector<uint64_t>& result);

  /// Return the maximum partition ID generated for the most recent input.
  uint64_t getMaxPartitionId() const {
    return maxId_;
  }

 private:
  static constexpr const int32_t kHasherReservePct = 20;

  // Generate sequential IDs for multi-partition input vector. Return true if
  // rehash happens; otherwise, return false.
  bool computeIds(
      const RowVectorPtr& input,
      const SelectivityVector& rows,
      raw_vector<uint64_t>& result);

  void movePartitions(const std::map<uint64_t, uint64_t>& indexMap);

  void addPartitions(
      const RowVectorPtr& input,
      const raw_vector<uint64_t>& inputIds);

  const std::vector<column_index_t> partitionChannels_;
  std::vector<std::unique_ptr<exec::VectorHasher>> hashers_;

  // All the distinct partition values received so far, indexed by their
  // partition ID.
  RowVectorPtr allPartitionsVector_;
  // Active partition IDs or indices of allPartitionsVector_.
  SelectivityVector activeIds_;
  // Maximum partition ID generated for the most recent input.
  uint64_t maxId_ = 0;
  // All rows are set valid to compute partition IDs for all input rows.
  SelectivityVector allRows_;
};

} // namespace facebook::velox::connector::hive
