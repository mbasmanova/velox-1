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
#include "velox/exec/Operator.h"
// TODO Move MergeJoinSource into this header file.
#include "velox/exec/MergeSource.h"

namespace facebook::velox::exec {
class MergeJoin : public Operator {
 public:
  MergeJoin(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::MergeJoinNode>& joinNode);

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

 private:
  // Compare rows on the left and right at index_ and rightIndex_ respectively.
  int32_t compare() const;

  // Compare two rows on the left: index_ and index.
  int32_t compareLeft(vector_size_t index) const;

  // Compare two rows on the right: rightIndex_ and index.
  int32_t compareRight(vector_size_t index) const;

  RowVectorPtr makeOutput(
      const BufferPtr& indices,
      const BufferPtr& rightIndices,
      vector_size_t size);

  struct Match {
    vector_size_t lastIndex;
    vector_size_t lastRightIndex;
    vector_size_t nextRightIndex;
  };

  vector_size_t remainingResults(const Match& match) const;

  std::optional<Match> addJoinResults(
      vector_size_t* indices,
      vector_size_t* rightIndices,
      vector_size_t lastIndex,
      vector_size_t lastRightIndex,
      vector_size_t maxResults);

  std::optional<Match> addJoinResults(
      vector_size_t* indices,
      vector_size_t* rightIndices,
      const Match& match,
      vector_size_t maxResults);

  const uint32_t outputBatchSize_;
  const size_t numKeys_;
  std::vector<ChannelIndex> leftKeys_;
  std::vector<ChannelIndex> rightKeys_;
  std::vector<IdentityProjection> rightProjections_;
  std::shared_ptr<MergeJoinSource> rightSource_;
  RowVectorPtr rightInput_;
  vector_size_t index_{0};
  vector_size_t rightIndex_{0};
  std::optional<Match> currentMatch_;
};
} // namespace facebook::velox::exec
