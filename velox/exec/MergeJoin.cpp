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
#include "velox/exec/MergeJoin.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

MergeJoin::MergeJoin(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::MergeJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "MergeJoin"),
      outputBatchSize_{
          driverCtx->execCtx->queryCtx()->config().preferredOutputBatchSize()},
      numKeys_{joinNode->leftKeys().size()} {
  VELOX_CHECK(joinNode->isInnerJoin());

  leftKeys_.resize(numKeys_);
  rightKeys_.resize(numKeys_);

  auto leftType = joinNode->sources()[0]->outputType();
  for (auto& key : joinNode->leftKeys()) {
    leftKeys_.push_back(leftType->getChildIdx(key->name()));
  }

  auto rightType = joinNode->sources()[1]->outputType();
  for (auto& key : joinNode->rightKeys()) {
    rightKeys_.push_back(rightType->getChildIdx(key->name()));
  }

  bool isIdentityProjection = true;

  for (auto i = 0; i < leftType->size(); ++i) {
    auto name = leftType->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      identityProjections_.emplace_back(i, outIndex.value());
      if (outIndex != i) {
        isIdentityProjection = false;
      }
    }
  }

  for (auto i = 0; i < rightType->size(); ++i) {
    auto name = rightType->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      rightProjections_.emplace_back(i, outIndex.value());
    }
  }

  if (isIdentityProjection && rightProjections_.empty()) {
    isIdentityProjection_ = true;
  }
}

BlockingReason MergeJoin::isBlocked(ContinueFuture* future) {
  if (!rightInput_) {
    if (!rightSource_) {
      rightSource_ = operatorCtx_->task()->getMergeJoinSource(planNodeId());
    }
    auto blockingReason = rightSource_->next(future, &rightInput_);
    if (blockingReason != BlockingReason::kNotBlocked) {
      return blockingReason;
    }
  }

  return BlockingReason::kNotBlocked;
}

bool MergeJoin::needsInput() const {
  return input_ == nullptr;
}

void MergeJoin::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

int32_t MergeJoin::compare() const {
  for (auto i = 0; i < numKeys_; ++i) {
    auto compare =
        input_->childAt(leftKeys_[i])
            ->compare(
                rightInput_->childAt(rightKeys_[i]).get(), index_, rightIndex_);
    if (compare != 0) {
      return compare;
    }
  }

  return 0;
}

int32_t MergeJoin::compareLeft(vector_size_t index) const {
  for (auto i = 0; i < numKeys_; ++i) {
    const auto* key = input_->childAt(leftKeys_[i]).get();
    auto compare = key->compare(key, index_, index);
    if (compare != 0) {
      return compare;
    }
  }

  return 0;
}

int32_t MergeJoin::compareRight(vector_size_t index) const {
  for (auto i = 0; i < numKeys_; ++i) {
    const auto* key = rightInput_->childAt(rightKeys_[i]).get();
    auto compare = key->compare(key, rightIndex_, index);
    if (compare != 0) {
      return compare;
    }
  }

  return 0;
}

std::optional<MergeJoin::Match> MergeJoin::addJoinResults(
    vector_size_t* indices,
    vector_size_t* rightIndices,
    vector_size_t lastIndex,
    vector_size_t lastRightIndex,
    vector_size_t maxResults) {
  vector_size_t k = 0;
  for (auto i = index_; i < lastIndex; ++i) {
    for (auto j = rightIndex_; j < lastRightIndex; j++) {
      if (k >= maxResults) {
        index_ = i;
        return Match{lastIndex, lastRightIndex, j};
      }
      indices[k] = i;
      rightIndices[k] = j;
      ++k;
    }
  }

  return std::nullopt;
}

std::optional<MergeJoin::Match> MergeJoin::addJoinResults(
    vector_size_t* indices,
    vector_size_t* rightIndices,
    const Match& match,
    vector_size_t maxResults) {
  vector_size_t k = 0;
  for (auto j = match.nextRightIndex; j < match.lastRightIndex; j++) {
    if (k >= maxResults) {
      return Match{match.lastIndex, match.lastRightIndex, j};
    }
    indices[k] = index_;
    rightIndices[k] = j;
    ++k;
  }

  for (auto i = index_ + 1; i < match.lastIndex; ++i) {
    for (auto j = rightIndex_; j < match.lastRightIndex; j++) {
      if (k >= maxResults) {
        index_ = i;
        return Match{match.lastIndex, match.lastRightIndex, j};
      }
      indices[k] = i;
      rightIndices[k] = j;
      ++k;
    }
  }

  return std::nullopt;
}

RowVectorPtr MergeJoin::makeOutput(
    const BufferPtr& indices,
    const BufferPtr& rightIndices,
    vector_size_t size) {
  if (size == 0) {
    return nullptr;
  }

  auto output = fillOutput(size, indices);
  for (const auto& projection : rightProjections_) {
    output->childAt(projection.outputChannel) = BaseVector::wrapInDictionary(
        nullptr,
        rightIndices,
        size,
        rightInput_->childAt(projection.inputChannel));
  }
  return output;
}

vector_size_t MergeJoin::remainingResults(const Match& match) const {
  return (match.lastIndex - index_ - 1) * (match.lastRightIndex - rightIndex_) +
      (match.lastRightIndex - match.nextRightIndex);
}

RowVectorPtr MergeJoin::getOutput() {
  if (!input_ || !rightInput_) {
    return nullptr;
  }

  BufferPtr indices = allocateIndices(outputBatchSize_, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  BufferPtr rightIndices = allocateIndices(outputBatchSize_, pool());
  auto* rawRightIndices = rightIndices->asMutable<vector_size_t>();
  vector_size_t totalResults = 0;

  if (currentMatch_) {
    // Pick up from where we left.
    auto lastIndex = currentMatch_->lastIndex;
    auto lastRightIndex = currentMatch_->lastRightIndex;
    totalResults += remainingResults(currentMatch_.value());

    currentMatch_ = addJoinResults(
        rawIndices, rawRightIndices, currentMatch_.value(), outputBatchSize_);
    if (currentMatch_) {
      // Reached max output batch size. Will continue emitting results on next
      // getOutput() call.
      return makeOutput(indices, rightIndices, outputBatchSize_);
    }

    index_ = lastIndex;
    rightIndex_ = lastRightIndex;
    if (index_ == input_->size() || rightIndex_ == rightInput_->size()) {
      auto output = makeOutput(indices, rightIndices, totalResults);

      // TODO Handle properly a match that spans two or more left and/or right
      // batches.

      if (index_ == input_->size()) {
        // Ran out of rows on the left.
        input_ = nullptr;
        index_ = 0;
      }

      if (rightIndex_ == rightInput_->size()) {
        // Ran out of rows on the right.
        rightInput_ = nullptr;
        rightIndex_ = 0;
      }

      return output;
    }

    if (totalResults == outputBatchSize_) {
      // Reached max output batch size. Will continue emitting results on next
      // getOutput() call.
      return makeOutput(indices, rightIndices, outputBatchSize_);
    }
  }

  auto compareResult = compare();

  for (;;) {
    // Catch up input_ with rightInput_.
    while (compareResult < 0) {
      ++index_;
      if (index_ == input_->size()) {
        // Ran out of rows on the left side. Return matches accumulated so far
        // (if any) and continue after receiving next batch.
        auto output = makeOutput(indices, rightIndices, totalResults);
        input_ = nullptr;
        index_ = 0;
        return output;
      }
      compareResult = compare();
    }

    // Catch up rightInput_ with input_.
    while (compareResult > 0) {
      ++rightIndex_;
      if (rightIndex_ == rightInput_->size()) {
        // Ran out of rows on the right side. Return matches accumulated so
        // far (if any) and continue after receiving next batch.
        auto output = makeOutput(indices, rightIndices, totalResults);
        rightInput_ = nullptr;
        rightIndex_ = 0;
        return output;
      }
      compareResult = compare();
    }

    if (compareResult == 0) {
      // Found a match. Identify all rows on the left and right that have the
      // matching keys.
      vector_size_t lastIndex = index_ + 1;
      while (lastIndex < input_->size() && compareLeft(lastIndex) == 0) {
        ++lastIndex;
      }

      vector_size_t lastRightIndex = rightIndex_ + 1;
      while (lastRightIndex < rightInput_->size() &&
             compareRight(lastRightIndex) == 0) {
        ++lastRightIndex;
      }

      currentMatch_ = addJoinResults(
          rawIndices + totalResults,
          rawRightIndices + totalResults,
          lastIndex,
          lastRightIndex,
          outputBatchSize_ - totalResults);
      if (currentMatch_) {
        // Reached max output batch size. Will continue emitting results on next
        // getOutput() call.
        return makeOutput(indices, rightIndices, outputBatchSize_);
      }

      totalResults += (lastIndex - index_) * (lastRightIndex - rightIndex_);

      index_ = lastIndex;
      rightIndex_ = lastRightIndex;
      if (index_ == input_->size() || rightIndex_ == rightInput_->size()) {
        auto output = makeOutput(indices, rightIndices, totalResults);

        // TODO Handle properly a match that spans two or more left and/or right
        // batches.

        if (index_ == input_->size()) {
          // Ran out of rows on the left.
          input_ = nullptr;
          index_ = 0;
        }

        if (rightIndex_ == rightInput_->size()) {
          // Ran out of rows on the right.
          rightInput_ = nullptr;
          rightIndex_ = 0;
        }

        return output;
      }

      if (totalResults == outputBatchSize_) {
        // Reached max output batch size. Will continue emitting results on next
        // getOutput() call.
        return makeOutput(indices, rightIndices, outputBatchSize_);
      }

      compareResult = compare();
    }
  }

  VELOX_UNREACHABLE();
}
} // namespace facebook::velox::exec
