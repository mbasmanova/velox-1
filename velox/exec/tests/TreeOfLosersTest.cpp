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
#include "velox/exec/TreeOfLosers.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/time/Timer.h"

#include <folly/Random.h>

#include <gtest/gtest.h>
#include <algorithm>
#include <optional>

using namespace facebook::velox;

class TreeOfLosersTest : public testing::Test {
 protected:
  void SetUp() override {
    rng_.seed(1);
  }

  folly::Random::DefaultGenerator rng_;
};

class Value {
 public:
  Value() = default;

  Value(uint32_t value) : value_(value) {}

  uint32_t value() const {
    return value_;
  }

  int64_t payload() const {
    return payload_;
  }

 private:
  uint32_t value_ = 0;
  uint64_t payload_ = 11;
};

class Source {
 public:
  Source(std::vector<uint32_t>&& numbers) : numbers_(std::move(numbers)) {}

  bool hasData() const {
    return !numbers_.empty();
  }

  Value* current() const {
    if (numbers_.empty()) {
      return nullptr;
    }
    if (!currentValid_) {
      currentValid_ = true;
      current_ = Value(numbers_.back());
    }
    return &current_;
  }

  void next() {
    numbers_.pop_back();
    currentValid_ = false;
  }

  bool operator<(const Source& other) const {
    if (numbers_.empty()) {
      return false;
    }
    if (other.numbers_.empty()) {
      return true;
    }
    return current()->value() < other.current()->value();
  }

 private:
  // True if 'current_' is initialized.
  mutable bool currentValid_{false};
  mutable Value current_;
  std::vector<uint32_t> numbers_;
};

TEST_F(TreeOfLosersTest, merge) {
  constexpr int32_t kNumValues = 100000000;
  constexpr int32_t kNumRuns = 31;
  std::vector<uint32_t> data;
  for (auto i = 0; i < kNumValues; ++i) {
    data.push_back(folly::Random::rand32(rng_));
  }
  std::vector<std::vector<uint32_t>> runs;
  int32_t offset = 0;
  for (auto i = 0; i < kNumRuns; ++i) {
    int size =
        i == kNumRuns - 1 ? data.size() - offset : data.size() / kNumRuns;
    runs.emplace_back();
    runs.back().insert(
        runs.back().begin(),
        data.begin() + offset,
        data.begin() + offset + size);
    std::sort(
        runs.back().begin(),
        runs.back().end(),
        [](uint32_t left, uint32_t right) { return left > right; });
    offset += size;
  }
  std::sort(data.begin(), data.end());

  std::vector<std::unique_ptr<Source>> sources;
  for (auto& run : runs) {
    sources.push_back(std::make_unique<Source>(std::move(run)));
  }
  TreeOfLosers<Source> tree(std::move(sources));
  uint64_t usec = 0;
  {
    MicrosecondTimer t(&usec);
    for (auto expected : data) {
      auto source = tree.next();
      if (!source) {
        FAIL() << "Premature end in TreeOfLosers";
      }
      auto result = source->current()->value();
      ASSERT_EQ(result, expected);
    }
    ASSERT_FALSE(tree.next());
  }
  std::cout << kNumValues << " values in " << kNumRuns << " streams " << usec
            << "us";
}
