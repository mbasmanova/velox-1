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
#include <boost/random/uniform_int_distribution.hpp>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/IOUtils.h"
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

namespace facebook::velox::aggregate::prestosql {

class ConservativeCountMinSketch {
 public:
  ConservativeCountMinSketch(double epsilon, double confidence) {
    VELOX_USER_CHECK_GT(epsilon, 0)
    VELOX_USER_CHECK_LT(epsilon, 1)
    VELOX_USER_CHECK_GT(confidence, 0)
    VELOX_USER_CHECK_LT(confidence, 1)

    static const double kEuler = std::exp(1.0);

    // e/w = eps ; w = e/eps  Where e is the natural log base.
    // 1/2^depth <= 1-confidence ; depth >= log2(1/(1-confidence))
    // Make sure 'width_' is not too small to prevent frequent hash collisions.
    width_ = std::max<int32_t>(101, std::ceil(kEuler / epsilon));
    depth_ = std::ceil(std::log(1.0 / (1.0 - confidence)));
    counts_.resize(depth_);
    for (auto i = 0; i < depth_; ++i) {
      counts_[i].resize(width_, 0);
    }
  }

  int64_t add(std::string_view item, int64_t count) {
    VELOX_USER_CHECK_GE(count, 1);

    hashBuckets(item, buckets_);

    const int64_t minCount = estimateCount(item);

    for (auto i = 0; i < depth_; i++) {
      int64_t newCount = std::max(counts_[i][buckets_[i]], minCount + count);
      counts_[i][buckets_[i]] = newCount;
    }

    return minCount + count;
  }

  int64_t estimateCount(std::string_view item) const {
    hashBuckets(item, buckets_);

    int64_t minCount = counts_[0][buckets_[0]];
    for (int i = 1; i < depth_; ++i) {
      minCount = std::min(minCount, counts_[i][buckets_[i]]);
    }

    return minCount;
  }

  size_t serializedSize() const {
    return sizeof(width_) + sizeof(depth_) + width_ * depth_ * sizeof(int64_t);
  }

  void serialize(common::OutputByteStream& out) const {
    out.appendOne(width_);
    out.appendOne(depth_);
    for (auto i = 0; i < depth_; i++) {
      for (auto j = 0; j < width_; j++) {
        out.appendOne(counts_[i][j]);
      }
    }
  }

  void mergeSerialized(common::InputByteStream& in) {
    const auto width = in.read<int32_t>();
    const auto depth = in.read<int32_t>();

    VELOX_CHECK_EQ(width, width_)
    VELOX_CHECK_EQ(depth, depth_)

    for (auto i = 0; i < depth_; i++) {
      for (auto j = 0; j < width_; j++) {
        counts_[i][j] += in.read<int64_t>();
      }
    }
  }

 private:
  void hashBuckets(std::string_view item, std::vector<int32_t>& buckets) const {
    buckets.clear();

    auto hash1 = bits::hashBytes(0, item.data(), item.size());
    auto hash2 = bits::hashBytes(hash1, item.data(), item.size());
    for (auto i = 0; i < depth_; i++) {
      hash1 = bits::hashMix(hash1, hash2);
      buckets.push_back(hash1 % width_);
    }
  }

  int32_t width_;
  int32_t depth_;
  std::vector<std::vector<int64_t>> counts_;

  // Scratch memory.
  mutable std::vector<int32_t> buckets_;
};

class TopElementsHistogram {
 public:
  TopElementsHistogram(
      double minPercentShare,
      double epsilon,
      double confidence)
      : minPercentShare_{minPercentShare},
        epsilon_{epsilon},
        confidence_{confidence} {}

  double minPercentShare() const {
    return minPercentShare_;
  }

  double epsilon() const {
    return epsilon_;
  }

  double confidence() const {
    return confidence_;
  }

  void add(const StringView& input, int64_t count) {
    totalCount_ += count;

    if (!sketch_.has_value()) {
      if (entries_.size() < kMaxTotalCount) {
        addOrIncrementEntry((std::string_view)input, count);
        return;
      }

      switchToSketch();
    }

    const auto estimatedCount = sketch_->add((std::string_view)input, count);

    const auto countThreshold = threshold();
    const auto minCount = sortedEntries_.begin()->estimatedCount;

    if (estimatedCount >= countThreshold && estimatedCount > minCount) {
      addOrUpdateEntry((std::string_view)input, estimatedCount);
      trimTopElements(countThreshold);
    }
  }

  void getTopElements(exec::out_type<Map<Varchar, int64_t>>& out) const {
    const auto countThreshold = threshold();

    for (const auto& [key, value] : entries_) {
      int64_t estimatedCount;
      if (sketch_.has_value()) {
        estimatedCount = sketch_->estimateCount(value.data);
      } else {
        estimatedCount = value.estimatedCount;
      }

      if (estimatedCount < countThreshold) {
        continue;
      }

      auto [keyWriter, valueWriter] = out.add_item();
      keyWriter = key;
      valueWriter = estimatedCount;
    }
  }

  size_t serializedSize() const {
    int32_t size = 0;
    size += sizeof(totalCount_);
    size += sizeof(minPercentShare_);
    size += sizeof(epsilon_);
    size += sizeof(confidence_);

    size += sizeof(int32_t);
    if (sketch_.has_value()) {
      size += sketch_->serializedSize();
    }

    size += sizeof(generation_);
    size += sizeof(int32_t);
    for (const auto& [k, v] : entries_) {
      size += sizeof(v.estimatedCount);
      size += sizeof(v.insertionOrder);
      size += sizeof(int32_t);
      size += v.data.size();
    }

    return size;
  }

  void serialize(common::OutputByteStream& out) {
    VELOX_CHECK_GT(totalCount_, 0);

    out.appendOne(totalCount_);
    out.appendOne(minPercentShare_);
    out.appendOne(epsilon_);
    out.appendOne(confidence_);

    if (sketch_.has_value()) {
      out.appendOne<int32_t>(sketch_->serializedSize());
      sketch_->serialize(out);
    } else {
      out.appendOne<int32_t>(0);
    }

    out.appendOne(generation_);
    out.appendOne<int32_t>(entries_.size());
    for (const auto& [k, v] : entries_) {
      out.appendOne(v.estimatedCount);
      out.appendOne(v.insertionOrder);
      out.appendOne<int32_t>(v.data.size());
      out.append(v.data.data(), v.data.size());
    }
  }

  static TopElementsHistogram deserialize(common::InputByteStream& in) {
    const auto totalCount = in.read<int64_t>();
    const auto minPercentShare = in.read<double>();
    const auto epsilon = in.read<double>();
    const auto confidence = in.read<double>();

    TopElementsHistogram histogram{minPercentShare, epsilon, confidence};
    histogram.totalCount_ = totalCount;

    const auto sketchSerializedSize = in.read<int32_t>();
    if (sketchSerializedSize > 0) {
      histogram.sketch_.emplace(epsilon, confidence);
      histogram.sketch_->mergeSerialized(in);
    }

    histogram.generation_ = in.read<int64_t>();

    const auto numEntries = in.read<int32_t>();
    for (auto i = 0; i < numEntries; ++i) {
      const auto estimatedCount = in.read<int64_t>();
      const auto insertionOrder = in.read<int64_t>();
      const auto size = in.read<int32_t>();
      const char* data = in.read<char>(size);

      Entry entry{std::string(data, size), estimatedCount, insertionOrder};
      histogram.entries_.emplace(entry.data, entry);

      if (sketchSerializedSize > 0) {
        histogram.sortedEntries_.emplace(entry);
      }
    }

    // TODO Run some sanity checks.
    return histogram;
  }

  void mergeSerialized(common::InputByteStream& in) {
    const auto totalCount = in.read<int64_t>();
    const auto minPercentShare = in.read<double>();
    const auto epsilon = in.read<double>();
    const auto confidence = in.read<double>();

    VELOX_CHECK_EQ(minPercentShare, minPercentShare_)
    VELOX_CHECK_EQ(epsilon, epsilon_)
    VELOX_CHECK_EQ(confidence, confidence_)

    const auto sketchSerializedSize = in.read<int32_t>();
    if (sketchSerializedSize == 0) {
      // Ignore 'generation'.
      in.read<int64_t>();

      const auto numEntries = in.read<int32_t>();
      for (auto i = 0; i < numEntries; ++i) {
        const auto count = in.read<int64_t>();
        // Ignore 'insertionOrder'.
        in.read<int64_t>();
        const auto size = in.read<int32_t>();
        const char* data = in.read<char>(size);

        add(StringView(data, size), count);
      }
    } else {
      if (!sketch_.has_value()) {
        switchToSketch();
      }

      sketch_->mergeSerialized(in);

      totalCount_ += totalCount;

      const auto countThreshold = threshold();

      // Ignore 'generation'.
      in.read<int64_t>();

      const auto numEntries = in.read<int32_t>();
      for (auto i = 0; i < numEntries; ++i) {
        // Ignore 'estimatedCount' and 'insertionOrder'.
        in.read<int64_t>();
        in.read<int64_t>();

        const auto size = in.read<int32_t>();
        const char* data = in.read<char>(size);

        const auto estimatedCount =
            sketch_->estimateCount(std::string_view(data, size));
        const auto minCount = sortedEntries_.begin()->estimatedCount;

        if (estimatedCount >= countThreshold && estimatedCount > minCount) {
          addOrUpdateEntry(std::string_view(data, size), estimatedCount);
          trimTopElements(countThreshold);
        }
      }
    }
  }

 private:
  struct Entry {
    std::string data;
    int64_t estimatedCount;
    int64_t insertionOrder;
  };

  struct Compare {
    bool operator()(const Entry& left, const Entry& right) const {
      if (left.estimatedCount > right.estimatedCount) {
        return false;
      }

      if (left.estimatedCount < right.estimatedCount) {
        return true;
      }

      if (left.insertionOrder > right.insertionOrder) {
        return false;
      }

      if (left.insertionOrder < right.insertionOrder) {
        return true;
      }

      return left.data < right.data;
    }
  };

  void addOrIncrementEntry(std::string_view data, int64_t count) {
    auto it = entries_.find(data);
    if (it == entries_.end()) {
      entries_.emplace(data, Entry{std::string(data), count, generation_});
      ++generation_;
    } else {
      it->second.estimatedCount += count;
    }
  }

  int64_t threshold() const {
    return minPercentShare_ * totalCount_ / 100.0;
  }

  void addOrUpdateEntry(std::string_view data, int64_t count) {
    auto it = entries_.find(data);
    if (it == entries_.end()) {
      const Entry entry{std::string(data), count, generation_};
      entries_.emplace(data, entry);
      sortedEntries_.emplace(entry);

      ++generation_;
    } else {
      auto numRemoved = sortedEntries_.erase(it->second);
      VELOX_CHECK_EQ(1, numRemoved);

      it->second.estimatedCount = count;
      sortedEntries_.emplace(it->second);
    }
  }

  void trimTopElements(int64_t threshold) {
    if (entries_.size() < kMaxTotalCount) {
      return;
    }

    const int64_t minCount = sortedEntries_.begin()->estimatedCount;

    for (auto it = entries_.begin(); it != entries_.end(); ++it) {
      const auto estimatedCount = sketch_->estimateCount(it->second.data);
      if (estimatedCount < threshold) {
        sortedEntries_.erase(it->second);
        entries_.erase(it);
        return;
      }

      if (estimatedCount > it->second.estimatedCount) {
        sortedEntries_.erase(it->second);
        it->second.estimatedCount = estimatedCount;
        sortedEntries_.emplace(it->second);
      }
    }

    const auto numToRemove = entries_.size() - kMaxTotalCount;
    for (auto i = 0; i < numToRemove; ++i) {
      auto it = sortedEntries_.begin();
      VELOX_CHECK(it != sortedEntries_.end())
      entries_.erase(it->data);
      sortedEntries_.erase(it);
    }
  }

  void switchToSketch() {
    sketch_.emplace(epsilon_, confidence_);

    VELOX_CHECK_EQ(0, sortedEntries_.size());

    for (const auto& [key, value] : entries_) {
      sketch_->add(value.data, value.estimatedCount);
      auto ok = sortedEntries_.emplace(value).second;
      VELOX_CHECK(ok);
    }

    VELOX_CHECK_EQ(entries_.size(), sortedEntries_.size());
  }

  static const int64_t kMaxTotalCount = 1'000;

  const double minPercentShare_;
  const double epsilon_;
  const double confidence_;

  std::optional<ConservativeCountMinSketch> sketch_;
  std::set<Entry, Compare> sortedEntries_;
  folly::F14FastMap<std::string, Entry> entries_;
  int64_t generation_{0};
  int64_t totalCount_{0};
};

// @tparam TInput List of input arguments as Row<input[, increment],
// minPercentShare[, epsilon[, confidence]]>
template <typename TInput>
class FBApproxMostFrequentAggregate {
 public:
  using InputType = TInput;

  using IntermediateType = Varbinary;

  using OutputType = Map<Varchar, int64_t>;

  // TODO Implement toIntermediate

  struct AccumulatorType {
    std::optional<TopElementsHistogram> histogram;

    AccumulatorType() = delete;

    explicit AccumulatorType(HashStringAllocator* /*allocator*/) {}

    void addInput(
        HashStringAllocator* allocator,
        const exec::arg_type<Varchar>& input,
        double minPercentShare) {
      addInputInt(
          allocator, input, 1, minPercentShare, std::nullopt, std::nullopt);
    }

    void addInput(
        HashStringAllocator* allocator,
        const exec::arg_type<Varchar>& input,
        double minPercentShare,
        double epsilon) {
      addInputInt(allocator, input, 1, minPercentShare, epsilon, std::nullopt);
    }

    void addInput(
        HashStringAllocator* allocator,
        const exec::arg_type<Varchar>& input,
        double minPercentShare,
        double epsilon,
        double confidence) {
      addInputInt(allocator, input, 1, minPercentShare, epsilon, confidence);
    }

    void addInput(
        HashStringAllocator* allocator,
        const exec::arg_type<Varchar>& input,
        int64_t increment,
        double minPercentShare) {
      addInputInt(
          allocator,
          input,
          increment,
          minPercentShare,
          std::nullopt,
          std::nullopt);
    }

    void addInput(
        HashStringAllocator* allocator,
        const exec::arg_type<Varchar>& input,
        int64_t increment,
        double minPercentShare,
        double epsilon) {
      addInputInt(
          allocator, input, increment, minPercentShare, epsilon, std::nullopt);
    }

    void addInput(
        HashStringAllocator* allocator,
        const exec::arg_type<Varchar>& input,
        int64_t increment,
        double minPercentShare,
        double epsilon,
        double confidence) {
      addInputInt(
          allocator, input, increment, minPercentShare, epsilon, confidence);
    }

    void addInputInt(
        HashStringAllocator* /*allocator*/,
        const exec::arg_type<Varchar>& input,
        int64_t increment,
        double minPercentShare,
        std::optional<double> epsilon,
        std::optional<double> confidence) {
      if (!histogram.has_value()) {
        if (!epsilon.has_value()) {
          // Default error rate. See
          // http://theory.stanford.edu/~tim/s17/l/l2.pdf
          // Given ε = 1/2k and minPercentShare = 100/k, we derive ε =
          // minPercentShare/200 This ensures that no element occurring less
          // than minPercentShare/2 will appear in the result.
          epsilon = minPercentShare / 200;
        }

        if (!confidence.has_value()) {
          static constexpr double kDefaultConfidence = 0.99;
          confidence = kDefaultConfidence;
        }

        VELOX_USER_CHECK_GT(
            minPercentShare,
            0,
            "minPercentShare must be between 0 and 100 exclusive")
        VELOX_USER_CHECK_LT(
            minPercentShare,
            100,
            "minPercentShare must be between 0 and 100 exclusive")

        VELOX_USER_CHECK_GT(
            epsilon, 0, "epsilon (error bound) must be greater than 0")

        VELOX_USER_CHECK_GT(
            confidence, 0, "confidence must be greater than 0 and less than 1")
        VELOX_USER_CHECK_LT(
            confidence, 1, "confidence must be greater than 0 and less than 1")

        histogram.emplace(minPercentShare, epsilon.value(), confidence.value());
      } else {
        VELOX_USER_CHECK_EQ(
            minPercentShare,
            histogram->minPercentShare(),
            "minPercentShare argument must be constant within a group")

        if (epsilon.has_value()) {
          VELOX_USER_CHECK_EQ(
              epsilon.value(),
              histogram->epsilon(),
              "epsilon argument must be constant within a group")
        }

        if (confidence.has_value()) {
          VELOX_USER_CHECK_EQ(
              confidence.value(),
              histogram->confidence(),
              "confidence argument must be constant within a group")
        }
      }

      histogram->add(input, increment);
    }

    void combine(
        HashStringAllocator* /*allocator*/,
        const exec::arg_type<Varbinary>& other) {
      common::InputByteStream stream(other.data());
      if (!histogram.has_value()) {
        histogram.emplace(TopElementsHistogram::deserialize(stream));
      } else {
        histogram->mergeSerialized(stream);
      }
    }

    bool writeFinalResult(exec::out_type<Map<Varchar, int64_t>>& out) {
      if (histogram.has_value()) {
        histogram->getTopElements(out);
      }
      return true;
    }

    bool writeIntermediateResult(exec::out_type<Varbinary>& out) {
      VELOX_CHECK(histogram.has_value())
      const auto serializedSize = histogram->serializedSize();
      out.reserve(serializedSize);

      common::OutputByteStream stream(out.data());
      histogram->serialize(stream);

      out.resize(serializedSize);

      return true;
    }
  };
};

template <typename TInput>
std::unique_ptr<exec::Aggregate> makeAggregate(const TypePtr& resultType) {
  return std::make_unique<
      exec::SimpleAggregateAdapter<FBApproxMostFrequentAggregate<TInput>>>(
      resultType);
}

void registerFBApproxMostFrequentAggregate(
    const std::string& prefix,
    bool withCompanionFunctions) {
  const std::string name = prefix + "fb_approx_most_frequent";

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  // (input, minPercentShare)
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("map(varchar, bigint)")
                           .intermediateType("varbinary")
                           .argumentType("varchar")
                           .argumentType("double")
                           .build());

  // (input, minPercentShare, epsilon)
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("map(varchar, bigint)")
                           .intermediateType("varbinary")
                           .argumentType("varchar")
                           .argumentType("double")
                           .argumentType("double")
                           .build());

  // (input, minPercentShare, epsilon, confidence)
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("map(varchar, bigint)")
                           .intermediateType("varbinary")
                           .argumentType("varchar")
                           .argumentType("double")
                           .argumentType("double")
                           .build());

  // (input, increment, minPercentShare)
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("map(varchar, bigint)")
                           .intermediateType("varbinary")
                           .argumentType("varchar")
                           .argumentType("bigint")
                           .argumentType("double")
                           .build());

  // (input, increment, minPercentShare, epsilon)
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("map(varchar, bigint)")
                           .intermediateType("varbinary")
                           .argumentType("varchar")
                           .argumentType("bigint")
                           .argumentType("double")
                           .argumentType("double")
                           .build());

  // (input, increment, minPercentShare, epsilon, confidence)
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("map(varchar, bigint)")
                           .intermediateType("varbinary")
                           .argumentType("varchar")
                           .argumentType("bigint")
                           .argumentType("double")
                           .argumentType("double")
                           .build());

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_GE(argTypes.size(), 2);

        if (argTypes[1]->isBigint()) {
          const auto numDoubleArgs = argTypes.size() - 2;
          switch (numDoubleArgs) {
            case 1:
              return makeAggregate<Row<Varchar, int64_t, double>>(resultType);
            case 2:
              return makeAggregate<Row<Varchar, int64_t, double, double>>(
                  resultType);
            case 3:
              return makeAggregate<Row<Varchar, int64_t, double, double>>(
                  resultType);
            default:
              VELOX_UNREACHABLE()
          }
        } else {
          const auto numDoubleArgs = argTypes.size() - 1;
          switch (numDoubleArgs) {
            case 1:
              return makeAggregate<Row<Varchar, double>>(resultType);
            case 2:
              return makeAggregate<Row<Varchar, double, double>>(resultType);
            case 3:
              return makeAggregate<Row<Varchar, double, double>>(resultType);
            default:
              VELOX_UNREACHABLE()
          }
        }
      },
      withCompanionFunctions);
}

class FBApproxMostFrequentAggregateTest
    : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();

    const auto seed = std::time(nullptr);
    LOG(INFO) << "Seed: " << seed;
    rng_.seed(seed);

    registerFBApproxMostFrequentAggregate("", false);
  }

  int32_t rand() {
    return boost::random::uniform_int_distribution<int32_t>()(rng_);
  }

  int32_t rand(int32_t min, int32_t max) {
    return boost::random::uniform_int_distribution<int32_t>(min, max)(rng_);
  }

  // Creates a dataset of ~150,000 values with 4 values repeated ~0.02% times
  // and the rest of the values repeated < N% times. N is determined by 'max'
  // parameter. max = 15 is N = 0.01%.
  RowVectorPtr makeData(int32_t max) {
    std::vector<int64_t> counts = {40, 41, 42, 43};

    int64_t total = 166;
    while (total < 150'000) {
      const auto count = rand(1, max);
      counts.push_back(count);
      total += count;
    }

    return makeRowVector({
        makeFlatVector<std::string>(
            counts.size(),
            [&](auto row) {
              if (row < 4) {
                return fmt::format("top{}", row);
              } else {
                return fmt::format("___{}", rand());
              }
            }),
        makeFlatVector(counts),
    });
  }

 private:
  std::mt19937 rng_;
};

TEST_F(FBApproxMostFrequentAggregateTest, largePercentShare) {
  std::unordered_map<std::string, int32_t> topValues = {
      {"apples", 10},
      {"oranges", 20},
      {"grapes", 20},
      {"pineapples", 30},
  };

  std::vector<std::string> selection;
  selection.reserve(100);
  for (const auto& [value, count] : topValues) {
    for (auto i = 0; i < count; ++i) {
      selection.push_back(value);
    }
  }

  for (auto i = selection.size(); i < 100; ++i) {
    selection.push_back("");
  }

  auto data = makeRowVector({
      makeFlatVector<int32_t>(10'000, [](auto row) { return row % 2; }),
      makeFlatVector<std::string>(
          10'000,
          [&](auto /*row*/) {
            const auto index = rand(0, selection.size());
            const auto& value = selection[index];
            return !value.empty() ? value : fmt::format("___{}", rand());
          }),
  });

  auto splitData = split(data, 3);

  auto doTest = [&](double minPercentShare,
                    const std::vector<std::string>& expectedKeys) {
    SCOPED_TRACE(fmt::format(
        "maxPercentShare: {}%, top keys: {}",
        minPercentShare,
        folly::join(", ", expectedKeys)));

    const std::string agg =
        fmt::format("fb_approx_most_frequent(c1, {:.2f})", minPercentShare);
    const std::string transform = "array_sort(map_keys(a0))";

    auto expected = makeRowVector({
        makeArrayVector<std::string>({expectedKeys}),
    });

    testAggregations(splitData, {}, {agg}, {transform}, {expected});

    expected = makeRowVector({
        makeFlatVector<int32_t>({0, 1}),
        makeArrayVector<std::string>({expectedKeys, expectedKeys}),
    });

    testAggregations(splitData, {"c0"}, {agg}, {"c0", transform}, {expected});
  };

  doTest(1.0, {"apples", "grapes", "oranges", "pineapples"});
  doTest(5.0, {"apples", "grapes", "oranges", "pineapples"});
  doTest(15.0, {"grapes", "oranges", "pineapples"});
  doTest(25.0, {"pineapples"});
  doTest(40.0, {});
}

// TODO Add tests for function calls with epsilon and confidence.
// TODO Allocate memory from HashStringAllocator.

TEST_F(FBApproxMostFrequentAggregateTest, smallPercentShare) {
  // Verify fb_approx_most_frequent(minPercentShare = 0.02) returns 4 top
  // values. 0.02% of 150K is 30. 0.01% of 150K is 15.

  // Global aggregation.
  auto data = makeData(15);

  auto expected = makeRowVector({
      makeArrayVector<std::string>({{"top0", "top1", "top2", "top3"}}),
  });

  testAggregations(
      split(data, 3),
      {},
      {"fb_approx_most_frequent(c0, c1, 0.02)"},
      {"array_sort(map_keys(a0))"},
      {expected});

  // Group-by with 2 groups. Each group handles approximately half of the data.
  // Since per-value counts are the same, but total count is half of the
  // original, we multiply minPercentShare by 2: 0.04%.
  auto groupByData = makeRowVector({
      // Grouping key: 0, 1, 0, 1,...
      makeFlatVector<int32_t>(data->size(), [](auto row) { return row % 2; }),
      data->childAt(0),
      data->childAt(1),
  });

  expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1}),
      makeArrayVector<std::string>({{"top0", "top2"}, {"top1", "top3"}}),
  });

  testAggregations(
      split(groupByData, 3),
      {"c0"},
      {"fb_approx_most_frequent(c1, c2, 0.04)"},
      {"c0", "array_sort(map_keys(a0))"},
      {expected});

  // Group-by with 3 groups. Each group handles approximately a third of the
  // data. Since per-value counts are the same, but total count is a third of
  // the original, we multiply minPercentShare by 3: 0.06%.
  groupByData = makeRowVector({
      // Grouping key: 0, 1, 2, 0, 1, 2,...
      makeFlatVector<int32_t>(data->size(), [](auto row) { return row % 3; }),
      data->childAt(0),
      data->childAt(1),
  });

  expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2}),
      makeArrayVector<std::string>({{"top0", "top3"}, {"top1"}, {"top2"}}),
  });

  testAggregations(
      split(groupByData, 3),
      {"c0"},
      {"fb_approx_most_frequent(c1, c2, 0.06)"},
      {"c0", "array_sort(map_keys(a0))"},
      {expected});
}

TEST_F(FBApproxMostFrequentAggregateTest, epsilon) {
  // Reduce epsilon by 2x from default 0.0001 to 0.00005 and increase the counts
  // of non-top values by a bit less than 2x. Verify fb_approx_most_frequent
  // returns only top values.
  auto data = makeData(20);

  auto expected = makeRowVector({
      makeArrayVector<std::string>({{"top0", "top1", "top2", "top3"}}),
  });

  testAggregations(
      split(data, 3),
      {},
      {"fb_approx_most_frequent(c0, c1, 0.02, 0.00005)"},
      {"array_sort(map_keys(a0))"},
      {expected});

  // Group-by with 3 groups.
  auto groupByData = makeRowVector({
      // Grouping key: 0, 1, 2, 0, 1, 2,...
      makeFlatVector<int32_t>(data->size(), [](auto row) { return row % 3; }),
      data->childAt(0),
      data->childAt(1),
  });

  expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2}),
      makeArrayVector<std::string>({{"top0", "top3"}, {"top1"}, {"top2"}}),
  });

  testAggregations(
      split(groupByData, 3),
      {"c0"},
      {"fb_approx_most_frequent(c1, c2, 0.06, 0.00015)"},
      {"c0", "array_sort(map_keys(a0))"},
      {expected});
}

} // namespace facebook::velox::aggregate::prestosql
