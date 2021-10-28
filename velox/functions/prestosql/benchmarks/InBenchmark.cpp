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
#include <folly/Benchmark.h>
#include <folly/container/F14Set.h>
#include <folly/init/Init.h>
#include "velox/expression/tests/VectorFuzzer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/VectorFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

/// Fast implementation of IN (a, b, c,..) using F14FastSet.
template <typename T>
VectorPtr fastIn(const folly::F14FastSet<T>& inSet, const VectorPtr& data) {
  const auto numRows = data->size();
  auto result = std::static_pointer_cast<FlatVector<bool>>(
      BaseVector::create(BOOLEAN(), numRows, data->pool()));
  auto rawResults = result->mutableRawValues<T>();

  auto rawData = data->asUnchecked<FlatVector<T>>()->rawValues();
  for (auto row = 0; row < numRows; ++row) {
    bits::setBit(rawResults, row, inSet.contains(rawData[row]));
  }

  return result;
}

class InBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  InBenchmark() : FunctionBenchmarkBase() {
    functions::registerVectorFunctions();
  }

  RowVectorPtr makeData(const TypePtr& type) {
    VectorFuzzer::Options opts;
    opts.vectorSize = 1'000;
    return vectorMaker_.rowVector({VectorFuzzer(opts, pool()).fuzzFlat(type)});
  }

  folly::F14FastSet<StringView> makeInListStrings(
      FlatVector<StringView>* strings,
      size_t numValues) {
    folly::F14FastSet<StringView> inSet;
    inSet.reserve(numValues);

    // Add half of the strings from 'strings' to ensure at least that many will
    // match the IN filter.
    auto rawStrings = strings->rawValues();
    for (auto i = 0; i < numValues && i < strings->size() / 2; i++) {
      auto index = i * 2;
      if (!strings->isNullAt(index)) {
        inSet.emplace(rawStrings[index]);
      }
    }

    // Add more strings.
    for (auto i = inSet.size(); i < numValues; ++i) {
      inSet.insert(StringView(fmt::format("{}", i)));
    }

    return inSet;
  }

  void run(size_t numValues) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(INTEGER());

    std::ostringstream inList;
    inList << "0";
    for (auto i = 1; i < numValues; ++i) {
      inList << ", " << i * 2;
    }

    auto sql = fmt::format("c0 IN ({})", inList.str());
    auto exprSet = compileExpression(sql, data->type());
    suspender.dismiss();

    doRun(exprSet, data);
  }

  void runStrings(size_t numValues) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(VARCHAR());

    auto inSet = makeInListStrings(
        data->childAt(0)->asFlatVector<StringView>(), numValues);

    std::ostringstream inList;
    bool first = true;
    for (auto s : inSet) {
      if (!first) {
        inList << ", ";
      } else {
        first = false;
      }
      inList << "'" << s.str() << "'";
    }

    auto sql = fmt::format("c0 IN ({})", inList.str());
    auto exprSet = compileExpression(sql, data->type());
    suspender.dismiss();

    doRun(exprSet, data);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 1000; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }

  void runFast(size_t numValues) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(INTEGER());

    folly::F14FastSet<int32_t> inSet;
    inSet.reserve(numValues);
    for (auto i = 0; i < numValues; ++i) {
      inSet.insert(i);
    }
    suspender.dismiss();

    doRunFast(inSet, data->childAt(0));
  }

  void runFastStrings(size_t numValues) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(VARCHAR());
    auto inSet = makeInListStrings(
        data->childAt(0)->asFlatVector<StringView>(), numValues);
    suspender.dismiss();

    doRunFast(inSet, data->childAt(0));
  }

  template <typename T>
  void doRunFast(const folly::F14FastSet<T>& inSet, const VectorPtr& data) {
    int cnt = 0;
    for (auto i = 0; i < 1000; i++) {
      cnt += fastIn(inSet, data)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(fastIn) {
  InBenchmark benchmark;
  benchmark.runFast(10);
}

BENCHMARK_RELATIVE(in) {
  InBenchmark benchmark;
  benchmark.run(10);
}

BENCHMARK(fastIn1K) {
  InBenchmark benchmark;
  benchmark.runFast(1'000);
}

BENCHMARK_RELATIVE(in1K) {
  InBenchmark benchmark;
  benchmark.run(1'000);
}

BENCHMARK(fastInStrings) {
  InBenchmark benchmark;
  benchmark.runFastStrings(10);
}

BENCHMARK_RELATIVE(inStrings) {
  InBenchmark benchmark;
  benchmark.runStrings(10);
}

BENCHMARK(fastIn1KStrings) {
  InBenchmark benchmark;
  benchmark.runFastStrings(1'000);
}

BENCHMARK_RELATIVE(in1KStrings) {
  InBenchmark benchmark;
  benchmark.runStrings(1'000);
}
} // namespace

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
