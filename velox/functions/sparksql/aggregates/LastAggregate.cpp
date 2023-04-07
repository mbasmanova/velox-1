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
#include "velox/functions/sparksql/aggregates/LastAggregate.h"

namespace facebook::velox::functions::sparksql::aggregate {

void registerLastAggregate(const std::string& prefix) {
  registerLast<false>(prefix + "last");
  registerLast<true>(prefix + "last_ignore_null");
}
} // namespace facebook::velox::functions::sparksql::aggregate
