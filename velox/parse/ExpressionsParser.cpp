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
#include "velox/parse/ExpressionsParser.h"
#include "velox/duckdb/conversion/DuckParser.h"

#include "velox/parse/prestosql/PrestoSqlErrorHandler.h"
#include "velox/parse/prestosql/PrestoSqlExprTranslator.h"
#include "velox/parse/prestosql/generated/CharStream.h"
#include "velox/parse/prestosql/generated/SqlParser.h"
#include "velox/parse/prestosql/generated/SqlParserTokenManager.h"

namespace facebook::velox::parse {

std::shared_ptr<const core::IExpr> parseExpr(
    const std::string& expr,
    const ParseOptions& options) {
  //  facebook::velox::duckdb::ParseOptions duckConversionOptions;
  //  duckConversionOptions.parseDecimalAsDouble = options.parseDecimalAsDouble;
  //  return facebook::velox::duckdb::parseExpr(expr, duckConversionOptions);

  // TODO Handle options.
  commonsql::parser::CharStream stream(expr.c_str(), expr.size(), 1, 1);
  commonsql::parser::SqlParserTokenManager scanner(&stream);
  commonsql::parser::SqlParser parser(&scanner);

  // Parser will destroy the error handler in SqlParser::clear().
  std::vector<std::string> errors;
  parser.setErrorHandler(new PrestoSqlErrorHandler(errors));

  parser.derived_column();
  if (!errors.empty()) {
    VELOX_FAIL("{}", errors.front());
  }

  commonsql::parser::SimpleNode* root =
      (commonsql::parser::SimpleNode*)parser.jjtree.peekNode();
  VELOX_CHECK_NOT_NULL(root);

  // TODO Remove.
  if (root) {
    JAVACC_STRING_TYPE buffer;
    root->dumpToBuffer(" ", "\n", &buffer);
    printf("%s\n", buffer.c_str());
  }

  std::stack<std::shared_ptr<facebook::velox::core::IExpr>> nodes;
  facebook::velox::parse::PrestoSqlExprTranslator translator;
  root->jjtAccept(&translator, &nodes);

  // TODO Remove.
  std::cout << nodes.top()->toString() << std::endl;

  return nodes.top();
}

std::pair<std::shared_ptr<const core::IExpr>, core::SortOrder> parseOrderByExpr(
    const std::string& expr) {
  return facebook::velox::duckdb::parseOrderByExpr(expr);
}

} // namespace facebook::velox::parse
