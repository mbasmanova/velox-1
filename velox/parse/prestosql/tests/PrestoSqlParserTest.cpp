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
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>

#include "CharStream.h"
#include "velox/parse/prestosql/PrestoSqlErrorHandler.h"
#include "velox/parse/prestosql/PrestoSqlExprTranslator.h"
#include "velox/parse/prestosql/generated/SqlParser.h"
#undef null
#include "velox/parse/prestosql/generated/SqlParserTokenManager.h"

// using namespace commonsql::parser;
// using namespace facebook::velox;

int main(int argc, char** argv) {
  //  JAVACC_STRING_TYPE s = "SELECT array_construct(power(x, 2), power(y, 3))";
  //  JAVACC_STRING_TYPE s = "SELECT sqrt(x);\n";
//  JAVACC_STRING_TYPE s = "map_filter(m, (k, v) -> k + v > 0)";
  JAVACC_STRING_TYPE s = "c0 % 2 = 0 AND c1 % 3 = 0 AND c2 % 5 = 0";

  clock_t start, finish;
  double time;
  start = clock();

  for (int i = 0; i < 1; i++) {
    commonsql::parser::CharStream stream(s.c_str(), s.size(), 1, 1);
    commonsql::parser::SqlParserTokenManager scanner(&stream);
    commonsql::parser::SqlParser parser(&scanner);
    // Parser will destroy the error handler in SqlParser::clear().
    std::vector<std::string> errors;
    parser.setErrorHandler(
        new facebook::velox::parse::PrestoSqlErrorHandler(errors));
    // Use for single expression.
    parser.derived_column();
    // Use for full query.
    //    parser.compilation_unit();

    if (!errors.empty()) {
      VELOX_FAIL("{}", errors.front());
    }

    commonsql::parser::SimpleNode* root =
        (commonsql::parser::SimpleNode*)parser.jjtree.peekNode();
    VELOX_CHECK_NOT_NULL(root);
    if (root) {
      JAVACC_STRING_TYPE buffer;
      root->dumpToBuffer(" ", "\n", &buffer);
      printf("%s\n", buffer.c_str());
    }
    std::stack<std::shared_ptr<facebook::velox::core::IExpr>> nodes;
    facebook::velox::parse::PrestoSqlExprTranslator translator;
    root->jjtAccept(&translator, &nodes);

    std::cout << nodes.top()->toString() << std::endl;
  }

  finish = clock();
  time = (double(finish) - double(start)) / CLOCKS_PER_SEC;
  printf("Avg parsing time: %lfms\n", (time * 1000) / 1);
}
