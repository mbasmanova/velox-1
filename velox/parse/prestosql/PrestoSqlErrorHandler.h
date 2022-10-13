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

#include <sstream>

#include "velox/parse/prestosql/generated/ErrorHandler.h"
#undef DOMAIN
#include "velox/parse/prestosql/generated/SqlParser.h"
#undef null

namespace facebook::velox::parse {

class PrestoSqlErrorHandler : public commonsql::parser::ErrorHandler {
 public:
  explicit PrestoSqlErrorHandler(std::vector<std::string>& errors)
      : errors_{errors} {}

  void handleUnexpectedToken(
      int expectedKind,
      const JJString& expectedToken,
      commonsql::parser::Token* actual,
      commonsql::parser::SqlParser* parser) override {
    std::ostringstream error;
    commonsql::parser::Token* possibleKeyword = getPossibleKeyword(parser);
    if (possibleKeyword == nullptr) {
      error << actual->beginLine << ":" << actual->beginColumn
            << " Unexpected token: \"" << (*actual).image << "\" after: \""
            << parser->getToken(0)->image << "\". Expecting: " << expectedKind
            << "("
            << (expectedKind <= 0 ? "EOF"
                                  : commonsql::parser::tokenImage[expectedKind])
            << ")\n";
    } else {
      error << actual->beginLine << ":" << actual->beginColumn
            << " Unexpected keyword: \"" << (*possibleKeyword).image << "\""
            << "\n";
    }
    errors_.push_back(error.str());
  }

  void handleParseError(
      commonsql::parser::Token* last,
      commonsql::parser::Token* unexpected,
      const JJSimpleString& production,
      commonsql::parser::SqlParser* parser) override {
    std::ostringstream error;
    error << last->beginLine << ":" << last->beginColumn
          << " Unexpected token: " << (*unexpected).image
          << " after: " << (*last).image << " while parsing: " << production
          << "\n";
    errors_.push_back(error.str());
  }

  void handleOtherError(
      const JJString& message,
      commonsql::parser::SqlParser* parser) override {
    errors_.push_back(message.c_str());
  }

 private:
  bool isKeyword(commonsql::parser::Token* t) {
    return t->kind >= commonsql::parser::MIN_RESERVED_WORD and
        t->kind <= commonsql::parser::MAX_RESERVED_WORD;
  }

  // Since we have a lookahead of 3, check the next 2 tokens to see if someone
  // is using a keyword as identifier.
  commonsql::parser::Token* getPossibleKeyword(
      commonsql::parser::SqlParser* parser) {
    if (isKeyword(parser->getToken(1)))
      return parser->getToken(1);
    if (isKeyword(parser->getToken(2)))
      return parser->getToken(2);
    return nullptr;
  }

  std::vector<std::string>& errors_;
};
} // namespace facebook::velox::parse
