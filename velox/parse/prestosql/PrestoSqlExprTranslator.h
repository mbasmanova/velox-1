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

#include "velox/parse/Expressions.h"
#include "velox/parse/prestosql/generated/SqlParserVisitor.h"

namespace facebook::velox::parse {

class PrestoSqlExprTranslator
    : public commonsql::parser::SqlParserDefaultVisitor {
 public:
  void visit(const commonsql::parser::AdditiveExpression* node, void* data)
      override {
    static const std::unordered_map<int, std::string> kOperatorNames = {
        {commonsql::parser::PLUS, "plus"},
        {commonsql::parser::MINUS, "minus"},
    };

    VELOX_CHECK(node->IsOperator());
    auto op = node->GetOperator();

    VELOX_CHECK(kOperatorNames.count(op), "Operator {} not found", op);

    std::string name = kOperatorNames.at(op);
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto inputs = getInputs(nodes, 2);

    nodes->push(std::make_shared<core::CallExpr>(
        std::move(name), std::move(inputs), std::nullopt));
  }

  void visit(
      const commonsql::parser::MultiplicativeExpression* node,
      void* data) override {
    static const std::unordered_map<int, std::string> kOperatorNames = {
        {commonsql::parser::STAR, "multiply"},
        {commonsql::parser::DIV, "divide"},
        {commonsql::parser::PERCENT, "mod"},
    };

    VELOX_CHECK(node->IsOperator());
    auto op = node->GetOperator();

    VELOX_CHECK(kOperatorNames.count(op), "Operator {} not found", op);

    std::string name = kOperatorNames.at(op);
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto inputs = getInputs(nodes, 2);

    nodes->push(std::make_shared<core::CallExpr>(
        std::move(name), std::move(inputs), std::nullopt));
  }

  void visit(const commonsql::parser::ArgumentList* node, void* data) override {
    visitChildren(node, data);
  }

  void visit(const commonsql::parser::BuiltinFunctionCall* node, void* data)
      override {
    std::string name = node->beginToken->image;
    node->GetChild(0)->jjtAccept(this, data);

    auto numArgs = node->GetChild(0)->NumChildren();
    std::vector<std::shared_ptr<const core::IExpr>> inputs(numArgs);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);

    for (auto i = 0; i < numArgs; ++i) {
      inputs[numArgs - 1 - i] = nodes->top();
      nodes->pop();
    }

    nodes->push(std::make_shared<core::CallExpr>(
        std::move(name), std::move(inputs), std::nullopt));
  }

  void visit(const commonsql::parser::FunctionCall* node, void* data) override {
    VELOX_CHECK_EQ(2, node->NumChildren());

    std::string name = node->beginToken->image;
    node->GetChild(1)->jjtAccept(this, data);

    auto numArgs = node->GetChild(1)->NumChildren();
    std::vector<std::shared_ptr<const core::IExpr>> inputs(numArgs);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);

    for (auto i = 0; i < numArgs; ++i) {
      inputs[numArgs - 1 - i] = nodes->top();
      nodes->pop();
    }

    nodes->push(std::make_shared<core::CallExpr>(
        std::move(name), std::move(inputs), std::nullopt));
  }

  void visit(const commonsql::parser::Lambda* node, void* data) override {
    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);

    // First child is LambdaParams.
    auto params = node->GetChild(0);
    std::vector<std::string> names;
    for (auto i = 0; i < params->NumChildren(); ++i) {
      auto param = dynamic_cast<const commonsql::parser::LambdaParam*>(
          params->GetChild(i));
      names.push_back(param->beginToken->image);
    }

    // Second child is LambdaBody.
    node->GetChild(1)->jjtAccept(this, data);
    auto body = getOneInput(nodes);

    nodes->push(std::make_shared<core::LambdaExpr>(names, body));
  }

  void visit(const commonsql::parser::LambdaParams* node, void* data) override {
    visitChildren(node, data);
  }

  void visit(const commonsql::parser::LambdaParam* node, void* data) override {
    VELOX_UNSUPPORTED();
  }

  void visit(const commonsql::parser::LambdaBody* node, void* data) override {
    // Pass through.
    visitChildren(node, data);
  }

  void visit(const commonsql::parser::Comparison* node, void* data) override {
    static const std::unordered_map<int, std::string> kOperatorNames = {
        {commonsql::parser::EQUAL, "eq"},
        {commonsql::parser::NOT_EQUAL, "neq"},
        {commonsql::parser::LESS_THAN, "lt"},
        {commonsql::parser::LESS_THAN_OR_EQUAL, "lte"},
        {commonsql::parser::GREATER_THAN, "gt"},
        {commonsql::parser::GREATER_THAN_OR_EQUAL, "gte"},
    };

    VELOX_CHECK(node->IsOperator());
    auto op = node->GetOperator();

    VELOX_CHECK(kOperatorNames.count(op), "Operator {} not found", op);

    std::string name = kOperatorNames.at(op);
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto inputs = getInputs(nodes, 2);

    nodes->push(std::make_shared<core::CallExpr>(
        std::move(name), std::move(inputs), std::nullopt));
  }

  void visit(const commonsql::parser::AndExpression* node, void* data)
      override {
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto inputs = getInputs(nodes, node->NumChildren());

    nodes->push(std::make_shared<core::CallExpr>(
        "and", std::move(inputs), std::nullopt));
  }

  void visit(const commonsql::parser::OrExpression* node, void* data) override {
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto inputs = getInputs(nodes, node->NumChildren());

    nodes->push(std::make_shared<core::CallExpr>(
        "or", std::move(inputs), std::nullopt));
  }

  void visit(const commonsql::parser::Coalesce* node, void* data) override {
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto inputs = getInputs(nodes, node->NumChildren());

    nodes->push(std::make_shared<core::CallExpr>(
        "coalesce", std::move(inputs), std::nullopt));
  }

  void visit(const commonsql::parser::IsNull* node, void* data) override {
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto inputs = getInputs(nodes, 1);

    auto isNull = std::make_shared<core::CallExpr>(
        "is_null", std::move(inputs), std::nullopt);
    if (node->IsNegated()) {
      nodes->push(std::make_shared<core::CallExpr>(
          "not",
          std::vector<std::shared_ptr<const core::IExpr>>{isNull},
          std::nullopt));
    } else {
      nodes->push(isNull);
    }
  }

  void visit(const commonsql::parser::NotExpression* node, void* data)
      override {
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto inputs = getInputs(nodes, 1);

    nodes->push(std::make_shared<core::CallExpr>(
        "not", std::move(inputs), std::nullopt));
  }

  void visit(const commonsql::parser::CastExpression* node, void* data)
      override {
    node->GetChild(0)->jjtAccept(this, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto expr = nodes->top();
    nodes->pop();

    auto typeName = ((commonsql::parser::PredefinedType*)node->GetChild(1))
                        ->beginToken->image;

    for (auto i = 0; i < typeName.size(); ++i) {
      typeName[i] = tolower(typeName[i]);
    }

    static const std::unordered_map<std::string, TypeKind> kTypeNames = {
        {"boolean", TypeKind::BOOLEAN},
        {"tinyint", TypeKind::TINYINT},
        {"smallint", TypeKind::SMALLINT},
        {"integer", TypeKind::INTEGER},
        {"bigint", TypeKind::BIGINT},
        {"real", TypeKind::REAL},
        {"double", TypeKind::DOUBLE},
        {"varchar", TypeKind::VARCHAR},
        {"timestamp", TypeKind::TIMESTAMP},
    };

    VELOX_CHECK(
        kTypeNames.count(typeName), "Unsupported cast type: {}", typeName);
    auto typeKind = kTypeNames.at(typeName);

    nodes->push(std::make_shared<core::CastExpr>(
        createType(typeKind, {}), expr, false /*nullOnFailure*/, std::nullopt));
  }

  void visit(const commonsql::parser::ArrayElement* node, void* data) override {
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto inputs = getInputs(nodes, 2);

    nodes->push(std::make_shared<core::CallExpr>(
        "subscript", std::move(inputs), std::nullopt));
  }

  void visit(const commonsql::parser::FieldReference* node, void* data)
      override {
    visitChildren(node, data);

    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);
    auto nameInput = getOneInput(nodes);
    auto name = dynamic_cast<const core::FieldAccessExpr*>(nameInput.get())
                    ->getFieldName();

    auto inputs = getInputs(nodes, 1);

    nodes->push(std::make_shared<core::FieldAccessExpr>(
        name, std::nullopt, std::move(inputs)));
  }

  void visit(const commonsql::parser::ParenthesizedExpression* node, void* data)
      override {
    // Pass through.
    visitChildren(node, data);
  }

  void visit(const commonsql::parser::Identifier* node, void* data) override {
    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);

    auto name = node->beginToken->image;
    if (name[0] == '\"') {
      // Strip double quotes: "c0" -> c0.
      name = name.substr(1, name.size() - 2);
    }
    nodes->push(std::make_shared<core::FieldAccessExpr>(name, std::nullopt));
  }

  void visit(const commonsql::parser::CharStringLiteral* node, void* data)
      override {
    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);

    auto value = node->beginToken->image;
    // Strip single quotes: 'test' -> test.
    value = value.substr(1, value.size() - 2);

    nodes->push(
        std::make_shared<core::ConstantExpr>(variant(value), std::nullopt));
  }

  void visit(const commonsql::parser::UnsignedNumericLiteral* node, void* data)
      override {
    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);

    auto n = (int64_t)stoi(node->beginToken->image);
    nodes->push(std::make_shared<core::ConstantExpr>(variant(n), std::nullopt));
  }

  void visit(const commonsql::parser::NullLiteral* node, void* data) override {
    auto* nodes = static_cast<std::stack<std::shared_ptr<core::IExpr>>*>(data);

    nodes->push(std::make_shared<core::ConstantExpr>(
        variant::null(TypeKind::UNKNOWN), std::nullopt));
  }

  void defaultVisit(const commonsql::parser::SimpleNode* node, void* data)
      override {
    JAVACC_STRING_TYPE buffer;
    node->dumpToBuffer(" ", "\n", &buffer);
    printf("%s\n", buffer.c_str());
    VELOX_UNSUPPORTED("Unexpected node: {}", buffer);
  }

 private:
  void visitChildren(const commonsql::parser::AstNode* node, void* data) {
    for (auto i = 0; i < node->NumChildren(); ++i) {
      node->GetChild(i)->jjtAccept(this, data);
    }
  }

  std::vector<std::shared_ptr<const core::IExpr>> getInputs(
      std::stack<std::shared_ptr<core::IExpr>>* nodes,
      int numInputs) {
    std::vector<std::shared_ptr<const core::IExpr>> inputs(numInputs);
    for (auto i = 0; i < numInputs; ++i) {
      inputs[numInputs - 1 - i] = nodes->top();
      nodes->pop();
    }

    return inputs;
  }

  std::shared_ptr<const core::IExpr> getOneInput(
      std::stack<std::shared_ptr<core::IExpr>>* nodes) {
    auto input = nodes->top();
    nodes->pop();
    return input;
  }
};
} // namespace facebook::velox::parse
