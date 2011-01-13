/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "abstractexpression.h"

#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/types.h"
#include "expressions/expressionutil.h"

#include <sstream>
#include <cassert>
#include <stdexcept>

namespace voltdb {

// ------------------------------------------------------------------
// AbstractExpression
// ------------------------------------------------------------------
AbstractExpression::AbstractExpression()
    : m_left(NULL), m_right(NULL),
      m_type(EXPRESSION_TYPE_INVALID),
      m_hasParameter(true)
{
}

AbstractExpression::AbstractExpression(ExpressionType type)
    : m_left(NULL), m_right(NULL),
      m_type(type),
      m_hasParameter(true)
{
}

AbstractExpression::AbstractExpression(ExpressionType type,
                                       AbstractExpression* left,
                                       AbstractExpression* right)
    : m_left(left), m_right(right),
      m_type(type),
      m_hasParameter(true)
{
}

AbstractExpression::~AbstractExpression()
{
    delete m_left;
    delete m_right;
}

void
AbstractExpression::substitute(const NValueArray &params)
{
    if (!m_hasParameter)
        return;

    // descend. nodes with parameters overload substitute()
    VOLT_TRACE("Substituting parameters for expression \n%s ...", debug(true).c_str());
    if (m_left) {
        VOLT_TRACE("Substitute processing left child...");
        m_left->substitute(params);
    }
    if (m_right) {
        VOLT_TRACE("Substitute processing right child...");
        m_right->substitute(params);
    }
}

bool
AbstractExpression::hasParameter() const
{
    if (m_left && m_left->hasParameter())
        return true;
    return (m_right && m_right->hasParameter());
}

bool
AbstractExpression::initParamShortCircuits()
{
    if (m_left && m_left->hasParameter())
        return true;
    if (m_right && m_right->hasParameter())
        return true;

    m_hasParameter = false;
    return false;
}

std::string
AbstractExpression::debug() const
{
    if (this == NULL) {
        return "NULL";
    }
    std::ostringstream buffer;
    //buffer << "Expression[" << expressionutil::getTypeName(getExpressionType()) << "]";
    buffer << "Expression[" << expressionToString(getExpressionType()) << ", " << getExpressionType() << "]";
    return (buffer.str());
}

std::string
AbstractExpression::debug(bool traverse) const
{
    if (this == NULL) {
        return "NULL";
    }
    return (traverse ? debug(std::string("")) : debug());
}

std::string
AbstractExpression::debug(const std::string &spacer) const
{
    if (this == NULL) {
        return "NULL";
    }
    std::ostringstream buffer;
    buffer << spacer << "+ " << debug() << "\n";

    std::string info_spacer = spacer + "   ";
    buffer << debugInfo(info_spacer);

    // process children
    if (m_left != NULL || m_right != NULL) {
        buffer << info_spacer << "left:  " <<
          (m_left != NULL  ? "\n" + m_left->debug(info_spacer)  : "<NULL>\n");
        buffer << info_spacer << "right: " <<
          (m_right != NULL ? "\n" + m_right->debug(info_spacer) : "<NULL>\n");
    }
    return (buffer.str());
}

// ------------------------------------------------------------------
// SERIALIZATION METHODS
// ------------------------------------------------------------------
AbstractExpression*
AbstractExpression::buildExpressionTree(json_spirit::Object &obj)
{
    AbstractExpression * exp =
      AbstractExpression::buildExpressionTree_recurse(obj);

    if (exp)
        exp->initParamShortCircuits();
    return exp;
}

AbstractExpression*
AbstractExpression::buildExpressionTree_recurse(json_spirit::Object &obj)
{
    // build a tree recursively from the bottom upwards.
    // when the expression node is instantiated, its type,
    // value and child types will have been discovered.

    ExpressionType peek_type = EXPRESSION_TYPE_INVALID;
    ValueType value_type = VALUE_TYPE_INVALID;
    AbstractExpression *left_child = NULL;
    AbstractExpression *right_child = NULL;

    // read the expression type
    json_spirit::Value expressionTypeValue = json_spirit::find_value(obj,
                                                                     "TYPE");
    if (expressionTypeValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractExpression::"
                                      "buildExpressionTree_recurse:"
                                      " Couldn't find TYPE value");
    }
    assert(stringToExpression(expressionTypeValue.get_str()) != EXPRESSION_TYPE_INVALID);
    peek_type = stringToExpression(expressionTypeValue.get_str());

    // and the value type
    json_spirit::Value valueTypeValue = json_spirit::find_value(obj,
                                                                "VALUE_TYPE");
    if (valueTypeValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractExpression::"
                                      "buildExpressionTree_recurse:"
                                      " Couldn't find VALUE_TYPE value");
    }
    std::string valueTypeString = valueTypeValue.get_str();
    value_type = stringToValue(valueTypeString);

    assert(value_type != VALUE_TYPE_INVALID);

    // add the value size
    json_spirit::Value valueSizeValue = json_spirit::find_value(obj,
                                                                "VALUE_SIZE");
    if (valueSizeValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractExpression::"
                                      "buildExpressionTree_recurse:"
                                      " Couldn't find VALUE_SIZE value");
    }
    int valueSize = valueSizeValue.get_int();

    // recurse to children
    try {
        json_spirit::Value leftValue = json_spirit::find_value(obj, "LEFT");
        if (!(leftValue == json_spirit::Value::null)) {
            left_child = AbstractExpression::buildExpressionTree_recurse(leftValue.get_obj());
        } else {
            left_child = NULL;
        }

        json_spirit::Value rightValue = json_spirit::find_value( obj, "RIGHT");
        if (!(rightValue == json_spirit::Value::null)) {
            right_child = AbstractExpression::buildExpressionTree_recurse(rightValue.get_obj());
        } else {
            right_child = NULL;
        }

        // invoke the factory. obviously it has to handle null children.
        // pass it the serialization stream in case a subclass has more
        // to read. yes, the per-class data really does follow the
        // child serializations.
        return expressionFactory(obj,
                                 peek_type, value_type, valueSize,
                                 left_child, right_child);
    }
    catch (SerializableEEException &ex) {
        delete left_child;
        delete right_child;
        throw;
    }
}

}
