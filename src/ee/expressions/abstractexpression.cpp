/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
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
    return (m_hasParameter = hasParameter());
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
AbstractExpression::buildExpressionTree(PlannerDomValue obj)
{
    AbstractExpression * exp =
      AbstractExpression::buildExpressionTree_recurse(obj);

    if (exp)
        exp->initParamShortCircuits();
    return exp;
}

AbstractExpression*
AbstractExpression::buildExpressionTree_recurse(PlannerDomValue obj)
{
    // build a tree recursively from the bottom upwards.
    // when the expression node is instantiated, its type,
    // value and child types will have been discovered.

    ExpressionType peek_type = EXPRESSION_TYPE_INVALID;
    ValueType value_type = VALUE_TYPE_INVALID;
    bool inBytes = false;
    AbstractExpression *left_child = NULL;
    AbstractExpression *right_child = NULL;
    std::vector<AbstractExpression*>* argsVector = NULL;

    // read the expression type
    peek_type = static_cast<ExpressionType>(obj.valueForKey("TYPE").asInt());
    assert(peek_type != EXPRESSION_TYPE_INVALID);

    if (obj.hasNonNullKey("VALUE_TYPE")) {
        int32_t value_type_int = obj.valueForKey("VALUE_TYPE").asInt();
        value_type = static_cast<ValueType>(value_type_int);
        assert(value_type != VALUE_TYPE_INVALID);

        if (obj.hasNonNullKey("IN_BYTES")) {
            inBytes = true;
        }
    }

    // add the value size
    int valueSize = -1;
    if (obj.hasNonNullKey("VALUE_SIZE")) {
        valueSize = obj.valueForKey("VALUE_SIZE").asInt();
    } else {
        // This value size should be consistent with VoltType.java
        valueSize = NValue::getTupleStorageSize(value_type);
    }

    // recurse to children
    try {
        if (obj.hasNonNullKey("LEFT")) {
            PlannerDomValue leftValue = obj.valueForKey("LEFT");
            left_child = AbstractExpression::buildExpressionTree_recurse(leftValue);
        }
        if (obj.hasNonNullKey("RIGHT")) {
            PlannerDomValue rightValue = obj.valueForKey("RIGHT");
            right_child = AbstractExpression::buildExpressionTree_recurse(rightValue);
        }

        // NULL argsVector corresponds to a missing ARGS value
        // vs. an empty argsVector which corresponds to an empty array ARGS value.
        // Different expression types could assert either a NULL or non-NULL argsVector initializer.
        if (obj.hasNonNullKey("ARGS")) {
            PlannerDomValue argsArray = obj.valueForKey("ARGS");
            argsVector = new std::vector<AbstractExpression*>();
            for (int i = 0; i < argsArray.arrayLen(); i++) {
                PlannerDomValue argValue = argsArray.valueAtIndex(i);
                AbstractExpression* argExpr = AbstractExpression::buildExpressionTree_recurse(argValue);
                argsVector->push_back(argExpr);
            }
        }

        // invoke the factory. obviously it has to handle null children.
        // pass it the serialization stream in case a subclass has more
        // to read. yes, the per-class data really does follow the
        // child serializations.
        AbstractExpression* finalExpr = ExpressionUtil::expressionFactory(obj, peek_type, value_type, valueSize,
                left_child, right_child, argsVector);

        finalExpr->setInBytes(inBytes);

        return finalExpr;
    }
    catch (const SerializableEEException &ex) {
        delete left_child;
        delete right_child;
        delete argsVector;
        throw;
    }
}

}
