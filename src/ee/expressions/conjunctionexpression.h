/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#pragma once

#include "common/common.h"
#include "common/serializeio.h"
#include "common/valuevector.h"

#include "expressions/abstractexpression.h"

#include <string>

namespace voltdb {

class ConjunctionAnd;
class ConjunctionOr;

template <typename C>
class ConjunctionExpression : public AbstractExpression {
  public:
      ConjunctionExpression(ExpressionType type,
              AbstractExpression *left,
              AbstractExpression *right) : AbstractExpression(type, left, right) {
        this->m_left = left;
        this->m_right = right;
    }

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ConjunctionExpression\n");
    }
    AbstractExpression *m_left;
    AbstractExpression *m_right;
};

template<> inline NValue
ConjunctionExpression<ConjunctionAnd>::eval(const TableTuple *tuple1,
        const TableTuple *tuple2) const {
    NValue leftBool = m_left->eval(tuple1, tuple2);
    // False False -> False
    // False True  -> False
    // False NULL  -> False
    if (leftBool.isFalse()) {
        return leftBool;
    }
    NValue rightBool = m_right->eval(tuple1, tuple2);
    // True  False -> False
    // True  True  -> True
    // True  NULL  -> NULL
    // NULL  False -> False
    if (leftBool.isTrue() || rightBool.isFalse()) {
        return rightBool;
    } else {
        // NULL  True  -> NULL
        // NULL  NULL  -> NULL
        return NValue::getNullValue(ValueType::tBOOLEAN);
    }
}

template<> inline NValue
ConjunctionExpression<ConjunctionOr>::eval(const TableTuple *tuple1,
        const TableTuple *tuple2) const {
    NValue leftBool = m_left->eval(tuple1, tuple2);
    // True True  -> True
    // True False -> True
    // True NULL  -> True
    if (leftBool.isTrue()) {
        return leftBool;
    }
    NValue rightBool = m_right->eval(tuple1, tuple2);
    // False True  -> True
    // False False -> False
    // False NULL  -> NULL
    // NULL  True  -> True
    if (leftBool.isFalse() || rightBool.isTrue()) {
        return rightBool;
    } else {
        // NULL  False -> NULL
        // NULL  NULL  -> NULL
        return NValue::getNullValue(ValueType::tBOOLEAN);
    }
}

}
