/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

#ifndef HSTOREOPREATOREXPRESSION_H
#define HSTOREOPERATOREXPRESSION_H

#include "common/common.h"
#include "common/serializeio.h"
#include "common/valuevector.h"

#include "expressions/abstractexpression.h"

#include <string>

namespace voltdb {


/*
 * Unary operators. Currently just operator NOT.
 */

class OperatorNotExpression : public AbstractExpression {
public:
    OperatorNotExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NOT) {
        m_left = left;
    };

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        assert (m_left);
        return m_left->eval(tuple1, tuple2).op_negate();
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OptimizedOperatorNotExpression");
    }

private:
    AbstractExpression *m_left;
};



/*
 * Binary operators.
 */

class OpPlus {
public:
    inline NValue op(NValue left, NValue right) const { return left.op_add(right); }
};

class OpMinus {
public:
    inline NValue op(NValue left, NValue right) const { return left.op_subtract(right); }
};

class OpMultiply {
public:
    inline NValue op(NValue left, NValue right) const { return left.op_multiply(right); }
};

class OpDivide {
public:
    inline NValue op(NValue left, NValue right) const { return left.op_divide(right); }
};


/*
 * Expressions templated on binary operator types
 */

template <typename OPER>
class OperatorExpression : public AbstractExpression {
  public:
    OperatorExpression(ExpressionType type,
                                AbstractExpression *left,
                                AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
    }

    NValue
    eval(const TableTuple *tuple1, const TableTuple *tuple2) const
    {
        assert(m_left);
        assert(m_right);
        return oper.op(m_left->eval(tuple1, tuple2),
                       m_right->eval(tuple1, tuple2));
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OptimizedOperatorExpression");
    }
private:
    OPER oper;
};


}
#endif
