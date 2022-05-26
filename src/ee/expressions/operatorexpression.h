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

#ifndef HSTOREOPERATOREXPRESSION_H
#define HSTOREOPERATOREXPRESSION_H

#include "common/common.h"
#include "common/serializeio.h"
#include "common/valuevector.h"

#include "expressions/abstractexpression.h"

#include <string>

namespace voltdb {


/*
 * Unary operators. (NOT and IS_NULL and UNARY_MINUS)
 */

class OperatorNotExpression : public AbstractExpression {
public:
    OperatorNotExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NOT) {
        m_left = left;
    };

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        vassert(m_left);
        NValue operand = m_left->eval(tuple1, tuple2);
        // NOT TRUE is FALSE
        if (operand.isTrue()) {
            return NValue::getFalse();
        }
        // NOT FALSE is TRUE
        if (operand.isFalse()) {
            return NValue::getTrue();
        }
        // NOT NULL is NULL
        return operand;
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OperatorNotExpression");
    }
};

class OperatorIsNullExpression : public AbstractExpression {
  public:
    OperatorIsNullExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_IS_NULL) {
            m_left = left;
    };

   NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
       vassert(m_left);
       NValue tmp = m_left->eval(tuple1, tuple2);
       if (tmp.isNull()) {
           return NValue::getTrue();
       }
       else {
           return NValue::getFalse();
       }
   }

   std::string debugInfo(const std::string &spacer) const {
       return (spacer + "OperatorIsNullExpression");
   }
};

class OperatorUnaryMinusExpression : public AbstractExpression {
  public:
    OperatorUnaryMinusExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_UNARY_MINUS) {
            m_left = left;
    };

   NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
       vassert(m_left);
       NValue tmp = m_left->eval(tuple1, tuple2);
       return tmp.op_unary_minus();
   }

   std::string debugInfo(const std::string &spacer) const {
       return (spacer + "OperatorUnaryMinusExpression");
   }
};

class OperatorCastExpression : public AbstractExpression {
public:
    OperatorCastExpression(ValueType vt, AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CAST)
        , m_targetType(vt)
    {
        m_left = left;
    };

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        vassert(m_left);
        return m_left->eval(tuple1, tuple2).castAs(m_targetType);
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "CastExpression");
    }
private:
    ValueType m_targetType;
};

class OperatorAlternativeExpression : public AbstractExpression {
public:
    OperatorAlternativeExpression(AbstractExpression *left, AbstractExpression *right)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_ALTERNATIVE, left, right)
    {
        vassert(m_left);
        vassert(m_right);
    };

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        throwFatalException("OperatorAlternativeExpression::eval function has no implementation.");
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "Operator ALTERNATIVE Expression");
    }

};

class OperatorCaseWhenExpression : public AbstractExpression {
public:
    OperatorCaseWhenExpression(ValueType vt, AbstractExpression *left, OperatorAlternativeExpression *right)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CASE_WHEN, left, right)
        , m_returnType(vt)
    {
    };

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        vassert(m_left);
        vassert(m_right);
        NValue thenClause = m_left->eval(tuple1, tuple2);

        if (thenClause.isTrue()) {
            return m_right->getLeft()->eval(tuple1, tuple2).castAs(m_returnType);
        } else {
            return m_right->getRight()->eval(tuple1, tuple2).castAs(m_returnType);
        }
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "Operator CASE WHEN Expression");
    }
private:
    ValueType m_returnType;
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
        vassert(m_left);
        vassert(m_right);
        return oper.op(m_left->eval(tuple1, tuple2),
                       m_right->eval(tuple1, tuple2));
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OptimizedOperatorExpression");
    }
private:
    OPER oper;
};

class OperatorExistsExpression : public AbstractExpression {
  public:
    OperatorExistsExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_EXISTS, left, NULL)
    {
    }

    NValue
    eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OperatorExistsExpression");
    }
};

}
#endif
