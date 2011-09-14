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

#ifndef HSTORECOMPARISONEXPRESSION_H
#define HSTORECOMPARISONEXPRESSION_H

#include "common/common.h"
#include "common/serializeio.h"
#include "common/valuevector.h"

#include "expressions/abstractexpression.h"
#include "expressions/parametervalueexpression.h"
#include "expressions/constantvalueexpression.h"
#include "expressions/tuplevalueexpression.h"

#include <string>
#include <cassert>

namespace voltdb {

class CmpEq {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_equals(r);}
};
class CmpNe {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_notEquals(r);}
};
class CmpLt {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_lessThan(r);}
};
class CmpGt {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_greaterThan(r);}
};
class CmpLte {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_lessThanOrEqual(r);}
};
class CmpGte {
public:
    inline NValue cmp(NValue l, NValue r) const { return l.op_greaterThanOrEqual(r);}
};

template <typename C>
class ComparisonExpression : public AbstractExpression {
public:
    ComparisonExpression(ExpressionType type,
                                  AbstractExpression *left,
                                  AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
        this->m_left = left;
        this->m_right = right;
    };

    inline NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        VOLT_TRACE("eval %s. left %s, right %s. ret=%s",
                   typeid(this->compare).name(), typeid(*(this->m_left)).name(),
                   typeid(*(this->m_right)).name(),
                   this->compare.cmp(this->m_left->eval(tuple1, tuple2),
                                     this->m_right->eval(tuple1, tuple2)).isTrue()
                   ? "TRUE" : "FALSE");

        assert(m_left != NULL);
        assert(m_right != NULL);

        return this->compare.cmp(
            this->m_left->eval(tuple1, tuple2),
            this->m_right->eval(tuple1, tuple2));
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ComparisonExpression\n");
    }

private:
    AbstractExpression *m_left;
    AbstractExpression *m_right;
    C compare;
};

template <typename C, typename L, typename R>
class InlinedComparisonExpression : public AbstractExpression {
public:
    InlinedComparisonExpression(ExpressionType type,
                                         AbstractExpression *left,
                                         AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
        this->m_left = left;
        this->m_leftTyped = dynamic_cast<L*>(left);
        this->m_right = right;
        this->m_rightTyped = dynamic_cast<R*>(right);

        assert (m_leftTyped != NULL);
        assert (m_rightTyped != NULL);
    };

    inline NValue eval(const TableTuple *tuple1, const TableTuple *tuple2 ) const {
        return this->compare.cmp(
            this->m_left->eval(tuple1, tuple2),
            this->m_right->eval(tuple1, tuple2));
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OptimizedInlinedComparisonExpression\n");
    }

  private:
    L *m_leftTyped;
    R *m_rightTyped;
    C compare;
};

}
#endif
