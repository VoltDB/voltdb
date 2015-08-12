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

// Each of these OP classes implements a standard static function interface
// for a different comparison operator assumed to apply to two non-null-valued
// NValues (**except CmpNotDistinct, which can take two null values).
// "compare" delegates to an NValue method implementing the specific
// comparison and returns either a true or false boolean NValue.
// "implies_true_for_row" returns true if a prior true return from compare
// applied to a row's prefix column implies a true result for the row comparison.
// This may require a recheck for strict inequality.
// "implies_false_for_row" returns true if a prior false return from compare
// applied to a row's prefix column implies a false result for the row comparison.
// This may require a recheck for strict inequality.
// "includes_equality" returns true if the comparison is true for (rows of) equal values.
// isNullRejecting() returns true if the comparison does not consider NULL values as valid ones
// during comparison. All comparison except "is distinct from" are null rejecting, therefore
// returning true.

class CmpEq {
public:
    inline static const char* op_name() { return "CmpEq"; }
    inline static NValue compare(const NValue& l, const NValue& r)
    { return l.op_equals_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r) { return false; }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_null_for_row() { return false; }
    inline static bool includes_equality() { return true; }
    inline static bool isNullRejecting() { return true; }
};


class CmpNotDistinct : public CmpEq {
public:
    inline static const char* op_name() { return "CmpNotDistinct"; }
    inline static NValue compare(const NValue& l, const NValue& r)
    { return l.op_equals(r);}
    inline static bool isNullRejecting() { return false; }
};

class CmpNe {
public:
    inline static const char* op_name() { return "CmpNe"; }
    inline static NValue compare(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r) { return false; }
    inline static bool implies_null_for_row() { return false; }
    inline static bool includes_equality() { return false; }
    inline static bool isNullRejecting() { return true; }
};

class CmpLt {
public:
    inline static const char* op_name() { return "CmpLt"; }
    inline static NValue compare(const NValue& l, const NValue& r)
    { return l.op_lessThan_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r).isTrue(); }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return false; }
    inline static bool isNullRejecting() { return true; }
};

class CmpGt {
public:
    inline static const char* op_name() { return "CmpGt"; }
    inline static NValue compare(const NValue& l, const NValue& r)
    { return l.op_greaterThan_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r).isTrue(); }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return false; }
    inline static bool isNullRejecting() { return true; }
};

class CmpLte {
public:
    inline static const char* op_name() { return "CmpLte"; }
    inline static NValue compare(const NValue& l, const NValue& r)
    { return l.op_lessThanOrEqual_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r).isTrue(); }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return true; }
    inline static bool isNullRejecting() { return true; }
};

class CmpGte {
public:
    inline static const char* op_name() { return "CmpGte"; }
    inline static NValue compare(const NValue& l, const NValue& r)
    { return l.op_greaterThanOrEqual_withoutNull(r);}
    inline static bool implies_true_for_row(const NValue& l, const NValue& r)
    { return l.op_notEquals_withoutNull(r).isTrue(); }
    inline static bool implies_false_for_row(const NValue& l, const NValue& r) { return true; }
    inline static bool implies_null_for_row() { return true; }
    inline static bool includes_equality() { return true; }
    inline static bool isNullRejecting() { return true; }
};

// CmpLike and CmpIn are slightly special in that they can never be
// instantiated in a row comparison context -- even "(a, b) IN (subquery)" is
// decomposed into column-wise equality comparisons "(a, b) = ANY (subquery)".
class CmpLike {
public:
    inline static const char* op_name() { return "CmpLike"; }
    inline static NValue compare(const NValue& l, const NValue& r) { return l.like(r);}
};

class CmpIn {
public:
    inline static const char* op_name() { return "CmpIn"; }
    inline static NValue compare(const NValue& l, const NValue& r)
    { return l.inList(r) ? NValue::getTrue() : NValue::getFalse(); }
};

template <typename OP>
class ComparisonExpression : public AbstractExpression {
public:
    ComparisonExpression(ExpressionType type,
                                  AbstractExpression *left,
                                  AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
        m_left = left;
        m_right = right;
    };

    inline NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const
    {
        VOLT_TRACE("eval %s. left %s, right %s. ret=%s",
                   OP::op_name(),
                   typeid(*(m_left)).name(),
                   typeid(*(m_right)).name(),
                   traceEval(tuple1, tuple2));

        assert(m_left != NULL);
        assert(m_right != NULL);

        NValue lnv = m_left->eval(tuple1, tuple2);
        if (lnv.isNull()) {
            return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
        }

        NValue rnv = m_right->eval(tuple1, tuple2);
        if (rnv.isNull()) {
            return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
        }

        // comparisons with null or NaN are always false
        // [This code is commented out because doing the right thing breaks voltdb atm.
        // We need to re-enable after we can verify that all plans in all configs give the
        // same answer.]
        /*if (lnv.isNull() || lnv.isNaN() || rnv.isNull() || rnv.isNaN()) {
            return NValue::getFalse();
        }*/

        return OP::compare(lnv, rnv);
    }

    inline const char* traceEval(const TableTuple *tuple1, const TableTuple *tuple2) const
    {
        NValue lnv;
        NValue rnv;
        return  (((lnv = m_left->eval(tuple1, tuple2)).isNull() ||
                  (rnv = m_right->eval(tuple1, tuple2)).isNull()) ?
                 "NULL" :
                 (OP::compare(lnv,
                                          rnv).isTrue() ?
                  "TRUE" :
                  "FALSE"));
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ComparisonExpression\n");
    }

private:
    AbstractExpression *m_left;
    AbstractExpression *m_right;
};

template <typename C, typename L, typename R>
class InlinedComparisonExpression : public ComparisonExpression<C> {
public:
    InlinedComparisonExpression(ExpressionType type,
                                         AbstractExpression *left,
                                         AbstractExpression *right)
        : ComparisonExpression<C>(type, left, right)
    {}
};


class NotDistinctExpression : public AbstractExpression {
public:
    NotDistinctExpression(ExpressionType type,
                          AbstractExpression *left,
                          AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
        m_left = left;
        m_right = right;
    };

    inline NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const
    {
        NValue lnv = m_left->eval(tuple1, tuple2);
        NValue rnv = m_right->eval(tuple1, tuple2);

        NValue result = lnv.op_equals(rnv);

        VOLT_TRACE("eval NotDistinct. left %s, right %s. ret=%s",
                           typeid(*(m_left)).name(),
                           typeid(*(m_right)).name(),
                           result.isTrue()? "TRUE" : "FALSE");
        return result;
    }


    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ComparisonExpression\n");
    }

private:
    AbstractExpression *m_left;
    AbstractExpression *m_right;
};
}
#endif
