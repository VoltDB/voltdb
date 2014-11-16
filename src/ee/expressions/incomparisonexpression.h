/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#ifndef HSTOREINCOMPARISONEXPRESSION_H
#define HSTOREINCOMPARISONEXPRESSION_H

#include "common/common.h"
#include "common/executorcontext.hpp"
#include "common/serializeio.h"
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "common/ValuePeeker.hpp"
#include "expressions/abstractexpression.h"
#include "storage/table.h"
#include "storage/tableiterator.h"

#include <string>
#include <cassert>

namespace voltdb {

template <typename ValueExtractor>
class InComparisonExpression : public AbstractExpression {
public:
    InComparisonExpression(AbstractExpression *left,
                           AbstractExpression *right)
        : AbstractExpression(EXPRESSION_TYPE_COMPARE_IN, left, right)
    {
        assert(left != NULL);
        assert(right != NULL);
    };

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "InComparisonExpression\n");
    }

};

struct NValueExtractor
{
    NValueExtractor(NValue value) :
        m_value(value)
    {}

    int activeTupleCount() const
    {
        return 1;
    }

    bool isNUllOrEmpty() const
    {
        return m_value.isNull();
    }

    bool equals(const TableTuple& tuple) const
    {
        return m_value.compare(tuple.getNValue(0)) == VALUE_COMPARE_EQUAL;
    }

    NValue m_value;
};

struct SubqueryValueExtractor
{
    SubqueryValueExtractor(NValue value)
    {
        int subqueryId = ValuePeeker::peekInteger(value);
        ExecutorContext* exeContext = ExecutorContext::getExecutorContext();
        m_table = exeContext->executeExecutors(subqueryId);
        assert(m_table != NULL);
        m_tuple.setSchema(m_table->schema());
    }

    int activeTupleCount() const
    {
        return m_table->activeTupleCount();
    }

    bool isNUllOrEmpty() const;

    bool equals(const TableTuple& tuple) const
    {
        assert(m_tuple.isNullTuple() == false);
        assert(tuple.isNullTuple() == false);
        return m_tuple.compare(tuple) == VALUE_COMPARE_EQUAL;
    }

    Table* m_table;
    TableTuple m_tuple;
};

template <typename ValueExtractor>
NValue InComparisonExpression<ValueExtractor>::eval(const TableTuple *tuple1, const TableTuple *tuple2) const
{
    // The outer_expr IN (SELECT inner_expr ...) evaluates as follows:
    // There is an exact match outer_expr = inner_expr => TRUE
    // There no match and the inner_expr produces a row where inner_expr is NULL => NULL
    // There no match and the inner_expr produces only non- NULL rows or empty => FASLE
    // The outer_expr is NULL or empty and the inner_expr is empty => FASLE
    // The outer_expr is NULL or empty and the inner_expr produces any row => NULL

    // Evaluate the outer_expr. The return value can be either the value itself or a subquery id
    // in case of the row expression on the left side
    NValue lvalue = m_left->eval(tuple1, tuple2);
    ValueExtractor extractor(lvalue);
    if (extractor.activeTupleCount() > 1) {
        // throw runtime exception
        char message[256];
        snprintf(message, 256, "More than one row returned by a scalar/row subquery");
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }

    // Evaluate the inner_expr. The return value is a subquery id
    NValue rvalue = m_right->eval(tuple1, tuple2);
    int rsubqueryId = ValuePeeker::peekInteger(rvalue);
    ExecutorContext* exeContext = ExecutorContext::getExecutorContext();
    Table* rightTable = exeContext->executeExecutors(rsubqueryId);
    assert(rightTable != NULL);

    if (extractor.isNUllOrEmpty()) {
        NValue retval = NValue::getFalse();
        if (rightTable->activeTupleCount() > 0) {
            retval.setNull();
        }
        return retval;
    }

    // Iterate over the inner results until the first match if any
    TableIterator& iterator = rightTable->iterator();
    TableTuple rtuple(rightTable->schema());
    int tuple_ctr = 0;
    int rschemaSize = rightTable->schema()->columnCount();
    bool hasOuterNull = false;
    while (iterator.next(rtuple))
    {
        VOLT_TRACE("INNER TUPLE: %s, %d/%d\n",
                       rtuple.debug(rightTable->name()).c_str(), tuple_ctr,
                       (int)rightTable->activeTupleCount());
        ++tuple_ctr;
        int columnIdx = 0;
        for (; columnIdx < rschemaSize; ++columnIdx)
        {
            if (rtuple.isNull(columnIdx)) {
                hasOuterNull = true;
                break;
            }
        }
        if (columnIdx == rschemaSize && extractor.equals(rtuple)) {
            return NValue::getTrue();
        }
    }

    // No match
    NValue retval = NValue::getFalse();
    if (hasOuterNull == true) {
        retval.setNull();
    }
    return retval;
}

bool SubqueryValueExtractor::isNUllOrEmpty() const
{
    if (m_tuple.isNullTuple() == false)
    {
        int size = m_table->schema()->columnCount();
        for (int i = 0; i < size; ++i)
        {
            if (m_tuple.isNull(i) == true)
            {
                    return true;
            }
        }
        return false;
    }
    return true;
}

}
#endif
