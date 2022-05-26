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
#include "common/executorcontext.hpp"
#include "common/serializeio.h"
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "common/ValuePeeker.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/comparisonexpression.h"
#include "storage/table.h"
#include "storage/tableiterator.h"

#include <string>
#include <cassert>

namespace voltdb {

// Compares two tuples column by column using lexicographical compare.
template<typename OP>
NValue compare_tuple(const TableTuple& tuple1, const TableTuple& tuple2) {
    vassert(tuple1.getSchema()->columnCount() == tuple2.getSchema()->columnCount());
    NValue fallback_result = OP::includes_equality() ? NValue::getTrue() : NValue::getFalse();
    int schemaSize = tuple1.getSchema()->columnCount();
    for (int columnIdx = 0; columnIdx < schemaSize; ++columnIdx) {
        NValue value1 = tuple1.getNValue(columnIdx);
        if (value1.isNull() && OP::isNullRejecting()) {
            fallback_result = NValue::getNullValue(ValueType::tBOOLEAN);
            if (OP::implies_null_for_row()) {
                return fallback_result;
            }
            continue;
        }
        NValue value2 = tuple2.getNValue(columnIdx);
        if (value2.isNull() && OP::isNullRejecting()) {
            fallback_result = NValue::getNullValue(ValueType::tBOOLEAN);
            if (OP::implies_null_for_row()) {
                return fallback_result;
            }
            continue;
        }
        if (OP::compare(value1, tuple2.getNValue(columnIdx)).isTrue()) {
            if (OP::implies_true_for_row(value1, value2)) {
                // allow early return on strict inequality
                return NValue::getTrue();
            }
        } else if (OP::implies_false_for_row(value1, value2)) {
                // allow early return on strict inequality
                return NValue::getFalse();
        }
    }
    // The only cases that have not already short-circuited involve all equal columns.
    // Each op either includes or excludes that particular case.
    return fallback_result;
}

//Assumption - quantifier is on the right
template <typename OP, typename ValueExtractorLeft, typename ValueExtractorRight>
class VectorComparisonExpression : public AbstractExpression {
    QuantifierType m_quantifier;
public:
    VectorComparisonExpression(ExpressionType et,
                           AbstractExpression *left,
                           AbstractExpression *right,
                           QuantifierType quantifier)
        : AbstractExpression(et, left, right),
          m_quantifier(quantifier) {
        vassert(left != NULL);
        vassert(right != NULL);
    };

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "VectorComparisonExpression\n");
    }
};

class NValueExtractor {
    NValue m_value;
    const NValue m_nullValue;
    bool m_hasNext = true;
public:
    using value_type = NValue;

    NValueExtractor(NValue value) : m_value(value), m_nullValue(value) {}
    int64_t resultSize() const {
        return hasNullValue() ? 0 : 1;
    }

    bool hasNullValue() const {
        return m_value.isNull();
    }

    bool hasNext() const {
        return m_hasNext;
    }

    NValue next() {
        m_hasNext = false;
        return m_value;
    }

    template<typename OP>
    NValue compare(const TableTuple& tuple) const {
        vassert(tuple.getSchema()->columnCount() == 1);
        return compare<OP>(tuple.getNValue(0));
    }

    template<typename OP>
    NValue compare(const NValue& nvalue) const {
        if (m_value.isNull() && OP::isNullRejecting()) {
            return NValue::getNullValue(ValueType::tBOOLEAN);
        } else if (nvalue.isNull() && OP::isNullRejecting()) {
            return NValue::getNullValue(ValueType::tBOOLEAN);
        } else {
            return OP::compare(m_value, nvalue);
        }
    }

    std::string debug() const {
        return m_value.isNull() ? "NULL" : m_value.debug();
    }

    const NValue& getNullValue() const {
        return m_nullValue;
    }
};

class TupleExtractor {
    Table* m_table;
    TableIterator m_iterator;
    TableTuple m_tuple;
    StandAloneTupleStorage m_null_tuple;
    int64_t m_size;

    static Table* getOutputTable(const NValue& value) {
        int subqueryId = ValuePeeker::peekInteger(value);
        ExecutorContext* exeContext = ExecutorContext::getExecutorContext();
        Table* table = exeContext->getSubqueryOutputTable(subqueryId);
        vassert(table != NULL);
        return table;
    }
public:
    using value_type = TableTuple;
    TupleExtractor(NValue value) : m_table(getOutputTable(value)),
        m_iterator(m_table->iterator()), m_tuple(m_table->schema()),
        m_null_tuple(m_table->schema()), m_size(m_table->activeTupleCount()) {}

    int64_t resultSize() const {
        return m_size;
    }

    bool hasNext() {
        return m_iterator.next(m_tuple);
    }

    TableTuple next() {
        return m_tuple;
    }

    bool hasNullValue() const {
        if (m_tuple.isNullTuple()) {
            return true;
        }
        int schemaSize = m_tuple.getSchema()->columnCount();
        for (int columnIdx = 0; columnIdx < schemaSize; ++columnIdx) {
            if (m_tuple.isNull(columnIdx)) {
                return true;
            }
        }
        return false;
    }

    template<typename OP>
    NValue compare(const TableTuple& tuple) const {
        return compare_tuple<OP>(m_tuple, tuple);
    }

    template<typename OP>
    NValue compare(const NValue& nvalue) const {
        vassert(m_tuple.getSchema()->columnCount() == 1);
        NValue lvalue = m_tuple.getNValue(0);
        if (lvalue.isNull() && OP::isNullRejecting()) {
            return NValue::getNullValue(ValueType::tBOOLEAN);
        }
        if (nvalue.isNull() && OP::isNullRejecting()) {
            return NValue::getNullValue(ValueType::tBOOLEAN);
        }
        return OP::compare(lvalue, nvalue);
    }

    std::string debug() const {
        return m_tuple.isNullTuple() ? "NULL" : m_tuple.debug("TEMP");
    }

    TableTuple getNullValue() {
        return m_null_tuple.tuple();
    }
};

template <typename OP, typename ValueExtractorOuter, typename ValueExtractorInner>
NValue VectorComparisonExpression<OP, ValueExtractorOuter, ValueExtractorInner>::eval(
        const TableTuple *tuple1, const TableTuple *tuple2) const {
    // Outer and inner expressions can be either a row (expr1, expr2, expr3...) or a single expr
    // The quantifier is expected on the right side of the expression "outer_expr OP ANY/ALL(inner_expr )"

    // The outer_expr OP ANY inner_expr evaluates as follows:
    // There is an exact match OP (outer_expr, inner_expr) == true => TRUE
    // There no match and the inner_expr produces a row where inner_expr is NULL => NULL
    // There no match and the inner_expr produces only non- NULL rows or empty => FALSE
    // The outer_expr is NULL or empty and the inner_expr is empty => FALSE
    // The outer_expr is NULL or empty and the inner_expr produces any row => NULL

    // The outer_expr OP ALL inner_expr evaluates as follows:
    // If inner_expr is empty => TRUE
    // If outer_expr OP inner_expr is TRUE for all inner_expr values => TRUE
    // If inner_expr contains NULL and outer_expr OP inner_expr is TRUE for all other inner values => NULL
    // If inner_expr contains NULL and outer_expr OP inner_expr is FALSE for some other inner values => FALSE
    // The outer_expr is NULL or empty and the inner_expr is empty => TRUE
    // The outer_expr is NULL or empty and the inner_expr produces any row => NULL

    // The outer_expr OP inner_expr evaluates as follows:
    // If inner_expr is NULL or empty => NULL
    // If outer_expr is NULL or empty => NULL
    // If outer_expr/inner_expr has more than 1 result => runtime exception
    // Else => outer_expr OP inner_expr

    // Evaluate the outer_expr. The return value can be either the value itself or a subquery id
    // in case of the row expression on the left side
    NValue lvalue = m_left->eval(tuple1, tuple2);
    ValueExtractorOuter outerExtractor(lvalue);
    if (outerExtractor.resultSize() > 1) {
        throw SerializableEEException("More than one row returned by a scalar/row subquery");
    }

    // Evaluate the inner_expr. The return value is a subquery id or a value as well
    NValue rvalue = m_right->eval(tuple1, tuple2);
    ValueExtractorInner innerExtractor(rvalue);
    if (m_quantifier == QUANTIFIER_TYPE_NONE && innerExtractor.resultSize() > 1) {
        throw SerializableEEException("More than one row returned by a scalar/row subquery");
    }

    if (innerExtractor.resultSize() == 0) {
        switch (m_quantifier) {
        case QUANTIFIER_TYPE_NONE: {
            // the inner extractor (RHS) value either does not have result or is NULL.
            // Check if the comparison operator is Null rejecting. If it is, return
            // NULL for boolean.
            if (OP::isNullRejecting() || !outerExtractor.hasNext()) {
                return NValue::getNullValue(ValueType::tBOOLEAN);
            }

            // If for the operator, NULL is a valid value in result, construct RHS value
            // with NULL and use that to compare against outer-extractor value.
            const typename ValueExtractorInner::value_type& innerNullValue = innerExtractor.getNullValue();
            vassert(innerExtractor.hasNullValue());
            return outerExtractor.template compare<OP>(innerNullValue);
        }
        case QUANTIFIER_TYPE_ANY: {
            return NValue::getFalse();
        }
        case QUANTIFIER_TYPE_ALL: {
            return NValue::getTrue();
        }
        }
    }

    vassert (innerExtractor.resultSize() > 0);
    if (!outerExtractor.hasNext() || (outerExtractor.hasNullValue() && OP::isNullRejecting()) ) {
        return NValue::getNullValue(ValueType::tBOOLEAN);
    }

    //  Iterate over the inner results until
    //  no qualifier - the first match ( single row at most)
    //  ANY qualifier - the first match
    //  ALL qualifier - the first mismatch
    bool hasInnerNull = false;
    NValue result;
    while (innerExtractor.hasNext()) {
        typename ValueExtractorInner::value_type innerValue = innerExtractor.next();
        result = outerExtractor.template compare<OP>(innerValue);
        if (result.isTrue()) {
            if (m_quantifier != QUANTIFIER_TYPE_ALL) {
                return result;
            }
        }
        else if (result.isFalse()) {
            if (m_quantifier != QUANTIFIER_TYPE_ANY) {
                return result;
            }
        }
        else { //  result is null
            hasInnerNull = true;
        }
    }

    // A NULL match along the way determines the result
    // for cases that never found a definitive result.
    if (hasInnerNull) {
        return NValue::getNullValue(ValueType::tBOOLEAN);
    }
    // Otherwise, return the unanimous result. false for ANY, true for ALL.
    return result;
}
}

