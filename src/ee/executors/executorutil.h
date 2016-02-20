/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef HSTOREEXECUTORUTIL_H
#define HSTOREEXECUTORUTIL_H

#include <cassert>

namespace voltdb {

class AbstractExpression;
class TableTuple;

// Helper struct to evaluate a postfilter and count the number of tuples that
// successfully passed the evaluation
struct CountingPostfilter {
    static const int NO_LIMIT = -1;
    static const int NO_OFFSET = -1;
    static const int LIMIT = NO_LIMIT + 1;

    CountingPostfilter(const AbstractExpression * wherePredicate, int limit, int offset);

    // Returns true is LIMIT is not reached yet
    bool isUnderLimit() const {
        return m_limit == NO_LIMIT || m_tuple_ctr < m_limit;
    }

    // Returns true if predicate evaluates to true and LIMIT/OFFSET conditions are satisfied.
    bool eval(const TableTuple* outer_tuple, const TableTuple* inner_tuple);

    private:
    friend struct AggCountingPostfilter;

    // Indicate that an inline (child) AggCountingPostfilter associated with this postfilter
    // has reached its limit
    void setAboveLimit() {
        m_tuple_ctr = LIMIT;
        m_limit = LIMIT;
        assert(m_tuple_ctr == m_limit && m_limit != NO_LIMIT);
    }

    const AbstractExpression *m_postfilter;
    int m_limit;
    int m_offset;

    int m_tuple_skipped;
    int m_tuple_ctr;
};

// Helper struct to evaluate an aggregate executor postfilter. If there is a limit,
// it's reached when the number of tuples inserted into aggregator's output table equals the LIMIT
class TempTable;

struct AggCountingPostfilter {

    AggCountingPostfilter();

    // table - Aggregate executor's output table
    // parentPostfilter - If the Aggregate executor is inlined, this is a pointer to a parent's node postfilter
    // that needs to be notified when the aggregator's limit is reached
    AggCountingPostfilter(const TempTable* table, const AbstractExpression * wherePredicate, int limit, int offset, CountingPostfilter* parentPostfilter);

    // Returns true is LIMIT is not reached yet
    bool isUnderLimit() const {
        return m_under_limit;
    }

    // Returns true if predicate evaluates to true and LIMIT/OFFSET conditions are satisfied.
    bool eval(const TableTuple* tuple);

    private:
    const TempTable* m_table;
    const AbstractExpression *m_postfilter;
    CountingPostfilter* m_parentPostfilter;
    int m_limit;
    int m_offset;

    int m_tuple_skipped;
    bool m_under_limit;
};

}

#endif
