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

#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "storage/AbstractTempTable.hpp"

#include <cstddef> // for NULL !
#include <common/debuglog.h>

namespace voltdb {

// Helper struct to evaluate a postfilter and count the number of tuples that
// successfully passed the evaluation
class CountingPostfilter {
    const AbstractTempTable *m_table = nullptr;
    const AbstractExpression *m_postPredicate = nullptr;
    CountingPostfilter* m_parentPostfilter = nullptr;

    int m_limit = NO_LIMIT;
    int m_offset = NO_OFFSET;

    int m_tuple_skipped = 0;
    bool m_under_limit = false;

    // Indicate that an inline (child) AggCountingPostfilter associated with this postfilter
    // has reached its limit
    void setAboveLimit() {
        m_under_limit = false;
    }
public:
    static const int NO_LIMIT = -1;
    static const int NO_OFFSET = -1;

    // A CountingPostfilter is not fully initialized by its default constructor.
    // It should be re-initialized before use via assignment from a properly initialized CountingPostfilter.
    CountingPostfilter() = default;

    // Constructor to initialize a CountingPostfilter
    CountingPostfilter(const AbstractTempTable* table, const AbstractExpression * postPredicate, int limit, int offset,
        CountingPostfilter* parentPostfilter = NULL);

    // Returns true is LIMIT is not reached yet
    bool isUnderLimit() const {
        return m_under_limit;
    }

    // Returns true if predicate evaluates to true and LIMIT/OFFSET conditions are satisfied.
    bool eval(const TableTuple* outer_tuple, const TableTuple* inner_tuple);
};

inline bool CountingPostfilter::eval(const TableTuple* outer_tuple, const TableTuple* inner_tuple) {
    if (m_postPredicate == nullptr || m_postPredicate->eval(outer_tuple, inner_tuple).isTrue()) {
        // Check if we have to skip this tuple because of offset
        if (m_tuple_skipped < m_offset) {
            m_tuple_skipped++;
            return false;
        } else if (m_limit >= 0) { // Evaluate LIMIT now
            vassert(m_table != nullptr);
            if (m_table->activeTupleCount() == m_limit) {
                m_under_limit = false;
                // Notify a parent that the limit is reached
                if (m_parentPostfilter) {
                    m_parentPostfilter->setAboveLimit();
                }
                return false;
            }
        }
        // LIMIT/OFFSET are satisfied
        return true;
    } else { // Predicate is not NULL and was evaluated to FALSE
        return false;
    }
}

}

