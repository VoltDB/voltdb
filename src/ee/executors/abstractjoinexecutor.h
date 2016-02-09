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

#ifndef HSTOREABSTRACTJOINEXECUTOR_H
#define HSTOREABSTRACTJOINEXECUTOR_H

#include "common/common.h"
#include "common/tabletuple.h"
#include "executors/abstractexecutor.h"

namespace voltdb {

class AbstractExpression;
class AbstractPlanNode;
class AggregateExecutorBase;
class ProgressMonitorProxy;
class TableTuple;
class TempTableLimits;
class VoltDBEngine;

/**
 *  Abstract base class for all join executors
 */
class AbstractJoinExecutor : public AbstractExecutor {
    protected:
        // Constructor
        AbstractJoinExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node) :
            AbstractExecutor(engine, abstract_node) { }

        bool p_init(AbstractPlanNode*, TempTableLimits* limits);

        void p_init_null_tuples(Table* inner_table, Table* outer_table);

        // Helper struct to evaluate a postfilter and count the number of tuples that
        // successfully passed the evaluation
        struct CountingPostfilter {
            void init(const AbstractExpression * wherePredicate, int limit, int offset);

            // Returns true is LIMIT is not reached yet
            bool isUnderLimit() const {
                return m_limit == -1 || m_tuple_ctr < m_limit;
            }

            void setAboveLimit() {
                assert (m_limit != -1);
                m_tuple_ctr = m_limit;
            }

            // Returns true if predicate evaluates to true and LIMIT/OFFSET conditions are satisfied.
            bool eval(const TableTuple& outer_tuple, const TableTuple& inner_tuple);

            private:
            const AbstractExpression *m_postfilter;
            int m_limit;
            int m_offset;

            int m_tuple_skipped;
            int m_tuple_ctr;
        };

        // Write tuple to the output table
        void outputTuple(TableTuple& join_tuple, ProgressMonitorProxy& pmp);

        JoinType m_joinType;

        StandAloneTupleStorage m_null_outer_tuple;
        StandAloneTupleStorage m_null_inner_tuple;

        AggregateExecutorBase* m_aggExec;
        CountingPostfilter m_postfilter;
};

}

#endif
