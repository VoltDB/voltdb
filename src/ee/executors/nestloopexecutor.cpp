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
#include <vector>
#include <string>
#include <stack>
#include "nestloopexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "executors/aggregateexecutor.h"
#include "execution/ProgressMonitorProxy.h"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tabletuplefilter.h"
#include "plannodes/nestloopnode.h"
#include "plannodes/limitnode.h"
#include "plannodes/aggregatenode.h"

#ifdef VOLT_DEBUG_ENABLED
#include <ctime>
#include <sys/times.h>
#include <unistd.h>
#endif

using namespace std;
using namespace voltdb;

static const char UNMATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE);
static const char MATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE + 1);

void NestLoopExecutor::CountingPostfilter::init(const AbstractExpression * postfilter, int limit, int offset) {
    m_postfilter = postfilter;
    m_limit = limit;
    m_offset = offset;
    m_tuple_skipped = 0;
    m_tuple_ctr = 0;
}

// Returns true if predicate evaluates to true and LIMIT/OFFSET conditions are satisfied.
bool NestLoopExecutor::CountingPostfilter::eval(const TableTuple& outer_tuple, const TableTuple& inner_tuple) {
    if (m_postfilter == NULL || m_postfilter->eval(&outer_tuple, &inner_tuple).isTrue()) {
        // Check if we have to skip this tuple because of offset
        if (m_tuple_skipped < m_offset) {
            m_tuple_skipped++;
            return false;
        }
        ++m_tuple_ctr;
        return true;
    }
    return false;
}

void NestLoopExecutor::outputTuple(TableTuple& join_tuple, ProgressMonitorProxy& pmp) {
    if (m_aggExec != NULL && m_aggExec->p_execute_tuple(join_tuple)) {
        m_postfilter.setAboveLimit();
    }
    m_tmpOutputTable->insertTempTuple(join_tuple);
    pmp.countdownProgress();
}

bool NestLoopExecutor::p_init(AbstractPlanNode* abstract_node,
                              TempTableLimits* limits)
{
    VOLT_TRACE("init NestLoop Executor");

    NestLoopPlanNode* node = dynamic_cast<NestLoopPlanNode*>(abstract_node);
    assert(node);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    assert(m_tmpOutputTable);

    // NULL tuple for outer join
    JoinType joinType = node->getJoinType();
    if (joinType != JOIN_TYPE_INNER) {
        Table* inner_table = node->getInputTable(1);
        assert(inner_table);
        m_null_inner_tuple.init(inner_table->schema());
        if (joinType == JOIN_TYPE_FULL) {
            Table* outer_table = node->getInputTable();
            assert(outer_table);
            m_null_outer_tuple.init(outer_table->schema());
        }
    }

    // Inline aggregation can be serial, partial or hash
    m_aggExec = voltdb::getInlineAggregateExecutor(m_abstractNode);

    return true;
}


bool NestLoopExecutor::p_execute(const NValueArray &params) {
    VOLT_DEBUG("executing NestLoop...");

    NestLoopPlanNode* node = dynamic_cast<NestLoopPlanNode*>(m_abstractNode);
    assert(node);
    assert(node->getInputTableCount() == 2);

    // output table must be a temp table
    assert(m_tmpOutputTable);

    Table* outer_table = node->getInputTable();
    assert(outer_table);

    Table* inner_table = node->getInputTable(1);
    assert(inner_table);

    VOLT_TRACE ("input table left:\n %s", outer_table->debug().c_str());
    VOLT_TRACE ("input table right:\n %s", inner_table->debug().c_str());

    //
    // Pre Join Expression
    //
    AbstractExpression *preJoinPredicate = node->getPreJoinPredicate();
    if (preJoinPredicate) {
        VOLT_TRACE ("Pre Join predicate: %s", preJoinPredicate == NULL ?
                    "NULL" : preJoinPredicate->debug(true).c_str());
    }
    //
    // Join Expression
    //
    AbstractExpression *joinPredicate = node->getJoinPredicate();
    if (joinPredicate) {
        VOLT_TRACE ("Join predicate: %s", joinPredicate == NULL ?
                    "NULL" : joinPredicate->debug(true).c_str());
    }
    //
    // Where Expression
    //
    AbstractExpression *wherePredicate = node->getWherePredicate();
    if (wherePredicate) {
        VOLT_TRACE ("Where predicate: %s", wherePredicate == NULL ?
                    "NULL" : wherePredicate->debug(true).c_str());
    }

    // Join type
    JoinType join_type = node->getJoinType();
    assert(join_type == JOIN_TYPE_INNER || join_type == JOIN_TYPE_LEFT || join_type == JOIN_TYPE_FULL);

    // The table filter to keep track of inner tuples that don't match any of outer tuples for FULL joins
    TableTupleFilter innerTableFilter;
    if (join_type == JOIN_TYPE_FULL) {
        // Prepopulate the view with all inner tuples
        innerTableFilter.init(inner_table);
    }

    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    int limit = -1;
    int offset = -1;
    if (limit_node) {
        limit_node->getLimitAndOffsetByReference(params, limit, offset);
    }

    int outer_cols = outer_table->columnCount();
    int inner_cols = inner_table->columnCount();
    TableTuple outer_tuple(node->getInputTable(0)->schema());
    TableTuple inner_tuple(node->getInputTable(1)->schema());
    const TableTuple& null_inner_tuple = m_null_inner_tuple.tuple();

    TableIterator iterator0 = outer_table->iteratorDeletingAsWeGo();
    ProgressMonitorProxy pmp(m_engine, this, inner_table);
    m_postfilter.init(wherePredicate, limit, offset);

    TableTuple join_tuple;
    if (m_aggExec != NULL) {
        VOLT_TRACE("Init inline aggregate...");
        const TupleSchema * aggInputSchema = node->getTupleSchemaPreAgg();
        join_tuple = m_aggExec->p_execute_init(params, &pmp, aggInputSchema, m_tmpOutputTable);
    } else {
        join_tuple = m_tmpOutputTable->tempTuple();
    }

    while (m_postfilter.isUnderLimit() && iterator0.next(outer_tuple)) {
        pmp.countdownProgress();

        // populate output table's temp tuple with outer table's values
        // probably have to do this at least once - avoid doing it many
        // times per outer tuple
        join_tuple.setNValues(0, outer_tuple, 0, outer_cols);

        // did this loop body find at least one match for this tuple?
        bool outerMatch = false;
        // For outer joins if outer tuple fails pre-join predicate
        // (join expression based on the outer table only)
        // it can't match any of inner tuples
        if (preJoinPredicate == NULL || preJoinPredicate->eval(&outer_tuple, NULL).isTrue()) {

            // By default, the delete as we go flag is false.
            TableIterator iterator1 = inner_table->iterator();
            while (m_postfilter.isUnderLimit() && iterator1.next(inner_tuple)) {
                pmp.countdownProgress();
                // Apply join filter to produce matches for each outer that has them,
                // then pad unmatched outers, then filter them all
                if (joinPredicate == NULL || joinPredicate->eval(&outer_tuple, &inner_tuple).isTrue()) {
                    outerMatch = true;
                    // The inner tuple passed the join predicate
                    if (join_type == JOIN_TYPE_FULL) {
                        // Mark it as matched
                        innerTableFilter.updateTuple(inner_tuple, MATCHED_TUPLE);
                    }
                    // Filter the joined tuple
                    if (m_postfilter.eval(outer_tuple, inner_tuple)) {
                        // Matched! Complete the joined tuple with the inner column values.
                        join_tuple.setNValues(outer_cols, inner_tuple, 0, inner_cols);
                        outputTuple(join_tuple, pmp);
                    }
                }
            } // END INNER WHILE LOOP
        } // END IF PRE JOIN CONDITION

        //
        // Left Outer Join
        //
        if (join_type != JOIN_TYPE_INNER && !outerMatch && m_postfilter.isUnderLimit()) {
            // Still needs to pass the filter
            if (m_postfilter.eval(outer_tuple, null_inner_tuple)) {
                // Matched! Complete the joined tuple with the inner column values.
                join_tuple.setNValues(outer_cols, null_inner_tuple, 0, inner_cols);
                outputTuple(join_tuple, pmp);
            }
        } // END IF LEFT OUTER JOIN
    } // END OUTER WHILE LOOP

    //
    // FULL Outer Join. Iterate over the unmatched inner tuples
    //
    if (join_type == JOIN_TYPE_FULL && m_postfilter.isUnderLimit()) {
        // Preset outer columns to null
        const TableTuple& null_outer_tuple = m_null_outer_tuple.tuple();
        join_tuple.setNValues(0, null_outer_tuple, 0, outer_cols);

        TableTupleFilter_iterator endItr = innerTableFilter.end(UNMATCHED_TUPLE);
        for (TableTupleFilter_iterator itr = innerTableFilter.begin(UNMATCHED_TUPLE);
                itr != endItr && m_postfilter.isUnderLimit(); ++itr) {
            // Restore the tuple value
            uint64_t tupleAddr = innerTableFilter.getTupleAddress(*itr);
            inner_tuple.move((char *)tupleAddr);
            // Still needs to pass the filter
            assert(inner_tuple.isActive());
            if (m_postfilter.eval(null_outer_tuple, inner_tuple)) {
                // Passed! Complete the joined tuple with the inner column values.
                join_tuple.setNValues(outer_cols, inner_tuple, 0, inner_cols);
                outputTuple(join_tuple, pmp);
            }
        }
   }

    if (m_aggExec != NULL) {
        m_aggExec->p_execute_finish();
    }

    cleanupInputTempTable(inner_table);
    cleanupInputTempTable(outer_table);

    return (true);
}
