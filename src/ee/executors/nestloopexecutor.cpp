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
#include "nestloopexecutor.h"

#include "executors/aggregateexecutor.h"
#include "storage/tableiterator.h"
#include "storage/tabletuplefilter.h"
#include "plannodes/nestloopnode.h"
#include "plannodes/limitnode.h"

#ifdef VOLT_DEBUG_ENABLED
#include <ctime>
#include <sys/times.h>
#include <unistd.h>
#endif

using namespace std;
using namespace voltdb;

const static int8_t UNMATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE);
const static int8_t MATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE + 1);

bool NestLoopExecutor::p_init(
        AbstractPlanNode* abstractNode, const ExecutorVector& executorVector) {
    VOLT_TRACE("init NLJ Executor");

    NestLoopPlanNode* node = dynamic_cast<NestLoopPlanNode*>(m_abstractNode);
    vassert(node);

    // Init parent first
    if (!AbstractJoinExecutor::p_init(abstractNode, executorVector)) {
        return false;
    }

    // NULL tuples for left and full joins
    p_init_null_tuples(node->getInputTable(), node->getInputTable(1));

    return true;
}

bool NestLoopExecutor::p_execute(const NValueArray &params) {
    VOLT_DEBUG("executing NestLoop...");

    NestLoopPlanNode* node = dynamic_cast<NestLoopPlanNode*>(m_abstractNode);
    vassert(node);
    vassert(node->getInputTableCount() == 2);

    // output table must be a temp table
    vassert(m_tmpOutputTable);

    Table* outer_table = node->getInputTable();
    vassert(outer_table);

    Table* inner_table = node->getInputTable(1);
    vassert(inner_table);

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

    // The table filter to keep track of inner tuples that don't match any of outer tuples for FULL joins
    TableTupleFilter innerTableFilter;
    if (m_joinType == JOIN_TYPE_FULL) {
        // Prepopulate the view with all inner tuples
        innerTableFilter.init(inner_table);
    }

    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PlanNodeType::Limit));
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (limit_node) {
        tie(limit, offset) = limit_node->getLimitAndOffset(params);
    }

    int outer_cols = outer_table->columnCount();
    int inner_cols = inner_table->columnCount();
    TableTuple outer_tuple(node->getInputTable(0)->schema());
    TableTuple inner_tuple(node->getInputTable(1)->schema());
    const TableTuple& null_inner_tuple = m_null_inner_tuple.tuple();

    TableIterator iterator0 = outer_table->iteratorDeletingAsWeGo();
    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
    // Init the postfilter
    CountingPostfilter postfilter(m_tmpOutputTable, wherePredicate, limit, offset);

    TableTuple join_tuple;
    if (m_aggExec != NULL) {
        VOLT_TRACE("Init inline aggregate...");
        const TupleSchema * aggInputSchema = node->getTupleSchemaPreAgg();
        join_tuple = m_aggExec->p_execute_init(params, &pmp, aggInputSchema, m_tmpOutputTable, &postfilter);
    } else {
        join_tuple = m_tmpOutputTable->tempTuple();
    }

    while (postfilter.isUnderLimit() && iterator0.next(outer_tuple)) {
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
            while (postfilter.isUnderLimit() && iterator1.next(inner_tuple)) {
                pmp.countdownProgress();
                // Apply join filter to produce matches for each outer that has them,
                // then pad unmatched outers, then filter them all
                if (joinPredicate == NULL || joinPredicate->eval(&outer_tuple, &inner_tuple).isTrue()) {
                    outerMatch = true;
                    // The inner tuple passed the join predicate
                    if (m_joinType == JOIN_TYPE_FULL) {
                        // Mark it as matched
                        innerTableFilter.updateTuple(inner_tuple, MATCHED_TUPLE);
                    }
                    // Filter the joined tuple
                    if (postfilter.eval(&outer_tuple, &inner_tuple)) {
                        // Matched! Complete the joined tuple with the inner column values.
                        join_tuple.setNValues(outer_cols, inner_tuple, 0, inner_cols);
                        outputTuple(postfilter, join_tuple, pmp);
                    }
                }
            } // END INNER WHILE LOOP
        } // END IF PRE JOIN CONDITION

        //
        // Left Outer Join
        //
        if (m_joinType != JOIN_TYPE_INNER && !outerMatch && postfilter.isUnderLimit()) {
            // Still needs to pass the filter
            if (postfilter.eval(&outer_tuple, &null_inner_tuple)) {
                // Matched! Complete the joined tuple with the inner column values.
                join_tuple.setNValues(outer_cols, null_inner_tuple, 0, inner_cols);
                outputTuple(postfilter, join_tuple, pmp);
            }
        } // END IF LEFT OUTER JOIN
    } // END OUTER WHILE LOOP

    //
    // FULL Outer Join. Iterate over the unmatched inner tuples
    //
    if (m_joinType == JOIN_TYPE_FULL && postfilter.isUnderLimit()) {
        // Preset outer columns to null
        const TableTuple& null_outer_tuple = m_null_outer_tuple.tuple();
        join_tuple.setNValues(0, null_outer_tuple, 0, outer_cols);

        TableTupleFilter_iter<UNMATCHED_TUPLE> endItr = innerTableFilter.end<UNMATCHED_TUPLE>();
        for (TableTupleFilter_iter<UNMATCHED_TUPLE> itr = innerTableFilter.begin<UNMATCHED_TUPLE>();
                itr != endItr && postfilter.isUnderLimit(); ++itr) {
            // Restore the tuple value
            uint64_t tupleAddr = innerTableFilter.getTupleAddress(*itr);
            inner_tuple.move((char *)tupleAddr);
            // Still needs to pass the filter
            vassert(inner_tuple.isActive());
            if (postfilter.eval(&null_outer_tuple, &inner_tuple)) {
                // Passed! Complete the joined tuple with the inner column values.
                join_tuple.setNValues(outer_cols, inner_tuple, 0, inner_cols);
                outputTuple(postfilter, join_tuple, pmp);
            }
        }
   }

    if (m_aggExec != NULL) {
        m_aggExec->p_execute_finish();
    }

    return (true);
}
