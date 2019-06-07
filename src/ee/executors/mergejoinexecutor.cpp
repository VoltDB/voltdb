/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
#include "mergejoinexecutor.h"

#include "indexscanexecutor.h"
#include "executors/aggregateexecutor.h"
#include "indexes/tableindex.h"
#include "plannodes/indexscannode.h"
#include "plannodes/limitnode.h"
#include "plannodes/mergejoinnode.h"
#include "storage/persistenttable.h"
#include "storage/tableiterator.h"
#include "storage/tabletuplefilter.h"


using namespace std;
using namespace voltdb;

const static int8_t UNMATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE);
const static int8_t MATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE + 1);

// This is a helper function to get the "next tuple" during an index scan, called by p_execute
bool getNextTuple(TableTuple* tuple, TableIndex* index, IndexCursor* cursor) {
        *tuple = index->nextValue(*cursor);
        return ! tuple->isNullTuple();
    }

bool MergeJoinExecutor::p_init(AbstractPlanNode* abstractNode, const ExecutorVector& executorVector) {
    VOLT_TRACE("init MergeJoin Executor");

    MergeJoinPlanNode* node = dynamic_cast<MergeJoinPlanNode*>(m_abstractNode);
    assert(node);

    // Init parent first
    if (!AbstractJoinExecutor::p_init(abstractNode, executorVector)) {
        return false;
    }

    return true;
}

bool MergeJoinExecutor::p_execute(const NValueArray &params) {
    VOLT_DEBUG("executing MergeJoin...");

    MergeJoinPlanNode* node = dynamic_cast<MergeJoinPlanNode*>(m_abstractNode);
    assert(node);
    assert(node->getInputTableCount() == 1);

    // output table must be a temp table
    assert(m_tmpOutputTable);

    // Outer table
    Table* outerTable = node->getInputTable();
    assert(outerTable);

    // inner table is a persistent table
    IndexScanPlanNode* innerIndexNode =
        dynamic_cast<IndexScanPlanNode*>(m_abstractNode->getInlinePlanNode(PLAN_NODE_TYPE_INDEXSCAN));
        assert(innerIndexNode);
        VOLT_TRACE("<MergeJoinPlanNode> %s, <IndexScanPlanNode> %s",
               m_abstractNode->debug().c_str(), innerIndexNode->debug().c_str());

    assert(dynamic_cast<PersistentTable*>(innerIndexNode->getTargetTable()));
    PersistentTable* innerTable = static_cast<PersistentTable*>(innerIndexNode->getTargetTable());
    assert(innerTable);
    TableIndex *innerTableIndex = innerTable->index(innerIndexNode->getTargetIndexName());
    IndexCursor innerIndexCursor(innerTableIndex->getTupleSchema());

    // NULL tuples for left and full joins
    p_init_null_tuples(node->getInputTable(), innerIndexNode->getTargetTable());

    VOLT_TRACE ("input table left:\n %s", outerTable->debug().c_str());
    VOLT_TRACE ("input table right:\n %s", innerTable->debug().c_str());

    //
    // Pre Join Expression
    //
    AbstractExpression *preJoinPredicate = node->getPreJoinPredicate();
    if (preJoinPredicate) {
        VOLT_TRACE ("Pre Join predicate: %s", preJoinPredicate == NULL ?
                    "NULL" : preJoinPredicate->debug(true).c_str());
    }
    //
    // Equivalence Expression
    //
    AbstractExpression *equalJoinPredicate = node->getJoinPredicate();
    assert(equalJoinPredicate != nullptr);
    VOLT_TRACE ("Equality predicate: %s", equalJoinPredicate->debug(true).c_str());
    //
    // Less Expression
    //
    AbstractExpression *lessJoinPredicate = node->getLessJoinPredicate();
    assert(lessJoinPredicate !=nullptr);
    VOLT_TRACE ("Less predicate: %s", lessJoinPredicate->debug(true).c_str());
    //
    // Where Expression
    //
    AbstractExpression *wherePredicate = node->getWherePredicate();
    VOLT_TRACE ("Where predicate: %s", wherePredicate == NULL ? "NULL" : wherePredicate->debug(true).c_str());

    // The table filter to keep track of inner tuples that don't match any of outer tuples for FULL joins
    TableTupleFilter innerTableFilter;
    if (m_joinType == JOIN_TYPE_FULL) {
        // Prepopulate the view with all inner tuples
        innerTableFilter.init(innerTable);
    }

    LimitPlanNode* limitNode = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (limitNode) {
        limitNode->getLimitAndOffsetByReference(params, limit, offset);
    }

    int const outerCols = outerTable->columnCount();
    int const innerCols = innerTable->columnCount();
    TableTuple outerTuple(outerTable->schema());
    TableTuple innerTuple(innerTable->schema());
    const TableTuple& nullInnerTuple = m_null_inner_tuple.tuple();

    TableIterator outerIterator = outerTable->iteratorDeletingAsWeGo();
    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
    // Init the postfilter
    CountingPostfilter postfilter(m_tmpOutputTable, wherePredicate, limit, offset);

    TableTuple joinTuple;
    if (m_aggExec != NULL) {
        VOLT_TRACE("Init inline aggregate...");
        const TupleSchema * aggInputSchema = node->getTupleSchemaPreAgg();
        joinTuple = m_aggExec->p_execute_init(params, &pmp, aggInputSchema, m_tmpOutputTable, &postfilter);
    } else {
        joinTuple = m_tmpOutputTable->tempTuple();
    }

    // Move both iterators to the first rows if possible
    bool hasOuter = outerIterator.next(outerTuple);
    innerTableIndex->moveToEnd(true, innerIndexCursor);
    bool hasInner = getNextTuple(&innerTuple, innerTableIndex, &innerIndexCursor);
    if (hasOuter && hasInner) { // Both tables have at least one row
        while (postfilter.isUnderLimit() && hasOuter && hasInner) {
            pmp.countdownProgress();

            // populate output table's temp tuple with outer table's values
            // probably have to do this at least once - avoid doing it many
            // times per outer tuple
            joinTuple.setNValues(0, outerTuple, 0, outerCols);

            // did this loop body find at least one match for this tuple?
            bool outerMatch = false;
            // For outer joins if outer tuple fails pre-join predicate
            // (join expression based on the outer table only)
            // it can't match any of inner tuples
            if (preJoinPredicate == NULL || preJoinPredicate->eval(&outerTuple, NULL).isTrue()) {
                if (equalJoinPredicate->eval(&outerTuple, &innerTuple).isTrue()) {
                    outerMatch = true;
                    // The inner tuple passed the join predicate
                    if (m_joinType == JOIN_TYPE_FULL) {
                        // Mark it as matched
                        innerTableFilter.updateTuple(innerTuple, MATCHED_TUPLE);
                    }
                    // Filter the joined tuple
                    if (postfilter.eval(&outerTuple, &innerTuple)) {
                        // Matched! Complete the joined tuple with the inner column values.
                        joinTuple.setNValues(outerCols, innerTuple, 0, innerCols);
                        outputTuple(postfilter, joinTuple, pmp);
                    }

                    // Output further tuples that match the outerTuple
                    IndexCursor innerIndexCursorTmp = innerIndexCursor;
                    TableTuple innerTupleTmp = innerTuple;
                    while (postfilter.isUnderLimit()
                        && getNextTuple(&innerTupleTmp, innerTableIndex, &innerIndexCursorTmp)
                        && equalJoinPredicate->eval(&outerTuple, &innerTupleTmp).isTrue()) {
                        pmp.countdownProgress();
                        if (m_joinType == JOIN_TYPE_FULL) {
                            // Mark it as matched
                            innerTableFilter.updateTuple(innerTupleTmp, MATCHED_TUPLE);
                        }
                        // Filter the joined tuple
                        if (postfilter.eval(&outerTuple, &innerTupleTmp)) {
                            // Matched! Complete the joined tuple with the inner column values.
                            joinTuple.setNValues(outerCols, innerTupleTmp, 0, innerCols);
                            outputTuple(postfilter, joinTuple, pmp);
                        }
                    }

                    // Output further tuples that match the innerTuple
                    TableIterator outerIteratorTmp = outerIterator;
                    TableTuple outerTupleTmp = outerTuple;
                    joinTuple.setNValues(outerCols, innerTuple, 0, innerCols);
                    while (postfilter.isUnderLimit() &&
                        outerIteratorTmp.next(outerTupleTmp) &&
                        equalJoinPredicate->eval(&outerTupleTmp, &innerTuple).isTrue()) {
                        pmp.countdownProgress();
                        if (m_joinType == JOIN_TYPE_FULL) {
                            // Mark it as matched
                            innerTableFilter.updateTuple(innerTuple, MATCHED_TUPLE);
                        }
                        // Filter the joined tuple
                        if (postfilter.eval(&outerTupleTmp, &innerTuple)) {
                            // Matched! Complete the joined tuple with the inner column values.
                            joinTuple.setNValues(0, outerTupleTmp, 0, outerCols);
                            outputTuple(postfilter, joinTuple, pmp);
                        }
                    }

                    // Advance both iterators
                    hasOuter = outerIterator.next(outerTuple);
                    hasInner = getNextTuple(&innerTuple, innerTableIndex, &innerIndexCursor);
                } else if (lessJoinPredicate->eval(&outerTuple, &innerTuple).isTrue()) {
                    // Advance outer
                    hasOuter = outerIterator.next(outerTuple);
                } else {
                    // Advance inner
                    hasInner = getNextTuple(&innerTuple, innerTableIndex, &innerIndexCursor);
                }
            } // END IF PRE JOIN CONDITION
            else {
                // Advance outer
                hasOuter = outerIterator.next(outerTuple);
            }

            //
            // Left Outer Join
            //
            if (m_joinType != JOIN_TYPE_INNER && !outerMatch && postfilter.isUnderLimit()) {
                // Still needs to pass the filter
                if (postfilter.eval(&outerTuple, &nullInnerTuple)) {
                    // Matched! Complete the joined tuple with the inner column values.
                    joinTuple.setNValues(outerCols, nullInnerTuple, 0, innerCols);
                    outputTuple(postfilter, joinTuple, pmp);
                }
            } // END IF LEFT OUTER JOIN
        } // END WHILE LOOP

        //
        // FULL Outer Join. Iterate over the unmatched inner tuples
        //
        if (m_joinType == JOIN_TYPE_FULL && postfilter.isUnderLimit()) {
            // Preset outer columns to null
            const TableTuple& nullOuterTuple = m_null_outer_tuple.tuple();
            joinTuple.setNValues(0, nullOuterTuple, 0, outerCols);

            TableTupleFilter_iter<UNMATCHED_TUPLE> endItr = innerTableFilter.end<UNMATCHED_TUPLE>();
            for (TableTupleFilter_iter<UNMATCHED_TUPLE> itr = innerTableFilter.begin<UNMATCHED_TUPLE>();
                    itr != endItr && postfilter.isUnderLimit(); ++itr) {
                // Restore the tuple value
                uint64_t tupleAddr = innerTableFilter.getTupleAddress(*itr);
                innerTuple.move((char *)tupleAddr);
                // Still needs to pass the filter
                assert(innerTuple.isActive());
                if (postfilter.eval(&nullOuterTuple, &innerTuple)) {
                    // Passed! Complete the joined tuple with the inner column values.
                    joinTuple.setNValues(outerCols, innerTuple, 0, innerCols);
                    outputTuple(postfilter, joinTuple, pmp);
                }
            }
        }
    } else if (hasOuter && m_joinType != JOIN_TYPE_INNER) {
        // This is an outer join but the inner table is empty
        // Add all outer tuples that pass the filter
        while (postfilter.isUnderLimit() && outerIterator.next(outerTuple)) {
            pmp.countdownProgress();
            if (postfilter.eval(&outerTuple, &nullInnerTuple)) {
                // Matched! Complete the joined tuple with the inner column values.
                joinTuple.setNValues(outerCols, nullInnerTuple, 0, innerCols);
                outputTuple(postfilter, joinTuple, pmp);
            }
        }
    } else if (hasInner && m_joinType == JOIN_TYPE_FULL) {
        // This is a full join and the outer table is empty
        // Add all inner tuples that pass the filter
        const TableTuple& nullOuterTuple = m_null_outer_tuple.tuple();
        joinTuple.setNValues(0, nullOuterTuple, 0, outerCols);
        while (postfilter.isUnderLimit() && hasInner) {
            pmp.countdownProgress();
            if (postfilter.eval(&nullOuterTuple, &innerTuple)) {
                // Passed! Complete the joined tuple with the inner column values.
                joinTuple.setNValues(outerCols, innerTuple, 0, innerCols);
                outputTuple(postfilter, joinTuple, pmp);
            }
            hasInner = getNextTuple(&innerTuple, innerTableIndex, &innerIndexCursor);
        }

    }

    if (m_aggExec != NULL) {
        m_aggExec->p_execute_finish();
    }

    return (true);
}
