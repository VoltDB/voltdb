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

#include "nestloopindexexecutor.h"

#include "execution/ProgressMonitorProxy.h"

#include "executors/aggregateexecutor.h"
#include "executors/indexscanexecutor.h"

#include "expressions/tuplevalueexpression.h"

#include "plannodes/nestloopindexnode.h"
#include "plannodes/indexscannode.h"
#include "plannodes/limitnode.h"
#include "plannodes/aggregatenode.h"

#include "storage/tabletuplefilter.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"

using namespace std;
using namespace voltdb;

const static int8_t UNMATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE);
const static int8_t MATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE + 1);

bool NestLoopIndexExecutor::p_init(
        AbstractPlanNode* abstractNode, const ExecutorVector& executorVector) {
    VOLT_TRACE("init NLIJ Executor");

    // Init parent first
    if (!AbstractJoinExecutor::p_init(abstractNode, executorVector)) {
        return false;
    }

    NestLoopIndexPlanNode* node = dynamic_cast<NestLoopIndexPlanNode*>(m_abstractNode);
    vassert(node);
    m_indexNode =
        dynamic_cast<IndexScanPlanNode*>(m_abstractNode->getInlinePlanNode(PlanNodeType::IndexScan));
    vassert(m_indexNode);
    VOLT_TRACE("<NestLoopIndexPlanNode> %s, <IndexScanPlanNode> %s",
               m_abstractNode->debug().c_str(), m_indexNode->debug().c_str());

    m_lookupType = m_indexNode->getLookupType();
    m_sortDirection = m_indexNode->getSortDirection();

    //
    // We need exactly one input table and a target table
    //
    vassert(node->getInputTableCount() == 1);

    node->getOutputColumnExpressions(m_outputExpressions);

    //
    // Make sure that we actually have search keys
    //
    int num_of_searchkeys = static_cast <int> (m_indexNode->getSearchKeyExpressions().size());
    //nshi commented this out in revision 4495 of the old repo in index scan executor
    VOLT_TRACE ("<Nested Loop Index exec, INIT...> Number of searchKeys: %d \n", num_of_searchkeys);

    for (int ctr = 0; ctr < num_of_searchkeys; ctr++) {
        if (m_indexNode->getSearchKeyExpressions()[ctr] == NULL) {
            VOLT_ERROR("The search key expression at position '%d' is NULL for"
                       " internal PlanNode '%s' of PlanNode '%s'",
                       ctr, m_indexNode->debug().c_str(), node->debug().c_str());
            return false;
        }
    }

    vassert(node->getInputTable());

    PersistentTable* inner_table = dynamic_cast<PersistentTable*>(m_indexNode->getTargetTable());
    vassert(inner_table);

    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    TableIndex* index = inner_table->index(m_indexNode->getTargetIndexName());
    if (index == NULL) {
        VOLT_ERROR("Failed to retreive index '%s' from inner table '%s' for"
                   " internal PlanNode '%s'",
                   m_indexNode->getTargetIndexName().c_str(),
                   inner_table->name().c_str(), m_indexNode->debug().c_str());
        return false;
    } else {
        // NULL tuples for left and full joins
        p_init_null_tuples(node->getInputTable(), m_indexNode->getTargetTable());

        m_indexValues.init(index->getKeySchema());
        return true;
    }
}

bool NestLoopIndexExecutor::p_execute(const NValueArray &params) {
    vassert(dynamic_cast<NestLoopIndexPlanNode*>(m_abstractNode));
    NestLoopIndexPlanNode* node = static_cast<NestLoopIndexPlanNode*>(m_abstractNode);

    // output table must be a temp table
    vassert(m_tmpOutputTable);
    // target table is a persistent table
    vassert(dynamic_cast<PersistentTable*>(m_indexNode->getTargetTable()));
    PersistentTable* inner_table = static_cast<PersistentTable*>(m_indexNode->getTargetTable());

    TableIndex* index = inner_table->index(m_indexNode->getTargetIndexName());
    vassert(index);
    IndexCursor indexCursor(index->getTupleSchema());

    //outer_table is the input table that have tuples to be iterated
    vassert(node->getInputTableCount() == 1);
    Table* outer_table = node->getInputTable();
    vassert(outer_table);
    VOLT_TRACE("executing NestLoopIndex with outer table: %s, inner table: %s",
               outer_table->debug("").c_str(), inner_table->debug("").c_str());

    //
    // Substitute parameter to SEARCH KEY Note that the expressions
    // will include TupleValueExpression even after this substitution
    //
    int num_of_searchkeys = static_cast <int> (m_indexNode->getSearchKeyExpressions().size());
    for (int ctr = 0; ctr < num_of_searchkeys; ctr++) {
        VOLT_TRACE("Search Key[%d]:\n%s",
                   ctr, m_indexNode->getSearchKeyExpressions()[ctr]->debug(true).c_str());
    }

    // end expression
    AbstractExpression* end_expression = m_indexNode->getEndExpression();
    if (end_expression) {
        VOLT_TRACE("End Expression:\n%s", end_expression->debug(true).c_str());
    }

    // post expression
    AbstractExpression* post_expression = m_indexNode->getPredicate();
    if (post_expression != NULL) {
        VOLT_TRACE("Post Expression:\n%s", post_expression->debug(true).c_str());
    }

    // initial expression
    AbstractExpression* initial_expression = m_indexNode->getInitialExpression();
    if (initial_expression != NULL) {
        VOLT_TRACE("Initial Expression:\n%s", initial_expression->debug(true).c_str());
    }

    // SKIP NULL EXPRESSION
    AbstractExpression* skipNullExpr = m_indexNode->getSkipNullPredicate();
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (skipNullExpr != NULL) {
        VOLT_DEBUG("Skip NULL Expression:\n%s", skipNullExpr->debug(true).c_str());
    }

    // pre join expression
    AbstractExpression* prejoin_expression = node->getPreJoinPredicate();
    if (prejoin_expression != NULL) {
        VOLT_TRACE("Prejoin Expression:\n%s", prejoin_expression->debug(true).c_str());
    }

    // where expression
    AbstractExpression* where_expression = node->getWherePredicate();
    if (where_expression != NULL) {
        VOLT_TRACE("Where Expression:\n%s", where_expression->debug(true).c_str());
    }

    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PlanNodeType::Limit));
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (limit_node) {
        tie(limit, offset) = limit_node->getLimitAndOffset(params);
    }
    // Init the postfilter
    CountingPostfilter postfilter(m_tmpOutputTable, where_expression, limit, offset);

    //
    // OUTER TABLE ITERATION
    //
    TableTuple outer_tuple(outer_table->schema());
    TableTuple inner_tuple(inner_table->schema());
    TableIterator outer_iterator = outer_table->iteratorDeletingAsWeGo();
    int num_of_outer_cols = outer_table->columnCount();
    vassert(outer_tuple.columnCount() == outer_table->columnCount());
    vassert(inner_tuple.columnCount() == inner_table->columnCount());
    const TableTuple &null_inner_tuple = m_null_inner_tuple.tuple();
    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);

    // The table filter to keep track of inner tuples that don't match any of outer tuples for FULL joins
    TableTupleFilter innerTableFilter;
    if (m_joinType == JOIN_TYPE_FULL) {
        // Prepopulate the set with all inner tuples
        innerTableFilter.init(inner_table);
    }

    TableTuple join_tuple;
    // It's not immediately obvious here, so there's some subtlety to
    // note with respect to the schema of the join_tuple.
    //
    // The inner_tuple is used to represent the values from the inner
    // table in the case of the join predicate passing, and for left
    // outer joins, the null_tuple is used if there is no match.  Both
    // of these tuples include the complete schema of the table being
    // scanned.  The inner table is being scanned via an inlined scan
    // node, so there is no temp table corresponding to it.
    //
    // Predicates that are evaluated against the inner table should
    // therefore use the complete schema of the table being scanned.
    //
    // The join_tuple is the tuple that contains the values that we
    // actually want to put in the output of the join (or to aggregate
    // if there is an inlined agg plan node).  This tuple needs to
    // omit the unused columns from the inner table.  The inlined
    // index scan itself has an inlined project node that defines the
    // columns that should be output by the join, and omits those that
    // are not needed.  So the join_tuple contains the columns we're
    // using from the outer table, followed by the "projected" schema
    // for the inlined scan of the inner table.
    if (m_aggExec != NULL) {
        VOLT_TRACE("Init inline aggregate...");
        const TupleSchema * aggInputSchema = node->getTupleSchemaPreAgg();
        join_tuple = m_aggExec->p_execute_init(params, &pmp, aggInputSchema, m_tmpOutputTable, &postfilter);
    } else {
        join_tuple = m_tmpOutputTable->tempTuple();
    }

    VOLT_TRACE("<num_of_outer_cols>: %d\n", num_of_outer_cols);
    while (postfilter.isUnderLimit() && outer_iterator.next(outer_tuple)) {
        VOLT_TRACE("outer_tuple:%s",
                   outer_tuple.debug(outer_table->name()).c_str());
        pmp.countdownProgress();

        // Set the join tuple columns that originate solely from the outer tuple.
        // Must be outside the inner loop in case of the empty inner table.
        join_tuple.setNValues(0, outer_tuple, 0, num_of_outer_cols);

        // did this loop body find at least one match for this tuple?
        bool outerMatch = false;
        // For outer joins if outer tuple fails pre-join predicate
        // (join expression based on the outer table only)
        // it can't match any of inner tuples
        if (prejoin_expression == NULL || prejoin_expression->eval(&outer_tuple, NULL).isTrue()) {
            int activeNumOfSearchKeys = num_of_searchkeys;
            VOLT_TRACE ("<Nested Loop Index exec, WHILE-LOOP...> Number of searchKeys: %d \n", num_of_searchkeys);
            IndexLookupType localLookupType = m_lookupType;
            SortDirectionType localSortDirection = m_sortDirection;
            VOLT_TRACE("Lookup type: %d\n", static_cast<int>(m_lookupType));
            VOLT_TRACE("SortDirectionType: %d\n", m_sortDirection);

            // did setting the search key fail (usually due to overflow)
            bool keyException = false;
            //
            // Now use the outer table tuple to construct the search key
            // against the inner table
            //
            TableTuple index_values = m_indexValues.tuple();
            index_values.setAllNulls();
            for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
                // in a normal index scan, params would be substituted here,
                // but this scan fills in params outside the loop

                // When any part of the search key is NULL, the result is false when it compares to anything.
                //   do early return optimization, our index comparator may not handle null comparison correctly.
                // However, if the search key expression is "IS NOT DISTINCT FROM", then NULL values cannot be skipped.
                // We will set the CompareNotDistinctFlags to true in the planner to mark this. (ENG-11096)
                NValue candidateValue = m_indexNode->getSearchKeyExpressions()[ctr]->eval(&outer_tuple, NULL);
                if (candidateValue.isNull() && m_indexNode->getCompareNotDistinctFlags()[ctr] == false) {
                    keyException = true;
                    break;
                }
                try {
                    index_values.setNValue(ctr, candidateValue);
                } catch (const SQLException &e) {
                    // This next bit of logic handles underflow and overflow while
                    // setting up the search keys.
                    // e.g. TINYINT > 200 or INT <= 6000000000

                    // re-throw if not an overflow or underflow
                    // currently, it's expected to always be an overflow or underflow
                    if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW | SQLException::TYPE_UNDERFLOW | SQLException::TYPE_VAR_LENGTH_MISMATCH)) == 0) {
                        throw e;
                    }

                    // handle the case where this is a comparison, rather than equality match
                    // comparison is the only place where the executor might return matching tuples
                    // e.g. TINYINT < 1000 should return all values
                    if (localLookupType != IndexLookupType::Equal &&
                        ctr == (activeNumOfSearchKeys - 1)) {

                        if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                            if (localLookupType == IndexLookupType::Greater ||
                                localLookupType == IndexLookupType::GreaterEqual) {

                                // gt or gte when key overflows breaks out
                                // and only returns for left-outer
                                keyException = true;
                                break; // the outer while loop
                            } else {
                                // overflow of LT or LTE should be treated as LTE
                                // to issue an "initial" forward scan
                                localLookupType = IndexLookupType::LessEqual;
                            }
                        }
                        if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                            if ((localLookupType == IndexLookupType::Less) ||
                                (localLookupType == IndexLookupType::LessEqual)) {
                                // overflow of LT or LTE should be treated as LTE
                                // to issue an "initial" forward scans
                                localLookupType = IndexLookupType::LessEqual;
                            } else {
                                // don't allow GTE because it breaks null handling
                                localLookupType = IndexLookupType::Greater;
                            }
                        }
                        if (e.getInternalFlags() & SQLException::TYPE_VAR_LENGTH_MISMATCH) {
                            // shrink the search key and add the updated key to search key table tuple
                            index_values.shrinkAndSetNValue(ctr, candidateValue);
                            // search will be performed on shrinked key, so update lookup operation
                            // to account for it
                            switch (localLookupType) {
                                case IndexLookupType::Less:
                                case IndexLookupType::LessEqual:
                                    localLookupType = IndexLookupType::LessEqual;
                                    break;
                                case IndexLookupType::Greater:
                                case IndexLookupType::GreaterEqual:
                                    localLookupType = IndexLookupType::Greater;
                                    break;
                                default:
                                    vassert(!"IndexScanExecutor::p_execute - can't index on not equals");
                                    return false;
                            }
                        }

                        // if here, means all tuples with the previous searchkey
                        // columns need to be scaned.
                        activeNumOfSearchKeys--;
                        if (localSortDirection == SORT_DIRECTION_TYPE_INVALID) {
                            localSortDirection = SORT_DIRECTION_TYPE_ASC;
                        }
                    } else {
                        // if a EQ comparison is out of range, then the tuple from
                        // the outer loop returns no matches (except left-outer)
                        keyException = true;
                    }
                    break;
                } // End catch block for under- or overflow when setting index key
            } // End for each active search key
            VOLT_TRACE("Searching %s", index_values.debug("").c_str());

            // if a search value didn't fit into the targeted index key, skip this key
            if (!keyException) {
                //
                // Our index scan on the inner table is going to have three parts:
                //  (1) Lookup tuples using the search key
                //
                //  (2) For each tuple that comes back, check whether the
                //      end_expression is false.  If it is, then we stop
                //      scanning. Otherwise...
                //
                //  (3) Check whether the tuple satisfies the post expression.
                //      If it does, then add it to the output table
                //
                // Use our search key to prime the index iterator
                // The loop through each tuple given to us by the iterator
                //
                // Essentially cut and pasted this if ladder from
                // index scan executor
                if (num_of_searchkeys > 0) {
                    switch(localLookupType) {
                        case IndexLookupType::Equal:
                            index->moveToKey(&index_values, indexCursor);
                            break;
                        case IndexLookupType::Greater:
                            index->moveToGreaterThanKey(&index_values, indexCursor);
                            break;
                        case IndexLookupType::GreaterEqual:
                            index->moveToKeyOrGreater(&index_values, indexCursor);
                            break;
                        case IndexLookupType::Less:
                            index->moveToLessThanKey(&index_values, indexCursor);
                            break;
                        case IndexLookupType::LessEqual:
                            // find the entry whose key is less than or equal to search key
                            // as the start point to do a reverse scan
                            index->moveToKeyOrLess(&index_values, indexCursor);
                            break;
                        case IndexLookupType::GeoContains:
                            index->moveToCoveringCell(&index_values, indexCursor);
                            break;
                        default:
                            return false;
                    }
                } else {
                    bool toStartActually = (localSortDirection != SORT_DIRECTION_TYPE_DESC);
                    index->moveToEnd(toStartActually, indexCursor);
                }

                AbstractExpression* skipNullExprIteration = skipNullExpr;

                while (postfilter.isUnderLimit() && IndexScanExecutor::getNextTuple(
                            localLookupType, &inner_tuple, index, &indexCursor, num_of_searchkeys)) {
                    if (inner_tuple.isPendingDelete()) {
                        continue;
                    }
                    VOLT_TRACE("inner_tuple:%s",
                               inner_tuple.debug(inner_table->name()).c_str());
                    pmp.countdownProgress();

                    //
                    // First check to eliminate the null index rows for UNDERFLOW case only
                    //
                    if (skipNullExprIteration != NULL) {
                        if (skipNullExprIteration->eval(&outer_tuple, &inner_tuple).isTrue()) {
                            VOLT_DEBUG("Index scan: find out null rows or columns.");
                            continue;
                        }
                        skipNullExprIteration = NULL;
                    }

                    //
                    // First check whether the end_expression is now false
                    //
                    if (end_expression != NULL &&
                        !end_expression->eval(&outer_tuple, &inner_tuple).isTrue()) {
                        VOLT_TRACE("End Expression evaluated to false, stopping scan\n");
                        break;
                    }
                    //
                    // Then apply our post-predicate to do further filtering
                    //
                    if (post_expression == NULL ||
                        post_expression->eval(&outer_tuple, &inner_tuple).isTrue()) {
                        outerMatch = true;
                        // The inner tuple passed the join conditions
                        if (m_joinType == JOIN_TYPE_FULL) {
                            // Mark inner tuple as matched
                            innerTableFilter.updateTuple(inner_tuple, MATCHED_TUPLE);
                        }
                        // Still need to pass where filtering
                        if (postfilter.eval(&outer_tuple, &inner_tuple)) {
                            //
                            // Try to put the tuple into our output table
                            // Append the inner values to the end of our join tuple
                            //
                            for (int col_ctr = num_of_outer_cols;
                                 col_ctr < join_tuple.columnCount();
                                 ++col_ctr) {
                                join_tuple.setNValue(col_ctr,
                                          m_outputExpressions[col_ctr]->eval(&outer_tuple, &inner_tuple));
                            }
                            VOLT_TRACE("join_tuple tuple: %s",
                                       join_tuple.debug(m_tmpOutputTable->name()).c_str());
                            VOLT_TRACE("MATCH: %s",
                                   join_tuple.debug(m_tmpOutputTable->name()).c_str());
                            outputTuple(postfilter, join_tuple, pmp);
                        }
                    }
                } // END INNER WHILE LOOP
            } // END IF INDEX KEY EXCEPTION CONDITION
        } // END IF PRE JOIN CONDITION

        //
        // Left/Full Outer Join
        //
        if (m_joinType != JOIN_TYPE_INNER && !outerMatch && postfilter.isUnderLimit()) {
            // Still needs to pass the filter
            if (postfilter.eval(&outer_tuple, &null_inner_tuple)) {
                // Matched! Complete the joined tuple with null inner column values.
                for (int col_ctr = num_of_outer_cols;
                     col_ctr < join_tuple.columnCount();
                     ++col_ctr) {
                    join_tuple.setNValue(col_ctr,
                            m_outputExpressions[col_ctr]->eval(&outer_tuple, &null_inner_tuple));
                }
                outputTuple(postfilter, join_tuple, pmp);
            }
        }
    } // END OUTER WHILE LOOP

    //
    // FULL Outer Join. Iterate over the unmatched inner tuples
    //
    if (m_joinType == JOIN_TYPE_FULL && postfilter.isUnderLimit()) {
        // Preset outer columns to null
        const TableTuple& null_outer_tuple = m_null_outer_tuple.tuple();
        join_tuple.setNValues(0, null_outer_tuple, 0, num_of_outer_cols);

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
                for (int col_ctr = num_of_outer_cols;
                     col_ctr < join_tuple.columnCount();
                     ++col_ctr) {
                    join_tuple.setNValue(col_ctr,
                            m_outputExpressions[col_ctr]->eval(&null_outer_tuple, &inner_tuple));
                }
                outputTuple(postfilter, join_tuple, pmp);
            }
        }
    }

    if (m_aggExec != NULL) {
        m_aggExec->p_execute_finish();
    }

    VOLT_TRACE ("result table:\n %s", m_tmpOutputTable->debug("").c_str());
    VOLT_TRACE("Finished NestLoopIndex");
    return true;
}

NestLoopIndexExecutor::~NestLoopIndexExecutor() { }

