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

#include <vector>
#include <string>
#include <stack>
#include "nestloopindexexecutor.h"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/VoltDBEngine.h"
#include "execution/ProgressMonitorProxy.h"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "plannodes/nestloopindexnode.h"
#include "plannodes/indexscannode.h"
#include "plannodes/limitnode.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"

using namespace std;
using namespace voltdb;

bool NestLoopIndexExecutor::p_init(AbstractPlanNode* abstractNode,
                                   TempTableLimits* limits)
{
    VOLT_TRACE("init NLIJ Executor");
    assert(limits);

    node = dynamic_cast<NestLoopIndexPlanNode*>(abstractNode);
    assert(node);
    inline_node = dynamic_cast<IndexScanPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_INDEXSCAN));
    assert(inline_node);
    VOLT_TRACE("<NestLoopIndexPlanNode> %s, <IndexScanPlanNode> %s", node->debug().c_str(), inline_node->debug().c_str());

    join_type = node->getJoinType();
    m_lookupType = inline_node->getLookupType();
    m_sortDirection = inline_node->getSortDirection();

    //
    // We need exactly one input table and a target table
    //
    assert(node->getInputTables().size() == 1);

    int schema_size = static_cast<int>(node->getOutputSchema().size());
    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    for (int i = 0; i < schema_size; i++)
    {
        m_outputExpressions.push_back(node->getOutputSchema()[i]->getExpression());
    }

    //
    // Make sure that we actually have search keys
    //
    int num_of_searchkeys = (int)inline_node->getSearchKeyExpressions().size();
    //nshi commented this out in revision 4495 of the old repo in index scan executor
    VOLT_TRACE ("<Nested Loop Index exec, INIT...> Number of searchKeys: %d \n", num_of_searchkeys);

    //the code is cut and paste in nest loop and the change is necessary here as well
//    if (num_of_searchkeys == 0) {
//        VOLT_ERROR("There are no search key expressions for the internal"
//                   " PlanNode '%s' of PlanNode '%s'",
//                   inline_node->debug().c_str(), node->debug().c_str());
//        return false;
//    }
    for (int ctr = 0; ctr < num_of_searchkeys; ctr++) {
        if (inline_node->getSearchKeyExpressions()[ctr] == NULL) {
            VOLT_ERROR("The search key expression at position '%d' is NULL for"
                       " internal PlanNode '%s' of PlanNode '%s'",
                       ctr, inline_node->debug().c_str(), node->debug().c_str());
            return false;
        }
    }

    // output must be a temp table
    output_table = dynamic_cast<TempTable*>(node->getOutputTable());
    assert(output_table);

    assert(node->getInputTables().size() == 1);
    outer_table = node->getInputTables()[0];
    assert(outer_table);

    inner_table = dynamic_cast<PersistentTable*>(inline_node->getTargetTable());
    assert(inner_table);
    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    index = inner_table->index(inline_node->getTargetIndexName());
    if (index == NULL) {
        VOLT_ERROR("Failed to retreive index '%s' from inner table '%s' for"
                   " internal PlanNode '%s'",
                   inline_node->getTargetIndexName().c_str(),
                   inner_table->name().c_str(), inline_node->debug().c_str());
        return false;
    }

    // NULL tuple for outer join
    if (node->getJoinType() == JOIN_TYPE_LEFT) {
        Table* inner_out_table = inline_node->getOutputTable();
        assert(inner_out_table);
        m_null_tuple.init(inner_out_table->schema());
    }

    index_values = TableTuple(index->getKeySchema());
    index_values_backing_store = new char[index->getKeySchema()->tupleLength()];
    index_values.move( index_values_backing_store - TUPLE_HEADER_SIZE);
    index_values.setAllNulls();

    return true;
}

void NestLoopIndexExecutor::updateTargetTableAndIndex() {
    inner_table = dynamic_cast<PersistentTable*>(inline_node->getTargetTable());
    assert(inner_table);

    index = inner_table->index(inline_node->getTargetIndexName());
    assert(index);

    // NULL tuple for outer join
    if (node->getJoinType() == JOIN_TYPE_LEFT) {
        Table* inner_out_table = inline_node->getOutputTable();
        assert(inner_out_table);
        m_null_tuple.init(inner_out_table->schema());
    }

    index_values = TableTuple(index->getKeySchema());
    index_values.move( index_values_backing_store - TUPLE_HEADER_SIZE);
    index_values.setAllNulls();
}

bool NestLoopIndexExecutor::p_execute(const NValueArray &params)
{
    assert (node == dynamic_cast<NestLoopIndexPlanNode*>(m_abstractNode));
    assert(node);

    updateTargetTableAndIndex();

    assert (output_table == dynamic_cast<TempTable*>(node->getOutputTable()));
    assert(output_table);

    //inner_table is the table that has the index to be used in this executor
    assert (inner_table == dynamic_cast<PersistentTable*>(inline_node->getTargetTable()));
    assert(inner_table);

    //outer_table is the input table that have tuples to be iterated
    assert(node->getInputTables().size() == 1);
    assert (outer_table == node->getInputTables()[0]);
    assert (outer_table);
    VOLT_TRACE("executing NestLoopIndex with outer table: %s, inner table: %s",
               outer_table->debug().c_str(), inner_table->debug().c_str());

    //
    // Substitute parameter to SEARCH KEY Note that the expressions
    // will include TupleValueExpression even after this substitution
    //
    int num_of_searchkeys = (int)inline_node->getSearchKeyExpressions().size();
    for (int ctr = 0; ctr < num_of_searchkeys; ctr++) {
        VOLT_TRACE("Search Key[%d]:\n%s",
                   ctr, inline_node->getSearchKeyExpressions()[ctr]->debug(true).c_str());
    }

    // end expression
    AbstractExpression* end_expression = inline_node->getEndExpression();
    if (end_expression) {
        VOLT_TRACE("End Expression:\n%s", end_expression->debug(true).c_str());
    }

    // post expression
    AbstractExpression* post_expression = inline_node->getPredicate();
    if (post_expression != NULL) {
        VOLT_TRACE("Post Expression:\n%s", post_expression->debug(true).c_str());
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

    // initial expression
    AbstractExpression* initial_expression = inline_node->getInitialExpression();
    if (initial_expression != NULL) {
        VOLT_TRACE("Initial Expression:\n%s", initial_expression->debug(true).c_str());
    }

    // SKIP NULL EXPRESSION
    AbstractExpression* skipNullExpr = inline_node->getSkipNullPredicate();
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (skipNullExpr != NULL) {
        VOLT_DEBUG("Skip NULL Expression:\n%s", skipNullExpr->debug(true).c_str());
    }

    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    int tuple_ctr = 0;
    int tuple_skipped = 0;
    int limit = -1;
    int offset = -1;
    if (limit_node) {
        limit_node->getLimitAndOffsetByReference(params, limit, offset);
    }


    //
    // OUTER TABLE ITERATION
    //
    TableTuple outer_tuple(outer_table->schema());
    TableTuple inner_tuple(inner_table->schema());
    TableIterator outer_iterator = outer_table->iteratorDeletingAsWeGo();
    int num_of_outer_cols = outer_table->columnCount();
    assert (outer_tuple.sizeInValues() == outer_table->columnCount());
    assert (inner_tuple.sizeInValues() == inner_table->columnCount());
    TableTuple &join_tuple = output_table->tempTuple();
    TableTuple null_tuple = m_null_tuple;
    int num_of_inner_cols = (join_type == JOIN_TYPE_LEFT)? null_tuple.sizeInValues() : 0;

    ProgressMonitorProxy pmp(m_engine, this, inner_table);
    VOLT_TRACE("<num_of_outer_cols>: %d\n", num_of_outer_cols);
    while ((limit == -1 || tuple_ctr < limit) && outer_iterator.next(outer_tuple)) {
        VOLT_TRACE("outer_tuple:%s",
                   outer_tuple.debug(outer_table->name()).c_str());
        pmp.countdownProgress();
        // Set the outer tuple columns. Must be outside the inner loop
        // in case of the empty inner table
        join_tuple.setNValues(0, outer_tuple, 0, num_of_outer_cols);

        // did this loop body find at least one match for this tuple?
        bool match = false;
        // For outer joins if outer tuple fails pre-join predicate
        // (join expression based on the outer table only)
        // it can't match any of inner tuples
        if (prejoin_expression == NULL || prejoin_expression->eval(&outer_tuple, NULL).isTrue()) {

            int activeNumOfSearchKeys = num_of_searchkeys;
            VOLT_TRACE ("<Nested Loop Index exec, WHILE-LOOP...> Number of searchKeys: %d \n", num_of_searchkeys);
            IndexLookupType localLookupType = m_lookupType;
            SortDirectionType localSortDirection = m_sortDirection;
            VOLT_TRACE("Lookup type: %d\n", m_lookupType);
            VOLT_TRACE("SortDirectionType: %d\n", m_sortDirection);

            // did setting the search key fail (usually due to overflow)
            bool keyException = false;

            //
            // Now use the outer table tuple to construct the search key
            // against the inner table
            //
            index_values.setAllNulls();
            for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
                // in a normal index scan, params would be substituted here,
                // but this scan fills in params outside the loop
                NValue candidateValue = inline_node->getSearchKeyExpressions()[ctr]->eval(&outer_tuple, NULL);
                try {
                    index_values.setNValue(ctr, candidateValue);
                }
                catch (const SQLException &e) {
                    // This next bit of logic handles underflow and overflow while
                    // setting up the search keys.
                    // e.g. TINYINT > 200 or INT <= 6000000000

                    // re-throw if not an overflow or underflow
                    // currently, it's expected to always be an overflow or underflow
                    if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW | SQLException::TYPE_UNDERFLOW)) == 0) {
                        throw e;
                    }

                    // handle the case where this is a comparison, rather than equality match
                    // comparison is the only place where the executor might return matching tuples
                    // e.g. TINYINT < 1000 should return all values
                    if ((localLookupType != INDEX_LOOKUP_TYPE_EQ) &&
                        (ctr == (activeNumOfSearchKeys - 1))) {

                        // sanity check that there is at least one EQ column
                        // or else the join wouldn't work, right?
                        assert(activeNumOfSearchKeys > 1);

                        if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                            if ((localLookupType == INDEX_LOOKUP_TYPE_GT) ||
                                (localLookupType == INDEX_LOOKUP_TYPE_GTE)) {

                                // gt or gte when key overflows breaks out
                                // and only returns for left-outer
                                keyException = true;
                                break; // the outer while loop
                            }
                            else {
                                // overflow of LT or LTE should be treated as LTE
                                // to issue an "initial" forward scan
                                localLookupType = INDEX_LOOKUP_TYPE_LTE;
                            }
                        }
                        if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                            if ((localLookupType == INDEX_LOOKUP_TYPE_LT) ||
                                (localLookupType == INDEX_LOOKUP_TYPE_LTE)) {
                                // overflow of LT or LTE should be treated as LTE
                                // to issue an "initial" forward scans
                                localLookupType = INDEX_LOOKUP_TYPE_LTE;
                            }
                            else {
                                // don't allow GTE because it breaks null handling
                                localLookupType = INDEX_LOOKUP_TYPE_GT;
                            }
                        }

                        // if here, means all tuples with the previous searchkey
                        // columns need to be scaned.
                        activeNumOfSearchKeys--;
                        if (localSortDirection == SORT_DIRECTION_TYPE_INVALID) {
                            localSortDirection = SORT_DIRECTION_TYPE_ASC;
                        }
                    }
                    // if a EQ comparison is out of range, then the tuple from
                    // the outer loop returns no matches (except left-outer)
                    else {
                        keyException = true;
                    }
                    break;
                }
            }
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
                if (num_of_searchkeys > 0)
                {
                    if (localLookupType == INDEX_LOOKUP_TYPE_EQ) {
                        index->moveToKey(&index_values);
                    }
                    else if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
                        index->moveToGreaterThanKey(&index_values);
                    }
                    else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
                        index->moveToKeyOrGreater(&index_values);
                    }
                    else if (localLookupType == INDEX_LOOKUP_TYPE_LT) {
                        index->moveToLessThanKey(&index_values);
                    } else if (localLookupType == INDEX_LOOKUP_TYPE_LTE) {
                        // find the entry whose key is greater than search key,
                        // do a forward scan using initialExpr to find the correct
                        // start point to do reverse scan
                        bool isEnd = index->moveToGreaterThanKey(&index_values);
                        if (isEnd) {
                            index->moveToEnd(false);
                        } else {
                            while (!(inner_tuple = index->nextValue()).isNullTuple()) {
                                pmp.countdownProgress();
                                if (initial_expression != NULL && !initial_expression->eval(&outer_tuple, &inner_tuple).isTrue()) {
                                    // just passed the first failed entry, so move 2 backward
                                    index->moveToBeforePriorEntry();
                                    break;
                                }
                            }
                            if (inner_tuple.isNullTuple()) {
                                index->moveToEnd(false);
                            }
                        }
                    }
                    else {
                        return false;
                    }
                } else {
                    bool toStartActually = (localSortDirection != SORT_DIRECTION_TYPE_DESC);
                    index->moveToEnd(toStartActually);
                }

                AbstractExpression* skipNullExprIteration = skipNullExpr;

                while ((limit == -1 || tuple_ctr < limit) &&
                       ((localLookupType == INDEX_LOOKUP_TYPE_EQ &&
                        !(inner_tuple = index->nextValueAtKey()).isNullTuple()) ||
                       ((localLookupType != INDEX_LOOKUP_TYPE_EQ || num_of_searchkeys == 0) &&
                        !(inner_tuple = index->nextValue()).isNullTuple())))
                {
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
                        } else {
                            skipNullExprIteration = NULL;
                        }
                    }

                    //
                    // First check whether the end_expression is now false
                    //
                    if (end_expression != NULL &&
                        !end_expression->eval(&outer_tuple, &inner_tuple).isTrue())
                    {
                        VOLT_TRACE("End Expression evaluated to false, stopping scan\n");
                        break;
                    }
                    //
                    // Then apply our post-predicate to do further filtering
                    //
                    if (post_expression == NULL ||
                        post_expression->eval(&outer_tuple, &inner_tuple).isTrue())
                    {
                        match = true;
                        // Still need to pass where filtering
                        if (where_expression == NULL || where_expression->eval(&outer_tuple, &inner_tuple).isTrue()) {
                            // Check if we have to skip this tuple because of offset
                            if (tuple_skipped < offset) {
                                tuple_skipped++;
                                continue;
                            }
                            ++tuple_ctr;
                            //
                            // Try to put the tuple into our output table
                            // Append the inner values to the end of our join tuple
                            //
                            for (int col_ctr = num_of_outer_cols;
                                 col_ctr < join_tuple.sizeInValues();
                                 ++col_ctr)
                            {
                                // For the sake of consistency, we don't try to do
                                // output expressions here with columns from both tables.
                                join_tuple.
                                setNValue(col_ctr,
                                          m_outputExpressions[col_ctr]->
                                          eval(&outer_tuple, &inner_tuple));
                            }
                            VOLT_TRACE("join_tuple tuple: %s",
                                       join_tuple.debug(output_table->name()).c_str());
                            VOLT_TRACE("MATCH: %s",
                                   join_tuple.debug(output_table->name()).c_str());
                            output_table->insertTupleNonVirtual(join_tuple);
                            pmp.countdownProgress();
                        }
                    }
                }
            }
        }
        //
        // Left Outer Join
        //
        if ((limit == -1 || tuple_ctr < limit) && join_type == JOIN_TYPE_LEFT && !match ) {
            if (where_expression == NULL || where_expression->eval(&outer_tuple, &null_tuple).isTrue()) {
                // Check if we have to skip this tuple because of offset
                if (tuple_skipped < offset) {
                    tuple_skipped++;
                    continue;
                }
                ++tuple_ctr;
                join_tuple.setNValues(num_of_outer_cols, m_null_tuple, 0, num_of_inner_cols);
                output_table->insertTupleNonVirtual(join_tuple);
                pmp.countdownProgress();
            }
        }
    }

    VOLT_TRACE ("result table:\n %s", output_table->debug().c_str());
    VOLT_TRACE("Finished NestLoopIndex");
    return (true);
}

NestLoopIndexExecutor::~NestLoopIndexExecutor() {
    delete [] index_values_backing_store;
}
