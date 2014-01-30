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

#include "indexscanexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/ProgressMonitorProxy.h"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "indexes/tableindex.h"

// Inline PlanNodes
#include "plannodes/indexscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"

#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

using namespace voltdb;

bool IndexScanExecutor::p_init(AbstractPlanNode *abstractNode,
                               TempTableLimits* limits)
{
    VOLT_TRACE("init IndexScan Executor");

    m_projectionNode = NULL;

    m_node = dynamic_cast<IndexScanPlanNode*>(abstractNode);
    assert(m_node);
    assert(m_node->getTargetTable());

    // Create output table based on output schema from the plan
    setTempOutputTable(limits, m_node->getTargetTable()->name());

    //
    // INLINE PROJECTION
    //
    if (m_node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION) != NULL)
    {
        m_projectionNode =
            static_cast<ProjectionPlanNode*>
            (m_node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));

        m_projectionExpressions =
            new AbstractExpression*[m_node->getOutputTable()->columnCount()];

        ::memset(m_projectionExpressions, 0,
                 (sizeof(AbstractExpression*) *
                  m_node->getOutputTable()->columnCount()));

        m_projectionAllTupleArrayPtr = ExpressionUtil::convertIfAllTupleValues(m_projectionNode->getOutputColumnExpressions());

        m_projectionAllTupleArray = m_projectionAllTupleArrayPtr.get();

        for (int ctr = 0;
             ctr < m_node->getOutputTable()->columnCount();
             ctr++)
        {
            assert(m_projectionNode->getOutputColumnExpressions()[ctr]);
            m_projectionExpressions[ctr] =
              m_projectionNode->getOutputColumnExpressions()[ctr];
        }
    }

    //
    // Make sure that we have search keys and that they're not null
    //
    m_numOfSearchkeys = (int)m_node->getSearchKeyExpressions().size();
    m_searchKeyArrayPtr =
      boost::shared_array<AbstractExpression*>
        (new AbstractExpression*[m_numOfSearchkeys]);
    m_searchKeyArray = m_searchKeyArrayPtr.get();

    for (int ctr = 0; ctr < m_numOfSearchkeys; ctr++)
    {
        if (m_node->getSearchKeyExpressions()[ctr] == NULL)
        {
            VOLT_ERROR("The search key expression at position '%d' is NULL for"
                       " PlanNode '%s'", ctr, m_node->debug().c_str());
            delete [] m_projectionExpressions;
            return false;
        }
        m_searchKeyArrayPtr[ctr] =
            m_node->getSearchKeyExpressions()[ctr];
    }

    //output table should be temptable
    m_outputTable = static_cast<TempTable*>(m_node->getOutputTable());
    m_numOfColumns = static_cast<int>(m_outputTable->columnCount());

    Table* targetTable = m_node->getTargetTable();
    //target table should be persistenttable
    assert(static_cast<PersistentTable*>(targetTable));

    TableIndex *tableIndex = targetTable->index(m_node->getTargetIndexName());
    m_searchKeyBackingStore = new char[tableIndex->getKeySchema()->tupleLength()];

    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    VOLT_TRACE("Index key schema: '%s'", tableIndex->getKeySchema()->debug().c_str());
    //
    // Miscellanous Information
    //
    m_lookupType = m_node->getLookupType();
    m_sortDirection = m_node->getSortDirection();

    VOLT_DEBUG("IndexScan: %s.%s\n", targetTable->name().c_str(), tableIndex->getName().c_str());

    return true;
}

bool IndexScanExecutor::p_execute(const NValueArray &params)
{
    assert(m_node);
    assert(m_node == dynamic_cast<IndexScanPlanNode*>(m_abstractNode));
    assert(m_outputTable);
    assert(m_outputTable == static_cast<TempTable*>(m_node->getOutputTable()));

    // update local target table with its most recent reference
    Table* targetTable = m_node->getTargetTable();
    TableIndex *tableIndex = targetTable->index(m_node->getTargetIndexName());
    TableTuple searchKey(tableIndex->getKeySchema());
    searchKey.moveNoHeader(m_searchKeyBackingStore);

    assert(m_lookupType != INDEX_LOOKUP_TYPE_EQ ||
            searchKey.getSchema()->columnCount() == m_numOfSearchkeys);

    int activeNumOfSearchKeys = m_numOfSearchkeys;
    IndexLookupType localLookupType = m_lookupType;
    SortDirectionType localSortDirection = m_sortDirection;

    // INLINE PROJECTION
    // Set params to expression tree via substitute()
    assert(m_numOfColumns == m_outputTable->columnCount());
    if (m_projectionNode != NULL && m_projectionAllTupleArray == NULL)
    {
        for (int ctr = 0; ctr < m_numOfColumns; ctr++)
        {
            assert(m_projectionNode->getOutputColumnExpressions()[ctr]);
            m_projectionExpressions[ctr]->substitute(params);
            assert(m_projectionExpressions[ctr]);
        }
    }

    //
    // INLINE LIMIT
    //
    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(m_abstractNode->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));

    //
    // SEARCH KEY
    //
    searchKey.setAllNulls();
    VOLT_TRACE("Initial (all null) search key: '%s'", searchKey.debugNoHeader().c_str());
    for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
        m_searchKeyArray[ctr]->substitute(params);
        NValue candidateValue = m_searchKeyArray[ctr]->eval(NULL, NULL);
        try {
            searchKey.setNValue(ctr, candidateValue);
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

                if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                    if ((localLookupType == INDEX_LOOKUP_TYPE_GT) ||
                        (localLookupType == INDEX_LOOKUP_TYPE_GTE)) {

                        // gt or gte when key overflows returns nothing
                        return true;
                    }
                    else {
                        // for overflow on reverse scan, we need to
                        // do a forward scan to find the correct start
                        // point, which is exactly what LTE would do.
                        // so, set the lookupType to LTE and the missing
                        // searchkey will be handled by extra post filters
                        localLookupType = INDEX_LOOKUP_TYPE_LTE;
                    }
                }
                if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                    if ((localLookupType == INDEX_LOOKUP_TYPE_LT) ||
                        (localLookupType == INDEX_LOOKUP_TYPE_LTE)) {

                        // lt or lte when key underflows returns nothing
                        return true;
                    }
                    else {
                        // don't allow GTE because it breaks null handling
                        localLookupType = INDEX_LOOKUP_TYPE_GT;
                    }
                }

                // if here, means all tuples with the previous searchkey
                // columns need to be scaned. Note, if only one column,
                // then all tuples will be scanned
                activeNumOfSearchKeys--;
                if (localSortDirection == SORT_DIRECTION_TYPE_INVALID) {
                    localSortDirection = SORT_DIRECTION_TYPE_ASC;
                }
            }
            // if a EQ comparison is out of range, then return no tuples
            else {
                return true;
            }
            break;
        }
    }
    assert((activeNumOfSearchKeys == 0) || (searchKey.getSchema()->columnCount() > 0));
    VOLT_TRACE("Search key after substitutions: '%s'", searchKey.debugNoHeader().c_str());

    //
    // END EXPRESSION
    //
    AbstractExpression* end_expression = m_node->getEndExpression();
    if (end_expression != NULL) {
        end_expression->substitute(params);
        VOLT_DEBUG("End Expression:\n%s", end_expression->debug(true).c_str());
    }

    //
    // POST EXPRESSION
    //
    AbstractExpression* post_expression = m_node->getPredicate();
    if (post_expression != NULL) {
        post_expression->substitute(params);
        VOLT_DEBUG("Post Expression:\n%s", post_expression->debug(true).c_str());
    }

    // INITIAL EXPRESSION
    AbstractExpression* initial_expression = m_node->getInitialExpression();
    if (initial_expression != NULL) {
        initial_expression->substitute(params);
        VOLT_DEBUG("Initial Expression:\n%s", initial_expression->debug(true).c_str());
    }

    //
    // SKIP NULL EXPRESSION
    //
    AbstractExpression* skipNullExpr = m_node->getSkipNullPredicate();
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (skipNullExpr != NULL) {
        skipNullExpr->substitute(params);
        VOLT_DEBUG("COUNT NULL Expression:\n%s", skipNullExpr->debug(true).c_str());
    }

    ProgressMonitorProxy pmp(m_engine, targetTable);
    //
    // An index scan has three parts:
    //  (1) Lookup tuples using the search key
    //  (2) For each tuple that comes back, check whether the
    //  end_expression is false.
    //  If it is, then we stop scanning. Otherwise...
    //  (3) Check whether the tuple satisfies the post expression.
    //      If it does, then add it to the output table
    //
    // Use our search key to prime the index iterator
    // Now loop through each tuple given to us by the iterator
    //

    TableTuple tuple;
    if (activeNumOfSearchKeys > 0) {
        VOLT_TRACE("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                   localLookupType, activeNumOfSearchKeys, searchKey.debugNoHeader().c_str());

        if (localLookupType == INDEX_LOOKUP_TYPE_EQ) {
            tableIndex->moveToKey(&searchKey);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
            tableIndex->moveToGreaterThanKey(&searchKey);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
            tableIndex->moveToKeyOrGreater(&searchKey);
        } else if (localLookupType == INDEX_LOOKUP_TYPE_LT) {
            tableIndex->moveToLessThanKey(&searchKey);
        } else if (localLookupType == INDEX_LOOKUP_TYPE_LTE) {
            // find the entry whose key is greater than search key,
            // do a forward scan using initialExpr to find the correct
            // start point to do reverse scan
            bool isEnd = tableIndex->moveToGreaterThanKey(&searchKey);
            if (isEnd) {
                tableIndex->moveToEnd(false);
            } else {
                while (!(tuple = tableIndex->nextValue()).isNullTuple()) {
                    pmp.countdownProgress();
                    if (initial_expression != NULL && !initial_expression->eval(&tuple, NULL).isTrue()) {
                        // just passed the first failed entry, so move 2 backward
                        tableIndex->moveToBeforePriorEntry();
                        break;
                    }
                }
                if (tuple.isNullTuple()) {
                    tableIndex->moveToEnd(false);
                }
            }
        }
        else {
            return false;
        }
    } else {
        bool toStartActually = (localSortDirection != SORT_DIRECTION_TYPE_DESC);
        tableIndex->moveToEnd(toStartActually);
    }

    int tuple_ctr = 0;
    int tuples_skipped = 0;     // for offset
    int limit = -1;
    int offset = -1;
    if (limit_node != NULL) {
        limit_node->getLimitAndOffsetByReference(params, limit, offset);
    }

    //
    // We have to different nextValue() methods for different lookup types
    //
    while ((limit == -1 || tuple_ctr < limit) &&
           ((localLookupType == INDEX_LOOKUP_TYPE_EQ &&
             !(tuple = tableIndex->nextValueAtKey()).isNullTuple()) ||
           ((localLookupType != INDEX_LOOKUP_TYPE_EQ || activeNumOfSearchKeys == 0) &&
            !(tuple = tableIndex->nextValue()).isNullTuple()))) {
        VOLT_TRACE("LOOPING in indexscan: tuple: '%s'\n", tuple.debug("tablename").c_str());
        pmp.countdownProgress();
        //
        // First check to eliminate the null index rows for UNDERFLOW case only
        //
        if (skipNullExpr != NULL) {
            if (skipNullExpr->eval(&tuple, NULL).isTrue()) {
                VOLT_DEBUG("Index scan: find out null rows or columns.");
                continue;
            } else {
                skipNullExpr = NULL;
            }
        }
        //
        // First check whether the end_expression is now false
        //
        if (end_expression != NULL && !end_expression->eval(&tuple, NULL).isTrue()) {
            VOLT_TRACE("End Expression evaluated to false, stopping scan");
            break;
        }
        //
        // Then apply our post-predicate to do further filtering
        //
        if (post_expression == NULL || post_expression->eval(&tuple, NULL).isTrue()) {
            //
            // INLINE OFFSET
            //
            if (tuples_skipped < offset)
            {
                tuples_skipped++;
                continue;
            }
            tuple_ctr++;

            if (m_projectionNode != NULL)
            {
                TableTuple &temp_tuple = m_outputTable->tempTuple();
                if (m_projectionAllTupleArray != NULL)
                {
                    VOLT_TRACE("sweet, all tuples");
                    for (int ctr = m_numOfColumns - 1; ctr >= 0; --ctr) {
                        temp_tuple.setNValue(ctr, tuple.getNValue(m_projectionAllTupleArray[ctr]));
                    }
                }
                else
                {
                    for (int ctr = m_numOfColumns - 1; ctr >= 0; --ctr) {
                        temp_tuple.setNValue(ctr, m_projectionExpressions[ctr]->eval(&tuple, NULL));
                    }
                }
                m_outputTable->insertTupleNonVirtual(temp_tuple);
            }
            else
                //
                // Straight Insert
                //
            {
                //
                // Try to put the tuple into our output table
                //
                m_outputTable->insertTupleNonVirtual(tuple);
            }
            pmp.countdownProgress();
        }
    }

    VOLT_DEBUG ("Index Scanned :\n %s", m_outputTable->debug().c_str());
    return true;
}

IndexScanExecutor::~IndexScanExecutor() {
    delete [] m_searchKeyBackingStore;
    delete [] m_projectionExpressions;
}
