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

#include <tuple>
#include "indexscanexecutor.h"

#include "executors/aggregateexecutor.h"
#include "executors/insertexecutor.h"
#include "expressions/expressionutil.h"

// Inline PlanNodes
#include "plannodes/indexscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"
#include "plannodes/aggregatenode.h"

#include "storage/tableiterator.h"
#include "storage/persistenttable.h"

using namespace voltdb;

bool IndexScanExecutor::p_init(
        AbstractPlanNode *abstractNode, const ExecutorVector& executorVector) {
    VOLT_TRACE("init IndexScan Executor");

    m_projectionNode = nullptr;

    m_node = dynamic_cast<IndexScanPlanNode*>(abstractNode);
    vassert(m_node);
    vassert(m_node->getTargetTable());

    // Inline aggregation can be serial, partial or hash
    m_aggExec = voltdb::getInlineAggregateExecutor(m_abstractNode);
    m_insertExec = voltdb::getInlineInsertExecutor(m_abstractNode);
    // If we have an inline insert node, then the output
    // schema is the ususal DML schema.  Otherwise it's in the
    // plan node.  So, create output table based on output schema from the plan.
    if (m_insertExec != nullptr) {
        setDMLCountOutputTable(executorVector.limits());
    } else {
        setTempOutputTable(executorVector, m_node->getTargetTable()->name());
    }

    //
    // INLINE PROJECTION
    //
    m_projectionNode = static_cast<ProjectionPlanNode*>(
            m_node->getInlinePlanNode(PlanNodeType::Projection));
    //
    // Optimize the projection if we can.
    //
    if (m_projectionNode != nullptr) {
        m_projector = OptimizedProjector(m_projectionNode->getOutputColumnExpressions());
        m_projector.optimize(m_projectionNode->getOutputTable()->schema(),
                             m_node->getTargetTable()->schema());
    }

    // For the moment we will not produce a plan with both an
    // inline aggregate and an inline insert node.  This just
    // confuses things.
    vassert(m_aggExec == nullptr || m_insertExec == nullptr);

    //
    // Make sure that we have search keys and that they're not null
    //
    m_numOfSearchkeys = (int)m_node->getSearchKeyExpressions().size();
    m_searchKeyArrayPtr = boost::shared_array<AbstractExpression*>(
            new AbstractExpression*[m_numOfSearchkeys]);
    m_searchKeyArray = m_searchKeyArrayPtr.get();

    for (int ctr = 0; ctr < m_numOfSearchkeys; ctr++) {
        if (m_node->getSearchKeyExpressions()[ctr] == nullptr) {
            VOLT_ERROR("The search key expression at position '%d' is NULL for"
                    " PlanNode '%s'", ctr, m_node->debug().c_str());
            return false;
        } else {
            m_searchKeyArrayPtr[ctr] = m_node->getSearchKeyExpressions()[ctr];
        }
    }

    //output table should be temptable
    m_outputTable = static_cast<AbstractTempTable*>(m_node->getOutputTable());

    // The target table should be a persistent table.
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
    vassert(targetTable);

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

    m_hasOffsetRankOptimization = m_node->hasOffsetRankOptimization();
    VOLT_DEBUG("IndexScan: %s.%s\n", targetTable->name().c_str(), tableIndex->getName().c_str());
    return true;
}

bool IndexScanExecutor::p_execute(const NValueArray &params) {
    vassert(m_node);
    vassert(m_node == dynamic_cast<IndexScanPlanNode*>(m_abstractNode));

    // update local target table with its most recent reference
    // The target table should be a persistent table.
    vassert(dynamic_cast<PersistentTable*>(m_node->getTargetTable()));
    PersistentTable* targetTable = static_cast<PersistentTable*>(m_node->getTargetTable());

    TableIndex *tableIndex = targetTable->index(m_node->getTargetIndexName());
    IndexCursor indexCursor(tableIndex->getTupleSchema());

    TableTuple searchKey(tableIndex->getKeySchema());
    searchKey.moveNoHeader(m_searchKeyBackingStore);

    // TODO: we may need to comment out this assertion for merge join.
    vassert(m_lookupType != IndexLookupType::Equal ||
            searchKey.getSchema()->columnCount() == m_numOfSearchkeys);

    int activeNumOfSearchKeys = m_numOfSearchkeys;
    IndexLookupType localLookupType = m_lookupType;
    SortDirectionType localSortDirection = m_sortDirection;

    //
    // INLINE LIMIT
    //
    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(
            m_abstractNode->getInlinePlanNode(PlanNodeType::Limit));
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (limit_node != nullptr) {
        std::tie(limit, offset) = limit_node->getLimitAndOffset(params);
    }

    //
    // POST EXPRESSION
    //
    AbstractExpression* post_expression = m_node->getPredicate();
    if (post_expression != nullptr) {
        VOLT_DEBUG("Post Expression:\n%s", post_expression->debug(true).c_str());
    }

    // Initialize the postfilter
    int postfilterOffset = offset;
    if (m_hasOffsetRankOptimization) {
        postfilterOffset = CountingPostfilter::NO_OFFSET;
    }
    CountingPostfilter postfilter(m_outputTable, post_expression, limit, postfilterOffset);

    // Progress monitor
    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);

    //
    // Set the temp_tuple.  The data flow is:
    //
    //  scannedTable -+-> inline Project -+-> inline Node -+-> outputTable
    //                |                   ^                ^
    //                |                   |                |
    //                V                   V                |
    //                +-------------------+----------------+
    // A tuple comes out of the scanned table, through the inline Project if
    // there is one, through the inline Node, aggregate or insert, if there
    // is one and into the output table.  The scanned table and the
    // output table have their schemas and we can get a temp tuple from
    // them if we need it.  The middle node, between the inline project
    // and the inline Node, doesn't have a table.  So, in this case,
    // we need to create a tuple with the appropriate schema.  This
    // will be the output schema of the inline project node.  The tuple
    // temp_tuple is exactly this tuple.
    //
    TableTuple temp_tuple;

    if (m_aggExec != nullptr || m_insertExec != nullptr) {
        const TupleSchema * temp_tuple_schema;
        if (m_projectionNode != nullptr) {
            temp_tuple_schema = m_projectionNode->getOutputTable()->schema();
        } else {
            temp_tuple_schema = tableIndex->getTupleSchema();
        }
        if (m_aggExec != nullptr) {
            temp_tuple = m_aggExec->p_execute_init(params, &pmp, temp_tuple_schema, m_outputTable, &postfilter);
        } else {
            // We may actually find out during initialization
            // that we are done.  The p_execute_init function
            // returns false if this is so.  See the definition
            // of InsertExecutor::p_execute_init.
            //
            // We know we're in an insert from select statement.
            // The temp_tuple has as its schema the
            // set of columns of the select statement.
            // This is in the input schema.  We don't
            // actually have a tuple with this schema
            // yet, because we don't have an output
            // table for the projection node.  That's
            // the reason for the inline insert node,
            // after all.  So we have to construct a
            // tuple which the inline insert will be
            // happy with.  The p_execute_init knows
            // how to do this.  Note that temp_tuple will
            // not be initialized if this returns false.
            if (!m_insertExec->p_execute_init(temp_tuple_schema, m_tmpOutputTable, temp_tuple)) {
                return true;
            }
            // We should have as many expressions in the
            // projection node as there are columns in the
            // input schema if there is an inline projection.
            vassert(m_projectionNode != nullptr
                       ? (temp_tuple.getSchema()->columnCount() == m_projectionNode->getOutputColumnExpressions().size())
                       : true);
        }
    } else {
        temp_tuple = m_outputTable->tempTuple();
    }

    // Short-circuit an empty scan
    if (m_node->isEmptyScan()) {
        VOLT_DEBUG ("Empty Index Scan :\n %s", m_outputTable->debug().c_str());
        if (m_aggExec != nullptr) {
            m_aggExec->p_execute_finish();
        } else if (m_insertExec != nullptr) {
            m_insertExec->p_execute_finish();
        }
        return true;
    }

    //
    // SEARCH KEY
    //
    bool earlyReturnForSearchKeyOutOfRange = false;

    searchKey.setAllNulls();
    VOLT_TRACE("Initial (all null) search key: '%s'", searchKey.debugNoHeader().c_str());

    for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
        NValue candidateValue = m_searchKeyArray[ctr]->eval(nullptr, nullptr);
        // When any part of the search key is NULL, the result is false when it compares to anything.
        //   do early return optimization, our index comparator may not handle null comparison correctly.
        // However, if the search key expression is "IS NOT DISTINCT FROM", then NULL values cannot be skipped.
        // We will set the CompareNotDistinctFlags to true in the planner to mark this. (ENG-11096)
        if (candidateValue.isNull() && m_node->getCompareNotDistinctFlags()[ctr] == false) {
            earlyReturnForSearchKeyOutOfRange = true;
            break;
        }

        try {
            searchKey.setNValue(ctr, candidateValue);
        } catch (const SQLException &e) {
            // This next bit of logic handles underflow, overflow and search key length
            // exceeding variable length column size (variable lenght mismatch) when
            // setting up the search keys.
            // e.g. TINYINT > 200 or INT <= 6000000000
            // VarChar(3 bytes) < "abcd" or VarChar(3) > "abbd"
            //
            // Shouldn't this all be the same as the code in indexcountexecutor?
            // Here the localLookupType can only be NE, EQ, GT or GTE, and never LT
            // or LTE.  But that seems like something a template could puzzle out.
            //
            // re-throw if not an overflow, underflow or variable length mismatch
            // currently, it's expected to always be an overflow or underflow
            //
            // Note that only one if these three bits will ever be asserted (cf. below).
            if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW |
                            SQLException::TYPE_UNDERFLOW |
                            SQLException::TYPE_VAR_LENGTH_MISMATCH)) == 0) {
                throw e;
            } else if (localLookupType != IndexLookupType::Equal && ctr == activeNumOfSearchKeys - 1) {
            // handle the case where this is a comparison, rather than equality match
            // comparison is the only place where the executor might return matching tuples
            // e.g. TINYINT < 1000 should return all values

                // We have three cases, one for overflow, one for underflow
                // and one for TYPE_VAR_LENGTH_MISMATCH.  These are
                // orthogonal here, though it's not clearly so.  See the
                // definitions of throwCastSQLValueOutOfRangeException,
                // whence these all come.
                if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                    if (localLookupType == IndexLookupType::Greater ||
                            localLookupType == IndexLookupType::GreaterEqual) {

                        // gt or gte when key overflows returns nothing except inline agg
                        earlyReturnForSearchKeyOutOfRange = true;
                        break;
                    } else {
                        // for overflow on reverse scan, we need to
                        // do a forward scan to find the correct start
                        // point, which is exactly what LTE would do.
                        // so, set the lookupType to LTE and the missing
                        // searchkey will be handled by extra post filters
                        localLookupType = IndexLookupType::LessEqual;
                    }
                }
                if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                    if (localLookupType == IndexLookupType::Less ||
                            localLookupType == IndexLookupType::LessEqual) {

                        // lt or lte when key underflows returns nothing except inline agg
                        earlyReturnForSearchKeyOutOfRange = true;
                        break;
                    } else {
                        // don't allow GTE because it breaks null handling
                        localLookupType = IndexLookupType::Greater;
                    }
                }
                if (e.getInternalFlags() & SQLException::TYPE_VAR_LENGTH_MISMATCH) {
                    // shrink the search key and add the updated key to search key table tuple
                    searchKey.shrinkAndSetNValue(ctr, candidateValue);
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
                // columns need to be scanned. Note, if only one column,
                // then all tuples will be scanned. Only exception to this
                // case is setting of search key in search tuple was due
                // to search key length exceeding the search column length
                // of variable length type
                if (!(e.getInternalFlags() & SQLException::TYPE_VAR_LENGTH_MISMATCH)) {
                    // for variable length mismatch error, the needed search key to perform the search
                    // has been generated and added to the search tuple. So no need to decrement
                    // activeNumOfSearchKeys
                    activeNumOfSearchKeys--;
                }
                if (localSortDirection == SORT_DIRECTION_TYPE_INVALID) {
                    localSortDirection = SORT_DIRECTION_TYPE_ASC;
                }
            } else { // if a EQ comparison is out of range, then return no tuples
                earlyReturnForSearchKeyOutOfRange = true;
            }
            break;
        }
    }

    if (earlyReturnForSearchKeyOutOfRange) {
        if (m_aggExec != nullptr) {
            m_aggExec->p_execute_finish();
        }
        if (m_insertExec != nullptr) {
            m_insertExec->p_execute_finish();
        }
        return true;
    }

    vassert((activeNumOfSearchKeys == 0) || (searchKey.getSchema()->columnCount() > 0));
    VOLT_TRACE("Search key after substitutions: '%s', # of active search keys: %d",
            searchKey.debugNoHeader().c_str(), activeNumOfSearchKeys);

    //
    // END EXPRESSION
    //
    AbstractExpression const* end_expression = m_node->getEndExpression();
    if (end_expression != nullptr) {
        VOLT_DEBUG("End Expression:\n%s", end_expression->debug(true).c_str());
    }

    // INITIAL EXPRESSION
    AbstractExpression const* initial_expression = m_node->getInitialExpression();
    if (initial_expression != nullptr) {
        VOLT_DEBUG("Initial Expression:\n%s", initial_expression->debug(true).c_str());
    }

    //
    // SKIP NULL EXPRESSION
    //
    AbstractExpression const* skipNullExpr = m_node->getSkipNullPredicate();
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (skipNullExpr != nullptr) {
        VOLT_DEBUG("COUNT NULL Expression:\n%s", skipNullExpr->debug(true).c_str());
    }

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
                   static_cast<int>(localLookupType), activeNumOfSearchKeys, searchKey.debugNoHeader().c_str());
        switch(localLookupType) {
            case IndexLookupType::Equal:
                tableIndex->moveToKey(&searchKey, indexCursor);
                break;
            case IndexLookupType::Greater:
                tableIndex->moveToGreaterThanKey(&searchKey, indexCursor);
                break;
            case IndexLookupType::GreaterEqual:
                tableIndex->moveToKeyOrGreater(&searchKey, indexCursor);
                break;
            case IndexLookupType::Less:
                tableIndex->moveToLessThanKey(&searchKey, indexCursor);
                break;
            case IndexLookupType::LessEqual:
                // find the entry whose key is less than or equal to search key
                // as the start point to do a reverse scan
                tableIndex->moveToKeyOrLess(&searchKey, indexCursor);
                break;
            case IndexLookupType::GeoContains:
                tableIndex->moveToCoveringCell(&searchKey, indexCursor);
                break;
            default:
                return false;
        }
    } else {
        bool forward = (localSortDirection != SORT_DIRECTION_TYPE_DESC);
        if (m_hasOffsetRankOptimization) {
            int rankOffset = offset + 1;
            if (!forward) {
                rankOffset = static_cast<int>(tableIndex->getSize() - offset);
            }
            // when rankOffset is not greater than 0, it means there are no matching tuples
            // then we do not need to update the IndexCursor which points to NULL tuple by default
            if (rankOffset > 0) {
                tableIndex->moveToRankTuple(rankOffset, forward, indexCursor);
            }
        } else {
            tableIndex->moveToEnd(forward, indexCursor);
        }
    }

    //
    // We have different nextValue() methods for different lookup types
    //
    while (postfilter.isUnderLimit() && getNextTuple(
                localLookupType, &tuple, tableIndex, &indexCursor, activeNumOfSearchKeys)) {
        bool skip = tuple.isPendingDelete();
        if (! skip && initial_expression != nullptr) { // jump until initial expression is satisified
            try {           // ENG-20394: Evaluating on the row may throw.
                // initial expr that does not match filter need to be skipped
                skip = ! initial_expression->eval(&tuple, nullptr).isTrue();
            } catch (SQLException const&) {
                skip = true;
            }
        }
        if (skip) {
            continue;
        } else if (initial_expression) {
            /**
             * ENG-20904
             * Evaluate all tuples on initial_expression, until we no
             * longer need to skip, after which there is no futher need to
             * evaluate on initial_expression.
             */
            initial_expression = nullptr;
        }
        VOLT_TRACE("LOOPING in indexscan: tuple: '%s'\n", tuple.debug("tablename").c_str());

        pmp.countdownProgress();
        //
        // First check to eliminate the null index rows for UNDERFLOW case only
        //
        if (skipNullExpr != nullptr) {
            if (skipNullExpr->eval(&tuple, NULL).isTrue()) {
                VOLT_DEBUG("Index scan: find out null rows or columns.");
                continue;
            } else {
                skipNullExpr = nullptr;
            }
        }
        //
        // First check whether the end_expression is now false
        //
        if (end_expression != nullptr && ! end_expression->eval(&tuple, nullptr).isTrue()) {
            VOLT_TRACE("End Expression evaluated to false, stopping scan");
            break;
        }
        //
        // Then apply our post-predicate and LIMIT/OFFSET to do further filtering
        //
        if (postfilter.eval(&tuple, nullptr)) {
            if (m_projector.numSteps() > 0) {
                m_projector.exec(temp_tuple, tuple);
                outputTuple(postfilter, temp_tuple);
            }
            else {
                outputTuple(postfilter, tuple);
            }
            pmp.countdownProgress();
        }
    }

    if (m_aggExec != nullptr) {
        m_aggExec->p_execute_finish();
    } else if (m_insertExec != nullptr) {
        m_insertExec->p_execute_finish();
    }
    VOLT_DEBUG ("Index Scanned :\n %s", m_outputTable->debug().c_str());
    return true;
}

void IndexScanExecutor::outputTuple(CountingPostfilter&, TableTuple& tuple) {
    if (m_aggExec != nullptr) {
        m_aggExec->p_execute_tuple(tuple);
    } else if (m_insertExec != nullptr) {
        m_insertExec->p_execute_tuple(tuple);
    } else {
        //
        // Insert the tuple into our output table
        //
        vassert(m_tmpOutputTable);
        m_tmpOutputTable->insertTempTuple(tuple);
    }
}

IndexScanExecutor::~IndexScanExecutor() {
    delete [] m_searchKeyBackingStore;
}
