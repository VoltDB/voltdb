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
#include "mergejoinexecutor.h"

#include "indexscanexecutor.h"
#include "executors/aggregateexecutor.h"
#include "indexes/tableindex.h"
#include "plannodes/indexscannode.h"
#include "plannodes/limitnode.h"
#include "plannodes/mergejoinnode.h"
#include "plannodes/projectionnode.h"
#include "storage/persistenttable.h"
#include "storage/tableiterator.h"
#include "storage/tabletuplefilter.h"

using namespace std;
using namespace voltdb;

const static int8_t UNMATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE);
const static int8_t MATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE + 1);
const static int8_t SKIPPED_TUPLE(TableTupleFilter::ACTIVE_TUPLE + 2);

// Helper class to iterate over either a temp table or
// a persistent table using its index
struct TableCursor {
    virtual ~TableCursor() {
    }

    // This is a helper function to get the "next tuple"
    virtual bool getNextTuple(TableTuple* tuple) = 0;

    virtual std::unique_ptr<TableCursor> clone() const = 0;

    virtual void populateJoinTuple(
            TableTuple& joinTuple, const TableTuple& childTuple, int startIndex) const {
        joinTuple.setNValues(startIndex, childTuple, 0, getColumnCount());
    }

    int getColumnCount() const {
        return table->columnCount();
    }

    TupleSchema const* getSchema() const {
        return table->schema();
    }

    Table* getTable() const {
        return table;
    }

    void updateTupleFilter(TableTuple& tuple, int status) {
        if (needTableTupleFilter) {
            tableTupleFilter.updateTuple(tuple, status);
        }
    }

    TableTupleFilter& getTupleFilter() {
        return tableTupleFilter;
    }

protected:
    TableCursor(Table* nodeTable, bool needTupleFilter) :
        table(nodeTable),
        tableTupleFilter(),
        needTableTupleFilter(needTupleFilter) {
            if (needTableTupleFilter) {
                tableTupleFilter.init(table);
            }
        }

    Table* table;
    // TableTupleFilter for outer joins to keep track of tuples that were skipped because they fail a predicate
    // or match tuples from another join's node
    TableTupleFilter tableTupleFilter;
    // Indicator whether we need to keep track of matched / skipped tuples
    bool needTableTupleFilter;
};

// A Temp Table Cursor
struct TempTableCursor : public TableCursor {

    TempTableCursor(AbstractPlanNode* childNode, Table* childTable, bool needTableTupleFilter) :
        TableCursor(childTable, needTableTupleFilter),
        tempTableIterator(childTable->iteratorDeletingAsWeGo()) {
        vassert(childNode);
        vassert(childTable);
        VOLT_TRACE("<MJ Child PlanNode> %s", childNode->debug().c_str());
        VOLT_TRACE("<MJ Child table :\n %s>", table->debug().c_str());
    }

    bool getNextTuple(TableTuple* tuple) override {
        return tempTableIterator.next(*tuple);
    }

    std::unique_ptr<TableCursor> clone() const override {
        return std::unique_ptr<TableCursor>(new TempTableCursor(*this));
    }

private:
    TableIterator tempTableIterator;
};

// IndexScan Cursor
struct IndexTableCursor : public TableCursor {
    IndexTableCursor(IndexScanPlanNode* indexNode, PersistentTable* persistTable, bool needTableTupleFilter):
        TableCursor(persistTable, needTableTupleFilter),
        tableIndex(persistTable->index(indexNode->getTargetIndexName())),
        indexCursor(tableIndex->getTupleSchema()),
        post_expression(indexNode->getPredicate()),
        projectionNode(dynamic_cast<ProjectionPlanNode*>(indexNode->getInlinePlanNode(PlanNodeType::Projection))) {
        tableIndex->moveToEnd(true, indexCursor);
        if (projectionNode) {
            numOfProjectColumns = projectionNode->getOutputColumnExpressions().size();
        }
        VOLT_TRACE("<MJ Index Child PlanNode> %s", indexNode->debug().c_str());
        VOLT_TRACE("<MJ Index Child table :\n %s>", table->debug().c_str());
    }

    void populateJoinTuple(TableTuple& joinTuple, const TableTuple& childTuple, int startIndex) const override {
        if (numOfProjectColumns == -1) {
            joinTuple.setNValues(startIndex, childTuple, 0, getColumnCount());
        } else {
            for (int ctr = 0; ctr < numOfProjectColumns; ctr++) {
                vassert(startIndex + ctr < joinTuple.columnCount());
                NValue value = projectionNode->getOutputColumnExpressions()[ctr]->eval(&childTuple, NULL);
                joinTuple.setNValue(startIndex + ctr, value);
            }
        }
    }

    bool getNextTuple(TableTuple* tuple) override {
        if (post_expression != NULL) {
            while (getNextTupleDo(tuple)) {
                if (post_expression->eval(tuple, NULL).isTrue()) {
                    return true;
                } else if (needTableTupleFilter) {
                    // Mark the tuple as skipped so it would not be added to the output table for outer joins
                    tableTupleFilter.updateTuple(*tuple, SKIPPED_TUPLE);
                }
            }
            return false;
        } else {
            return getNextTupleDo(tuple);
        }
    }

    std::unique_ptr<TableCursor> clone() const override {
        return std::unique_ptr<TableCursor>(new IndexTableCursor(*this));
    }

private:

    bool getNextTupleDo(TableTuple* tuple) {
        *tuple = tableIndex->nextValue(indexCursor);
        return ! tuple->isNullTuple();
    }

    // Persistent table
    TableIndex *tableIndex;
    IndexCursor indexCursor;
    AbstractExpression* post_expression;
    // Possible projection
    ProjectionPlanNode* projectionNode;
    int numOfProjectColumns = -1;
};

// Helper method to instantiate either an Index or Temp table's cursor
// depending on the type of the node
std::unique_ptr<TableCursor> buildTableCursor(AbstractPlanNode* node, Table* nodeTable, bool needTableTupleFilter = false) {
    if (node->getPlanNodeType() == PlanNodeType::IndexScan) {
        auto* indexNode = dynamic_cast<IndexScanPlanNode*>(node);
        vassert(dynamic_cast<PersistentTable*>(indexNode->getTargetTable()));
        return std::unique_ptr<TableCursor>(
            new IndexTableCursor(indexNode,
                static_cast<PersistentTable*>(indexNode->getTargetTable()),
                needTableTupleFilter));
    } else {
        return std::unique_ptr<TableCursor>(new TempTableCursor(node, nodeTable, needTableTupleFilter));
    }
}

bool MergeJoinExecutor::p_init(AbstractPlanNode* abstractNode, const ExecutorVector& executorVector) {
    VOLT_TRACE("init MergeJoin Executor");
    // Init parent first
    return AbstractJoinExecutor::p_init(abstractNode, executorVector);
}

void tracePredicate(char const* name, AbstractExpression* expr) {
    VOLT_TRACE("%s predicate: %s", name, expr == nullptr ? "NULL" : expr->debug(true).c_str());
}

bool MergeJoinExecutor::p_execute(const NValueArray &params) {
    VOLT_DEBUG("executing MergeJoin...");

    auto* node = dynamic_cast<MergeJoinPlanNode*>(m_abstractNode);
    vassert(node);
    vassert(node->getInputTableCount() == 1);

    // output table must be a temp table
    vassert(m_tmpOutputTable);

    // Outer table can be either an IndexScan or another MJ. In the latter case we have to
    // iterarte over its output temp table
    vassert(!node->getChildren().empty());
    AbstractPlanNode* outerNode = node->getChildren()[0];
    Table* outerTable = node->getInputTable();
    auto outerCursorPtr = buildTableCursor(outerNode, outerTable, (m_joinType != JOIN_TYPE_INNER));

    // inner table is guaranteed to be an index scan over a persistent table
    auto* innerIndexNode = dynamic_cast<IndexScanPlanNode*>(
            m_abstractNode->getInlinePlanNode(PlanNodeType::IndexScan));
    vassert(innerIndexNode);
    auto* persistTable = dynamic_cast<PersistentTable*>(innerIndexNode->getTargetTable());
    vassert(persistTable);

    IndexTableCursor innerCursor(innerIndexNode, persistTable, m_joinType == JOIN_TYPE_FULL);

    // NULL tuples for left and full joins
    p_init_null_tuples(outerTable, innerCursor.getTable());

    //
    // Pre Join Expression
    //
    AbstractExpression *preJoinPredicate = node->getPreJoinPredicate();
    tracePredicate("Pred Join", preJoinPredicate);
    //
    // Equivalence Expression
    //
    AbstractExpression *equalJoinPredicate = node->getJoinPredicate();
    vassert(equalJoinPredicate != nullptr);
    tracePredicate("Equality", equalJoinPredicate);
    //
    // Less Expression
    //
    AbstractExpression *lessJoinPredicate = node->getLessJoinPredicate();
    vassert(lessJoinPredicate != nullptr);
    tracePredicate("Less", lessJoinPredicate);
    //
    // Where Expression
    //
    AbstractExpression *wherePredicate = node->getWherePredicate();
    tracePredicate("Where", wherePredicate);

    auto* limitNode = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PlanNodeType::Limit));
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (limitNode) {
        tie(limit, offset) = limitNode->getLimitAndOffset(params);
    }

    int const outerCols = outerTable->columnCount();
    TableTuple outerTuple(outerTable->schema());
    TableTuple innerTuple(innerCursor.getSchema());
    const TableTuple& nullInnerTuple = m_null_inner_tuple.tuple();

    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
    // Init the postfilter
    CountingPostfilter postfilter(m_tmpOutputTable, wherePredicate, limit, offset);

    TableTuple joinTuple = m_aggExec == nullptr ? m_tmpOutputTable->tempTuple() :
        m_aggExec->p_execute_init(params, &pmp, node->getTupleSchemaPreAgg(), m_tmpOutputTable, &postfilter);

    // Move both iterators to the first rows if possible
    bool hasOuter = outerCursorPtr->getNextTuple(&outerTuple);
    bool hasInner = innerCursor.getNextTuple(&innerTuple);
    while (postfilter.isUnderLimit() && hasOuter && hasInner) {
        pmp.countdownProgress();

        // populate output table's temp tuple with outer table's values
        // probably have to do this at least once - avoid doing it many
        // times per outer tuple
        outerCursorPtr->populateJoinTuple(joinTuple, outerTuple, 0);

        // For outer joins if outer tuple fails pre-join predicate
        // (join expression based on the outer table only)
        // it can't match any of inner tuples
        if (preJoinPredicate == nullptr || preJoinPredicate->eval(&outerTuple, nullptr).isTrue()) {
            if (equalJoinPredicate->eval(&outerTuple, &innerTuple).isTrue()) {
                // The outer tuple passed the join predicate. Mark it as matched
                outerCursorPtr->updateTupleFilter(outerTuple, MATCHED_TUPLE);

                // The inner tuple passed the join predicate. Mark it as matched
                innerCursor.updateTupleFilter(innerTuple, MATCHED_TUPLE);

                // Filter the joined tuple
                if (postfilter.eval(&outerTuple, &innerTuple)) {
                // Matched! Complete the joined tuple with the inner column values.
                    innerCursor.populateJoinTuple(joinTuple, innerTuple, outerCols);
                    outputTuple(postfilter, joinTuple, pmp);
                }

                // Output further tuples that match the outerTuple
                IndexTableCursor innerCursorTmp = innerCursor;
                TableTuple innerTupleTmp = innerTuple;  // TODO: can we spare this construction?
                while (postfilter.isUnderLimit()
                        && innerCursorTmp.getNextTuple(&innerTupleTmp)
                        && equalJoinPredicate->eval(&outerTuple, &innerTupleTmp).isTrue()) {
                    pmp.countdownProgress();
                    // Mark outer tuple as matched
                    outerCursorPtr->updateTupleFilter(outerTuple, MATCHED_TUPLE);
                    // Mark inner tuple as matched
                    innerCursor.updateTupleFilter(innerTupleTmp, MATCHED_TUPLE);
                    // Filter the joined tuple
                    if (postfilter.eval(&outerTuple, &innerTupleTmp)) {
                        // Matched! Complete the joined tuple with the inner column values.
                        innerCursorTmp.populateJoinTuple(joinTuple, innerTupleTmp, outerCols);
                        outputTuple(postfilter, joinTuple, pmp);
                    }
                }

                // Output further tuples that match the innerTuple
                auto outerCursorPtrTmp = outerCursorPtr->clone();
                TableTuple outerTupleTmp = outerTuple;
                innerCursor.populateJoinTuple(joinTuple, innerTuple, outerCols);
                while (postfilter.isUnderLimit() &&
                        outerCursorPtrTmp->getNextTuple(&outerTupleTmp) &&
                        equalJoinPredicate->eval(&outerTupleTmp, &innerTuple).isTrue()) {
                    pmp.countdownProgress();
                    // Mark outer tuple as matched
                    outerCursorPtr->updateTupleFilter(outerTupleTmp, MATCHED_TUPLE);
                    // Mark inner tuple as matched
                    innerCursor.updateTupleFilter(innerTuple, MATCHED_TUPLE);

                    // Filter the joined tuple
                    if (postfilter.eval(&outerTupleTmp, &innerTuple)) {
                        // Matched! Complete the joined tuple with the inner column values.
                        outerCursorPtrTmp->populateJoinTuple(joinTuple, outerTupleTmp, 0);
                        outputTuple(postfilter, joinTuple, pmp);
                    }
                }

                // Advance both cursors
                hasOuter = outerCursorPtr->getNextTuple(&outerTuple);
                hasInner = innerCursor.getNextTuple(&innerTuple);
            } else if (lessJoinPredicate->eval(&outerTuple, &innerTuple).isTrue()) { // Advance outer
                hasOuter = outerCursorPtr->getNextTuple(&outerTuple);
            } else { // Advance inner
                hasInner = innerCursor.getNextTuple(&innerTuple);
            }
        } else { // Advance outer
            hasOuter = outerCursorPtr->getNextTuple(&outerTuple);
        }
    } // END WHILE LOOP

    if (m_joinType != JOIN_TYPE_INNER) {
        // This is an outer join. Add all unmatched outer tuples that pass the filter
        innerCursor.populateJoinTuple(joinTuple, nullInnerTuple, outerCols);

        TableTupleFilter& outerTableFilter = outerCursorPtr->getTupleFilter();
        auto const endItr = outerTableFilter.end<UNMATCHED_TUPLE>();
        for (auto itr = outerTableFilter.begin<UNMATCHED_TUPLE>();
                itr != endItr && postfilter.isUnderLimit(); ++itr) {
            // Restore the tuple value
            outerTuple.move(reinterpret_cast<char*>(outerTableFilter.getTupleAddress(*itr)));
            // Still needs to pass the filter
            vassert(outerTuple.isActive());
            if (postfilter.eval(&outerTuple, &nullInnerTuple)) {
                // Passed! Complete the joined tuple with the inner column values.
                outerCursorPtr->populateJoinTuple(joinTuple, outerTuple, 0);
                outputTuple(postfilter, joinTuple, pmp);
            }
        }
     }
     if (m_joinType == JOIN_TYPE_FULL) {
        // This is a full join. Add all unmatched inner tuples that pass the filter
        // Preset outer columns to null
        const TableTuple& nullOuterTuple = m_null_outer_tuple.tuple();
        joinTuple.setNValues(0, nullOuterTuple, 0, outerCols);

        TableTupleFilter& innerTableFilter = innerCursor.getTupleFilter();
        auto const endItr = innerTableFilter.end<UNMATCHED_TUPLE>();
        for (auto itr = innerTableFilter.begin<UNMATCHED_TUPLE>();
                itr != endItr && postfilter.isUnderLimit(); ++itr) {
            // Restore the tuple value
            innerTuple.move(reinterpret_cast<char*>(innerTableFilter.getTupleAddress(*itr)));
            // Still needs to pass the filter
            vassert(innerTuple.isActive());
            if (postfilter.eval(&nullOuterTuple, &innerTuple)) {
                // Passed! Complete the joined tuple with the inner column values.
                innerCursor.populateJoinTuple(joinTuple, innerTuple, outerCols);
                outputTuple(postfilter, joinTuple, pmp);
            }
        }
    }

    if (m_aggExec != nullptr) {
        m_aggExec->p_execute_finish();
    }
    return true;
}
