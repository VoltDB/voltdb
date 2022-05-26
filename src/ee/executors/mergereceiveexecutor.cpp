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

#include "mergereceiveexecutor.h"
#include "plannodes/mergereceivenode.h"
#include "execution/ProgressMonitorProxy.h"
#include "executors/aggregateexecutor.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"
#include "plannodes/receivenode.h"
#include "plannodes/orderbynode.h"
#include "plannodes/limitnode.h"

namespace voltdb {

namespace {

typedef std::vector<TableTuple>::const_iterator tuple_iterator;
typedef std::pair<tuple_iterator, tuple_iterator> tuple_range;
typedef std::vector<tuple_range>::iterator range_iterator;

// Functor to compare two non-empty tuple ranges by comparing their first tuples using provided TupleComparer
struct TupleRangeComparer : std::binary_function<tuple_range, tuple_range, bool>
{
    TupleRangeComparer(AbstractExecutor::TupleComparer comp) :
        m_comp(comp)
    {}

    bool operator()(const tuple_range& ta, const tuple_range& tb) const
    {
        // Assert both ranges are not empty
        vassert(ta.first != ta.second);
        vassert(tb.first != tb.second);
        return m_comp(*ta.first, *tb.first);
    }
    AbstractExecutor::TupleComparer m_comp;
};

}

void MergeReceiveExecutor::merge_sort(const std::vector<TableTuple>& tuples,
                                      std::vector<int64_t>& partitionTupleCounts,
                                      AbstractExecutor::TupleComparer comp,
                                      CountingPostfilter& postfilter,
                                      AggregateExecutorBase* agg_exec,
                                      AbstractTempTable* output_table,
                                      ProgressMonitorProxy* pmp) {

    if (partitionTupleCounts.empty()) {
        return;
    }

    size_t nonEmptyPartitions = partitionTupleCounts.size();

    // Vector to hold pairs of iterators denoting the range of tuples
    // for a given partition
    std::vector<tuple_range> partitions;
    partitions.reserve(nonEmptyPartitions);
    tuple_iterator begin = tuples.begin();
    for (size_t i = 0; i < nonEmptyPartitions; ++i) {
        // Partitions are supposed to be non-empty
        vassert(partitionTupleCounts[i] > 0);
        tuple_iterator end = begin + partitionTupleCounts[i];
        partitions.push_back(std::make_pair(begin, end));
        begin = end;
        vassert( i != nonEmptyPartitions -1 || end == tuples.end());
    }

    // Make a heap out of partitions where the partition with a tuple with a minimal value is on top
    TupleRangeComparer tupleRangeComp(comp);
    std::binary_negate<TupleRangeComparer> reversedTupleRangeComp = std::not2(TupleRangeComparer(comp));
    std::make_heap(partitions.begin(), partitions.end(), reversedTupleRangeComp);

    while (postfilter.isUnderLimit() && !partitions.empty()) {
        // Get the first partition from the heap that has the next tuple to be inserted
        range_iterator rangeIt = partitions.begin();
        vassert(rangeIt->first != rangeIt->second);
        TableTuple tuple = *rangeIt->first;
        ++rangeIt->first;

        if (partitions.size() == 1 && rangeIt->first == rangeIt->second) {
            // The last partition is empty. Done.
            partitions.pop_back();
        } else if (partitions.size() > 1) {
            // There are more than one partition left. Remove the current top partition from the heap
            // Dereferencing the iterator must be done prior to the std::pop_heap which would
            // change the tuple_range this iterator points to.
            tuple_range nextPartition = *rangeIt;
            std::pop_heap(partitions.begin(), partitions.end(), reversedTupleRangeComp);
            partitions.pop_back();
            if (nextPartition.first != nextPartition.second) {
                // The partition is not empty yet. Reinsert it back to the heap.
                partitions.push_back(nextPartition);
                std::push_heap(partitions.begin(), partitions.end(), reversedTupleRangeComp);
            }
        }

        // Run the postfilter to evaluate the LIMIT/OFFSET
        if (postfilter.eval(&tuple, NULL)) {
            if (agg_exec != NULL) {
                agg_exec->p_execute_tuple(tuple);
            } else {
                output_table->insertTempTuple(tuple);
            }

            if (pmp != NULL) {
                // Should only be NULL when unit testing
                pmp->countdownProgress();
            }
        }
    }
}

MergeReceiveExecutor::MergeReceiveExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node), m_orderby_node(NULL), m_limit_node(NULL),
    m_agg_exec(NULL)
{ }

bool MergeReceiveExecutor::p_init(AbstractPlanNode* abstract_node,
                                  const ExecutorVector& executorVector)
{
    VOLT_TRACE("init MergeReceive Executor");
    vassert(!executorVector.isLargeQuery());

    MergeReceivePlanNode* merge_receive_node = dynamic_cast<MergeReceivePlanNode*>(abstract_node);
    vassert(merge_receive_node != NULL);

    // Create output table based on output schema from the plan
    setTempOutputTable(executorVector);

    // inline OrderByPlanNode
    m_orderby_node = dynamic_cast<OrderByPlanNode*>(merge_receive_node->
                                     getInlinePlanNode(PlanNodeType::OrderBy));
    vassert(m_orderby_node != NULL);
    #if defined(VOLT_LOG_LEVEL)
    #if VOLT_LOG_LEVEL<=VOLT_LEVEL_TRACE
        const std::vector<AbstractExpression*>& sortExprs = m_orderby_node->getSortExpressions();
        for (int i = 0; i < sortExprs.size(); ++i) {
            VOLT_TRACE("Sort key[%d]:\n%s", i, sortExprs[i]->debug(true).c_str());
        }
    #endif
    #endif

    // pickup an inlined limit, if one exists
    m_limit_node = dynamic_cast<LimitPlanNode*>(merge_receive_node->
                                     getInlinePlanNode(PlanNodeType::Limit));

    // aggregate
    m_agg_exec = voltdb::getInlineAggregateExecutor(merge_receive_node);

    // Create a temp "scratch" table to collect tuples from multiple partitions
    TupleSchema* pre_agg_schema = (m_agg_exec != NULL) ?
        merge_receive_node->allocateTupleSchemaPreAgg() : m_abstractNode->generateTupleSchema();
    std::vector<std::string> column_names(pre_agg_schema->columnCount());
    std::vector<Table*> scratchTable(1);
    scratchTable[0] = TableFactory::buildTempTable("tempInput",
                                                 pre_agg_schema,
                                                 column_names,
                                                 executorVector.limits());
    // Making the scratch table input to the plan node
    // ensures that it will be cleaned up after executions.
    merge_receive_node->setInputTables(scratchTable);

    // The scratch table is owned by the plan node (similar to output temp tables)
    // so it can be freed when the plan is destroyed.
    merge_receive_node->setScratchTable(static_cast<AbstractTempTable*>(scratchTable[0]));
    return true;
}

bool MergeReceiveExecutor::p_execute(const NValueArray &params) {
    int loadedDeps = 0;

    // iterate over dependencies and merge them into the temp table.
    // The assumption is that each dependency result is already sorted.
    int64_t previousTupleCount = 0;
    std::vector<int64_t> partitionTupleCounts;
    Table* inputTempTable = getPlanNode()->getInputTable();
    do {
        loadedDeps = m_engine->loadNextDependency(inputTempTable);
        int64_t currentTupleCount = inputTempTable->activeTupleCount();
        if (currentTupleCount != previousTupleCount) {
            partitionTupleCounts.push_back(currentTupleCount - previousTupleCount);
            previousTupleCount = currentTupleCount;
        }
    } while (loadedDeps > 0);

    // Unload tuples into a vector to be merge-sorted
    VOLT_TRACE("Running MergeReceive '%s'", m_abstractNode->debug().c_str());
    VOLT_TRACE("Input Table PreSort:\n '%s'", inputTempTable->debug().c_str());
    std::vector<TableTuple> xs;
    xs.reserve(inputTempTable->activeTupleCount());

    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);

    //
    // OPTIMIZATION: NESTED LIMIT
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (m_limit_node != NULL) {
        std::tie(limit, offset) = m_limit_node->getLimitAndOffset(params);
    }
    // Init the postfilter to evaluate LIMIT/OFFSET conditions
    CountingPostfilter postfilter(m_tmpOutputTable, NULL, limit, offset);

    TableTuple input_tuple;
    if (m_agg_exec != NULL) {
        VOLT_TRACE("Init inline aggregate...");
        input_tuple = m_agg_exec->p_execute_init(params, &pmp, inputTempTable->schema(), m_tmpOutputTable, &postfilter);
    } else {
        input_tuple = m_tmpOutputTable->tempTuple();
    }


    TableIterator iterator = inputTempTable->iterator();
    while (iterator.next(input_tuple))
    {
        pmp.countdownProgress();
        vassert(input_tuple.isActive());
        xs.push_back(input_tuple);
    }

    // Merge Sort
    AbstractExecutor::TupleComparer comp(m_orderby_node->getSortExpressions(), m_orderby_node->getSortDirections());
    merge_sort(xs, partitionTupleCounts, comp, postfilter, m_agg_exec, m_tmpOutputTable, &pmp);

    VOLT_TRACE("Result of MergeReceive:\n '%s'", m_tmpOutputTable->debug().c_str());

    if (m_agg_exec != NULL) {
        m_agg_exec->p_execute_finish();
    }

    return true;
}

}
