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

#include "mergereceiveexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "plannodes/mergereceivenode.h"
#include "execution/VoltDBEngine.h"
#include "execution/ProgressMonitorProxy.h"
#include "executors/aggregateexecutor.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "plannodes/receivenode.h"
#include "plannodes/orderbynode.h"
#include "plannodes/limitnode.h"

#include <boost/lexical_cast.hpp>

#include <vector>
#include <utility>
#include <algorithm>

namespace voltdb {

namespace {

typedef std::vector<TableTuple>::const_iterator tuple_iterator;
typedef std::pair<tuple_iterator, tuple_iterator> tuple_range;
typedef std::vector<tuple_range>::iterator range_iterator;

struct TupleRangeComparer
{
    TupleRangeComparer(AbstractExecutor::TupleComparer comp) :
        m_comp(comp)
    {}

    bool operator()(const tuple_range& ta, const tuple_range& tb) const
    {
        // Assert both ranges are not empty
        assert(ta.first != ta.second);
        assert(tb.first != tb.second);
        // We need the range with a first min tuple
        return !m_comp(*ta.first, *tb.first);
    }
    AbstractExecutor::TupleComparer m_comp;
};

}

void MergeReceiveExecutor::merge_sort(const std::vector<TableTuple>& tuples,
    std::vector<int64_t>& partitionTupleCounts,
    AbstractExecutor::TupleComparer comp,
    int limit,
    int offset,
    AggregateExecutorBase* agg_exec,
    TempTable* output_table,
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
        tuple_iterator end = begin + partitionTupleCounts[i];
        partitions.push_back(std::make_pair(begin, end));
        begin = end;
        assert( i != nonEmptyPartitions -1 || end == tuples.end());
    }

    // make a heap out of partitions
    TupleRangeComparer tupleRangeComp(comp);
    std::make_heap(partitions.begin(), partitions.end(), tupleRangeComp);

    int tupleCnt = 0;
    int tupleSkipped = 0;

    while ((limit == -1 || tupleCnt < limit) && !partitions.empty()) {
        // Find the range that has the next tuple to be inserted
        // and remove from the heap
        assert(!partitions.empty());
        TableTuple tuple;
        if (partitions.size() == 1) {
            range_iterator rangeIt = partitions.begin();
            if (rangeIt->first != rangeIt->second) {
                tuple = *rangeIt->first;
                ++rangeIt->first;
            } else {
                // the last partition is empty
                return;
            }
        } else {
            // Pop the top of the heap with the minimal tuple
            std::pop_heap(partitions.begin(), partitions.end(), tupleRangeComp);
            tuple_range nextPartition = partitions.back();
            partitions.pop_back();

            // Get the first tuple from that partition
            assert(nextPartition.first != nextPartition.second);
            tuple = *nextPartition.first;

            // advance the partition iterator
            ++nextPartition.first;
            if (nextPartition.first != nextPartition.second) {
                // put the range back to the heap
                partitions.push_back(nextPartition);
                std::push_heap(partitions.begin(), partitions.end(), tupleRangeComp);
            }
        }

        if (tupleSkipped++ < offset) {
            continue;
        }

        if (agg_exec != NULL) {
            if (agg_exec->p_execute_tuple(tuple)) {
                // Get enough rows for LIMIT
                break;
            }
        } else {
            output_table->insertTempTuple(tuple);
        }

        ++tupleCnt;
        if (pmp != NULL) {
            pmp->countdownProgress();
        }
    }
}

MergeReceiveExecutor::MergeReceiveExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node), m_orderby_node(NULL), m_limit_node(NULL),
    m_agg_exec(NULL), m_tmpInputTable()
{ }

bool MergeReceiveExecutor::p_init(AbstractPlanNode* abstract_node,
                             TempTableLimits* limits)
{
    VOLT_TRACE("init MergeReceive Executor");

    MergeReceivePlanNode* merge_receive_node = dynamic_cast<MergeReceivePlanNode*>(abstract_node);
    assert(merge_receive_node != NULL);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    // inline OrderByPlanNode
    m_orderby_node = dynamic_cast<OrderByPlanNode*>(merge_receive_node->
                                     getInlinePlanNode(PLAN_NODE_TYPE_ORDERBY));
    assert(m_orderby_node != NULL);
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
                                     getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));

    // aggregate
    m_agg_exec = voltdb::getInlineAggregateExecutor(merge_receive_node);

    // Create a temp table to collect tuples from multiple partitions
    TupleSchema* pre_agg_schema = (m_agg_exec != NULL) ?
        merge_receive_node->allocateTupleSchemaPreAgg() : m_abstractNode->generateTupleSchema();
    std::vector<std::string> column_names(pre_agg_schema->columnCount());
    m_tmpInputTable.reset(TableFactory::getTempTable(m_abstractNode->databaseId(),
                                                         "tempInput",
                                                         m_abstractNode->generateTupleSchema(),
                                                         column_names,
                                                         limits));
    return true;
}

bool MergeReceiveExecutor::p_execute(const NValueArray &params) {
    int loadedDeps = 0;

    // iterate over dependencies and merge them into the temp table.
    // The assumption is that each dependency result is already sorted.
    int64_t previousTupleCount = 0;
    std::vector<int64_t> partitionTupleCounts;
    do {
        loadedDeps = m_engine->loadNextDependency(m_tmpInputTable.get());
        int64_t currentTupleCount = m_tmpInputTable->activeTupleCount();
        if (currentTupleCount != previousTupleCount) {
            partitionTupleCounts.push_back(currentTupleCount - previousTupleCount);
            previousTupleCount = currentTupleCount;
        }
    } while (loadedDeps > 0);

    // Unload tuples into a vector to be merge-sorted
    VOLT_TRACE("Running MergeReceive '%s'", m_abstractNode->debug().c_str());
    VOLT_TRACE("Input Table PreSort:\n '%s'", m_tmpInputTable->debug().c_str());
    std::vector<TableTuple> xs;
    xs.reserve(m_tmpInputTable->activeTupleCount());

    ProgressMonitorProxy pmp(m_engine, this);

    TableTuple input_tuple;
    if (m_agg_exec != NULL) {
        VOLT_TRACE("Init inline aggregate...");
        input_tuple = m_agg_exec->p_execute_init(params, &pmp, m_tmpInputTable->schema(), m_tmpOutputTable);
    } else {
        input_tuple = m_tmpOutputTable->tempTuple();
    }


    TableIterator iterator = m_tmpInputTable->iterator();
    while (iterator.next(input_tuple))
    {
        pmp.countdownProgress();
        assert(input_tuple.isActive());
        xs.push_back(input_tuple);
    }

    //
    // OPTIMIZATION: NESTED LIMIT
    int limit = -1;
    int offset = 0;
    if (m_limit_node != NULL) {
        m_limit_node->getLimitAndOffsetByReference(params, limit, offset);
    }

    // Merge Sort
    AbstractExecutor::TupleComparer comp(m_orderby_node->getSortExpressions(), m_orderby_node->getSortDirections());
    merge_sort(xs, partitionTupleCounts, comp, limit, offset, m_agg_exec, m_tmpOutputTable, &pmp);

    VOLT_TRACE("Result of MergeReceive:\n '%s'", m_tmpOutputTable->debug().c_str());

    if (m_agg_exec != NULL) {
        m_agg_exec->p_execute_finish();
    }

    cleanupInputTempTable(m_tmpInputTable.get());

    return true;
}

}
