/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "seqscanexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "executors/aggregateexecutor.h"
#include "executors/executorutil.h"
#include "execution/ProgressMonitorProxy.h"
#include "expressions/abstractexpression.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/seqscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"

using namespace voltdb;

bool SeqScanExecutor::p_init(AbstractPlanNode* abstract_node,
                             TempTableLimits* limits)
{
    VOLT_TRACE("init SeqScan Executor");

    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(abstract_node);
    assert(node);
    bool isSubquery = node->isSubQuery();
    assert(isSubquery || node->getTargetTable());
    assert((! isSubquery) || (node->getChildren().size() == 1));

    //
    // OPTIMIZATION: If there is no predicate for this SeqScan,
    // then we want to just set our OutputTable pointer to be the
    // pointer of our TargetTable. This prevents us from just
    // reading through the entire TargetTable and copying all of
    // the tuples. We are guarenteed that no Executor will ever
    // modify an input table, so this operation is safe
    //
    if (node->getPredicate() != NULL || node->getInlinePlanNodes().size() > 0) {
        // Create output table based on output schema from the plan
        const std::string& temp_name = (node->isSubQuery()) ?
                node->getChildren()[0]->getOutputTable()->name():
                node->getTargetTable()->name();
        setTempOutputTable(limits, temp_name);
    }
    //
    // Otherwise create a new temp table that mirrors the
    // output schema specified in the plan (which should mirror
    // the output schema for any inlined projection)
    //
    else {
        node->setOutputTable(isSubquery ?
                             node->getChildren()[0]->getOutputTable() :
                             node->getTargetTable());
    }

    // Inline aggregation can be serial, partial or hash
    m_aggExec = voltdb::getInlineAggregateExecutor(node);

    return true;
}

bool SeqScanExecutor::p_execute(const NValueArray &params) {
    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(m_abstractNode);
    assert(node);

    // Short-circuit an empty scan
    if (node->isEmptyScan()) {
        VOLT_DEBUG ("Empty Seq Scan :\n %s", node->getOutputTable()->debug().c_str());
        return true;
    }

    Table* input_table = (node->isSubQuery()) ?
            node->getChildren()[0]->getOutputTable():
            node->getTargetTable();

    assert(input_table);

    //* for debug */std::cout << "SeqScanExecutor: node id " << node->getPlanNodeId() <<
    //* for debug */    " input table " << (void*)input_table <<
    //* for debug */    " has " << input_table->activeTupleCount() << " tuples " << std::endl;
    VOLT_TRACE("Sequential Scanning table :\n %s",
               input_table->debug().c_str());
    VOLT_DEBUG("Sequential Scanning table : %s which has %d active, %d"
               " allocated",
               input_table->name().c_str(),
               (int)input_table->activeTupleCount(),
               (int)input_table->allocatedTupleCount());

    //
    // OPTIMIZATION: NESTED PROJECTION
    //
    // Since we have the input params, we need to call substitute to
    // change any nodes in our expression tree to be ready for the
    // projection operations in execute
    //
    int num_of_columns = -1;
    ProjectionPlanNode* projection_node = dynamic_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
    if (projection_node != NULL) {
        num_of_columns = static_cast<int> (projection_node->getOutputColumnExpressions().size());
    }
    //
    // OPTIMIZATION: NESTED LIMIT
    // How nice! We can also cut off our scanning with a nested limit!
    //
    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));

    //
    // OPTIMIZATION:
    //
    // If there is no predicate and no Projection for this SeqScan,
    // then we have already set the node's OutputTable to just point
    // at the TargetTable. Therefore, there is nothing we more we need
    // to do here
    //
    if (node->getPredicate() != NULL || projection_node != NULL ||
        limit_node != NULL || m_aggExec != NULL)
    {
        //
        // Just walk through the table using our iterator and apply
        // the predicate to each tuple. For each tuple that satisfies
        // our expression, we'll insert them into the output table.
        //
        TableTuple tuple(input_table->schema());
        TableIterator iterator = input_table->iteratorDeletingAsWeGo();
        AbstractExpression *predicate = node->getPredicate();

        if (predicate)
        {
            VOLT_TRACE("SCAN PREDICATE :\n%s\n", predicate->debug(true).c_str());
        }

        int limit = CountingPostfilter::NO_LIMIT;
        int offset = CountingPostfilter::NO_OFFSET;
        if (limit_node) {
            limit_node->getLimitAndOffsetByReference(params, limit, offset);
        }
        // Initialize the postfilter
        CountingPostfilter postfilter(m_tmpOutputTable, predicate, limit, offset);

        ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
        TableTuple temp_tuple;
        assert(m_tmpOutputTable);
        if (m_aggExec != NULL) {
            const TupleSchema * inputSchema = input_table->schema();
            if (projection_node != NULL) {
                inputSchema = projection_node->getOutputTable()->schema();
            }
            temp_tuple = m_aggExec->p_execute_init(params, &pmp,
                    inputSchema, m_tmpOutputTable, &postfilter);
        } else {
            temp_tuple = m_tmpOutputTable->tempTuple();
        }

        while (postfilter.isUnderLimit() && iterator.next(tuple))
        {
#if   defined(VOLT_TRACE_ENABLED)
            int tuple_ctr = 0;
#endif
            VOLT_TRACE("INPUT TUPLE: %s, %d/%d\n",
                       tuple.debug(input_table->name()).c_str(),
                       ++tuple_ctr,
                       (int)input_table->activeTupleCount());
            pmp.countdownProgress();

            //
            // For each tuple we need to evaluate it against our predicate and limit/offset
            //
            if (postfilter.eval(&tuple, NULL))
            {
                //
                // Nested Projection
                // Project (or replace) values from input tuple
                //
                if (projection_node != NULL)
                {
                    VOLT_TRACE("inline projection...");
                    for (int ctr = 0; ctr < num_of_columns; ctr++) {
                        NValue value = projection_node->getOutputColumnExpressions()[ctr]->eval(&tuple, NULL);
                        temp_tuple.setNValue(ctr, value);
                    }
                    outputTuple(postfilter, temp_tuple);
                }
                else
                {
                    outputTuple(postfilter, tuple);
                }
                pmp.countdownProgress();
            }
        }

        if (m_aggExec != NULL) {
            m_aggExec->p_execute_finish();
        }
    }
    //* for debug */std::cout << "SeqScanExecutor: node id " << node->getPlanNodeId() <<
    //* for debug */    " output table " << (void*)output_table <<
    //* for debug */    " put " << output_table->activeTupleCount() << " tuples " << std::endl;
    VOLT_TRACE("\n%s\n", node->getOutputTable()->debug().c_str());
    VOLT_DEBUG("Finished Seq scanning");

    return true;
}

void SeqScanExecutor::outputTuple(CountingPostfilter& postfilter, TableTuple& tuple) {
    if (m_aggExec != NULL) {
        m_aggExec->p_execute_tuple(tuple);
        return;
    }
    //
    // Insert the tuple into our output table
    //
    assert(m_tmpOutputTable);
    m_tmpOutputTable->insertTempTuple(tuple);
}
