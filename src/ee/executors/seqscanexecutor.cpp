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

#include "seqscanexecutor.h"
#include "executors/aggregateexecutor.h"
#include "executors/insertexecutor.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/insertnode.h"
#include "plannodes/seqscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"

using namespace voltdb;

bool SeqScanExecutor::p_init(AbstractPlanNode* abstract_node,
                             const ExecutorVector& executorVector)
{
    VOLT_TRACE("init SeqScan Executor");

    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(abstract_node);
    vassert(node);

    // persistent table scan node must have a target table
    vassert(!node->isPersistentTableScan() || node->getTargetTable());

    // Subquery scans must have a child that produces the output to scan
    vassert(!node->isSubqueryScan() || (node->getChildren().size() == 1));

    // In the case of CTE scans, we will resolve target table below.
    vassert(!node->isCteScan() || (node->getChildren().size() == 0
                                   && node->getTargetTable() == NULL));

    // Inline aggregation can be serial, partial or hash
    m_aggExec = voltdb::getInlineAggregateExecutor(node);
    m_insertExec = voltdb::getInlineInsertExecutor(node);
    // For the moment we will not produce a plan with both an
    // inline aggregate and an inline insert node.  This just
    // confuses things.
    vassert(m_aggExec == NULL || m_insertExec == NULL);

    //
    // OPTIMIZATION: If there is no predicate for this SeqScan,
    // then we want to just set our OutputTable pointer to be the
    // pointer of our TargetTable. This prevents us from just
    // reading through the entire TargetTable and copying all of
    // the tuples. We are guarenteed that no Executor will ever
    // modify an input table, so this operation is safe
    //
    if (node->getPredicate() != NULL || node->getInlinePlanNodes().size() > 0 || node->isCteScan()) {
        // TODO: can this optimization be performed for CTE scans?
        if (m_insertExec) {
            setDMLCountOutputTable(executorVector.limits());
        }
        else {
            // Create output table based on output schema from the plan.
            std::string temp_name = (node->isSubqueryScan()) ?
                node->getChildren()[0]->getOutputTable()->name():
                node->getTargetTableName();
            setTempOutputTable(executorVector, temp_name);
        }
    }
    else {
        // Otherwise create a new temp table that mirrors the output
        // schema specified in the plan (which should mirror the
        // output schema for any inlined projection)
        node->setOutputTable(node->isSubqueryScan() ?
                             node->getChildren()[0]->getOutputTable() :
                             node->getTargetTable());
    }

    return true;
}

bool SeqScanExecutor::p_execute(const NValueArray &params) {
    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(m_abstractNode);
    vassert(node);


    // Short-circuit an empty scan
    if (node->isEmptyScan()) {
        VOLT_DEBUG ("Empty Seq Scan :\n %s", node->getOutputTable()->debug().c_str());
        return true;
    }

    Table* input_table = NULL;
    if (node->isCteScan()) {
        ExecutorContext* ec = ExecutorContext::getExecutorContext();
        input_table = ec->getCommonTable(node->getTargetTableName(),
                                         node->getCteStmtId());
    }
    else if (node->isSubqueryScan()) {
        input_table = node->getChildren()[0]->getOutputTable();
    }
    else {
        vassert(node->isPersistentTableScan());
        input_table = node->getTargetTable();
    }

    vassert(input_table);

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
    ProjectionPlanNode* projectionNode = dynamic_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PlanNodeType::Projection));
    if (projectionNode != NULL) {
        num_of_columns = static_cast<int> (projectionNode->getOutputColumnExpressions().size());
    }
    //
    // OPTIMIZATION: NESTED LIMIT
    // How nice! We can also cut off our scanning with a nested limit!
    //
    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PlanNodeType::Limit));

    //
    // OPTIMIZATION:
    //
    // If there is no predicate and no Projection for this SeqScan,
    // then we have already set the node's OutputTable to just point
    // at the TargetTable. Therefore, there is nothing we more we need
    // to do here
    //
    if (node->getPredicate() != NULL || projectionNode != NULL ||
        limit_node != NULL || m_aggExec != NULL || m_insertExec != NULL ||
        node->isCteScan())
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
            std::tie(limit, offset) = limit_node->getLimitAndOffset(params);
        }
        // Initialize the postfilter
        CountingPostfilter postfilter(m_tmpOutputTable, predicate, limit, offset);

        ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
        TableTuple temp_tuple;
        vassert(m_tmpOutputTable);
        if (m_aggExec != NULL || m_insertExec != NULL) {
            const TupleSchema * inputSchema = input_table->schema();
            if (projectionNode != NULL) {
                inputSchema = projectionNode->getOutputTable()->schema();
            }
            if (m_aggExec != NULL) {
                temp_tuple = m_aggExec->p_execute_init(params, &pmp,
                        inputSchema, m_tmpOutputTable, &postfilter);
            }
            else {
                // We may actually find out during initialization
                // that we are done.  The p_execute_init operation
                // will tell us by returning false if so.  See the
                // definition of InsertExecutor::p_execute_init.
                //
                // We know we have an inline insert here.  So there
                // must have been an insert into select.  The input
                // schema is the schema of the output of the select
                // statement.  The inline projection wants to
                // project the columns of the scanned table onto
                // the select columns.
                //
                // Now, we don't have a table between the inline projection
                // and the inline insert - that's why they are
                // inlined.  The p_execute_init function will compute
                // this and tell us by setting temp_tuple.  Note
                // that temp_tuple is initialized if this returns
                // false.  If it returns true all bets are off.
                if (!m_insertExec->p_execute_init(inputSchema, m_tmpOutputTable, temp_tuple)) {
                    return true;
                }
                // We should have as many expressions in the
                // projection node as there are columns in the
                // input schema if there is an inline projection.
                vassert(projectionNode != NULL
                          ? (temp_tuple.getSchema()->columnCount() == projectionNode->getOutputColumnExpressions().size())
                          : true);
            }
        }
        else {
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
                if (projectionNode != NULL)
                {
                    VOLT_TRACE("inline projection...");
                    // Project the scanned table row onto
                    // the columns of the select list in the
                    // select statement.
                    for (int ctr = 0; ctr < num_of_columns; ctr++) {
                        NValue value = projectionNode->getOutputColumnExpressions()[ctr]->eval(&tuple, NULL);
                        temp_tuple.setNValue(ctr, value);
                    }
                    outputTuple(temp_tuple);
                }
                else
                {
                    outputTuple(tuple);
                }
                pmp.countdownProgress();
            }
        } // end while we have more tuples to scan

        if (m_aggExec != NULL) {
            m_aggExec->p_execute_finish();
        }
        else if (m_insertExec != NULL) {
            m_insertExec->p_execute_finish();
        }
    }
    //* for debug */std::cout << "SeqScanExecutor: node id " << node->getPlanNodeId() <<
    //* for debug */    " output table " << (void*)output_table <<
    //* for debug */    " put " << output_table->activeTupleCount() << " tuples " << std::endl;
    VOLT_TRACE("\n%s\n", node->getOutputTable()->debug().c_str());
    VOLT_DEBUG("Finished Seq scanning");

    return true;
}

/*
 * We may output a tuple to an inline aggregate or
 * inline insert node.  If there is a limit or projection, this will have
 * been applied already.  So we don't really care about those here.
 */
void SeqScanExecutor::outputTuple(TableTuple& tuple) {
    if (m_aggExec != NULL) {
        m_aggExec->p_execute_tuple(tuple);
        return;
    }
    else if (m_insertExec != NULL) {
        m_insertExec->p_execute_tuple(tuple);
        return;
    }
    //
    // Insert the tuple into our output table
    //
    vassert(m_tmpOutputTable);
    m_tmpOutputTable->insertTempTuple(tuple);
}
