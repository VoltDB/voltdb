/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include <iostream>
#include "seqscanexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
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
    assert(node->getTargetTable());

    //
    // OPTIMIZATION: If there is no predicate for this SeqScan,
    // then we want to just set our OutputTable pointer to be the
    // pointer of our TargetTable. This prevents us from just
    // reading through the entire TargetTable and copying all of
    // the tuples. We are guarenteed that no Executor will ever
    // modify an input table, so this operation is safe
    //
    if (!this->needsOutputTableClear())
    {
        node->setOutputTable(node->getTargetTable());
    }
    //
    // Otherwise create a new temp table that mirrors the
    // output schema specified in the plan (which should mirror
    // the output schema for any inlined projection)
    //
    else
    {
        // Create output table based on output schema from the plan
        setTempOutputTable(limits, node->getTargetTable()->name());
    }
    return true;
}

bool SeqScanExecutor::needsOutputTableClear() {
    // clear the temporary output table only when it has a predicate.
    // if it doesn't have a predicate, it's the original persistent table
    // and we don't have to (and must not) clear it.
    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(m_abstractNode);
    assert(node);
    return node->needsOutputTableClear();
}

bool SeqScanExecutor::p_execute(const NValueArray &params) {
    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(m_abstractNode);
    assert(node);
    Table* output_table = node->getOutputTable();
    assert(output_table);
    Table* target_table = dynamic_cast<Table*>(node->getTargetTable());
    assert(target_table);
    //cout << "SeqScanExecutor: node id" << node->getPlanNodeId() << endl;
    VOLT_TRACE("Sequential Scanning table :\n %s",
               target_table->debug().c_str());
    VOLT_DEBUG("Sequential Scanning table : %s which has %d active, %d"
               " allocated",
               target_table->name().c_str(),
               (int)target_table->activeTupleCount(),
               (int)target_table->allocatedTupleCount());

    //
    // OPTIMIZATION: NESTED PROJECTION
    //
    // Since we have the input params, we need to call substitute to
    // change any nodes in our expression tree to be ready for the
    // projection operations in execute
    //
    int num_of_columns = (int)output_table->columnCount();
    ProjectionPlanNode* projection_node = dynamic_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
    if (projection_node != NULL) {
        for (int ctr = 0; ctr < num_of_columns; ctr++) {
            assert(projection_node->getOutputColumnExpressions()[ctr]);
            projection_node->getOutputColumnExpressions()[ctr]->substitute(params);
        }
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
        limit_node != NULL)
    {
        //
        // Just walk through the table using our iterator and apply
        // the predicate to each tuple. For each tuple that satisfies
        // our expression, we'll insert them into the output table.
        //
        TableTuple tuple(target_table->schema());
        TableIterator iterator = target_table->iterator();
        AbstractExpression *predicate = node->getPredicate();

        if (predicate)
        {
            VOLT_TRACE("SCAN PREDICATE A:\n%s\n", predicate->debug(true).c_str());
            predicate->substitute(params);
            assert(predicate != NULL);
            VOLT_DEBUG("SCAN PREDICATE B:\n%s\n",
                       predicate->debug(true).c_str());
        }

        int limit = -1;
        int offset = -1;
        if (limit_node) {
            limit_node->getLimitAndOffsetByReference(params, limit, offset);
        }

        int tuple_ctr = 0;
        int tuple_skipped = 0;
        while ((limit == -1 || tuple_ctr < limit) && iterator.next(tuple))
        {
            VOLT_TRACE("INPUT TUPLE: %s, %d/%d\n",
                       tuple.debug(target_table->name()).c_str(), tuple_ctr,
                       (int)target_table->activeTupleCount());
            //
            // For each tuple we need to evaluate it against our predicate
            //
            if (predicate == NULL || predicate->eval(&tuple, NULL).isTrue())
            {
                // Check if we have to skip this tuple because of offset
                if (tuple_skipped < offset) {
                    tuple_skipped++;
                    continue;
                }
                ++tuple_ctr;

                //
                // Nested Projection
                // Project (or replace) values from input tuple
                //
                if (projection_node != NULL)
                {
                    TableTuple &temp_tuple = output_table->tempTuple();
                    for (int ctr = 0; ctr < num_of_columns; ctr++)
                    {
                        NValue value =
                            projection_node->
                          getOutputColumnExpressions()[ctr]->eval(&tuple, NULL);
                        temp_tuple.setNValue(ctr, value);
                    }
                    if (!output_table->insertTuple(temp_tuple))
                    {
                        VOLT_ERROR("Failed to insert tuple from table '%s' into"
                                   " output table '%s'",
                                   target_table->name().c_str(),
                                   output_table->name().c_str());
                        return false;
                    }
                }
                else
                {
                    //
                    // Insert the tuple into our output table
                    //
                    if (!output_table->insertTuple(tuple)) {
                        VOLT_ERROR("Failed to insert tuple from table '%s' into"
                                   " output table '%s'",
                                   target_table->name().c_str(),
                                   output_table->name().c_str());
                        return false;
                    }
                }
            }
        }
    }
    VOLT_TRACE("\n%s\n", output_table->debug().c_str());
    VOLT_DEBUG("Finished Seq scanning");

    return true;
}
