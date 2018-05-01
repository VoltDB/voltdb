/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#include "swaptablesexecutor.h"

#include "common/ExecuteWithMpMemory.h"
#include "plannodes/swaptablesnode.h"

using namespace std;
using namespace voltdb;
int64_t SwapTablesExecutor::s_modifiedTuples;

bool SwapTablesExecutor::p_init(AbstractPlanNode* abstract_node,
                                const ExecutorVector& executorVector)
{
    VOLT_TRACE("init SwapTable Executor");
    SwapTablesPlanNode* node = dynamic_cast<SwapTablesPlanNode*>(m_abstractNode);
#ifndef NDEBUG
    assert(node);
    assert(node->getTargetTable());
    assert(node->getOtherTargetTable());
    assert(node->getInputTableCount() == 0);
#endif

    m_replicatedTableOperation = static_cast<PersistentTable*>(node->getTargetTable())->isCatalogTableReplicated();
    setDMLCountOutputTable(executorVector.limits());
    return true;
}

bool SwapTablesExecutor::p_execute(NValueArray const& params) {
    // target tables should be persistent tables
    // update target table references from table delegates
    SwapTablesPlanNode* node = static_cast<SwapTablesPlanNode*>(m_abstractNode);
    assert(dynamic_cast<SwapTablesPlanNode*>(m_abstractNode));
    PersistentTable* theTargetTable = static_cast<PersistentTable*>(node->getTargetTable());
    assert(theTargetTable);
    assert(theTargetTable == dynamic_cast<PersistentTable*>(node->getTargetTable()));
    PersistentTable* otherTargetTable = node->getOtherTargetTable();
    assert(otherTargetTable);

    int64_t modified_tuples = 0;

    VOLT_TRACE("swap tables %s and %s",
               theTargetTable->name().c_str(),
               otherTargetTable->name().c_str());
    {
        assert(m_replicatedTableOperation == theTargetTable->isCatalogTableReplicated());
        ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory(
                m_replicatedTableOperation, m_engine->isLowestSite(), &s_modifiedTuples, int64_t(-1));
        if (possiblySynchronizedUseMpMemory.okToExecute()) {
            // count the active tuples in both tables as modified
            modified_tuples = theTargetTable->visibleTupleCount() +
                    otherTargetTable->visibleTupleCount();

            VOLT_TRACE("Swap Tables: %s with %d active, %d visible, %d allocated"
                       " and %s with %d active, %d visible, %d allocated",
                       theTargetTable->name().c_str(),
                       (int)theTargetTable->activeTupleCount(),
                       (int)theTargetTable->visibleTupleCount(),
                       (int)theTargetTable->allocatedTupleCount(),
                       otherTargetTable->name().c_str(),
                       (int)otherTargetTable->activeTupleCount(),
                       (int)otherTargetTable->visibleTupleCount(),
                       (int)otherTargetTable->allocatedTupleCount());

            // Swap the table catalog delegates and corresponding indexes and views.
            theTargetTable->swapTable(otherTargetTable,
                                      node->theIndexes(),
                                      node->otherIndexes());
            s_modifiedTuples = modified_tuples;
        }
        else {
            if (s_modifiedTuples == -1) {
                // An exception was thrown on the lowest site thread and we need to throw here as well so
                // all threads are in the same state
                char msg[1024];
                snprintf(msg, 1024, "Replicated table swap threw an unknown exception on other thread for table %s",
                        theTargetTable->name().c_str());
                VOLT_DEBUG("%s", msg);
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE, msg);
            }
        }
    }
    if (m_replicatedTableOperation) {
        // Use the static value assigned above to propagate the result to the other engines
        // that skipped the replicated table work
        modified_tuples = s_modifiedTuples;
    }
    TableTuple& count_tuple = m_tmpOutputTable->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(modified_tuples));
    // try to put the tuple into the output table
    m_tmpOutputTable->insertTempTuple(count_tuple);
    m_engine->addToTuplesModified(modified_tuples);
    return true;
}
