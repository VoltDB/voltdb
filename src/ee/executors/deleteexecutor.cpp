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

#include "deleteexecutor.h"

#include "common/ExecuteWithMpMemory.h"

#include "indexes/tableindex.h"
#include "storage/persistenttable.h"
#include "storage/tableutil.h"

namespace voltdb {
int64_t DeleteExecutor::s_modifiedTuples;

bool DeleteExecutor::p_init(AbstractPlanNode *abstract_node, const ExecutorVector& executorVector) {
    VOLT_TRACE("init Delete Executor");

    m_node = dynamic_cast<DeletePlanNode*>(abstract_node);
    vassert(m_node);

    setDMLCountOutputTable(executorVector.limits());

    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
    vassert(targetTable);
    m_replicatedTableOperation = targetTable->isReplicatedTable();

    m_truncate = m_node->getTruncate();
    if (m_truncate) {
        vassert(m_node->getInputTableCount() == 0);
        return true;
    }

    vassert(m_node->getInputTableCount() == 1);
    m_inputTable = dynamic_cast<AbstractTempTable*>(m_node->getInputTable()); //input table should be temptable
    vassert(m_inputTable);

    m_inputTuple = TableTuple(m_inputTable->schema());
    return true;
}

bool DeleteExecutor::p_execute(const NValueArray &params) {
    // target table should be persistenttable
    // update target table reference from table delegate
    // Note that the target table pointer in the node's tcd can change between p_init and p_execute
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
    vassert(targetTable);
    TableTuple targetTuple(targetTable->schema());

    int64_t modified_tuples = 0;

    {
        vassert(targetTable->isReplicatedTable() ==
                (m_replicatedTableOperation || SynchronizedThreadLock::isInSingleThreadMode()));
        ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory(
                m_replicatedTableOperation, m_engine->isLowestSite(),
                []() { s_modifiedTuples = -1l; });
        if (possiblySynchronizedUseMpMemory.okToExecute()) {
            if (m_truncate) {
                VOLT_TRACE("truncating table %s...", targetTable->name().c_str());
                // count the truncated tuples as deleted
                modified_tuples = targetTable->visibleTupleCount();

                VOLT_TRACE("Delete all rows from table : %s with %d active, %d visible, %d allocated",
                           targetTable->name().c_str(),
                           (int)targetTable->activeTupleCount(),
                           (int)targetTable->visibleTupleCount(),
                           (int)targetTable->allocatedTupleCount());

                // empty the table either by table swap or iteratively deleting tuple-by-tuple
                targetTable->truncateTable(m_engine);
            } else {
                vassert(m_inputTable);
                vassert(m_inputTuple.columnCount() == m_inputTable->columnCount());
                vassert(targetTuple.columnCount() == targetTable->columnCount());
                TableIterator inputIterator = m_inputTable->iterator();
                while (inputIterator.next(m_inputTuple)) {
                    //
                    // OPTIMIZATION: Single-Sited Query Plans
                    // If our beloved DeletePlanNode is apart of a single-site query plan,
                    // then the first column in the input table will be the address of a
                    // tuple on the target table that we will want to blow away. This saves
                    // us the trouble of having to do an index lookup
                    //
                    void *targetAddress = m_inputTuple.getNValue(0).castAsAddress();
                    targetTuple.move(targetAddress);

                    // Delete from target table
                    targetTable->deleteTuple(targetTuple, true);
                }
                modified_tuples = m_inputTable->tempTableTupleCount();
                VOLT_TRACE("Deleted %d rows from table : %s with %d active, %d visible, %d allocated",
                           (int)modified_tuples,
                           targetTable->name().c_str(),
                           (int)targetTable->activeTupleCount(),
                           (int)targetTable->visibleTupleCount(),
                           (int)targetTable->allocatedTupleCount());

            }
            if (m_replicatedTableOperation) {
                s_modifiedTuples = modified_tuples;
            }
        } else if (s_modifiedTuples == -1) {
            // An exception was thrown on the lowest site thread and we need to throw here as well so
            // all threads are in the same state
            throwSerializableTypedEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE,
                    "Replicated table delete threw an unknown exception on other thread for table %s",
                    targetTable->name().c_str());
        }
    }
    if (m_replicatedTableOperation) {
        // Use the static value assigned above to propagate the result to the other engines
        // that skipped the replicated table work
        modified_tuples = s_modifiedTuples;
    }
    TableTuple& count_tuple = m_node->getOutputTable()->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(modified_tuples));
    // try to put the tuple into the output table
    if (!m_node->getOutputTable()->insertTuple(count_tuple)) {
        VOLT_ERROR("Failed to insert tuple count (%ld) into output table '%s'",
                   static_cast<long int>(modified_tuples), m_node->getOutputTable()->name().c_str());
        return false;
    } else {
        m_engine->addToTuplesModified(modified_tuples);
        return true;
    }
}

} // end namespace voltdb
