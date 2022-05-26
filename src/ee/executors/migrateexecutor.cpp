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

#include <common/debuglog.h>
#include <boost/scoped_ptr.hpp>
#include <boost/foreach.hpp>

#include "migrateexecutor.h"

#include "common/ExecuteWithMpMemory.h"
#include "plannodes/updatenode.h"
#include "plannodes/projectionnode.h"
#include "storage/tablefactory.h"
#include "indexes/tableindex.h"
#include "storage/tableutil.h"
#include "storage/ConstraintFailureException.h"


namespace voltdb {
int64_t MigrateExecutor::s_modifiedTuples;

bool MigrateExecutor::p_init(AbstractPlanNode* abstract_node,
                            const ExecutorVector& executorVector)
{
    VOLT_TRACE("init Migrate Executor");

    m_node = dynamic_cast<MigratePlanNode*>(abstract_node);
    vassert(m_node);
    vassert(m_node->getInputTableCount() == 1);
    // input table should be temptable
    m_inputTable = dynamic_cast<AbstractTempTable*>(m_node->getInputTable());
    vassert(m_inputTable);

    // target table should be persistenttable
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
    vassert(targetTable);

    setDMLCountOutputTable(executorVector.limits());

    AbstractPlanNode *child = m_node->getChildren()[0];
    if (NULL == child) {
        VOLT_ERROR("Attempted to initialize migrate executor with NULL child");
        return false;
    }

    m_inputTuple = TableTuple(m_inputTable->schema());

    // for target table related info.
    m_partitionColumn = targetTable->partitionColumn();

    // for shared replicated table special handling
    m_replicatedTableOperation = targetTable->isReplicatedTable();
    return true;
}

bool MigrateExecutor::p_execute(const NValueArray &params) {
    vassert(m_inputTable);

    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
    vassert(targetTable);

    TableTuple targetTuple = TableTuple(targetTable->schema());

    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug("").c_str());
    VOLT_TRACE("TARGET TABLE - BEFORE: %s\n", targetTable->debug("").c_str());

    int64_t migrated_tuples = 0;
    {
        vassert(m_replicatedTableOperation == targetTable->isReplicatedTable());
        ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory(
                m_replicatedTableOperation, m_engine->isLowestSite(),
                []() { s_modifiedTuples = -1l; });
        if (possiblySynchronizedUseMpMemory.okToExecute()) {
            std::vector<TableIndex*> indexesToUpdate;
            const std::vector<TableIndex*>& allIndexes = targetTable->allIndexes();

            // Only care about the indexes associated with the hidden column for migrate
            BOOST_FOREACH(TableIndex *index, allIndexes) {
                if (index->isMigratingIndex()) {
                    indexesToUpdate.push_back(index);
                }
            }

            vassert(m_inputTuple.columnCount() == m_inputTable->columnCount());
            vassert(targetTuple.columnCount() == targetTable->columnCount());
            TableIterator input_iterator = m_inputTable->iterator();
            while (input_iterator.next(m_inputTuple)) {
                // The first column in the input table will be the address of a
                // tuple to update in the target table.
                void *target_address = m_inputTuple.getNValue(0).castAsAddress();
                targetTuple.move(target_address);

                if (targetTuple.getHiddenNValue(targetTable->getMigrateColumnIndex()).isNull()) {
                    TableTuple &tempTuple = targetTable->copyIntoTempTuple(targetTuple);
                    targetTable->updateTupleWithSpecificIndexes(targetTuple, tempTuple,
                                                                indexesToUpdate, true, false, true);
                    migrated_tuples++;
                }
            }
            if (m_replicatedTableOperation) {
                s_modifiedTuples = migrated_tuples;
            }
        }
        else {
            if (s_modifiedTuples == -1) {
                // An exception was thrown on the lowest site thread and we need to throw here as well so
                // all threads are in the same state
                throwSerializableTypedEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE,
                        "Replicated table update threw an unknown exception on other thread for table %s",
                        targetTable->name().c_str());
            }
        }
    }
    if (m_replicatedTableOperation) {
        // Use the static value assigned above to propagate the result to the other engines
        // that skipped the replicated table work
        migrated_tuples = s_modifiedTuples;
    }
    TableTuple& count_tuple = m_node->getOutputTable()->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(migrated_tuples));
    // try to put the tuple into the output table
    m_node->getOutputTable()->insertTuple(count_tuple);

    VOLT_TRACE("TARGET TABLE - AFTER: %s\n", targetTable->debug("").c_str());
    // add to the planfragments count of modified tuples
    m_engine->addToTuplesModified(m_inputTable->tempTableTupleCount());

    return true;
}

} // end namespace voltdb
