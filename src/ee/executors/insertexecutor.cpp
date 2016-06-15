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

#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/types.h"
#include "execution/VoltDBEngine.h"
#include "expressions/functionexpression.h"
#include "insertexecutor.h"
#include "plannodes/insertnode.h"
#include "storage/ConstraintFailureException.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"

#include <vector>
#include <set>

using namespace std;
using namespace voltdb;

bool InsertExecutor::p_init(AbstractPlanNode* abstractNode,
                            TempTableLimits* limits)
{
    VOLT_TRACE("init Insert Executor");

    m_node = dynamic_cast<InsertPlanNode*>(abstractNode);
    assert(m_node);
    assert(m_node->getTargetTable());
    assert(m_node->getInputTableCount() == 1);

    Table* targetTable = m_node->getTargetTable();
    m_isUpsert = m_node->isUpsert();

    setDMLCountOutputTable(limits);

    m_inputTable = dynamic_cast<TempTable*>(m_node->getInputTable()); //input table should be temptable
    assert(m_inputTable);

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    PersistentTable *persistentTarget = dynamic_cast<PersistentTable*>(targetTable);
    m_partitionColumn = -1;
    StreamedTable *streamTarget = dynamic_cast<StreamedTable*>(targetTable);
    m_hasStreamView = false;
    if (streamTarget != NULL) {
        m_isStreamed = true;
        //See if we have any views.
        m_hasStreamView = streamTarget->hasViews();
        m_partitionColumn = streamTarget->partitionColumn();
    }
    if (m_isUpsert) {
        VOLT_TRACE("init Upsert Executor actually");
        if (m_isStreamed) {
            VOLT_ERROR("UPSERT is not supported for Stream table %s", targetTable->name().c_str());
        }
        // look up the tuple whether it exists already
        if (persistentTarget->primaryKeyIndex() == NULL) {
            VOLT_ERROR("No primary keys were found in our target table '%s'",
                    targetTable->name().c_str());
        }
    }

    if (persistentTarget) {
        m_partitionColumn = persistentTarget->partitionColumn();
    }

    m_multiPartition = m_node->isMultiPartition();

    m_sourceIsPartitioned = m_node->sourceIsPartitioned();

    // allocate memory for template tuple, set defaults for all columns
    m_templateTuple.init(targetTable->schema());


    TableTuple tuple = m_templateTuple.tuple();

    std::set<int> fieldsExplicitlySet(m_node->getFieldMap().begin(), m_node->getFieldMap().end());
    // These default values are used for an INSERT including the INSERT sub-case of an UPSERT.
    // The defaults are purposely ignored in favor of existing column values
    // for the UPDATE subcase of an UPSERT.
    m_node->initTupleWithDefaultValues(m_engine,
                                       &m_memoryPool,
                                       fieldsExplicitlySet,
                                       tuple,
                                       m_nowFields);
    m_hasPurgeFragment = persistentTarget ? persistentTarget->hasPurgeFragment() : false;

    return true;
}

void InsertExecutor::executePurgeFragmentIfNeeded(PersistentTable** ptrToTable) {
    PersistentTable* table = *ptrToTable;
    int tupleLimit = table->tupleLimit();
    int numTuples = table->visibleTupleCount();

    // Note that the number of tuples may be larger than the limit.
    // This can happen we data is redistributed after an elastic
    // rejoin for example.
    if (numTuples >= tupleLimit) {
        // Next insert will fail: run the purge fragment
        // before trying to insert.
        m_engine->executePurgeFragment(table);

        // If the purge fragment did a truncate table, then the old
        // table is still around for undo purposes, but there is now a
        // new empty table we can insert into.  Update the caller's table
        // pointer to use it.
        //
        // The plan node will go through the table catalog delegate to get
        // the correct instance of PersistentTable.
        *ptrToTable = static_cast<PersistentTable*>(m_node->getTargetTable());
    }
}

bool InsertExecutor::p_execute(const NValueArray &params) {
    assert(m_node == dynamic_cast<InsertPlanNode*>(m_abstractNode));
    assert(m_node);
    assert(m_inputTable == dynamic_cast<TempTable*>(m_node->getInputTable()));
    assert(m_inputTable);

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    // Update target table reference from table delegate
    Table* targetTable = m_node->getTargetTable();
    assert(targetTable);
    assert((targetTable == dynamic_cast<PersistentTable*>(targetTable)) ||
            (targetTable == dynamic_cast<StreamedTable*>(targetTable)));

    PersistentTable* persistentTable = m_isStreamed ?
        NULL : static_cast<PersistentTable*>(targetTable);
    TableTuple upsertTuple = TableTuple(targetTable->schema());

    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug().c_str());

    // count the number of successful inserts
    int modifiedTuples = 0;

    Table* outputTable = m_node->getOutputTable();
    assert(outputTable);
    TableTuple& count_tuple = outputTable->tempTuple();

    // For export tables with no partition column,
    // if the data is from a replicated source,
    // only insert into one partition (the one for hash(0)).
    // Other partitions can just return a 0 modified tuple count.
    // OTOH, if the data is coming from a (sub)query with
    // partitioned tables, perform the insert on every partition.
    if (m_partitionColumn == -1 &&
            m_isStreamed &&
            m_multiPartition &&
            !m_sourceIsPartitioned &&
            !m_engine->isLocalSite(ValueFactory::getBigIntValue(0L))) {
        count_tuple.setNValue(0, ValueFactory::getBigIntValue(0L));
        // put the tuple into the output table
        outputTable->insertTuple(count_tuple);
        return true;
    }

    TableTuple templateTuple = m_templateTuple.tuple();

    std::vector<int>::iterator it;
    for (it = m_nowFields.begin(); it != m_nowFields.end(); ++it) {
        templateTuple.setNValue(*it, NValue::callConstant<FUNC_CURRENT_TIMESTAMP>());
    }

    VOLT_DEBUG("This is a %s-row insert on partition with id %d",
               m_node->getChildren()[0]->getPlanNodeType() == PLAN_NODE_TYPE_MATERIALIZE ?
               "single" : "multi", m_engine->getPartitionId());
    VOLT_DEBUG("Offset of partition column is %d", m_partitionColumn);

    //
    // An insert is quite simple really. We just loop through our m_inputTable
    // and insert any tuple that we find into our targetTable. It doesn't get any easier than that!
    //
    TableTuple inputTuple(m_inputTable->schema());
    assert (inputTuple.sizeInValues() == m_inputTable->columnCount());
    TableIterator iterator = m_inputTable->iterator();
    Pool* tempPool = ExecutorContext::getTempStringPool();
    const std::vector<int>& fieldMap = m_node->getFieldMap();
    std::size_t mapSize = fieldMap.size();
    while (iterator.next(inputTuple)) {

        for (int i = 0; i < mapSize; ++i) {
            // Most executors will just call setNValue instead of
            // setNValueAllocateForObjectCopies.
            //
            // However, We need to call
            // setNValueAllocateForObjectCopies here.  Sometimes the
            // input table's schema has an inlined string field, and
            // it's being assigned to the target table's outlined
            // string field.  In this case we need to tell the NValue
            // where to allocate the string data.
            // For an "upsert", this templateTuple setup has two effects --
            // It sets the primary key column(s) and it sets the
            // updated columns to their new values.
            // If the primary key value (combination) is new, the
            // templateTuple is the exact combination of new values
            // and default values required by the insert.
            // If the primary key value (combination) already exists,
            // only the NEW values stored on the templateTuple get updated
            // in the existing tuple and its other columns keep their existing
            // values -- the DEFAULT values that are stored in templateTuple
            // DO NOT get copied to an existing tuple.
            templateTuple.setNValueAllocateForObjectCopies(fieldMap[i],
                                                           inputTuple.getNValue(i),
                                                           tempPool);
        }

        VOLT_TRACE("Inserting tuple '%s' into target table '%s' with table schema: %s",
                   templateTuple.debug(targetTable->name()).c_str(), targetTable->name().c_str(),
                   targetTable->schema()->debug().c_str());

        // If there is a partition column for the target table
        if (m_partitionColumn != -1) {
            // get the value for the partition column
            NValue value = templateTuple.getNValue(m_partitionColumn);
            bool isLocal = m_engine->isLocalSite(value);

            // if it doesn't map to this partiton
            if (!isLocal) {
                if (m_multiPartition) {
                    // The same row is presumed to also be generated
                    // on some other partition, where the partition key
                    // belongs.
                    continue;
                }
                // When a streamed table has no views, let an SP insert execute.
                // This is backward compatible with when there were only export
                // tables with no views on them.
                // When there are views, be strict and throw mispartitioned
                // tuples to force partitioned data to be generated only
                // where partitoned view rows are maintained.
                if (!m_isStreamed || m_hasStreamView) {
                    throw ConstraintFailureException(
                        targetTable, templateTuple,
                        "Mispartitioned tuple in single-partition insert statement.");
                }
            }
        }

        if (m_isUpsert) {
            // upsert execution logic
            assert(persistentTable->primaryKeyIndex() != NULL);
            TableTuple existsTuple = persistentTable->lookupTupleByValues(templateTuple);

            if (!existsTuple.isNullTuple()) {
                // The tuple exists already, update (only) the templateTuple columns
                // that were initialized from the input tuple via the field map.
                // Technically, this includes setting primary key values,
                // but they are getting set to equivalent values, so that's OK.
                // A simple setNValue works here because any required object
                // allocations were handled when copying the input values into
                // the templateTuple.
                upsertTuple.move(templateTuple.address());
                TableTuple &tempTuple = persistentTable->copyIntoTempTuple(existsTuple);
                for (int i = 0; i < mapSize; ++i) {
                    tempTuple.setNValue(fieldMap[i],
                                        templateTuple.getNValue(fieldMap[i]));
                }

                persistentTable->updateTupleWithSpecificIndexes(existsTuple, tempTuple,
                                                                persistentTable->allIndexes());
                // successfully updated
                ++modifiedTuples;
                continue;
            }
            // else, the primary key did not match,
            // so fall through to the "insert" logic
        }

        // try to put the tuple into the target table
        if (m_hasPurgeFragment) {
            executePurgeFragmentIfNeeded(&persistentTable);
            // purge fragment might have truncated the table, and
            // refreshed the persistent table pointer.  Make sure to
            // use it when doing the insert below.
            targetTable = persistentTable;
        }
        targetTable->insertTuple(templateTuple);
        // successfully inserted
        ++modifiedTuples;
    }

    count_tuple.setNValue(0, ValueFactory::getBigIntValue(modifiedTuples));
    // put the tuple into the output table
    outputTable->insertTuple(count_tuple);

    // add to the planfragments count of modified tuples
    m_engine->addToTuplesModified(modifiedTuples);
    VOLT_DEBUG("Finished inserting %d tuples", modifiedTuples);
    return true;
}
