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
    m_isStreamed = (persistentTarget == NULL);

    if (m_isUpsert) {
        VOLT_TRACE("init Upsert Executor actually");
        if (m_isStreamed) {
            VOLT_ERROR("UPSERT is not supported for Stream table %s", targetTable->name().c_str());
        }
        // look up the tuple whether it exists already
        if (targetTable->primaryKeyIndex() == NULL) {
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
    m_node->initTupleWithDefaultValues(m_engine,
                                       &m_memoryPool,
                                       fieldsExplicitlySet,
                                       tuple,
                                       m_nowFields);
    m_hasPurgeFragment = persistentTarget ? persistentTarget->hasPurgeFragment() : false;

    return true;
}

bool InsertExecutor::executePurgeFragmentIfNeeded(PersistentTable** ptrToTable) {
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

    return true;
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
    while (iterator.next(inputTuple)) {

        for (int i = 0; i < m_node->getFieldMap().size(); ++i) {
            // Most executors will just call setNValue instead of
            // setNValueAllocateForObjectCopies.
            //
            // However, We need to call
            // setNValueAlocateForObjectCopies here.  Sometimes the
            // input table's schema has an inlined string field, and
            // it's being assigned to the target table's outlined
            // string field.  In this case we need to tell the NValue
            // where to allocate the string data.
            templateTuple.setNValueAllocateForObjectCopies(m_node->getFieldMap()[i],
                                                           inputTuple.getNValue(i),
                                                           ExecutorContext::getTempStringPool());
        }

        VOLT_TRACE("Inserting tuple '%s' into target table '%s' with table schema: %s",
                   templateTuple.debug(targetTable->name()).c_str(), targetTable->name().c_str(),
                   targetTable->schema()->debug().c_str());

        // if there is a partition column for the target table
        if (m_partitionColumn != -1) {

            // get the value for the partition column
            NValue value = templateTuple.getNValue(m_partitionColumn);
            bool isLocal = m_engine->isLocalSite(value);

            // if it doesn't map to this site
            if (!isLocal) {
                if (!m_multiPartition) {
                    throw ConstraintFailureException(
                            dynamic_cast<PersistentTable*>(targetTable),
                            templateTuple,
                            "Mispartitioned tuple in single-partition insert statement.");
                }

                // don't insert
                continue;
            }
        }

        // for multi partition export tables, only insert into one
        // place (the partition with hash(0)), if the data is from a
        // replicated source.  If the data is coming from a subquery
        // with partitioned tables, we need to perform the insert on
        // every partition.
        if (m_isStreamed && m_multiPartition && !m_sourceIsPartitioned) {
            bool isLocal = m_engine->isLocalSite(ValueFactory::getBigIntValue(0));
            if (!isLocal) continue;
        }


        if (! m_isUpsert) {
            // try to put the tuple into the target table

            if (m_hasPurgeFragment) {
                if (!executePurgeFragmentIfNeeded(&persistentTable))
                    return false;
                // purge fragment might have truncated the table, and
                // refreshed the persistent table pointer.  Make sure to
                // use it when doing the insert below.
                targetTable = persistentTable;
            }

            if (!targetTable->insertTuple(templateTuple)) {
                VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                           " target table '%s'",
                           m_inputTable->name().c_str(),
                           targetTable->name().c_str());
                return false;
            }

        } else {
            // upsert execution logic
            assert(persistentTable->primaryKeyIndex() != NULL);
            TableTuple existsTuple = persistentTable->lookupTupleByValues(templateTuple);

            if (existsTuple.isNullTuple()) {
                // try to put the tuple into the target table

                if (m_hasPurgeFragment) {
                    if (!executePurgeFragmentIfNeeded(&persistentTable))
                        return false;
                }

                if (!persistentTable->insertTuple(templateTuple)) {
                    VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                               " target table '%s'",
                               m_inputTable->name().c_str(),
                               persistentTable->name().c_str());
                    return false;
                }
            } else {
                // tuple exists already, try to update the tuple instead
                upsertTuple.move(templateTuple.address());
                TableTuple &tempTuple = persistentTable->getTempTupleInlined(upsertTuple);

                if (!persistentTable->updateTupleWithSpecificIndexes(existsTuple, tempTuple,
                        persistentTable->allIndexes())) {
                    VOLT_INFO("Failed to update existsTuple from table '%s'",
                            persistentTable->name().c_str());
                    return false;
                }
            }
        }

        // successfully inserted or updated
        modifiedTuples++;
    }

    TableTuple& count_tuple = outputTable->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(modifiedTuples));
    // try to put the tuple into the output table
    if (!outputTable->insertTuple(count_tuple)) {
        VOLT_ERROR("Failed to insert tuple count (%d) into"
                   " output table '%s'",
                   modifiedTuples,
                   outputTable->name().c_str());
        return false;
    }

    // add to the planfragments count of modified tuples
    m_engine->addToTuplesModified(modifiedTuples);
    VOLT_DEBUG("Finished inserting %d tuples", modifiedTuples);
    return true;
}
