/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "insertexecutor.h"
#include "common/debuglog.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "common/types.h"
#include "plannodes/insertnode.h"
#include "execution/VoltDBEngine.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"
#include "storage/ConstraintFailureException.h"

#include <vector>

using namespace std;
using namespace voltdb;

bool InsertExecutor::p_init(AbstractPlanNode* abstractNode,
                            TempTableLimits* limits)
{
    VOLT_TRACE("init Insert Executor");

    m_node = dynamic_cast<InsertPlanNode*>(abstractNode);
    assert(m_node);
    assert(m_node->getTargetTable());
    assert(m_node->getInputTables().size() == 1);

    setDMLCountOutputTable(limits);

    m_inputTable = dynamic_cast<TempTable*>(m_node->getInputTables()[0]); //input table should be temptable
    assert(m_inputTable);

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    PersistentTable *persistentTarget = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
    m_partitionColumn = -1;
    m_partitionColumnIsString = false;
    m_isStreamed = (persistentTarget == NULL);
    if (persistentTarget) {
        m_partitionColumn = persistentTarget->partitionColumn();
        if (m_partitionColumn != -1) {
            if (m_inputTable->schema()->columnType(m_partitionColumn) == VALUE_TYPE_VARCHAR) {
                m_partitionColumnIsString = true;
            }
        }
    }

    m_multiPartition = m_node->isMultiPartition();
    return true;
}

bool InsertExecutor::p_execute(const NValueArray &params) {
    assert(m_node == dynamic_cast<InsertPlanNode*>(m_abstractNode));
    assert(m_node);
    assert(m_inputTable == dynamic_cast<TempTable*>(m_node->getInputTables()[0]));
    assert(m_inputTable);

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    // Update target table reference from table delegate
    Table* targetTable = m_node->getTargetTable();
    assert(targetTable);
    assert((targetTable == dynamic_cast<PersistentTable*>(targetTable)) ||
            (targetTable == dynamic_cast<StreamedTable*>(targetTable)));

    TableTuple tbTuple = TableTuple(m_inputTable->schema());

    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug().c_str());
#ifdef DEBUG
    //
    // This should probably just be a warning in the future when we are
    // running in a distributed cluster
    //
    if (m_inputTable->isTempTableEmpty()) {
        VOLT_ERROR("No tuples were found in our input table '%s'",
                   m_inputTable->name().c_str());
        return false;
    }
#endif
    assert ( ! m_inputTable->isTempTableEmpty());

    // count the number of successful inserts
    int modifiedTuples = 0;

    Table* outputTable = m_node->getOutputTable();
    assert(outputTable);

    //
    // An insert is quite simple really. We just loop through our m_inputTable
    // and insert any tuple that we find into our targetTable. It doesn't get any easier than that!
    //
    assert (tbTuple.sizeInValues() == m_inputTable->columnCount());
    TableIterator iterator = m_inputTable->iterator();
    while (iterator.next(tbTuple)) {
        VOLT_TRACE("Inserting tuple '%s' into target table '%s' with table schema: %s",
                   tbTuple.debug(targetTable->name()).c_str(), targetTable->name().c_str(),
                   targetTable->schema()->debug().c_str());

        // if there is a partition column for the target table
        if (m_partitionColumn != -1) {

            // get the value for the partition column
            NValue value = tbTuple.getNValue(m_partitionColumn);
            bool isLocal = m_engine->isLocalSite(value);

            // if it doesn't map to this site
            if (!isLocal) {
                if (!m_multiPartition) {
                    throw ConstraintFailureException(
                            dynamic_cast<PersistentTable*>(targetTable),
                            tbTuple,
                            "Mispartitioned tuple in single-partition insert statement.");
                }

                // don't insert
                continue;
            }
        }

        // for multi partition export tables,
        //  only insert them into one place (the partition with hash(0))
        if (m_isStreamed && m_multiPartition) {
            bool isLocal = m_engine->isLocalSite(ValueFactory::getBigIntValue(0));
            if (!isLocal) continue;
        }

        // try to put the tuple into the target table
        if (!targetTable->insertTuple(tbTuple)) {
            VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                       " target table '%s'",
                       m_inputTable->name().c_str(),
                       targetTable->name().c_str());
            return false;
        }

        // successfully inserted
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
    m_engine->m_tuplesModified += modifiedTuples;
    VOLT_DEBUG("Finished inserting tuple");
    return true;
}
