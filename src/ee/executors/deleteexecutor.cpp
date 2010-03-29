/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "indexes/tableindex.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

#include <cassert>

namespace voltdb {

bool DeleteExecutor::p_init(AbstractPlanNode *abstract_node, const catalog::Database* catalog_db, int* tempTableMemoryInBytes) {
    VOLT_TRACE("init Delete Executor");

    DeletePlanNode* node = dynamic_cast<DeletePlanNode*>(abstract_node);
    assert(node);
    assert(node->getTargetTable());
    m_targetTable = dynamic_cast<PersistentTable*>(node->getTargetTable()); //target table should be persistenttable
    assert(m_targetTable);
    m_truncate = node->getTruncate();
    if (m_truncate) {
        assert(node->getInputTables().size() == 0);
        // TODO : we can't use target table here because
        // it will report that "0 tuples deleted" as it's already truncated as of Result node..
        node->setOutputTable(TableFactory::getCopiedTempTable(m_targetTable->databaseId(), "result_table", m_targetTable, tempTableMemoryInBytes));
        return true;
    }

    assert(node->getInputTables().size() == 1);
    m_inputTable = dynamic_cast<TempTable*>(node->getInputTables()[0]); //input table should be temptable
    assert(m_inputTable);

    // Our output is just our input table (regardless if plan is single-sited or not)
    node->setOutputTable(node->getInputTables()[0]);

    m_inputTuple = TableTuple(m_inputTable->schema());
    m_targetTuple = TableTuple(m_targetTable->schema());

    return true;
}

bool DeleteExecutor::p_execute(const NValueArray &params) {
    assert(m_targetTable);
    if (m_truncate) {
        VOLT_TRACE("truncating table %s...", m_targetTable->name().c_str());
        // count the truncated tuples as deleted
        m_engine->m_tuplesModified += m_inputTable->activeTupleCount();
        //m_engine->context().incrementTuples(m_targetTable->activeTupleCount());
        // actually delete all the tuples
        m_targetTable->deleteAllTuples(true);
        return true;
    }
    assert(m_inputTable);

    assert(m_inputTuple.sizeInValues() == m_inputTable->columnCount());
    assert(m_targetTuple.sizeInValues() == m_targetTable->columnCount());
    TableIterator inputIterator(m_inputTable);
    while (inputIterator.next(m_inputTuple)) {
        //
        // OPTIMIZATION: Single-Sited Query Plans
        // If our beloved DeletePlanNode is apart of a single-site query plan,
        // then the first column in the input table will be the address of a
        // tuple on the target table that we will want to blow away. This saves
        // us the trouble of having to do an index lookup
        //
        void *targetAddress = m_inputTuple.getNValue(0).castAsAddress();
        m_targetTuple.move(targetAddress);

        // Delete from target table
        if (!m_targetTable->deleteTuple(m_targetTuple, true)) {
            VOLT_ERROR("Failed to delete tuple from table '%s'",
                       m_targetTable->name().c_str());
            return false;
        }
    }

    // add to the planfragments count of modified tuples
    m_engine->m_tuplesModified += m_inputTable->activeTupleCount();
    //m_engine->context().incrementTuples(m_inputTable->activeTupleCount());

    return true;
}

DeleteExecutor::~DeleteExecutor() {
}
}
