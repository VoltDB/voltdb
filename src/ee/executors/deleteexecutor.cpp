/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

#include "common/ValueFactory.hpp"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "indexes/tableindex.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

#include <vector>
#include <cassert>

using namespace std;
using namespace voltdb;

namespace voltdb {
namespace detail {

    struct DeleteExecutorState
    {
        DeleteExecutorState(AbstractExecutor* childExec, Table* outputTable) :
            m_tuplesToDelete(),
            m_childExecutor(childExec),
            m_outputTable(outputTable),
            m_outputTableName(outputTable->name()),
            m_modifiedTuples(0),
            m_done(false)
        {}

        std::vector<void*> m_tuplesToDelete;
        AbstractExecutor* m_childExecutor;
        Table* m_outputTable;
        std::string m_outputTableName;
        long m_modifiedTuples;
        bool m_done;
    };

} // namespace detail
} // namespace voltdb

DeleteExecutor::DeleteExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node),
    m_node(NULL), m_truncate(), m_inputTable(NULL), m_targetTable(NULL),
    m_inputTuple(), m_targetTuple(), m_engine(engine), m_state()
{}


bool DeleteExecutor::p_init(AbstractPlanNode *abstract_node,
                            TempTableLimits* limits)
{
    VOLT_TRACE("init Delete Executor");

    m_node = dynamic_cast<DeletePlanNode*>(abstract_node);
    assert(m_node);
    assert(m_node->getTargetTable());
    m_targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable()); //target table should be persistenttable
    assert(m_targetTable);

    TupleSchema* schema = m_node->generateTupleSchema(false);
    int column_count = static_cast<int>(m_node->getOutputSchema().size());
    std::string* column_names = new std::string[column_count];
    for (int ctr = 0; ctr < column_count; ctr++)
    {
        column_names[ctr] = m_node->getOutputSchema()[ctr]->getColumnName();
    }
    m_node->setOutputTable(TableFactory::getTempTable(m_node->databaseId(),
                                                      "temp",
                                                      schema,
                                                      column_names,
                                                      limits));
    delete[] column_names;

    m_truncate = m_node->getTruncate();
    if (m_truncate) {
        assert(m_node->getInputTables().size() == 0);
        return true;
    }

    assert(m_node->getInputTables().size() == 1);
    m_inputTable = dynamic_cast<TempTable*>(m_node->getInputTables()[0]); //input table should be temptable
    assert(m_inputTable);

    m_inputTuple = TableTuple(m_inputTable->schema());
    m_targetTuple = TableTuple(m_targetTable->schema());

    return true;
}

bool DeleteExecutor::p_execute(const NValueArray &params) {
    assert(m_targetTable);
    int64_t modified_tuples = 0;

    if (m_truncate) {
        VOLT_TRACE("truncating table %s...", m_targetTable->name().c_str());
        // count the truncated tuples as deleted
        modified_tuples = m_targetTable->activeTupleCount();

        // actually delete all the tuples
        m_targetTable->deleteAllTuples(true);
    }
    else
    {
        assert(m_inputTable);
        assert(m_inputTuple.sizeInValues() == m_inputTable->columnCount());
        assert(m_targetTuple.sizeInValues() == m_targetTable->columnCount());
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
            m_targetTuple.move(targetAddress);

            // Delete from target table
            if (!m_targetTable->deleteTuple(m_targetTuple, true)) {
                VOLT_ERROR("Failed to delete tuple from table '%s'",
                           m_targetTable->name().c_str());
                return false;
            }
        }
        modified_tuples = m_inputTable->activeTupleCount();
    }

    TableTuple& count_tuple = m_node->getOutputTable()->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(modified_tuples));
    // try to put the tuple into the output table
    if (!m_node->getOutputTable()->insertTuple(count_tuple)) {
        VOLT_ERROR("Failed to insert tuple count (%ld) into"
                   " output table '%s'",
                   static_cast<long int>(modified_tuples),
                   m_node->getOutputTable()->name().c_str());
        return false;
    }
    m_engine->m_tuplesModified += modified_tuples;

    return true;
}

bool DeleteExecutor::support_pull() const
{
    return true;
}

void DeleteExecutor::p_pre_execute_pull(const NValueArray &params)
{
    assert(m_targetTable);
    std::vector<AbstractPlanNode*>& children = m_node->getChildren();
    assert(children.size() == 1);
    AbstractExecutor* childExec = children[0]->getExecutor();

    Table* outputTable = m_node->getOutputTable();
    assert(outputTable);

    m_state.reset(new detail::DeleteExecutorState(childExec, outputTable));
//{ printf("\nDEBUG: %s %ld\n", "PRE DELETE NO PROBLEM, REALLY", (long)m_targetTable->activeTupleCount()); }
}

TableTuple DeleteExecutor::p_next_pull()
{
    if (m_state->m_done) {
        return TableTuple(m_node->getOutputTable()->schema());
    }
    else if (m_truncate) {
        VOLT_TRACE("truncating table %s...", m_targetTable->name().c_str());
        // count the truncated tuples as deleted
        m_state->m_modifiedTuples = m_targetTable->activeTupleCount();
        // actually delete all the tuples
        m_targetTable->deleteAllTuples(true);
    }
    else
    {
        while (true)
        {
            TableTuple tuple = m_state->m_childExecutor->next_pull();
            if (tuple.isNullTuple())
            {
                break;
            }
            // We can't delete the tuple while iterating the table because
            // that would invalidate table iterator. Save the tuple address
            // to be used after the iteration is complete
            m_state->m_tuplesToDelete.push_back(tuple.address());
        }
        size_t tupleCount = m_state->m_tuplesToDelete.size();
        m_state->m_modifiedTuples = tupleCount;
        for (size_t i = 0; i < tupleCount; ++i) {
            void* targetAddress = m_state->m_tuplesToDelete[i];
            m_targetTuple.move(targetAddress);
            // Delete from target table
            if (!m_targetTable->deleteTuple(m_targetTuple, true)) {
                char message[128];
                snprintf(message, 128, "Failed to delete tuple from table '%s'",
                         m_targetTable->name().c_str());
                VOLT_ERROR("%s", message);
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
            }
//{ printf("\nDEBUG: %s\n", "DELETING NO PROBLEM, REALLY"); }
        }
    }
//{ printf("\nDEBUG: %s %ld\n", "DELETED NO PROBLEM, REALLY", (long)m_targetTable->activeTupleCount()); }
    m_state->m_done = true;
    TableTuple countTuple = m_node->getOutputTable()->tempTuple();
    countTuple.setNValue(0, ValueFactory::getBigIntValue(m_state->m_modifiedTuples));

    return countTuple;
}

void DeleteExecutor::p_post_execute_pull()
{
    // add to the planfragments count of modified tuples
    m_engine->m_tuplesModified += m_state->m_modifiedTuples;
    VOLT_INFO("Finished deleting tuples");
//{ printf("\nDEBUG: %s %ld\n", "DELETE DONE NO PROBLEM, REALLY", (long)m_targetTable->activeTupleCount()); }
}

void DeleteExecutor::p_insert_output_table_pull(TableTuple& tuple)
{
    // try to put the tuple into the output table
    if (!m_state->m_outputTable->insertTuple(tuple))
    {
        char message[128];
        snprintf(message, 128, "Failed to insert tuple count (%ld) into output table '%s'",
                 static_cast<long int>(m_state->m_modifiedTuples),
                 m_state->m_outputTableName.c_str());
        VOLT_ERROR("%s", message);
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
//{ printf("\nDEBUG: %s\n", "DELETE DONE DONE NO PROBLEM, REALLY"); }
}
