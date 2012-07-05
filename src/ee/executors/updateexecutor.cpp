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

#include <cassert>
#include "boost/scoped_ptr.hpp"
#include "updateexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/types.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "plannodes/updatenode.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

using namespace std;
using namespace voltdb;

namespace voltdb {
namespace detail {

struct UpdateExecutorState
{
    UpdateExecutorState(AbstractExecutor* childExec, TempTable* inputTable, TempTable* outputTable) :
//        m_tempInputTable(),
        m_tempInputTable(inputTable),
        m_childExecutor(childExec),
        m_outputTable(outputTable),
        m_done(false)
    {
//        m_tempInputTable.reset(TableFactory::getCopiedTempTable(
//            inputTable->databaseId(), std::string("temp_input"),
//            inputTable, inputTable->m_limits));

    }

//    boost::scoped_ptr<TempTable> m_tempInputTable;
    TempTable* m_tempInputTable;
    AbstractExecutor* m_childExecutor;
    TempTable* m_outputTable;
    bool m_done;
};

} // namespace detail
} // namespace voltdb

UpdateExecutor::UpdateExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node),
    m_node(NULL), m_inputTargetMap(), m_inputTargetMapSize(-1),
    m_inputTable(NULL), m_targetTable(NULL), m_inputTuple(),
    m_targetTuple(), m_partitionColumn(-1), m_partitionColumnIsString(),
    m_updatesIndexes(), m_engine(engine), m_state()
{}

bool UpdateExecutor::p_init(AbstractPlanNode* abstract_node,
                            TempTableLimits* limits)
{
    VOLT_TRACE("init Update Executor");

    m_node = dynamic_cast<UpdatePlanNode*>(abstract_node);
    assert(m_node);
    assert(m_node->getTargetTable());
    assert(m_node->getInputTables().size() == 1);
    m_inputTable = dynamic_cast<TempTable*>(m_node->getInputTables()[0]); //input table should be temptable
    assert(m_inputTable);
    m_targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable()); //target table should be persistenttable
    assert(m_targetTable);
    assert(m_node->getTargetTable());

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

    // record if a full index update is needed, or if these checks can be skipped
    m_updatesIndexes = m_node->doesUpdateIndexes();

    AbstractPlanNode *child = m_node->getChildren()[0];
    ProjectionPlanNode *proj_node = NULL;
    if (NULL == child) {
        VOLT_ERROR("Attempted to initialize update executor with NULL child");
        return false;
    }

    PlanNodeType pnt = child->getPlanNodeType();
    if (pnt == PLAN_NODE_TYPE_PROJECTION) {
        proj_node = dynamic_cast<ProjectionPlanNode*>(child);
    } else if (pnt == PLAN_NODE_TYPE_SEQSCAN ||
            pnt == PLAN_NODE_TYPE_INDEXSCAN) {
        proj_node = dynamic_cast<ProjectionPlanNode*>(child->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
        assert(NULL != proj_node);
    }

    vector<string> output_column_names = proj_node->getOutputColumnNames();
    vector<string> targettable_column_names = m_targetTable->getColumnNames();

    /*
     * The first output column is the tuple address expression and it isn't part of our output so we skip
     * it when generating the map from input columns to the target table columns.
     */
    for (int ii = 1; ii < output_column_names.size(); ii++) {
        for (int jj=0; jj < targettable_column_names.size(); ++jj) {
            if (targettable_column_names[jj].compare(output_column_names[ii]) == 0) {
                m_inputTargetMap.push_back(pair<int,int>(ii, jj));
                break;
            }
        }
    }

    assert(m_inputTargetMap.size() == (output_column_names.size() - 1));
    m_inputTargetMapSize = (int)m_inputTargetMap.size();
    m_inputTuple = TableTuple(m_inputTable->schema());
    m_targetTuple = TableTuple(m_targetTable->schema());

    m_partitionColumn = m_targetTable->partitionColumn();
    m_partitionColumnIsString = false;
    if (m_partitionColumn != -1) {
        if (m_targetTable->schema()->columnType(m_partitionColumn) == VALUE_TYPE_VARCHAR) {
            m_partitionColumnIsString = true;
        }
    }

    return true;
}

bool UpdateExecutor::p_execute(const NValueArray &params) {
    assert(m_inputTable);
    assert(m_targetTable);

    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug().c_str());
    VOLT_TRACE("TARGET TABLE - BEFORE: %s\n", m_targetTable->debug().c_str());

    assert(m_inputTuple.sizeInValues() == m_inputTable->columnCount());
    assert(m_targetTuple.sizeInValues() == m_targetTable->columnCount());
    TableIterator input_iterator = m_inputTable->iterator();
    while (input_iterator.next(m_inputTuple)) {
        //
        // OPTIMIZATION: Single-Sited Query Plans
        // If our beloved UpdatePlanNode is apart of a single-site query plan,
        // then the first column in the input table will be the address of a
        // tuple on the target table that we will want to update. This saves us
        // the trouble of having to do an index lookup
        //
        void *target_address = m_inputTuple.getNValue(0).castAsAddress();
        m_targetTuple.move(target_address);

        // Loop through INPUT_COL_IDX->TARGET_COL_IDX mapping and only update
        // the values that we need to. The key thing to note here is that we
        // grab a temp tuple that is a copy of the target tuple (i.e., the tuple
        // we want to update). This insures that if the input tuple is somehow
        // bringing garbage with it, we're only going to copy what we really
        // need to into the target tuple.
        //
        TableTuple &tempTuple = m_targetTable->getTempTupleInlined(m_targetTuple);
        for (int map_ctr = 0; map_ctr < m_inputTargetMapSize; map_ctr++) {
            tempTuple.setNValue(m_inputTargetMap[map_ctr].second,
                                m_inputTuple.getNValue(m_inputTargetMap[map_ctr].first));
        }

        // if there is a partition column for the target table
        if (m_partitionColumn != -1) {
            // check for partition problems
            // get the value for the partition column
            NValue value = tempTuple.getNValue(m_partitionColumn);
            bool isLocal = m_engine->isLocalSite(value);

            // if it doesn't map to this site
            if (!isLocal) {
                VOLT_ERROR("Mispartitioned tuple in single-partition plan for"
                           " table '%s'", m_targetTable->name().c_str());
                return false;
            }
        }

        if (!m_targetTable->updateTuple(tempTuple, m_targetTuple,
                                        m_updatesIndexes)) {
            VOLT_INFO("Failed to update tuple from table '%s'",
                      m_targetTable->name().c_str());
            return false;
        }
    }

    TableTuple& count_tuple = m_node->getOutputTable()->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(m_inputTable->activeTupleCount()));
    // try to put the tuple into the output table
    if (!m_node->getOutputTable()->insertTuple(count_tuple)) {
        VOLT_ERROR("Failed to insert tuple count (%ld) into"
                   " output table '%s'",
                   static_cast<long int>(m_inputTable->activeTupleCount()),
                   m_node->getOutputTable()->name().c_str());
        return false;
    }

    VOLT_TRACE("TARGET TABLE - AFTER: %s\n", m_targetTable->debug().c_str());
    // TODO lets output result table here, not in result executor. same thing in
    // delete/insert

    // add to the planfragments count of modified tuples
    m_engine->m_tuplesModified += m_inputTable->activeTupleCount();

    return true;
}

bool UpdateExecutor::support_pull() const
{
    return true;
}

void UpdateExecutor::p_pre_execute_pull(const NValueArray &params)
{
    assert(m_inputTable);
    assert(m_targetTable);

    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug().c_str());
    VOLT_TRACE("TARGET TABLE - BEFORE: %s\n", m_targetTable->debug().c_str());

    assert(m_inputTuple.sizeInValues() == m_inputTable->columnCount());
    assert(m_targetTuple.sizeInValues() == m_targetTable->columnCount());

    std::vector<AbstractPlanNode*>& children = m_node->getChildren();
    assert(children.size() == 1);
    AbstractExecutor* childExec = children[0]->getExecutor();

    TempTable* outputTable = dynamic_cast<TempTable*>(m_node->getOutputTable());
    assert(outputTable);
    m_state.reset(new detail::UpdateExecutorState(childExec, m_inputTable, outputTable));
}

TableTuple UpdateExecutor::p_next_pull()
{
    if (m_state->m_done) {
        return TableTuple(m_state->m_outputTable->schema());
    }

    while (true)
    {
        TableTuple tuple = m_state->m_childExecutor->next_pull();
        if (tuple.isNullTuple())
        {
            break;
        }

        // We can't update the tuple while iterating the table because
        // that may invalidate input table iterator. Save the tuple into the
        // temp table to be used after the iteration is complete
        m_state->m_tempInputTable->insertTupleNonVirtual(tuple);
    }

    // Now iterate over the temp input table and update tuples
    // in the target table
    TableIterator input_iterator = m_state->m_tempInputTable->iterator();
    while (input_iterator.next(m_inputTuple)) {
        //
        // OPTIMIZATION: Single-Sited Query Plans
        // If our beloved UpdatePlanNode is apart of a single-site query plan,
        // then the first column in the input table will be the address of a
        // tuple on the target table that we will want to update. This saves us
        // the trouble of having to do an index lookup
        //
        void *target_address = m_inputTuple.getNValue(0).castAsAddress();
        m_targetTuple.move(target_address);

        // Loop through INPUT_COL_IDX->TARGET_COL_IDX mapping and only update
        // the values that we need to. The key thing to note here is that we
        // grab a temp tuple that is a copy of the target tuple (i.e., the tuple
        // we want to update). This insures that if the input tuple is somehow
        // bringing garbage with it, we're only going to copy what we really
        // need to into the target tuple.
        //
        TableTuple &tempTuple = m_targetTable->getTempTupleInlined(m_targetTuple);
        for (int map_ctr = 0; map_ctr < m_inputTargetMapSize; map_ctr++) {
            tempTuple.setNValue(m_inputTargetMap[map_ctr].second,
                                m_inputTuple.getNValue(m_inputTargetMap[map_ctr].first));
        }

        // if there is a partition column for the target table
        if (m_partitionColumn != -1) {
            // check for partition problems
            // get the value for the partition column
            NValue value = tempTuple.getNValue(m_partitionColumn);
            bool isLocal = m_engine->isLocalSite(value);

            // if it doesn't map to this site
            if (!isLocal) {
                char message[128];
                snprintf(message, 128, "Mispartitioned tuple in single-partition plan for"
                               " table '%s'", m_targetTable->name().c_str());
                VOLT_INFO(message);
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
            }
        }

        if (!m_targetTable->updateTuple(tempTuple, m_targetTuple,
                                        m_updatesIndexes)) {
                char message[128];
                snprintf(message, 128, "Failed to update tuple from table '%s'",
                      m_targetTable->name().c_str());
                VOLT_INFO(message);
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
        }
    }

    m_state->m_done = true;
    TableTuple& count_tuple = m_state->m_outputTable->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(m_state->m_tempInputTable->activeTupleCount()));
    return count_tuple;
}

void UpdateExecutor::p_post_execute_pull()
{
    VOLT_TRACE("TARGET TABLE - AFTER: %s\n", m_targetTable->debug().c_str());
    // TODO lets output result table here, not in result executor. same thing in
    // delete/insert

    // add to the planfragments count of modified tuples
    m_engine->m_tuplesModified += m_state->m_tempInputTable->activeTupleCount();

}

void UpdateExecutor::p_insert_output_table_pull(TableTuple& tuple)
{
    // try to put the tuple into the output table
    if (!m_state->m_outputTable->insertTuple(tuple))
    {
        char message[128];
        snprintf(message, 128, "Failed to insert tuple count (%ld) into"
                   " output table '%s'",
                   static_cast<long int>(m_state->m_tempInputTable->activeTupleCount()),
                   m_state->m_outputTable->name().c_str());
        VOLT_INFO(message);
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
}
