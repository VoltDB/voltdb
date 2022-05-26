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
#include <set>

#include "updateexecutor.h"

#include "common/ExecuteWithMpMemory.h"
#include "expressions/expressionutil.h"
#include "plannodes/updatenode.h"
#include "plannodes/projectionnode.h"
#include "storage/tablefactory.h"
#include "indexes/tableindex.h"
#include "storage/tableutil.h"
#include "storage/ConstraintFailureException.h"


namespace voltdb {
int64_t UpdateExecutor::s_modifiedTuples;

bool UpdateExecutor::p_init(AbstractPlanNode* abstract_node, const ExecutorVector& executorVector) {
    VOLT_TRACE("init Update Executor");

    m_node = dynamic_cast<UpdatePlanNode*>(abstract_node);
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
        VOLT_ERROR("Attempted to initialize update executor with NULL child");
        return false;
    }

    ProjectionPlanNode *proj_node;
    switch (child->getPlanNodeType()) {
        case PlanNodeType::Projection:
            proj_node = dynamic_cast<ProjectionPlanNode*>(child);
            break;
        case PlanNodeType::SeqScan:
        case PlanNodeType::IndexScan:
            proj_node = dynamic_cast<ProjectionPlanNode*>(
                    child->getInlinePlanNode(PlanNodeType::Projection));
            vassert(NULL != proj_node);
            break;
        default:
            VOLT_ERROR("Unrecognized plan node type");
            return false;
    }

    vector<string> output_column_names = proj_node->getOutputColumnNames();
    const vector<string> &targettable_column_names = targetTable->getColumnNames();

    /*
     * The first output column is the tuple address expression and it isn't part of our output so we skip
     * it when generating the map from input columns to the target table columns.
     */
    for (int ii = 1; ii < output_column_names.size(); ii++) {
        for (int jj = 0; jj < targettable_column_names.size(); ++jj) {
            if (targettable_column_names[jj].compare(output_column_names[ii]) == 0) {
                m_inputTargetMap.emplace_back(ii, jj);
                break;
            }
        }
    }

    vassert(m_inputTargetMap.size() == output_column_names.size() - 1);
    m_inputTargetMapSize = m_inputTargetMap.size();
    m_inputTuple = TableTuple(m_inputTable->schema());

    // for target table related info.
    m_partitionColumn = targetTable->partitionColumn();

    // for shared replicated table special handling
    m_replicatedTableOperation = targetTable->isReplicatedTable();
    return true;
}

bool UpdateExecutor::p_execute(const NValueArray &params) {
    vassert(m_inputTable);

    // target table should be persistenttable
    // Note that the target table pointer in the node's tcd can change between p_init and p_execute (at least for delete)
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
    vassert(targetTable);

    TableTuple targetTuple = TableTuple(targetTable->schema());

    VOLT_TRACE("INPUT TABLE: %s\n", m_inputTable->debug("").c_str());
    VOLT_TRACE("TARGET TABLE - BEFORE: %s\n", targetTable->debug("").c_str());

    int64_t modified_tuples = 0;

    {
        vassert(m_replicatedTableOperation == targetTable->isReplicatedTable());
        ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory(
                m_replicatedTableOperation, m_engine->isLowestSite(),
                []() { s_modifiedTuples = -1l; });
        if (possiblySynchronizedUseMpMemory.okToExecute()) {
            // determine which indices are updated by this executor
            // iterate through all target table indices and see if they contain
            // columns mutated by this executor
            //
            // Shouldn't this be done in p_init?  See ticket ENG-8668.
            std::vector<TableIndex*> indexesToUpdate;
            for(auto* index : targetTable->allIndexes()) {
               if (index->isMigratingIndex()) {
                  indexesToUpdate.push_back(index);
               } else {
                  auto const indicesArray = index->getAllColumnIndices();
                  std::set<int> const colIndices(indicesArray.cbegin(), indicesArray.cend());
                  if (m_inputTargetMap.cend() !=
                        std::find_if(m_inputTargetMap.cbegin(), m_inputTargetMap.cend(),
                           [&colIndices](std::pair<int, int> const& col) {return colIndices.count(col.second);})) {
                     indexesToUpdate.push_back(index);
                  }
               }
            }

            vassert(m_inputTuple.columnCount() == m_inputTable->columnCount());
            vassert(targetTuple.columnCount() == targetTable->columnCount());
            TableIterator input_iterator = m_inputTable->iterator();
            while (input_iterator.next(m_inputTuple)) {
                // The first column in the input table will be the address of a
                // tuple to update in the target table.
                targetTuple.move(m_inputTuple.getNValue(0).castAsAddress());

                // Loop through INPUT_COL_IDX->TARGET_COL_IDX mapping and only update
                // the values that we need to. The key thing to note here is that we
                // grab a temp tuple that is a copy of the target tuple (i.e., the tuple
                // we want to update). This ensures that if the input tuple is somehow
                // bringing garbage with it, we're only going to copy what we really
                // need to into the target tuple.
                //
                TableTuple &tempTuple = targetTable->copyIntoTempTuple(targetTuple);
                for (int map_ctr = 0; map_ctr < m_inputTargetMapSize; map_ctr++) {
                    try {
                        tempTuple.setNValue(m_inputTargetMap[map_ctr].second,
                                        m_inputTuple.getNValue(m_inputTargetMap[map_ctr].first));
                    } catch (SQLException& ex) {
                        std::string errorMsg = ex.message()
                                + " '" + targetTable->getColumnNames().at(m_inputTargetMap[map_ctr].second) + "'";
                        throw SQLException(ex.getSqlState(), errorMsg, ex.getInternalFlags());
                    }
                }

                // if there is a partition column for the target table
                if (m_partitionColumn != -1) {
                    // check for partition problems
                    // get the value for the partition column
                    bool isLocal = m_engine->isLocalSite(tempTuple.getNValue(m_partitionColumn));
                    // if it doesn't map to this site
                    if (!isLocal) {
                        throw ConstraintFailureException(targetTable, tempTuple,
                                "An update to a partitioning column triggered a partitioning error. "
                                "Updating a partitioning column is not supported. Try delete followed by insert.");
                    }
                }
                targetTable->updateTupleWithSpecificIndexes(targetTuple, tempTuple, indexesToUpdate);
            }
            modified_tuples = m_inputTable->tempTableTupleCount();
            if (m_replicatedTableOperation) {
                s_modifiedTuples = modified_tuples;
            }
        } else {
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
        modified_tuples = s_modifiedTuples;
    }
    TableTuple& count_tuple = m_node->getOutputTable()->tempTuple();
    count_tuple.setNValue(0, ValueFactory::getBigIntValue(modified_tuples));
    // try to put the tuple into the output table
    m_node->getOutputTable()->insertTuple(count_tuple);

    VOLT_TRACE("TARGET TABLE - AFTER: %s\n", targetTable->debug("").c_str());
    // TODO lets output result table here, not in result executor. same thing in
    // delete/insert

    // add to the planfragments count of modified tuples
    m_engine->addToTuplesModified(m_inputTable->tempTableTupleCount());

    return true;
}

} // end namespace voltdb
