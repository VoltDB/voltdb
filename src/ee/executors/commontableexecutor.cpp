/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

#include "executors/commontableexecutor.h"
#include "plannodes/commontablenode.h"
#include "storage/tableiterator.h"

namespace voltdb {

bool CommonTableExecutor::p_init(AbstractPlanNode*,
                                 const ExecutorVector& executorVector) {
    setTempOutputTable(executorVector);
    return true;
}

bool CommonTableExecutor::p_execute(const NValueArray& params) {
    ExecutorContext *ec = ExecutorContext::getExecutorContext();
    CommonTablePlanNode* node = static_cast<CommonTablePlanNode*>(m_abstractNode);
    AbstractTempTable* inputTable = m_abstractNode->getTempInputTable();
    AbstractTempTable* finalOutputTable = m_abstractNode->getTempOutputTable();

    TableTuple iterTuple(inputTable->schema());
    TableIterator iter = inputTable->iterator();
    while (iter.next(iterTuple)) {
        finalOutputTable->insertTuple(iterTuple);
    }

    ec->setCommonTable(node->getCommonTableName(), inputTable);
    int recursiveStmtId = node->getRecursiveStmtId();

#ifndef NDEBUG
    // Schemas produced by the base query and the recursive query must
    // match exactly!
    const AbstractTempTable* recOutput = ec->getExecutors(recursiveStmtId).back()->getTempOutputTable();
    assert(recOutput->schema()->isCompatibleForMemcpy(inputTable->schema()));
#endif

    while (inputTable->activeTupleCount() > 0) {
        // At head of this loop, inputTable should contain the results
        // of the base query, or the results of the last invocation of
        // the recursive query.

        AbstractTempTable* recursiveOutputTable = ec->executeExecutors(recursiveStmtId).release();

        // Add the recursive output to the final result
        iter = recursiveOutputTable->iterator();
        while (iter.next(iterTuple)) {
            finalOutputTable->insertTuple(iterTuple);
        }

        // Now prepare for the next iteration...
        inputTable->deleteAllTempTuples();
        inputTable->swapContents(recursiveOutputTable);

        // inputTable now has recursive output
        // recursiveOutputTable is now empty
        assert(recursiveOutputTable->activeTupleCount() == 0);
    }

    ec->setCommonTable(node->getCommonTableName(), finalOutputTable);

    return true;
}


} // end namespace voltdb
