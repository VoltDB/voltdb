/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
    // Not much to do here... just create an output table that
    // has the same schema as our input table
    setTempOutputTable(executorVector);
    return true;
}

bool CommonTableExecutor::p_execute(const NValueArray& params) {
    ExecutorContext *ec = ExecutorContext::getExecutorContext();
    CommonTablePlanNode* node = static_cast<CommonTablePlanNode*>(m_abstractNode);
    AbstractTempTable* inputTable = m_abstractNode->getTempInputTable();
    AbstractTempTable* finalOutputTable = m_abstractNode->getTempOutputTable();

    // To start, add whatever the base query produced (this executor's
    // plan node has the plan tree for the base query as its child) to
    // the final result.
    TableTuple iterTuple(inputTable->schema());
    TableIterator iter = inputTable->iterator();
    while (iter.next(iterTuple)) {
        finalOutputTable->insertTuple(iterTuple);
    }

    int recursiveStmtId = node->getRecursiveStmtId();
    if (recursiveStmtId == -1) {
        // If there is no recursive statement, this is a non-recursive
        // CTE.  Just return now, we're done.
        return true;
    }

    // We're about the execute the recursive query.  The recursive
    // query has a CTE scan should scan the output of the base query
    // on its first iteration.
    ec->setCommonTable(node->getCommonTableName(), inputTable);

#ifndef NDEBUG
    // Schemas produced by the base query and the recursive query must
    // match exactly!  Otherwise memory corruption will occur.
    const AbstractTempTable* recOutput = ec->getExecutors(recursiveStmtId).back()->getTempOutputTable();
    vassert(recOutput->schema()->isCompatibleForMemcpy(inputTable->schema()));
#endif

    while (inputTable->activeTupleCount() > 0) {
        // At head of this loop, inputTable should contain the results
        // of the base query, or the results of the last invocation of
        // the recursive query.

        // Execute the recursive query...
        AbstractTempTable* recursiveOutputTable = ec->executeExecutors(recursiveStmtId).release();

        // Add the recursive output to the final result
        iter = recursiveOutputTable->iterator();
        while (iter.next(iterTuple)) {
            finalOutputTable->insertTuple(iterTuple);
        }

        // Now prepare for the next iteration...
        inputTable->deleteAllTuples();
        inputTable->swapContents(recursiveOutputTable);

        // inputTable now has recursive output
        // recursiveOutputTable is now empty
        vassert(recursiveOutputTable->activeTupleCount() == 0);
    }

    // Finally, the main query that references this CTE should see the
    // final output.
    ec->setCommonTable(node->getCommonTableName(), finalOutputTable);

    return true;
}

} // end namespace voltdb
