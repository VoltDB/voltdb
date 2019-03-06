/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

#include "migrateexecutor.h"

#include "common/ExecuteWithMpMemory.h"

#include "indexes/tableindex.h"
#include "storage/tableutil.h"

namespace voltdb {

bool MigrateExecutor::p_init(AbstractPlanNode *abstract_node, const ExecutorVector& executorVector) {
    VOLT_TRACE("init Delete Executor");

    m_node = dynamic_cast<MigratePlanNode*>(abstract_node);
    assert(m_node);

    setDMLCountOutputTable(executorVector.limits());

    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
    assert(targetTable);
    m_replicatedTableOperation = targetTable->isCatalogTableReplicated();

    assert(m_node->getInputTableCount() == 1);
    return true;
}

bool MigrateExecutor::p_execute(const NValueArray &params) {
   // target table should be persistenttable
   // update target table reference from table delegate
   // Note that the target table pointer in the node's tcd can change between p_init and p_execute
   PersistentTable* targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
   assert(targetTable);
   TableTuple targetTuple(targetTable->schema());
   int64_t modified_tuples = 0;
   TableTuple& count_tuple = m_node->getOutputTable()->tempTuple();
   count_tuple.setNValue(0, ValueFactory::getBigIntValue(modified_tuples));
   m_engine->addToTuplesModified(modified_tuples);
   return true;
}

} // end namespace voltdb

