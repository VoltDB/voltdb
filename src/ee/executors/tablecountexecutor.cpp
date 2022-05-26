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

#include <iostream>
#include "tablecountexecutor.h"
#include "plannodes/tablecountnode.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"

using namespace voltdb;

bool TableCountExecutor::p_init(AbstractPlanNode* abstract_node,
                                const ExecutorVector& executorVector)
{
    VOLT_TRACE("init Table Count Executor");

    vassert(dynamic_cast<TableCountPlanNode*>(abstract_node));
    vassert(dynamic_cast<TableCountPlanNode*>(abstract_node)->isSubqueryScan() ||
           dynamic_cast<TableCountPlanNode*>(abstract_node)->getTargetTable());
    vassert(abstract_node->getOutputSchema().size() == 1);

    // Create output table based on output schema from the plan
    setTempOutputTable(executorVector);

    return true;
}

bool TableCountExecutor::p_execute(const NValueArray &params) {
    TableCountPlanNode* node = dynamic_cast<TableCountPlanNode*>(m_abstractNode);
    vassert(node);
    vassert(node->getPredicate() == NULL);

    Table* output_table = node->getOutputTable();
    vassert(output_table);
    vassert((int)output_table->columnCount() == 1);

    int64_t rowCounts = 0;
    if (node->isSubqueryScan()) {
        Table* input_table = node->getChildren()[0]->getOutputTable();
        vassert(input_table);
        AbstractTempTable* temp_table = dynamic_cast<AbstractTempTable*>(input_table);
        if ( ! temp_table) {
            throw SerializableEEException("May not iterate a streamed table.");
        }
        rowCounts = temp_table->tempTableTupleCount();
    } else {
        PersistentTable* target_table = dynamic_cast<PersistentTable*>(node->getTargetTable());
        VOLT_DEBUG("Table Count table : %s which has %d active, %d visible, %d allocated",
                   target_table->name().c_str(),
                   (int)target_table->activeTupleCount(),
                   (int)target_table->visibleTupleCount(),
                   (int)target_table->allocatedTupleCount());

        rowCounts = target_table->visibleTupleCount();
    }

    TableTuple& tmptup = output_table->tempTuple();
    tmptup.setNValue(0, ValueFactory::getBigIntValue(rowCounts));
    output_table->insertTuple(tmptup);

    VOLT_DEBUG("\n%s\n", output_table->debug().c_str());
    VOLT_DEBUG("Finished Table Counting");

    return true;
}

TableCountExecutor::~TableCountExecutor() {
}
