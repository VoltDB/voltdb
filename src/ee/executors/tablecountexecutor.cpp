/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "expressions/abstractexpression.h"
#include "plannodes/tablecountnode.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"

using namespace voltdb;

bool TableCountExecutor::p_init(AbstractPlanNode* abstract_node,
                             TempTableLimits* limits)
{
    VOLT_TRACE("init Table Count Executor");

    assert(dynamic_cast<TableCountPlanNode*>(abstract_node));
    assert(dynamic_cast<TableCountPlanNode*>(abstract_node)->isSubQuery() ||
           dynamic_cast<TableCountPlanNode*>(abstract_node)->getTargetTable());
    assert(abstract_node->getOutputSchema().size() == 1);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    return true;
}

bool TableCountExecutor::p_execute(const NValueArray &params) {
    TableCountPlanNode* node = dynamic_cast<TableCountPlanNode*>(m_abstractNode);
    assert(node);
    assert(node->getPredicate() == NULL);

    Table* output_table = node->getOutputTable();
    assert(output_table);
    assert ((int)output_table->columnCount() == 1);

    int64_t rowCounts = 0;
    if (node->isSubQuery()) {
        Table* input_table = node->getChildren()[0]->getOutputTable();
        assert(input_table);
        TempTable* temp_table = dynamic_cast<TempTable*>(input_table);
        if ( ! temp_table) {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                    "May not iterate a streamed table.");
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

