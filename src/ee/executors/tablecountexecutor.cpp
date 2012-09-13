/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

#include <iostream>
#include "tablecountexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "expressions/abstractexpression.h"
#include "plannodes/tablecountnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"

using namespace voltdb;

bool TableCountExecutor::p_init(AbstractPlanNode* abstract_node,
                             TempTableLimits* limits)
{
    VOLT_TRACE("init Table Count Executor");

    TableCountPlanNode* node = dynamic_cast<TableCountPlanNode*>(abstract_node);
    assert(node);
    assert(node->getTargetTable());

    TupleSchema* schema = node->generateTupleSchema(true);
    int column_count = static_cast<int>(node->getOutputSchema().size());
    assert(column_count == 1);

    std::string* column_names = new std::string[column_count];
    column_names[0] = node->getOutputSchema()[0]->getColumnName();

    node->setOutputTable(TableFactory::getTempTable(node->databaseId(),
            node->getTargetTable()->name(),
            schema,
            column_names,
            limits));
    delete[] column_names;
    return true;
}

bool TableCountExecutor::p_execute(const NValueArray &params) {
    TableCountPlanNode* node = dynamic_cast<TableCountPlanNode*>(m_abstractNode);
    assert(node);
    Table* output_table = node->getOutputTable();
    assert(output_table);
    assert ((int)output_table->columnCount() == 1);

    Table* target_table = dynamic_cast<Table*>(node->getTargetTable());
    assert(target_table);
    VOLT_TRACE("Table Count table :\n %s",
               target_table->debug().c_str());
    VOLT_DEBUG("Table Count table : %s which has %d active, %d"
               " allocated, %d used tuples",
               target_table->name().c_str(),
               (int)target_table->activeTupleCount(),
               (int)target_table->allocatedTupleCount(),
               (int)target_table->usedTupleCount());

    assert (node->getPredicate() == NULL);

    TableTuple& tmptup = output_table->tempTuple();
    tmptup.setNValue(0, ValueFactory::getBigIntValue( target_table->activeTupleCount() ));
    output_table->insertTuple(tmptup);


    //printf("Table count answer: %d", iterator.getSize());
    //printf("\n%s\n", output_table->debug().c_str());
    VOLT_TRACE("\n%s\n", output_table->debug().c_str());
    VOLT_DEBUG("Finished Table Counting");

    return true;
}

TableCountExecutor::~TableCountExecutor() {
}

