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

#include <iostream>
#include <set>
#include "materializedscanexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "common/StlFriendlyNValue.h"
#include "expressions/abstractexpression.h"
#include "plannodes/materializedscanplannode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"

using namespace voltdb;

bool MaterializedScanExecutor::p_init(AbstractPlanNode* abstract_node,
                                      TempTableLimits* limits)
{
    VOLT_TRACE("init Materialized Scan Executor");

    assert(dynamic_cast<MaterializedScanPlanNode*>(abstract_node));
    assert(abstract_node->getOutputSchema().size() == 1);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);
    return true;
}

bool MaterializedScanExecutor::p_execute(const NValueArray &params) {
    MaterializedScanPlanNode* node = dynamic_cast<MaterializedScanPlanNode*>(m_abstractNode);
    assert(node);

    // output table has one column
    Table* output_table = node->getOutputTable();
    TableTuple& tmptup = output_table->tempTuple();
    assert(output_table);
    assert ((int)output_table->columnCount() == 1);

    // get the output type
    const TupleSchema::ColumnInfo *columnInfo = output_table->schema()->getColumnInfo(0);
    ValueType outputType = columnInfo->getVoltType();
    bool outputCantBeNull = !columnInfo->allowNull;

    AbstractExpression* rowsExpression = node->getTableRowsExpression();
    assert(rowsExpression);

    // get array nvalue
    NValue arrayNValue = rowsExpression->eval();

    SortDirectionType sort_direction = node->getSortDirection();

    // make a set to eliminate unique values in O(nlogn) time
    std::vector<NValue> sortedUniques;

    // iterate over the array of values and build a sorted/deduped set of
    // values that don't overflow or violate unique constaints
    arrayNValue.castAndSortAndDedupArrayForInList(outputType, sortedUniques);

    // insert all items in the set in order
    if (sort_direction != SORT_DIRECTION_TYPE_DESC) {
        std::vector<NValue>::const_iterator iter;
        for (iter = sortedUniques.begin(); iter != sortedUniques.end(); iter++) {
            if ((*iter).isNull() && outputCantBeNull) {
                continue;
            }
            tmptup.setNValue(0, *iter);
            output_table->insertTuple(tmptup);
        }
    } else {
        std::vector<NValue>::reverse_iterator reverse_iter;
        for (reverse_iter = sortedUniques.rbegin(); reverse_iter != sortedUniques.rend(); reverse_iter++) {
            if ((*reverse_iter).isNull() && outputCantBeNull) {
                continue;
            }
            tmptup.setNValue(0, *reverse_iter);
            output_table->insertTuple(tmptup);
        }
    }

    VOLT_TRACE("\n%s\n", output_table->debug().c_str());
    VOLT_DEBUG("Finished Materializing a Table");

    return true;
}

MaterializedScanExecutor::~MaterializedScanExecutor() {
}

