/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "setopexecutor.h"

#include "common/tabletuple.h"
#include "executors/setoperator.h"
#include "plannodes/setopnode.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"


namespace voltdb {

SetOpExecutor::SetOpExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node), m_setOperator()
{ }

bool SetOpExecutor::p_init(AbstractPlanNode* abstract_node,
                           TempTableLimits* limits)
{
    VOLT_TRACE("init Union Executor");

    SetOpPlanNode* node = dynamic_cast<SetOpPlanNode*>(abstract_node);
    assert(node);

    //
    // First check to make sure they have the same number of columns
    //
    assert(node->getInputTableCount() > 0);

    Table* input_table_0 = node->getInputTable(0);
    const TupleSchema *table_0_schema = input_table_0->schema();

    for (int table_ctr = 1, table_cnt = (int)node->getInputTableCount();
         table_ctr < table_cnt;
         ++table_ctr) {
        Table* input_table_n = node->getInputTable(table_ctr);
        if (input_table_0->columnCount() != input_table_n->columnCount()) {
            VOLT_ERROR("Table '%s' has %d columns, but table '%s' has %d"
                       " columns",
                       input_table_0->name().c_str(),
                       input_table_0->columnCount(),
                       input_table_n->name().c_str(),
                       input_table_n->columnCount());
            return false;
        }

        //
        // Then check that they have the same types
        //

        // iterate over all columns in the first table
        for (int col_ctr = 0, col_cnt = table_0_schema->columnCount();
             col_ctr < col_cnt;
             col_ctr++) {
            // get the type for the current column
            ValueType type_0 = table_0_schema->columnType(col_ctr);

            const TupleSchema *table_n_schema = input_table_n->schema();
            ValueType type_n = table_n_schema->columnType(col_ctr);
            if (type_0 != type_n) {
                VOLT_ERROR("Table '%s' has value type '%s' for column '%d',"
                           " table '%s' has value type '%s' for column '%d'",
                           input_table_0->name().c_str(), getTypeName(type_0).c_str(), col_ctr,
                           input_table_n->name().c_str(), getTypeName(type_n).c_str(), col_ctr);
                return false;
            }
        }
    }
    //
    // Create our output table that will hold all the tuples that we are appending into.
    // If we don't need to send children results to the coordinator to do cross-partition set op
    // we can just grab the first table in the list because all of the tables have the same number
    // of columns with the same format.
    // Otherwise we need to use the output schema that have one extra column to tag each output row
    // to help the coordinator to sort out input rows into individual temp tables.
    bool needSendChildrenRows = node->needSendChildrenRows();
    TempTable* outputTable;
    if (needSendChildrenRows) {
        setTempOutputTable(limits, node->getInputTable(0)->name());
        outputTable = m_tmpOutputTable;
    } else {
        node->setOutputTable(TableFactory::buildCopiedTempTable(node->getInputTable(0)->name(),
                                                                node->getInputTable(0),
                                                                limits));
        outputTable = node->getTempOutputTable();
    }
    m_setOperator.reset(SetOperator::getSetOperator(
        node->getSetOpType(), node->getInputTableRefs(), outputTable, needSendChildrenRows));

    return true;
}

bool SetOpExecutor::p_execute(const NValueArray &params) {
    return m_setOperator->processTuples();
}

}
