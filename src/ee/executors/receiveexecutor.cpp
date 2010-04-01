/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

#include "receiveexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "plannodes/receivenode.h"
#include "execution/VoltDBEngine.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"

namespace voltdb {

bool ReceiveExecutor::p_init(AbstractPlanNode *abstract_node,
                             const catalog::Database* catalog_db,
                             int* tempTableMemoryInBytes) {
    VOLT_TRACE("init Receive Executor");
    assert(tempTableMemoryInBytes);

    ReceivePlanNode* node = dynamic_cast<ReceivePlanNode*>(abstract_node);
    assert(node);

    //
    // Construct the output table
    //
    int num_of_columns = (int)node->getOutputColumnNames().size();
    assert(num_of_columns >= 0);
    assert(num_of_columns == node->getOutputColumnTypes().size());
    assert(num_of_columns == node->getOutputColumnSizes().size());
    const std::vector<std::string> outputColumnNames = node->getOutputColumnNames();
    const std::vector<voltdb::ValueType> outputColumnTypes = node->getOutputColumnTypes();
    const std::vector<int32_t> outputColumnSizes = node->getOutputColumnSizes();
    const std::vector<bool> outputColumnAllowNull(num_of_columns, true);
    TupleSchema *schema = TupleSchema::createTupleSchema(outputColumnTypes, outputColumnSizes, outputColumnAllowNull, true);
    std::string *columnNames = new std::string[num_of_columns];
    for (int ctr = 0; ctr < num_of_columns; ctr++) {
        columnNames[ctr] = node->getOutputColumnNames()[ctr];
    }
    node->setOutputTable(TableFactory::getTempTable(node->databaseId(), "temp", schema, columnNames, tempTableMemoryInBytes));

    delete[] columnNames;
    return true;
}

bool ReceiveExecutor::p_execute(const NValueArray &params) {
    int loadedDeps = 0;
    ReceivePlanNode* node = dynamic_cast<ReceivePlanNode*>(abstract_node);
    Table* output_table = dynamic_cast<Table*>(node->getOutputTable());

    // iterate dependencies stored in the frontend and union them
    // into the output_table. The engine does this work for peanuts.

    // todo: should pass the transaction's string pool through
    // as the underlying table loader would use it.
    do {
        loadedDeps =
        engine->loadNextDependency(output_table);
    } while (loadedDeps > 0);

    return true;
}

ReceiveExecutor::~ReceiveExecutor() {
}

}
