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

#include <tuple>
#include "limitexecutor.h"
#include "plannodes/limitnode.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"

using namespace voltdb;

bool
LimitExecutor::p_init(AbstractPlanNode* abstract_node,
                      const ExecutorVector& executorVector) {
    VOLT_TRACE("init limit Executor");

    LimitPlanNode* node = dynamic_cast<LimitPlanNode*>(abstract_node);
    vassert(node);

    //
    // Skip if we are inline
    //
    if (!node->isInline()) {
        //
        // Just copy the table schema of our input table
        //
        vassert(node->getInputTableCount() == 1);
        node->
            setOutputTable(TableFactory::
                           buildCopiedTempTable(node->getInputTable()->name(),
                                                node->getInputTable(),
                                                executorVector));
    }
    return true;
}

bool LimitExecutor::p_execute(const NValueArray &params) {
    LimitPlanNode* node = dynamic_cast<LimitPlanNode*>(m_abstractNode);
    vassert(node);
    Table* output_table = node->getOutputTable();
    vassert(output_table);
    Table* input_table = node->getInputTable();
    vassert(input_table);

    //
    // Grab the iterator for our input table, and loop through until
    // we have copy enough tuples for the limit specified by the node
    //
    TableTuple tuple(input_table->schema());
    TableIterator iterator = input_table->iteratorDeletingAsWeGo();

    int tuple_ctr = 0;
    int limit = -1;
    int offset = -1;
    std::tie(limit, offset) = node->getLimitAndOffset(params);

    if (iterator.advance(tuple, offset) < offset) {
        return true;     // offset beyond table count: empty table
    } else {
        while((limit < 0 || tuple_ctr++ < limit) && iterator.next(tuple)) {
            if (!output_table->insertTuple(tuple)) {
                VOLT_ERROR("Failed to insert tuple from input table '%s' into output table '%s'",
                        input_table->name().c_str(), output_table->name().c_str());
                return false;
            }
        }
        return true;
    }
}

