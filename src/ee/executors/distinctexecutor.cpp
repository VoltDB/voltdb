/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "distinctexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "plannodes/distinctnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

#include <set>
#include <cassert>

using namespace voltdb;

bool DistinctExecutor::p_init(AbstractPlanNode*,
                              TempTableLimits* limits)
{
    VOLT_DEBUG("init Distinct Executor");
    DistinctPlanNode* node = dynamic_cast<DistinctPlanNode*>(m_abstractNode);
    assert(node);
    //
    // Create a duplicate of input table
    //
    if (!node->isInline()) {
        assert(node->getInputTableCount() == 1);
        assert(node->getInputTable()->columnCount() > 0);
        assert(node->getChildren()[0] != NULL);

        node->
            setOutputTable(TableFactory::
                           getCopiedTempTable(node->databaseId(),
                                              node->getInputTable()->name(),
                                              node->getInputTable(),
                                              limits));
    }
    return (true);
}

bool DistinctExecutor::p_execute(const NValueArray &params) {
    DistinctPlanNode* node = dynamic_cast<DistinctPlanNode*>(m_abstractNode);
    assert(node);
    Table* output_table = node->getOutputTable();
    assert(output_table);
    Table* input_table = node->getInputTable();
    assert(input_table);

    TableIterator iterator = input_table->iteratorDeletingAsWeGo();
    TableTuple tuple(input_table->schema());

    AbstractExpression *distinctExpression = node->getDistinctExpression();
    std::set<NValue, NValue::ltNValue> found_values;
    while (iterator.next(tuple)) {
        //
        // Check whether this value already exists in our list
        //
        NValue tuple_value = distinctExpression->eval(&tuple, NULL);
        if (found_values.find(tuple_value) == found_values.end()) {
            found_values.insert(tuple_value);
            if (!output_table->insertTuple(tuple)) {
                VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                           " output table '%s'",
                           input_table->name().c_str(),
                           output_table->name().c_str());
                cleanupInputTempTable(input_table);
                return false;
            }
        }
    }

    cleanupInputTempTable(input_table);
    return true;
}

DistinctExecutor::~DistinctExecutor() {
}
