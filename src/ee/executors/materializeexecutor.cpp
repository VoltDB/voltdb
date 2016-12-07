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

#include "materializeexecutor.h"
#include "common/debuglog.h"
#include "common/executorcontext.hpp"
#include "common/common.h"
#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "plannodes/materializenode.h"
#include "storage/table.h"
#include "storage/temptable.h"

namespace voltdb {

bool MaterializeExecutor::p_init(AbstractPlanNode* abstractNode,
                                 TempTableLimits* limits)
{
    VOLT_TRACE("init Materialize Executor");

    node = dynamic_cast<MaterializePlanNode*>(abstractNode);
    assert(node);
    batched = node->isBatched();

    // Construct the output table
    m_columnCount = static_cast<int>(node->getOutputSchema().size());
    assert(m_columnCount >= 0);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    // initialize local variables
    all_param_array_ptr = ExpressionUtil::convertIfAllParameterValues(node->getOutputColumnExpressions());
    all_param_array = all_param_array_ptr.get();

    needs_substitute_ptr = boost::shared_array<bool>(new bool[m_columnCount]);
    needs_substitute = needs_substitute_ptr.get();

    expression_array_ptr =
      boost::shared_array<AbstractExpression*>(new AbstractExpression*[m_columnCount]);
    expression_array = expression_array_ptr.get();

    for (int ctr = 0; ctr < m_columnCount; ctr++) {
        assert (node->getOutputColumnExpressions()[ctr] != NULL);
        expression_array_ptr[ctr] = node->getOutputColumnExpressions()[ctr];
        needs_substitute_ptr[ctr] = node->getOutputColumnExpressions()[ctr]->hasParameter();
    }

    //output table should be temptable
    output_table = dynamic_cast<TempTable*>(node->getOutputTable());

    return (true);
}

bool MaterializeExecutor::p_execute(const NValueArray &params) {
    assert (node == dynamic_cast<MaterializePlanNode*>(m_abstractNode));
    assert(node);
    assert (!node->isInline()); // inline projection's execute() should not be called
    assert (output_table == dynamic_cast<TempTable*>(node->getOutputTable()));
    assert (output_table);
    assert (m_columnCount == (int)node->getOutputColumnNames().size());

    // batched insertion
    if (batched) {
        int paramcnt = engine->getExecutorContext()->getUsedParameterCount();
        VOLT_TRACE("batched insertion with %d params. %d for each tuple.", paramcnt, m_columnCount);
        TableTuple &temp_tuple = output_table->tempTuple();
        for (int i = 0, tuples = paramcnt / m_columnCount; i < tuples; ++i) {
            for (int j = m_columnCount - 1; j >= 0; --j) {
                temp_tuple.setNValue(j, params[i * m_columnCount + j]);
            }
            output_table->insertTempTuple(temp_tuple);
        }
        VOLT_TRACE ("Materialized :\n %s", this->output_table->debug().c_str());
        return true;
    }


    if (all_param_array == NULL) {
        for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
            assert(expression_array[ctr]);
            VOLT_TRACE("predicate[%d]: %s", ctr, expression_array[ctr]->debug(true).c_str());
        }
    }

    // For now a MaterializePlanNode can make at most one new tuple We
    // should think about whether we would ever want to materialize
    // more than one tuple and whether such a thing is possible with
    // the AbstractExpression scheme
    TableTuple &temp_tuple = output_table->tempTuple();
    if (all_param_array != NULL) {
        VOLT_TRACE("sweet, all params\n");
        for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, params[all_param_array[ctr]]);
        }
    }
    else {
        TableTuple dummy;
        // add the generated value to the temp tuple. it must have the
        // same value type as the output column.
        for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
            temp_tuple.setNValue(ctr, expression_array[ctr]->eval(&dummy, NULL));
        }
    }

    // Add tuple to the output
    output_table->insertTempTuple(temp_tuple);

    return true;
}

MaterializeExecutor::~MaterializeExecutor() {
}

}
