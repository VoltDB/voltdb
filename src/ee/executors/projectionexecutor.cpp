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

#include "projectionexecutor.h"
#include "expressions/expressionutil.h"
#include "plannodes/projectionnode.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

namespace voltdb {

bool ProjectionExecutor::p_init(AbstractPlanNode *abstractNode,
                                const ExecutorVector& executorVector)
{
    VOLT_TRACE("init Projection Executor");

    ProjectionPlanNode* node = dynamic_cast<ProjectionPlanNode*>(abstractNode);
    vassert(node);

    // Create output table based on output schema from the plan
    setTempOutputTable(executorVector);

    m_columnCount = static_cast<int>(node->getOutputSchema().size());

    // initialize local variables
    m_allTupleArrayPtr = ExpressionUtil::convertIfAllTupleValues(node->getOutputColumnExpressions());
    m_allTupleArray = m_allTupleArrayPtr.get();
    m_allParamArrayPtr = ExpressionUtil::convertIfAllParameterValues(node->getOutputColumnExpressions());
    m_allParamArray = m_allParamArrayPtr.get();

    m_needsSubstitutePtr = boost::shared_array<bool>(new bool[m_columnCount]);
    m_needsSubstitute = m_needsSubstitutePtr.get();
    typedef AbstractExpression* ExpRawPtr;
    expression_array_ptr = boost::shared_array<ExpRawPtr>(new ExpRawPtr[m_columnCount]);
    expression_array = expression_array_ptr.get();
    for (int ctr = 0; ctr < m_columnCount; ctr++) {
        vassert(node->getOutputColumnExpressions()[ctr] != NULL);

        VOLT_TRACE("OutputColumnExpressions [%d]: %s", ctr,
                node->getOutputColumnExpressions()[ctr]->debug(true).c_str());

        expression_array_ptr[ctr] = node->getOutputColumnExpressions()[ctr];
        m_needsSubstitutePtr[ctr] = node->getOutputColumnExpressions()[ctr]->hasParameter();
    }


    m_outputTable = dynamic_cast<AbstractTempTable*>(node->getOutputTable()); //output table should be temptable

    if (!node->isInline()) {
        Table* input_table = node->getInputTable();
        m_tuple = TableTuple(input_table->schema());
    }
    return true;
}

bool ProjectionExecutor::p_execute(const NValueArray &params) {
    __attribute__((unused)) ProjectionPlanNode* node =
        dynamic_cast<ProjectionPlanNode*>(m_abstractNode);
    vassert(node);
    vassert(! node->isInline()); // inline projection's execute() should not be
                                // called
    vassert(m_outputTable == dynamic_cast<AbstractTempTable*>(node->getOutputTable()));
    vassert(m_outputTable);
    Table* input_table = m_abstractNode->getInputTable();
    vassert(input_table);

    VOLT_TRACE("INPUT TABLE: %s\n", input_table->debug().c_str());

    vassert(m_columnCount == (int)node->getOutputColumnNames().size());
    if (m_allTupleArray == NULL && m_allParamArray == NULL) {
        for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
            vassert(expression_array[ctr]);
            VOLT_TRACE("predicate[%d]: %s", ctr,
                       expression_array[ctr]->debug(true).c_str());
        }
    }

    //
    // Now loop through all the tuples and push them through our output
    // expression This will generate new tuple values that we will insert into
    // our output table
    //
    TableIterator iterator = input_table->iteratorDeletingAsWeGo();
    vassert(m_tuple.columnCount() == input_table->columnCount());
    while (iterator.next(m_tuple)) {
        //
        // Project (or replace) values from input tuple
        //
        TableTuple &temp_tuple = m_outputTable->tempTuple();
        if (m_allTupleArray != NULL) {
            VOLT_TRACE("sweet, all tuples");
            for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, m_tuple.getNValue(m_allTupleArray[ctr]));
            }
        } else if (m_allParamArray != NULL) {
            VOLT_TRACE("sweet, all params");
            for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, params[m_allParamArray[ctr]]);
            }
        } else {
            for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, expression_array[ctr]->eval(&m_tuple, NULL));
            }
        }
        m_outputTable->insertTempTuple(temp_tuple);

        VOLT_TRACE("OUTPUT TABLE: %s\n", m_outputTable->debug().c_str());
    }

    return true;
}

ProjectionExecutor::~ProjectionExecutor() {
}

}
