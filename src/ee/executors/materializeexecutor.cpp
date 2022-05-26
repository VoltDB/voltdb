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

#include "materializeexecutor.h"
#include "expressions/expressionutil.h"
#include "plannodes/materializenode.h"

namespace voltdb {

bool MaterializeExecutor::p_init(AbstractPlanNode* abstractNode,
                                 const ExecutorVector& executorVector)
{
    VOLT_TRACE("init Materialize Executor");

    m_node = dynamic_cast<MaterializePlanNode*>(abstractNode);
    vassert(m_node);
    m_batched = m_node->isBatched();

    // Construct the output table
    m_columnCount = static_cast<int>(m_node->getOutputSchema().size());
    vassert(m_columnCount >= 0);

    // Create output table based on output schema from the plan
    setTempOutputTable(executorVector);

    // initialize local variables
    m_allParamArrayPtr = ExpressionUtil::convertIfAllParameterValues(m_node->getOutputColumnExpressions());
    m_allParamArray = m_allParamArrayPtr.get();

    m_needsSubstitutePtr = boost::shared_array<bool>(new bool[m_columnCount]);
    m_needsSubstitute = m_needsSubstitutePtr.get();

    m_expressionArrayPtr =
      boost::shared_array<AbstractExpression*>(new AbstractExpression*[m_columnCount]);
    m_expressionArray = m_expressionArrayPtr.get();

    for (int ctr = 0; ctr < m_columnCount; ctr++) {
        vassert(m_node->getOutputColumnExpressions()[ctr] != NULL);
        m_expressionArrayPtr[ctr] = m_node->getOutputColumnExpressions()[ctr];
        m_needsSubstitutePtr[ctr] = m_node->getOutputColumnExpressions()[ctr]->hasParameter();
    }

    //output table should be temptable
    m_outputTable = dynamic_cast<AbstractTempTable*>(m_node->getOutputTable());

    return (true);
}

bool MaterializeExecutor::p_execute(const NValueArray &params) {
    vassert(m_node == dynamic_cast<MaterializePlanNode*>(m_abstractNode));
    vassert(m_node);
    vassert(!m_node->isInline()); // inline projection's execute() should not be called
    vassert(m_outputTable == dynamic_cast<AbstractTempTable*>(m_node->getOutputTable()));
    vassert(m_outputTable);
    vassert(m_columnCount == (int)m_node->getOutputColumnNames().size());

    // batched insertion
    if (m_batched) {
        int paramcnt = m_engine->getExecutorContext()->getUsedParameterCount();
        VOLT_TRACE("batched insertion with %d params. %d for each tuple.", paramcnt, m_columnCount);
        TableTuple &temp_tuple = m_outputTable->tempTuple();
        for (int i = 0, tuples = paramcnt / m_columnCount; i < tuples; ++i) {
            for (int j = m_columnCount - 1; j >= 0; --j) {
                temp_tuple.setNValue(j, params[i * m_columnCount + j]);
            }
            m_outputTable->insertTempTuple(temp_tuple);
        }
        VOLT_TRACE ("Materialized :\n %s", this->m_outputTable->debug().c_str());
        return true;
    }


    if (m_allParamArray == NULL) {
        for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
            vassert(m_expressionArray[ctr]);
            VOLT_TRACE("predicate[%d]: %s", ctr, m_expressionArray[ctr]->debug(true).c_str());
        }
    }

    // For now a MaterializePlanNode can make at most one new tuple We
    // should think about whether we would ever want to materialize
    // more than one tuple and whether such a thing is possible with
    // the AbstractExpression scheme
    TableTuple &temp_tuple = m_outputTable->tempTuple();
    if (m_allParamArray != NULL) {
        VOLT_TRACE("sweet, all params\n");
        for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
            try {
                temp_tuple.setNValue(ctr, params[m_allParamArray[ctr]]);
            } catch (SQLException& ex) {
                std::string errorMsg = ex.message()
                                    + " '" + (m_outputTable -> getColumnNames()).at(ctr) + "'";

                throw SQLException(ex.getSqlState(), errorMsg, ex.getInternalFlags());
            }
        }
    }
    else {
        TableTuple dummy;
        // add the generated value to the temp tuple. it must have the
        // same value type as the output column.
        for (int ctr = m_columnCount - 1; ctr >= 0; --ctr) {
            try {
                temp_tuple.setNValue(ctr, m_expressionArray[ctr]->eval(&dummy, NULL));
            } catch (SQLException& ex) {
                std::string errorMsg = ex.message()
                                    + " '" + (m_outputTable -> getColumnNames()).at(ctr) + "'";

                throw SQLException(ex.getSqlState(), errorMsg, ex.getInternalFlags());
            }
        }
    }

    // Add tuple to the output
    m_outputTable->insertTempTuple(temp_tuple);

    return true;
}

MaterializeExecutor::~MaterializeExecutor() {
}

}
