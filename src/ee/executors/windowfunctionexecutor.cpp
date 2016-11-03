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
/*
 * partitionbyexecutor.cpp
 */

#include "executors/windowfunctionexecutor.h"

#include "../plannodes/windowfunctionnode.h"

namespace voltdb {

WindowFunctionExecutor::~WindowFunctionExecutor() {
}

bool WindowFunctionExecutor::p_init(AbstractPlanNode *init_node, TempTableLimits *limits) {
#if 0
    WindowFunctionPlanNode* node = dynamic_cast<WindowFunctionPlanNode*>(m_abstractNode);
    assert(node);
    assert(node == init_node);

    std::string spacer("");
    VOLT_DEBUG("WINDOW FUNCTION_EXECUTOR P_INIT:\n");
    for (int i = 0; i < m_aggregateInputExpressions.size(); i++) {

    }
    for (int i = 0; i < 0; i++) {
        VOLT_DEBUG("AGG INPUT EXPRESSION[%d]: %s",
                   i,
                   m_aggregateInputExpressions[i] ? m_aggregateInputExpressions[i]->debug().c_str() : "null\n");
    }

    /*
     * Find the difference between the set of aggregate output columns
     * (output columns resulting from an aggregate) and output columns.
     * Columns that are not the result of aggregates are being passed
     * through from the input table. Do this extra work here rather then
     * serialize yet more data.
     */
    std::vector<bool> outputColumnsResultingFromAggregates(node->getOutputSchema().size(), false);
    m_aggregateOutputColumns = node->getAggregateOutputColumns();
    BOOST_FOREACH(int aOC, m_aggregateOutputColumns) {
        outputColumnsResultingFromAggregates[aOC] = true;
    }
    for (int ii = 0; ii < outputColumnsResultingFromAggregates.size(); ii++) {
        if (outputColumnsResultingFromAggregates[ii] == false) {
            m_passThroughColumns.push_back(ii);
        }
    }

    if (!node->isInline()) {
        setTempOutputTable(limits);
    }
    m_partialSerialGroupByColumns = node->getPartialGroupByColumns();

    m_aggTypes = node->getAggregates();
    m_distinctAggs = node->getDistinctAggregates();
    m_groupByExpressions = node->getGroupByExpressions();
    node->collectOutputExpressions(m_outputColumnExpressions);

    // m_passThroughColumns.size() == m_groupByExpressions.size() is not true,
    // Because group by unique column may be able to select other columns
    m_prePredicate = node->getPrePredicate();
    m_postPredicate = node->getPostPredicate();

    m_groupByKeySchema = constructGroupBySchema(false);
    m_groupByKeyPartialHashSchema = NULL;
    if (m_partialSerialGroupByColumns.size() > 0) {
        for (int ii = 0; ii < m_groupByExpressions.size(); ii++) {
            if (std::find(m_partialSerialGroupByColumns.begin(),
                          m_partialSerialGroupByColumns.end(), ii)
                == m_partialSerialGroupByColumns.end() )
            {
                // Find the partial hash group by columns
                m_partialHashGroupByColumns.push_back(ii);;
            }
        }
        m_groupByKeyPartialHashSchema = constructGroupBySchema(true);
    }

    return true;
#else
    return false;
#endif
}

bool WindowFunctionExecutor::p_execute(const NValueArray& params) {
#if 0
    return true;
#else
    return false;
#endif
}

} /* namespace voltdb */
