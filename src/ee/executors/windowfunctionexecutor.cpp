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

/**
 * When this function is called, the AbstractExecutor's init function
 * will have set the input tables in the plan node, but nothing else.
 */
bool WindowFunctionExecutor::p_init(AbstractPlanNode *init_node, TempTableLimits *limits) {
#if 0
    AggregatePlanNode* node = dynamic_cast<AggregatePlanNode*>(m_abstractNode);
    assert(node);

    m_inputExpressions = node->getAggregateInputExpressions();
    for (int i = 0; i < m_inputExpressions.size(); i++) {
        VOLT_DEBUG("AGG INPUT EXPRESSION[%d]: %s",
                   i,
                   m_inputExpressions[i] ? m_inputExpressions[i]->debug().c_str() : "null");
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
    // Input table
    Table* input_table = m_abstractNode->getInputTable();
    assert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());
    TableIterator it = input_table->iteratorDeletingAsWeGo();
    TableTuple nextTuple(input_table->schema());

    ProgressMonitorProxy pmp(m_engine, this);
    AggregateSerialExecutor::p_execute_init(params, &pmp, input_table->schema(), NULL);

    while (m_postfilter.isUnderLimit() && it.next(nextTuple)) {
        m_pmp->countdownProgress();
        AggregateSerialExecutor::p_execute_tuple(nextTuple);
    }
    AggregateSerialExecutor::p_execute_finish();
    VOLT_TRACE("finalizing..");

    cleanupInputTempTable(input_table);
    return true;
#else
    return false;
#endif
}

} /* namespace voltdb */
