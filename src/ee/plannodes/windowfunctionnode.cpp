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
 * partitionbynode.cpp
 */
#include "windowfunctionnode.h"

#include <sstream>
#include "common/SerializableEEException.h"

namespace voltdb {
WindowFunctionPlanNode::~WindowFunctionPlanNode()
{

}

PlanNodeType WindowFunctionPlanNode::getPlanNodeType() const
{
    return PLAN_NODE_TYPE_WINDOWFUNCTION;
}

std::string WindowFunctionPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << "PartitionByPlanNode:";
    buffer << spacer << "\nAggregates[" << (int) m_aggregates.size() << "]: {";
    for (int ctr = 0, cnt = (int) m_aggregates.size(); ctr < cnt; ctr++) {
        buffer << spacer << "type="
               << expressionToString(m_aggregates[ctr]) << "\n";
        buffer << spacer << "distinct="
               << m_distinctAggregates[ctr] << "\n";
        buffer << spacer << "outcol="
               << m_aggregateOutputColumns[ctr] << "\n";
        buffer << spacer << "expr="
               << (m_aggregateInputExpressions[ctr] ?
                   m_aggregateInputExpressions[ctr]->debug(spacer) :
                   "null")
               << "\n";
    }
    buffer << spacer << "}";

    buffer << spacer << "\nGroupByExpressions[";
    std::string add = "";
    for (int ctr = 0, cnt = (int) m_partitionByExpressions.size();
         ctr < cnt; ctr++) {
        buffer << spacer << m_partitionByExpressions[ctr]->debug(spacer);
        add = ", ";
    }
    buffer << "]\n";

    return buffer.str();
}

void WindowFunctionPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    PlannerDomValue aggregateColumnsArray = obj.valueForKey("AGGREGATE_COLUMNS");
    for (int i = 0; i < aggregateColumnsArray.arrayLen(); i++) {
        PlannerDomValue aggregateColumnValue = aggregateColumnsArray.valueAtIndex(i);
        bool containsType = false;
        bool containsDistinct = false;
        bool containsOutputColumn = false;
        bool containsExpression = false;
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_TYPE")) {
            containsType = true;
            std::string aggregateColumnTypeString = aggregateColumnValue.valueForKey("AGGREGATE_TYPE").asStr();
            m_aggregates.push_back(stringToExpression(aggregateColumnTypeString));
        }
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_DISTINCT")) {
            containsDistinct = true;
            bool distinct = aggregateColumnValue.valueForKey("AGGREGATE_DISTINCT").asInt() == 1;
            m_distinctAggregates.push_back(distinct);
        }
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_OUTPUT_COLUMN")) {
            containsOutputColumn = true;
            int column = aggregateColumnValue.valueForKey("AGGREGATE_OUTPUT_COLUMN").asInt();
            m_aggregateOutputColumns.push_back(column);
        }
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_EXPRESSION")) {
            containsExpression = true;
            PlannerDomValue exprDom = aggregateColumnValue.valueForKey("AGGREGATE_EXPRESSION");
            m_aggregateInputExpressions.push_back(AbstractExpression::buildExpressionTree(exprDom));
        }

        if(!(containsType && containsDistinct && containsOutputColumn)) {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AggregatePlanNode::loadFromJSONObject:"
                                      " Missing type, distinct, or outputcolumn.");
        }
        if ( ! containsExpression) {
            m_aggregateInputExpressions.push_back(NULL);
        }
    }

    m_partitionByExpressions.loadExpressionArrayFromJSONObject("PARTITIONBY_EXPRESSIONS", obj);
    std::vector<AbstractExpression*>  orderByExpressions;
    // AggregatePlanNode knows there is an aggregate but
    // it doesn't know the input expression.  Since we are
    // coding the order by expression as the input expression
    // we will need special handling below.
    assert(m_aggregateInputExpressions.size() == 1);
    // The Java PartitionByPlanNode puts the order by
    // expressions in a sensible place.  However, we want
    // to subvert this sensible behavior by putting the
    // first and only one in the input expression list for
    // the only windowed aggregate in this node.  This is
    // temporizing around our unfortunate inability to
    // add order by expressions to Agg subobjects in the
    // executors.
    AbstractPlanNode::loadSortListFromJSONObject(obj, &orderByExpressions, NULL);
    assert(orderByExpressions.size() == 1);
    m_aggregateInputExpressions[0] = orderByExpressions[0];
}
}
