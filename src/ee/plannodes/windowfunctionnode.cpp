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

void WindowFunctionPlanNode::debugWriteAggregateExpressionList(
        std::ostringstream &buffer,
        const std::string &spacer,
        const std::string &label,
        const AggregateExpressionList& exprs) const {
    buffer << spacer << label << "= {";
    for (int ictr = 0; ictr < exprs.size(); ictr += 1) {
        std::vector<AbstractExpression*> &argVec = m_aggregateInputExpressions[ictr];
        buffer << spacer << "  "
                << (argVec[ictr] ? argVec[ictr]->debug(spacer) : "null")
                << "\n";
    }
    buffer << spacer << "}\n";
}

std::string WindowFunctionPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << "WindowFunctionPlanNode:\n";
    buffer << spacer << "\nAggregates[" << (int) m_aggregates.size() << "]: {";
    for (int ctr = 0, cnt = (int) m_aggregates.size(); ctr < cnt; ctr++) {
        buffer << spacer << "type="
               << expressionToString(m_aggregates[ctr]) << "\n";
        buffer << spacer << "distinct="
               << m_distinctAggregates[ctr] << "\n";
        buffer << spacer << "outcol="
               << m_aggregateOutputColumns[ctr] << "\n";
        debugWriteAggregateExpressionList(buffer, spacer, "arguments", m_aggregateInputExpressions);
        debugWriteAggregateExpressionList(buffer, spacer, "partitionBys", m_partitionByExpressions);
        debugWriteAggregateExpressionList(buffer, spacer, "orderBys", m_orderByExpressions);
    }
    buffer << spacer << "}";
    return buffer.str();
}

void WindowFunctionPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    PlannerDomValue aggregateColumnsArray = obj.valueForKey("AGGREGATE_COLUMNS");
    for (int i = 0; i < aggregateColumnsArray.arrayLen(); i++) {
        PlannerDomValue aggregateColumnValue = aggregateColumnsArray.valueAtIndex(i);
        bool containsType = false;
        bool containsDistinct = false;
        bool containsOutputColumn = false;
        bool containsExpressions = false;
        bool containsPartitionExpressions = false;
        bool containsOrderByExpressions = false;
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
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_EXPRESSIONS")) {
            containsExpressions = true;
            OwningExpressionVector exprVec;
            exprVec.loadExpressionArrayFromJSONObject("AGGREGATE_EXPRESSIONS", aggregateColumnValue);
            m_aggregateInputExpressions.push_back(exprVec);
        }
        if (aggregateColumnValue.hasNonNullKey("PARTITIONBY_EXPRESSIONS")) {
            containsPartitionExpressions = true;
            OwningExpressionVector exprVec;
            exprVec.loadExpressionArrayFromJSONObject("PARTITIONBY_EXPRESSIONS",
                                                      aggregateColumnValue);
            m_partitionByExpressions.push_back(exprVec);
        }
        if (aggregateColumnValue.hasNonNullKey("SORT_COLUMNS")) {
            containsOrderByExpressions = true;
            OwningExpressionVector exprVec;
            PlannerDomValue sortListObj = aggregateColumnValue.valueForKey("SORT_COLUMNS");
            // We don't need the sort directions here.
            loadSortListFromJSONObject(sortListObj,
                                       &exprVec,
                                       NULL);
            m_orderByExpressions.push_back(exprVec);
        }

        if(!(containsType && containsDistinct && containsOutputColumn
                && containsPartitionExpressions
                && containsOrderByExpressions
                && containsExpressions)) {
            std::ostringstream buffer;
            std::string sep = "";
            if (!containsType) {
                buffer << "Aggregate Type";
                sep = ", ";
            }
            if (!containsDistinct) {
                buffer << sep << "Distinct";
                sep = ", ";
            }
            if (!containsOutputColumn) {
                buffer << sep << "Output Column";
                sep = ", ";
            }
            if (!containsPartitionExpressions) {
                buffer << sep << "Partition By List";
                sep = ", ";
            }
            if (!containsOrderByExpressions) {
                buffer << sep << "Order By List";
                sep = ", ";
            }
            if (!containsExpressions) {
                buffer << sep << "Aggregate Argument Expressions";
            }
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "WindowFunctionPlanNode::loadFromJSONObject:"
                                      " Missing components: "
                                      + buffer.str());
        }
    }
}
}
