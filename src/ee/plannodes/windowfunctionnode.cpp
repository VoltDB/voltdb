/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

namespace voltdb {
WindowFunctionPlanNode::~WindowFunctionPlanNode() { }

PlanNodeType WindowFunctionPlanNode::getPlanNodeType() const {
    return PlanNodeType::WindowFunction;
}

void WindowFunctionPlanNode::debugWriteAggregateExpressionList(
        std::ostringstream &buffer,
        const std::string &spacer,
        const std::string &label,
        const OwningExpressionVector& argVec) const {
    buffer << spacer << label
           << "(" << argVec.size() << ") = {\n";
    for (int ictr = 0; ictr < argVec.size(); ictr += 1) {
        buffer << spacer << "  "
                << ictr << ".) "
                << (argVec[ictr] ? argVec[ictr]->debug("") : "null")
                << "\n";
    }
    buffer << spacer << "}\n";
}

std::string WindowFunctionPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << "\n" << spacer << "Aggregates[" << (int) m_aggregates.size() << "]: {\n";
    std::string nspacer = spacer + "   ";
    for (int ctr = 0, cnt = (int) m_aggregates.size(); ctr < cnt; ctr++) {
        buffer << nspacer << "type="
               << expressionToString(m_aggregates[ctr]) << "\n";
        buffer << nspacer << "outcol="
               << m_aggregateOutputColumns[ctr] << "\n";
        debugWriteAggregateExpressionList(buffer, nspacer, "arguments", m_aggregateInputExpressions[ctr]);
    }
    debugWriteAggregateExpressionList(buffer, spacer, "partitionBys", m_partitionByExpressions);
    debugWriteAggregateExpressionList(buffer, spacer, "orderBys", m_orderByExpressions);
    buffer << spacer << "}";
    return buffer.str();
}

void WindowFunctionPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    PlannerDomValue aggregateColumnsArray = obj.valueForKey("AGGREGATE_COLUMNS");
    bool containsType = false;
    bool containsOutputColumn = false;
    bool containsExpressions = false;
    bool containsPartitionExpressions = false;
    bool containsOrderByExpressions = false;
    for (int i = 0; i < aggregateColumnsArray.arrayLen(); i++) {
        PlannerDomValue aggregateColumnValue = aggregateColumnsArray.valueAtIndex(i);
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_TYPE")) {
            containsType = true;
            std::string aggregateColumnTypeString = aggregateColumnValue.valueForKey("AGGREGATE_TYPE").asStr();
            m_aggregates.push_back(stringToExpression(aggregateColumnTypeString));
        }
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_OUTPUT_COLUMN")) {
            containsOutputColumn = true;
            int column = aggregateColumnValue.valueForKey("AGGREGATE_OUTPUT_COLUMN").asInt();
            m_aggregateOutputColumns.push_back(column);
        }
        if (aggregateColumnValue.hasNonNullKey("AGGREGATE_EXPRESSIONS")) {
            containsExpressions = true;
            size_t nExprs = m_aggregateInputExpressions.size();
            m_aggregateInputExpressions.push_back(OwningExpressionVector());
            OwningExpressionVector &exprVec = m_aggregateInputExpressions[nExprs];
            exprVec.loadExpressionArrayFromJSONObject("AGGREGATE_EXPRESSIONS", aggregateColumnValue);
        }
        if(!(containsType && containsOutputColumn && containsExpressions)) {
            std::ostringstream buffer;
            std::string sep = "";
            if (!containsType) {
                buffer << "Aggregate Type";
                sep = ", ";
            }
            if (!containsOutputColumn) {
                buffer << sep << "Output Column";
                sep = ", ";
            }
            if (!containsExpressions) {
                buffer << sep << "Aggregate Argument Expressions";
            }
            throwSerializableEEException(
                    "WindowFunctionPlanNode::loadFromJSONObject: Aggregate missing components: %s",
                    buffer.str().c_str());
        }

    }
    if (obj.hasNonNullKey("PARTITIONBY_EXPRESSIONS")) {
        containsPartitionExpressions = true;
        m_partitionByExpressions.loadExpressionArrayFromJSONObject("PARTITIONBY_EXPRESSIONS", obj);
    }
    if (obj.hasNonNullKey("SORT_COLUMNS")) {
        containsOrderByExpressions = true;
        m_orderByExpressions.clear();
        loadSortListFromJSONObject(obj, &m_orderByExpressions, NULL);
    }
    if (!(containsPartitionExpressions && containsOrderByExpressions)) {
        std::ostringstream buffer;
        std::string sep = "";
        if (!containsPartitionExpressions) {
            buffer << sep << "Partition By List";
            sep = ", ";
        }
        if (!containsOrderByExpressions) {
            buffer << sep << "Order By List";
            sep = ", ";
        }
        throwSerializableEEException(
                "WindowFunctionPlanNode::loadFromJSONObject: Missing components: %s",
                buffer.str().c_str());
    }
}

void WindowFunctionPlanNode::collectOutputExpressions(std::vector<AbstractExpression *>&outputColumnExpressions) const
{
    const std::vector<SchemaColumn*>& outputSchema = getOutputSchema();
    size_t size = outputSchema.size();
    outputColumnExpressions.resize(size);
    for (int ii = 0; ii < size; ii++) {
        SchemaColumn* outputColumn = outputSchema[ii];
        outputColumnExpressions[ii] = outputColumn->getExpression();
    }
}

}
