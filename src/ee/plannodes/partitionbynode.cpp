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
#include <sstream>
#include "partitionbynode.h"
#include "common/SerializableEEException.h"

namespace voltdb {
PartitionByPlanNode::~PartitionByPlanNode()
{

}

PlanNodeType PartitionByPlanNode::getPlanNodeType() const
{
    return PLAN_NODE_TYPE_PARTITIONBY;
}

void PartitionByPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    // Start with the base class.
    AggregatePlanNode::loadFromJSONObject(obj);
    // Read the sort expressions and directions.
    PlannerDomValue sortByColumnArray = obj.valueForKey("SORT_COLUMNS");
    for (int idx = 0; idx < sortByColumnArray.arrayLen(); idx += 1) {
        PlannerDomValue sortColumnValue = sortByColumnArray.valueAtIndex(idx);
        if (sortColumnValue.hasNonNullKey("SORT_EXPRESSION")) {
            PlannerDomValue exprDom = sortColumnValue.valueForKey("SORT_EXPRESSION");
            m_sortExpressions.push_back(AbstractExpression::buildExpressionTree(exprDom));
        } else {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                                       "PartitionByPlanNode::loadFromJSONObject:"
                                                       " Missing sort expression.");
        }
        if (sortColumnValue.hasNonNullKey("SORT_DIRECTION")) {
            std::string dirStr = sortColumnValue.valueForKey("SORT_DIRECTION").asStr();
            m_sortDirections.push_back(stringToSortDirection(dirStr));
        } else {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                                       "PartitionByPlanNode::loadFromJSONObject:"
                                                       " Missing sort direction.");
        }
    }
}

std::string PartitionByPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << "PartitionByPlanNode: ";
    buffer << AggregatePlanNode::debug(spacer);
    for (int idx = 0; idx < m_sortExpressions.size(); idx += 1) {
        buffer << m_sortExpressions[idx]->debug(spacer);
    }
        return buffer.str();
}
}
