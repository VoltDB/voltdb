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

std::string PartitionByPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << "PartitionByPlanNode: ";
    buffer << AggregatePlanNode::debugInfo(spacer);
    return buffer.str();
}

void PartitionByPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    AggregatePlanNode::loadFromJSONObject(obj);
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
