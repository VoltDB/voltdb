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
    loadSortListFromJSONObject(obj, &orderByExpressions, NULL);
    assert(orderByExpressions.size() == 1);
    // We push the first sort expression here.  This is not
    // actually right, but it all works out.  When we want to
    // allow more sort expressions, say when we want to implement
    // row units and not range units for windowed functions, we
    // need to do a better job of this.
    m_aggregateInputExpressions.push_back(orderByExpressions[0]);
}
}
