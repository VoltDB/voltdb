/*
 * partitionbynode.cpp
 *
 *  Created on: Apr 11, 2016
 *      Author: bwhite
 */
#include "partitionbynode.h"

namespace voltdb {
PartitionByPlanNode::~PartitionByPlanNode()
{

}

PlanNodeType ParitionByPlanNode::getPlanNodeType() const
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
			m_sortExpressions.push_back(AbstractExpression.buildExpressionTree(exprDom));
		} else {
			throw SerializedEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
										"PartitionByPlanNode::loadFromJSONObject:"
										" Missing sort expression.");
		}
		if (sortColumnValue.hasNonNullKey("SORT_DIRECTION")) {
			std::string dirStr = sortColumnValue.valueForKey("SORT_DIRECTION").asStr();
			m_sortDirections.push_back(sortDirectionToString(dirStr));
		} else {
			throw SerializedEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
										"PartitionByPlanNode::loadFromJSONObject:"
										" Missing sort direction.");
		}
	}
}

std::string PartitionByPlanNode::debugInfo(const std::string &spacer)
{
	std::ostringstream buffer;
	buffer << "PartitionByPlanNode: ";
	buffer << AggregatePlanNode::debug(spacer);
	for (int idx = 0; idx < m_sortExpressions.size(); idx += 1) {
		buffer << m_sortExpressions[idx]->debug(spacer);
	}
}
}



