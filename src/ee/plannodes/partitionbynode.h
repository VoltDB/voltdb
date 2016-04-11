#ifndef SRC_EE_PLANNODES_PARTITIONBYNODE_H_
#define SRC_EE_PLANNODES_PARTITIONBYNODE_H_
#include "aggregatenode.h"

namespace voltdb {
class PartitionByPlanNode : public AggregatePlanNode {
public:
	PartitionByPlanNode(PlanNodeType type)
		: AggregatePlanNode(type) {
	}
	~PartitionByPlanNode();

	PlanNodeType getPlanNodeType() const;
	std::string debugInfo(const std::string &spacer) const;

	const std::vector<AbstractExpression*> getSortExpressions() const {
		return m_sortExpressions;
	}

	const std::vector<SortDirectionType> getSortDirections() const {
		return m_sortDirections;
	}
protected:
	void loadFromJSONObject(PlannerDomValue obj);

private:
	OwningExpressionVector          m_sortExpressions;
	std::vector<SortDirectionType>  m_sortDirections;
};
}
#endif /* SRC_EE_PLANNODES_PARTITIONBYNODE_H_ */
