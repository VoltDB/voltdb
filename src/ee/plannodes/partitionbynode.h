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
#ifndef SRC_EE_PLANNODES_PARTITIONBYNODE_H_
#define SRC_EE_PLANNODES_PARTITIONBYNODE_H_
#include "aggregatenode.h"

namespace voltdb {
class PartitionByPlanNode : public AggregatePlanNode {
public:
    PartitionByPlanNode()
        : AggregatePlanNode(PLAN_NODE_TYPE_HASHAGGREGATE) {
    }
    ~PartitionByPlanNode();

    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;
protected:
    void loadFromJSONObject(PlannerDomValue obj);
};
}
#endif /* SRC_EE_PLANNODES_PARTITIONBYNODE_H_ */
