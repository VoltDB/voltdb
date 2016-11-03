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
#ifndef SRC_EE_PLANNODES_WINDOWFUNCTIONNODE_H_
#define SRC_EE_PLANNODES_WINDOWFUNCTIONNODE_H_
#include "aggregatenode.h"

namespace voltdb {
class WindowFunctionPlanNode : public AbstractPlanNode {
public:
    typedef std::vector<OwningExpressionVector> AggregateExpressionList;
    void debugWriteAggregateExpressionList(std::ostringstream &buffer,
                               const std::string &spacer,
                               const std::string &label,
                               const OwningExpressionVector & exprs) const;
private:
    std::vector<ExpressionType> m_aggregates;
    std::vector<bool> m_distinctAggregates;
    std::vector<int> m_aggregateOutputColumns;
    AggregateExpressionList m_aggregateInputExpressions;
    //
    // What columns to partition.
    //
    AggregateExpressionList m_partitionByExpressions;
    // What columns to sort.
    AggregateExpressionList m_orderByExpressions;
public:
    WindowFunctionPlanNode()
        : AbstractPlanNode() {}
    virtual ~WindowFunctionPlanNode();

    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;

    const std::vector<int>& getAggregateOutputColumns() const {
        return m_aggregateOutputColumns;
    }

    const std::vector<ExpressionType>& getAggregates() const {
        return m_aggregates;
    }

    const std::vector<bool>& getDistinctAggregates() const {
        return m_distinctAggregates;
    }

    const AggregateExpressionList& getAggregateInputExpressions() const {
        return m_aggregateInputExpressions;
    }

    const AggregateExpressionList& getOrderByExpressions() const {
        return m_orderByExpressions;
    }

    const AggregateExpressionList& getPartitionByExpressions() const {
        return m_partitionByExpressions;
    }

protected:
    void loadFromJSONObject(PlannerDomValue obj);
};
}
#endif /* SRC_EE_PLANNODES_WINDOWFUNCTIONNODE_H_ */
