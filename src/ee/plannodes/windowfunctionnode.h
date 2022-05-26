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
#ifndef SRC_EE_PLANNODES_WINDOWFUNCTIONNODE_H_
#define SRC_EE_PLANNODES_WINDOWFUNCTIONNODE_H_
#include "abstractplannode.h"

namespace voltdb {
class WindowFunctionPlanNode : public AbstractPlanNode {
public:
    typedef std::vector<OwningExpressionVector> AggregateExpressionList;
    void debugWriteAggregateExpressionList(std::ostringstream &buffer,
                               const std::string &spacer,
                               const std::string &label,
                               const OwningExpressionVector & exprs) const;
    WindowFunctionPlanNode()
        : AbstractPlanNode()
        , m_aggregates()
        , m_aggregateOutputColumns()
        , m_aggregateInputExpressions()
        , m_partitionByExpressions()
        , m_orderByExpressions()
    {
    }

    virtual ~WindowFunctionPlanNode();

    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;

    const std::vector<ExpressionType>& getAggregates() const {
        return m_aggregates;
    }

    const AggregateExpressionList& getAggregateInputExpressions() const {
        return m_aggregateInputExpressions;
    }

    const OwningExpressionVector& getOrderByExpressions() const {
        return m_orderByExpressions;
    }

    const OwningExpressionVector& getPartitionByExpressions() const {
        return m_partitionByExpressions;
    }

    void collectOutputExpressions(std::vector<AbstractExpression *>&columnExpressions) const;
protected:
    void loadFromJSONObject(PlannerDomValue obj);
private:
    std::vector<ExpressionType> m_aggregates;
    std::vector<int> m_aggregateOutputColumns;
    AggregateExpressionList m_aggregateInputExpressions;
    //
    // What columns to partition.
    //
    OwningExpressionVector m_partitionByExpressions;
    // What columns to sort.
    OwningExpressionVector m_orderByExpressions;
};
}
#endif /* SRC_EE_PLANNODES_WINDOWFUNCTIONNODE_H_ */
