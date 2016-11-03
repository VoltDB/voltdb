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
#ifndef SRC_EE_EXECUTORS_PARTITIONBYEXECUTOR_H_
#define SRC_EE_EXECUTORS_PARTITIONBYEXECUTOR_H_

#include "abstractexecutor.h"
#include "plannodes/windowfunctionnode.h"

namespace voltdb {

/**
 * This is the executor for a WindowFunctionPlanNode.
 */
class WindowFunctionExecutor: public AbstractExecutor {
    /**
     * Remember which columns are pass through columns.
     */
    std::vector<int> m_passThroughColumns;
    /**
     * Remember which columns are aggregate output columns.
     */
    std::vector<int> m_aggregateOutputColumns;
    Pool m_memoryPool;
    /**
     * The operation type of the aggregates.
     */
    const std::vector<ExpressionType> &m_aggTypes;
    /**
     * Element j is true iff aggregate j is distinct.
     */
    const std::vector<bool> &m_distinctAggs;
    /**
     * Element j is the list of partition by expressions for
     * aggregate j.
     */
    const WindowFunctionPlanNode::AggregateExpressionList &m_partitionByExpressions;
    /**
     * Element j is the list of order by expressions for aggregate j.
     */
    const WindowFunctionPlanNode::AggregateExpressionList &m_orderByExpressions;
    /**
     * Element j is the list of aggregate arguments for aggregate j.
     */
    const WindowFunctionPlanNode::AggregateExpressionList &m_aggregateInputExpressions;
    /**
     * All output column expressions.
     */
    const std::vector<AbstractExpression*> m_outputColumnExpressions;

public:
    WindowFunctionExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node)
      : AbstractExecutor(engine, abstract_node),
        m_aggTypes(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getAggregates()),
        m_distinctAggs(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getDistinctAggregates()),
        m_partitionByExpressions(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getPartitionByExpressions()),
        m_orderByExpressions(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getOrderByExpressions()),
        m_aggregateInputExpressions(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getAggregateInputExpressions()) {
    }
    virtual ~WindowFunctionExecutor();

    /**
     * Returns the integer output column index for each aggregate function.
     */
    const std::vector<int>& getAggregateOutputColumns() const {
        return m_aggregateOutputColumns;
    }

    /**
     * Returns the input expressions for each aggregate in the
     * list.  Only one input expression per aggregate is used
     * right now, but we need room for expansion.
     */
    const WindowFunctionPlanNode::AggregateExpressionList &getAggregateInputExpressions() const {
        return m_aggregateInputExpressions;
    }

    /**
     * Returns the list of partition by expressions for each
     * aggregate.  We will need these to partition the input.
     */
    const WindowFunctionPlanNode::AggregateExpressionList& getPartitionByExpressions() const {
        return m_partitionByExpressions;
    }

    /**
     * Returns the aggregate types for each expression.  The
     * aggregate types are the operation type and not the
     * value's type. So, they are things like "MIN", "MAX",
     * "RANK" and so forth.  They are not "TINYINT" or "FLOAT".
     */
    const std::vector<ExpressionType>& getAggTypes() const {
        return m_aggTypes;
    }

    /**
     * Returns true in index j if the aggregate expression
     * for j is distinct.
     */
    const std::vector<bool>& getDistinctAggs() const {
        return m_distinctAggs;
    }

    /**
     * Returns the output column expressions.  These are
     * used to calculate the passthrough columns.
     */
    const std::vector<AbstractExpression*>& getOutputColumnExpressions() const {
        return m_outputColumnExpressions;
    }

    const std::vector<int>& getPassThroughColumns() const {
        return m_passThroughColumns;
    }


protected:
    virtual bool p_init(AbstractPlanNode*, TempTableLimits*);

    /** Concrete executor classes implement execution in p_execute() */
    virtual bool p_execute(const NValueArray& params);
};

} /* namespace voltdb */

#endif /* SRC_EE_EXECUTORS_PARTITIONBYEXECUTOR_H_ */
