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
 * A WindowAgg is used to calculate one kind of
 * windowed aggregate.  The algorithm
 */
struct WindowedAgg {
    /**
     * Initialize the window state for a given aggregate function.
     */
    virtual void initWindowState(Table *inputTable);
    /**
     * Given a new current row, update the window.
     */
    virtual void updateWindowState(TableIterator &currentRow);
    /**
     * Calculate the value of the aggregate for the given row.
     */
    virtual void calculateRowValue(TableIterator &currentRow);
};

/**
 * This is the executor for a WindowFunctionPlanNode.
 */
class WindowFunctionExecutor: public AbstractExecutor {
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
    const AbstractPlanNode::OwningExpressionVector &m_partitionByExpressions;
    /**
     * Element j is the list of order by expressions for aggregate j.
     */
    const AbstractPlanNode::OwningExpressionVector &m_orderByExpressions;
    /**
     * Element j is the list of aggregate arguments for aggregate j.
     */
    const WindowFunctionPlanNode::AggregateExpressionList &m_aggregateInputExpressions;
    /**
     * All output column expressions.
     */
    const std::vector<AbstractExpression*> m_outputColumnExpressions;
    /**
     * We maintain a sequence of WindowAggs, one for each windowed aggregate.
     * A WindowAgg contains the window and aggregate state.  The window state
     * is an iterator for the start and end of a window.  This is an iterator
     * into the input table.  There is an iterator for the row we are traversing,
     * but this is not part of the WindowAgg state.  It's shared.
     *
     * Since we only support one windowed aggregate, this is a sequence of one.
     */
    std::vector<WindowAgg *>m_aggregateState;
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
     * Returns the input expressions for each aggregate in the
     * list.  Only one input expression per aggregate is used
     * right now, but we need room for expansion.
     */
    const WindowFunctionPlanNode::AggregateExpressionList &getAggregateInputExpressions() const {
        return m_aggregateInputExpressions;
    }

    /**
     * Returns the list of partition by expressions.  There is
     * one, shared among all aggregates.  We will need these to partition the input.
     */
    const AbstractPlanNode::OwningExpressionVector& getPartitionByExpressions() const {
        return m_partitionByExpressions;
    }

    /**
     * Returns the order by expressions.  Like the partition by expressions,
     * these are shared among all the aggregates.
     */
    const AbstractPlanNode::OwningExpressionVector &getOrderByExpressions() const {
        return m_orderByExpressions;
    }

    /**
     * Returns the aggregate types for each expression.  The
     * aggregate types are the operation type and not the
     * value's type. So, they are things like "MIN", "MAX",
     * "RANK" and so forth.  They are not "TINYINT" or "FLOAT".
     */
    const std::vector<ExpressionType>& getAggregateTypes() const {
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
