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

class ProgressMonitorProxy;
class WindowAggregate;
class WindowAggregateRow;
/**
 * This is the executor for a WindowFunctionPlanNode.
 */
class WindowFunctionExecutor: public AbstractExecutor {

public:
    WindowFunctionExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node)
      : AbstractExecutor(engine, abstract_node),
        m_aggTypes(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getAggregates()),
        m_distinctAggs(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getDistinctAggregates()),
        m_partitionByExpressions(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getPartitionByExpressions()),
        m_orderByExpressions(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getOrderByExpressions()),
        m_aggregateInputExpressions(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getAggregateInputExpressions()),
        m_pmp(NULL),
        m_orderByKeySchema(NULL),
        m_partitionByKeySchema(NULL),
        m_inputSchema(NULL),
        m_aggregateRow(NULL)
        {
            dynamic_cast<WindowFunctionPlanNode *>(abstract_node)->collectOutputExpressions(m_outputColumnExpressions);
        }
    virtual ~WindowFunctionExecutor();


    /**
     * How many aggregate functions are there?
     */
    int getAggregateCount() const {
        return m_aggregateInputExpressions.size();
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
     * used to calculate the pass through columns.
     */
    const std::vector<AbstractExpression*>& getOutputColumnExpressions() const {
        return m_outputColumnExpressions;
    }

    /**
     * Initiate the member variables for execute the window function.
     */
    TableTuple p_execute_init(const NValueArray& params,
                              const TupleSchema * schema,
                              TempTable* newTempTable = NULL);

    /**
     * Execute a single tuple.
     */
    void p_execute_tuple(const TableTuple& nextTuple, bool firstTuple);

    /**
     * Last method to clean up memory or variables.  We may
     * also need to output some last rows, when we have more
     * sophisticated windowing support.
     */
    virtual void p_execute_finish();

private:
    virtual bool p_init(AbstractPlanNode*, TempTableLimits*);

    /** Concrete executor classes implement execution in p_execute() */
    virtual bool p_execute(const NValueArray& params);

    /**
     * Evaluate the partition by expressions in
     * the context of nextTuple.  Leave the results scattered
     * around the house.
     */
    void initPartitionByKeyTuple(const TableTuple& nextTuple);

    /**
     * Evaluate the order by expressions in
     * the context of nextTuple.  Leave the results scattered
     * around the house.
     */
    void initOrderByKeyTuple(const TableTuple& nextTuple);
    /**
     * Create all the WindowAggregate objects in aggregateRow.
     * These hold the state of a window function computation.
     */
    void initAggInstances(WindowAggregateRow *aggregateRow);

    /**
     * Apply the tuple, which is the next input row table,
     * to each of the aggs in aggregateRow.
     */
    void advanceAggs(const TableTuple& tuple, bool newOrderByGroup);

    /**
     * Swap the current group by key tuple with the in-progress partition by key tuple.
     * Return the new partition by key tuple associated with in-progress tuple address.
     */
    void swapPartitionByKeyTupleData();

    /**
     * Swap the current group by key tuple with the in-progress order by key tuple.
     * Return the new order by key tuple associated with in-progress tuple address.
     */
    void swapOrderByKeyTupleData();

    void insertOutputTuple(WindowAggregateRow* winFunRow);

    TupleSchema* constructSchemaFromExpressionVector(const AbstractPlanNode::OwningExpressionVector &exprs);

    int compareTuples(const TableTuple &tuple1,
                      const TableTuple &tuple2) const;

    void initWorkingTupleStorage();

private:
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
     * This is the list of all partition by expressions.
     * It resides in the plan node.
     */
    const AbstractPlanNode::OwningExpressionVector &m_partitionByExpressions;
    /**
     * This is the list of all order by expressions.
     * It resides in the plan node.
     */
    const AbstractPlanNode::OwningExpressionVector &m_orderByExpressions;
    /**
     * This list contains all the expressions for arguments
     * of window functions.  It's a vector of OwningExpressionVectors,
     * which makes it a vector of vectors.
     *
     * Element j is the list of aggregate arguments for aggregate j.
     */
    const WindowFunctionPlanNode::AggregateExpressionList &m_aggregateInputExpressions;
    /**
     * This is the list of all output column expressions.
     */
    std::vector<AbstractExpression*> m_outputColumnExpressions;
    /*
     * This progress monitor is a local variable in p_execute.  So, it
     * does not need to be deleted.  But we need to reset it to NULL there.
     */
    ProgressMonitorProxy* m_pmp;

    /*
     * These next three are for handling partition by and
     * order by values.
     */
    /**
     * This is the schema of a row of evaluations
     * of order by expressions in the order the user
     * gave them to us.
     */
    TupleSchema *m_orderByKeySchema;
    /**
     * This is the schema of a row of evaluations
     * of all partition by expressions in our preferred
     * order.
     */
    TupleSchema* m_partitionByKeySchema;
    /**
     * This holds the evaluations for the current partition.
     * Note that this is essentially a TableTuple, but that it
     * uses the pool associated with this executor to allocate
     * its memory.
     */
    PoolBackedTupleStorage  m_inProgressPartitionByKeyStorage;
    TableTuple  & getInProgressPartitionByKeyTuple() {
        return m_inProgressPartitionByKeyStorage;
    }
    /**
     * This holds the evaluations for the next row.
     * Before we evaluate the expressions, on the next tuple,
     * we will swap the data in this
     * with the data in m_inProgressPartitionByKeyTuple, and
     * evaluate into this tuple.  If the
     * next row is in the current partition, these two tuple values will
     * be the same as those for m_inProgressPartitionByKeyTuple.
     * If not, these will be different, we will have the last
     * values, but the new values will be where we want them.
     *
     * Note that this is almost just an ordinary
     * TableTuple, but that its data comes from the pool
     * associated with this executor.
     */
    PoolBackedTupleStorage  m_lastPartitionByKeyStorage;
    TableTuple & getLastPartitionByKeyTuple() {
        return m_lastPartitionByKeyStorage;
    }
    /**
     * This holds the evaluations for the current order by expressions.
     * Note that this is essentially a TableTuple, but that it
     * uses the pool associated with this executor to allocate
     * its memory.
     */
    PoolBackedTupleStorage  m_inProgressOrderByKeyStorage;
    TableTuple & getInProgressOrderByKeyTuple() {
        return m_inProgressOrderByKeyStorage;
    }
    /**
     * This holds the result of evaluating the order by
     * expressions.
     */
    PoolBackedTupleStorage  m_lastOrderByKeyStorage;
    TableTuple & getLastOrderByKeyTuple() {
        return m_lastOrderByKeyStorage;
    }
    /**
     * This is the schema of the input table.
     */
    const TupleSchema * m_inputSchema;
    /**
     * This is where the windowed aggregate values are calculated.
     * This is essentially a C array of WindowAggregate objects, and
     * each aggregate has an element in the array.  There is also
     * a tuple for the values passed through from the input.
     */
    WindowAggregateRow *m_aggregateRow;
};

} /* namespace voltdb */

#endif /* SRC_EE_EXECUTORS_PARTITIONBYEXECUTOR_H_ */
