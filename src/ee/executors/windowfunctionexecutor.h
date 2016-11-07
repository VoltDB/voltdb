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
class WindowAggregate {
public:
	virtual ~WindowAggregate() {

	}
	void* operator new(size_t size, Pool& memoryPool) { return memoryPool.allocate(size); }
    void operator delete(void*, Pool& memoryPool) { /* NOOP -- on alloc error unroll nothing */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    virtual void advance(const AbstractPlanNode::OwningExpressionVector &) = 0;

    virtual NValue finalize(ValueType type)
    {
        m_value.castAs(type);
        return m_value;
    }

    virtual void resetAgg()
    {
        m_value.setNull();
    }
private:
    NValue m_value;
};

/**
 * This class is fancification of a C array of pointers to
 * WindowAgg.  It has a pass through tuple and at the end has a
 * bunch of pointers to WindowAggregate objects.  These latter
 * calculate the actual aggregate values.
 *
 * Don't define anything which needs virtuality here, or awful things will happen.
 */
class WindowAggregateRow {
public:
	void resetAggs()
    {
        // Stop at the terminating null agg pointer that has been allocated as an extra and ignored since.
        for (int ii = 0; m_aggregates[ii] != NULL; ++ii) {
            m_aggregates[ii]->resetAgg();
        }
    }

    void recordPassThroughTuple(TableTuple &passThroughTupleSource, const TableTuple &tuple)
    {
        passThroughTupleSource.copy(tuple);
        m_passThroughTuple = passThroughTupleSource;
    }

private:
	TableTuple m_passThroughTuple;
	WindowAggregate   *m_aggregates[0];
};
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
		m_partitionByKeySchema(NULL),
		m_inputSchema(NULL),
		m_aggregateRow(NULL)
		{ }
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
							  TempTable* newTempTable = NULL,
							  CountingPostfilter* parentPredicate = NULL);

    /**
     * Execute a single tuple.
     */
    void p_execute_tuple(const TableTuple& nextTuple);

    /**
     * Last method to insert the results to output table and clean up memory or variables.
     */
    virtual void p_execute_finish();

private:
    virtual bool p_init(AbstractPlanNode*, TempTableLimits*);

    /** Concrete executor classes implement execution in p_execute() */
    virtual bool p_execute(const NValueArray& params);

    void initPartitionByKeyTuple(const TableTuple& nextTuple);

    void initAggInstances(WindowAggregateRow *aggregateRow);

    void advanceAggs(WindowAggregateRow* aggregateRow, const TableTuple& tuple);

    /**
     * Swap the current group by key tuple with the in-progress group by key tuple.
     * Return the new group by key tuple associated with in-progress tuple address.
     * This function is only used in serial or partial aggregation.
     */
    TableTuple& swapWithInprogressPartitionByKeyTuple();

    void insertOutputTuple(WindowAggregateRow* winFunRow);

    TupleSchema* constructPartitionBySchema();

    /**
     * Do any initialization which needs the first tuple
     * to be done.  This means priming the pass through tuple
     * and partition by key if necessary.
     */
    void p_init_tuple(const TableTuple &nextTuple);

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
     * We maintain a sequence of WindowAggregates, one for each windowed aggregate.
     * A WindowAggregate contains the window and aggregate state.  The window state
     * is an iterator for the start and end of a window.  This is an iterator
     * into the input table.  There is an iterator for the row we are traversing,
     * but this is not part of the WindowAggregate state.  It's shared.
     *
     * Since we only support one windowed aggregate, this is a sequence of one.
     */
    std::vector<WindowAggregate *>m_aggregateState;
    /*
     * This progress monitor is a local variable in p_execute.  So, it
     * does not need to be deleted.  But we need to reset it to NULL there.
     */
    ProgressMonitorProxy* m_pmp;

    TupleSchema* m_partitionByKeySchema;
    PoolBackedTupleStorage m_nextPartitionByKeyStorage;
    TableTuple m_inProgressPartitionByKeyTuple;
    WindowAggregateRow *m_aggregateRow;

    const TupleSchema * m_inputSchema;
    TableTuple m_passThroughTupleSource;
};

} /* namespace voltdb */

#endif /* SRC_EE_EXECUTORS_PARTITIONBYEXECUTOR_H_ */
