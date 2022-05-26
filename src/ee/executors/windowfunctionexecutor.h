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
#ifndef WINDOWFUNCTIONEXECUTOR_H_
#define WINDOWFUNCTIONEXECUTOR_H_

#include "abstractexecutor.h"
#include "plannodes/windowfunctionnode.h"

namespace voltdb {

class ProgressMonitorProxy;
struct WindowAggregateRow;
struct TableWindow;
/**
 * This is the executor for a WindowFunctionPlanNode.
 */
class WindowFunctionExecutor: public AbstractExecutor {
    /*
     * EnsureCleanupOnExit is really part of WindowFunctionExecutor.
     * It needs access to private members that need to be finalized.
     * So this is not a dubious use of friendship.
     */
    friend struct EnsureCleanupOnExit;
public:
    WindowFunctionExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node)
      : AbstractExecutor(engine, abstract_node),
        m_aggTypes(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getAggregates()),
        m_partitionByExpressions(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getPartitionByExpressions()),
        m_orderByExpressions(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getOrderByExpressions()),
        m_aggregateInputExpressions(dynamic_cast<const WindowFunctionPlanNode*>(abstract_node)->getAggregateInputExpressions()),
        m_pmp(NULL),
        m_orderByKeySchema(NULL),
        m_partitionByKeySchema(NULL),
        m_inputTable(NULL),
        m_inputSchema(NULL),
        m_aggregateRow(NULL)
        {
            dynamic_cast<WindowFunctionPlanNode *>(abstract_node)->collectOutputExpressions(m_outputColumnExpressions);
        }
    virtual ~WindowFunctionExecutor();

    /**
     * When calculating an window function, the value at a row may
     * depend on the order by peers of the row.  So, we need to
     * scan the input forward to the next edge between order by
     * groups.  Edges between partition by groups are a kind of
     * edge between order by groups as well.
     *
     * This enum type gives the type of kinds of edges between groups
     * of rows.
     */
    enum EdgeType {
        INVALID_EDGE_TYPE,           /** No Type. */
        START_OF_INPUT,              /** Start of input. */
        START_OF_PARTITION_GROUP,    /** Start of a new partition group. */
        START_OF_PARTITION_BY_GROUP, /** Start of an order by group. */
        END_OF_INPUT                 /** End of all input rows */
    };

private:
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
     * Initiate the member variables for execute the window function.
     */
    TableTuple p_execute_init(const NValueArray& params,
                              const TupleSchema * schema,
                              AbstractTempTable* newTempTable = NULL);

    /**
     * Last method to clean up memory or variables.  We may
     * also need to output some last rows, when we have more
     * sophisticated windowing support.
     */
    virtual void p_execute_finish();

    TableTuple & getBufferedInputTuple() {
        return m_bufferedInputStorage;
    }

    /**
     * This tuple is the set of partition by keys for the
     * current row.
     */
    TableTuple  & getInProgressPartitionByKeyTuple() {
        return m_inProgressPartitionByKeyStorage;
    }

    /**
     * This tuple is the set of partition by keys for the last
     * row.
     */
    TableTuple & getLastPartitionByKeyTuple() {
        return m_lastPartitionByKeyStorage;
    }

    /**
     * This tuple is the set of order by keys for the
     * current row.
     */
    TableTuple & getInProgressOrderByKeyTuple() {
        return m_inProgressOrderByKeyStorage;
    }

    /**
     * This tuple is the set of order by keys for the last row.
     */
    TableTuple & getLastOrderByKeyTuple() {
        return m_lastOrderByKeyStorage;
    }

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
    void initAggInstances();

    virtual bool p_init(AbstractPlanNode*, const ExecutorVector&);

    /** Concrete executor classes implement execution in p_execute() */
    virtual bool p_execute(const NValueArray& params);

    /**
     * Apply the tuple, which is the next input row table,
     * to each of the aggs in aggregateRow.
     */
    void advanceAggs(const TableTuple& tuple,
                     bool newOrderByGroup);

    /**
     * Call lookaheadOneRow for each aggregate.  This
     * will happen for each row, and can be disabled
     * by setting m_needsLookahead to false.
     */
    void lookaheadOneRowForAggs(const TableTuple &tuple, TableWindow &tableWindow);

    /**
     * Call lookaheadNextGroup for each aggregate
     * before the calls to advance.
     * This will happen for each group and cannot be
     * disabled.
     */
    void lookaheadNextGroupForAggs(TableWindow &tableWindow);

    /**
     * Call endGroup for each aggregate.  This will happen
     * for each group and cannot be disabled.
     */
    void endGroupForAggs(TableWindow &tableWindow, EdgeType edgeType);

    /**
    * Call endRow for each column after insertion to output tuple.  This will happen
    * for each row and cannot be disabled.
    */
    void endRowForAggs(TableWindow &tableWindow, EdgeType edgeType);

    /**
     * Insert the output tuple.
     */
    void insertOutputTuple();

    int compareTuples(const TableTuple &tuple1,
                      const TableTuple &tuple2) const;

    void initWorkingTupleStorage();

    /**
     * Find the next edge, given that the current group's
     * leading edge is the given edge type.  Return the edge type
     * of the next group, not this group.  Note that
     * the edge type is the type of the group after the current
     * group.
     */
    EdgeType findNextEdge(EdgeType edgeType, TableWindow &);

    Pool m_memoryPool;
    /**
     * The operation type of the aggregates.
     */
    const std::vector<ExpressionType> &m_aggTypes;
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
     * This holds tuples read while looking ahead in the
     * input table.  Saving them here lets us avoid reading them
     * multiple times.
     */
    PoolBackedTupleStorage m_bufferedInputStorage;
    /**
     * This holds the evaluations for the current partition.
     * Note that this is essentially a TableTuple, but that it
     * uses the pool associated with this executor to allocate
     * its memory.
     */
    PoolBackedTupleStorage  m_inProgressPartitionByKeyStorage;
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

    /**
     * This holds the evaluations for the current order by expressions.
     * Note that this is essentially a TableTuple, but that it
     * uses the pool associated with this executor to allocate
     * its memory.
     */
    PoolBackedTupleStorage  m_inProgressOrderByKeyStorage;

    /**
     * This holds the result of evaluating the order by
     * expressions.
     */
    PoolBackedTupleStorage  m_lastOrderByKeyStorage;
    /**
     * This is the input table.
     */
    const Table * m_inputTable;
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
    WindowAggregateRow * m_aggregateRow;
};

} /* namespace voltdb */

#endif /* WINDOWFUNCTIONEXECUTOR_H_ */
