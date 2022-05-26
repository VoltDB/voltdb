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

#include "executors/windowfunctionexecutor.h"

#include "execution/ProgressMonitorProxy.h"
#include "storage/tableiterator.h"

namespace voltdb {

/**
 * This class holds all the iterators used when iterating
 * through an input table.  There is one of these each time
 * the executor runs.  Since it contains table iterators
 * which have no default constructor, it really needs to
 * know its input table at construction time.  This is not
 * available when the executor object is constructed, so
 * this needs to be heap allocated.
 */
struct TableWindow {
    TableWindow(Table *tbl)
        : m_middleEdge(tbl->iterator()),
          m_leadingEdge(tbl->iterator()),
          m_orderByGroupSize(0) {}
    std::string debug() {
        std::stringstream stream;
        stream << "Table Window: [Middle: "
                << m_middleEdge.getFoundTuples() << ", Leading: "
                << m_leadingEdge.getFoundTuples() << "], "
                << "ssize = " << m_orderByGroupSize
                << "\n";
        return stream.str();
    }

    void resetCounts() {
        m_orderByGroupSize = 0;
    }
    TableIterator m_middleEdge;
    TableIterator m_leadingEdge;
    /**
     * This is handy for the aggregators.  It's maintained
     * in findOrderByEdge();
     */
    size_t m_orderByGroupSize;
};

/**
 * A WindowAggregate is the base class of aggregate calculations.
 * In the algorithm for calculating window function values we are
 * sensitive to some requirements.
 *   <ol>
 *     <li>All aggregates look at each input row in each order by group to
 *         calculate a value at each input row.</li>
 *     <li>For each such input row, some aggregates can use only values
 *         which can be computed before the input row, and some need to know
 *         values after the input row.  For example, RANK and DENSE_RANK
 *         only need to know how many rows precede the input row.  On the
 *         other hand, COUNT(*) needs to know how many rows are in the
 *         order by group of the input row, which includes rows after the
 *         input row.<li>
 *     <li>Some aggregates needs to inspect each row to compute values.
 *         For example, COUNT(E) must evaluate E in each input row in
 *         the order by group and only count those where the evaluation
 *         of E is non-null.</li>
 *   </ol>
 * Since it's expensive to evaluate expressions when they are not used,
 * we want to be able to turn off evaluation when it's not needed.
 */
struct WindowAggregate {
    WindowAggregate()
      : m_needsLookahead(true),
        m_inlineCopiedToNonInline(false) {
    }
    virtual ~WindowAggregate() {
    }
    void* operator new(size_t size, Pool& memoryPool) {
        void *answer = memoryPool.allocate(size);
        return answer;
    }
    void operator delete(void*, Pool& memoryPool) { /* NOOP -- on alloc error unroll nothing */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    virtual const char *getAggName() const = 0;

    /**
     * Do calculations needed when scanning each row ahead for
     * the end of an order by or partition by group.
     */
    virtual void lookaheadOneRow(TableWindow &window, NValueArray &argValues) {
        ;
    }

    /**
     * Do calculations at the end of a scan of an order by
     * group.
     */
    virtual void lookaheadNextGroup(TableWindow &window) {
        ;
    }

    /**
     * Do calculations to end the group and start the
     * next group.
     */
    virtual void endGroup(TableWindow &window,
                          WindowFunctionExecutor::EdgeType edgeType) {
        ;
    }

    /**
     * Do calculations after row insertion to end the current row and start the
     * next row.
     */
    virtual void endRow(TableWindow &window,
                          WindowFunctionExecutor::EdgeType edgeType) {
        ;
    }

    /**
     * Calculate the final value for the output tuple.
     */
    virtual NValue finalize(ValueType type)
    {
        m_value.castAs(type);
        if (m_inlineCopiedToNonInline) {
            m_value.allocateObjectFromPool();
        }
        return m_value;
    }

    /**
     * Initialize the aggregate.  This is called at the
     * beginning of each partition by group.
     */
    virtual void resetAgg()
    {
        m_value.setNull();
    }

    NValue m_value;
    bool   m_needsLookahead;

    bool   m_inlineCopiedToNonInline;

    const static NValue m_one;
    const static NValue m_zero;

};

const NValue WindowAggregate::m_one  = ValueFactory::getBigIntValue(1);
const NValue WindowAggregate::m_zero = ValueFactory::getBigIntValue(0);

/**
 * Dense rank is the easiest.  We just count
 * the number of times the order by expression values
 * change.
 */
class WindowedDenseRankAgg : public WindowAggregate {
public:
    WindowedDenseRankAgg() {
        m_value = ValueFactory::getBigIntValue(1);
        m_orderByPeerIncrement = m_value;
        m_needsLookahead = false;
    }

    virtual ~WindowedDenseRankAgg() {
    }

    virtual const char *getAggName() const {
        return "DENSE_RANK";
    }
    virtual void endGroup(TableWindow &window, WindowFunctionExecutor::EdgeType etype) {
        m_value = m_value.op_add(orderByPeerIncrement());
    }

    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_value = ValueFactory::getBigIntValue(1);
        m_orderByPeerIncrement = m_value;
    }

    virtual NValue orderByPeerIncrement() {
        return m_orderByPeerIncrement;
    }

    NValue m_orderByPeerIncrement;
};

/**
 * Rank is like dense rank, but we increment
 * the m_rank by the size of the order by group.
 */
class WindowedRankAgg : public WindowedDenseRankAgg {
public:
    virtual const char *getAggName() const {
        return "RANK";
    }
    void lookaheadNextGroup(TableWindow &window) {
        m_orderByPeerIncrement = ValueFactory::getBigIntValue(window.m_orderByGroupSize);
    }
    ~WindowedRankAgg() {
    }
};

/**
 * Row number returns a unique number for each row starting with 1.
 */
class WindowedRowNumberAgg : public WindowAggregate {
public:
    WindowedRowNumberAgg() {
        m_value = ValueFactory::getBigIntValue(1);
        m_needsLookahead = false;
    }

    virtual ~WindowedRowNumberAgg() {
    }

    virtual const char *getAggName() const {
        return "ROW_NUMBER";
    }

    virtual void endRow(TableWindow &window, WindowFunctionExecutor::EdgeType etype) {
        m_value = m_value.op_add(m_one);
    }

    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_value = ValueFactory::getBigIntValue(1);
    }
};

/**
 * Count is a bit like rank, but we need to contrive
 * to calculate when the argument expression is null,
 * and add the count of non-null rows to the count output
 * before we output the rows.
 */
class WindowedCountAgg : public WindowAggregate {
public:
    virtual ~WindowedCountAgg() {
    }

    virtual const char *getAggName() const {
        return "COUNT";
    }
    virtual void lookaheadOneRow(TableWindow &window, NValueArray &argVals) {
        /*
         * COUNT(*) has no arguments.  If there are arguments,
         * and the argument value is null, then don't count the row.
         */
        if (argVals.size() == 0 || ! argVals[0].isNull()) {
            m_value = m_value.op_add(m_one);
        }
    }

    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_value = m_zero;
    }
};

/*
 * Calculate MIN.
 */
class WindowedMinAgg : public WindowAggregate {
public:
    /*
     * Since min can operate on strings, we need to
     * be careful that the memory is allocated in
     * our pool.
     */
    WindowedMinAgg(Pool &pool)
        : WindowAggregate(),
          m_isEmpty(true),
          m_pool(pool) {
    }

    ~WindowedMinAgg() {
    }
    virtual const char *getAggName() const {
        return "MIN";
    }
    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_isEmpty = true;
    }
    virtual NValue finalize(ValueType type)
    {
        /*
         * If we looked at no values, this is null.
         */
        if (! m_isEmpty) {
            return WindowAggregate::finalize(type);
        }
        m_value = NValue::getNullValue(type);
        return m_value;
    }
    /**
     * Calculate the min by looking ahead in the
     * order by group.
     */
    virtual void lookaheadOneRow(TableWindow &window, NValueArray &argVals) {
        vassert(argVals.size() == 1);
        if ( ! argVals[0].isNull()) {
            if (m_isEmpty || argVals[0].op_lessThan(m_value).isTrue()) {
                m_value = argVals[0];
                if (m_value.getVolatile()) {
                    m_value.allocateObjectFromPool(&m_pool);
                    m_inlineCopiedToNonInline = true;
                }
                m_isEmpty = false;
            }
        }
    }
    bool m_isEmpty;
    Pool &m_pool;
};

class WindowedMaxAgg : public WindowAggregate {
public:
    WindowedMaxAgg(Pool &pool) : WindowAggregate(), m_isEmpty(true), m_pool(pool) {
    }
    ~WindowedMaxAgg() {
    }
    virtual const char *getAggName() const {
        return "MAX";
    }
    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_isEmpty = true;
    }
    virtual NValue finalize(ValueType type)
    {
        /*
         * If we looked at no values, this is null.
         */
        if (! m_isEmpty) {
            return WindowAggregate::finalize(type);
        }
        m_value = NValue::getNullValue(type);
        return m_value;
    }
    /**
     * Calculate the min by looking ahead in the
     * order by group.
     */
    virtual void lookaheadOneRow(TableWindow &window, NValueArray &argVals) {
        vassert(argVals.size() == 1);
        if ( ! argVals[0].isNull()) {
            if (m_isEmpty || argVals[0].op_greaterThan(m_value).isTrue()) {
                m_value = argVals[0];
                if (m_value.getVolatile()) {
                    m_value.allocateObjectFromPool(&m_pool);
                    m_inlineCopiedToNonInline = true;
                }
                m_isEmpty = false;
            }
        }
    }
    bool m_isEmpty;
    Pool &m_pool;
};

class WindowedSumAgg : public WindowAggregate {
public:
    WindowedSumAgg() {
    }
    ~WindowedSumAgg() {
    }
    virtual const char *getAggName() const {
        return "SUM";
    }
    virtual void resetAgg() {
        WindowAggregate::resetAgg();
    }
    /**
     * Calculate the min by looking ahead in the
     * order by group.
     */
    virtual void lookaheadOneRow(TableWindow &window, NValueArray &argVals) {
        vassert(argVals.size() == 1);
        if ( ! argVals[0].isNull()) {
            if (m_value.isNull()) {
                m_value = argVals[0];
            } else {
                m_value = m_value.op_add(argVals[0]);
            }
        }
    }
};

/**
 * This class is fancification of a C array of pointers to
 * WindowAgg.  It has a pass through tuple and at the end has a
 * bunch of pointers to WindowAggregate objects.  These latter
 * calculate the actual aggregate values.
 */
struct WindowAggregateRow {
    WindowAggregateRow(const TupleSchema *inputSchema, Pool &pool) {
        m_passThroughStorage.init(inputSchema, &pool);
        m_passThroughStorage.allocateActiveTuple();
    }
    void* operator new(size_t size, Pool& memoryPool, size_t nAggs)
    {
        return memoryPool.allocateZeroes(size + (sizeof(void*) * (nAggs + 1)));
    }
    void operator delete(void*, Pool& memoryPool, size_t nAggs) { /* NOOP -- on alloc error unroll */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    void resetAggs()
    {
        // Stop at the terminating null agg pointer that has been allocated as an extra and ignored since.
        for (int ii = 0; m_aggregates[ii] != NULL; ++ii) {
            m_aggregates[ii]->resetAgg();
        }
    }

    WindowAggregate **getAggregates() {
        return &(m_aggregates[0]);
    }
    void recordPassThroughTuple(const TableTuple &nextTuple) {
        getPassThroughTuple().copy(nextTuple);
    }
    TableTuple &getPassThroughTuple() {
        return m_passThroughStorage;
    }
private:
    PoolBackedTupleStorage  m_passThroughStorage;
    WindowAggregate *m_aggregates[0];
};

WindowFunctionExecutor::~WindowFunctionExecutor() {
    // NULL Safe Operation
    TupleSchema::freeTupleSchema(m_partitionByKeySchema);
    TupleSchema::freeTupleSchema(m_orderByKeySchema);
}

/**
 * When this function is called, the AbstractExecutor's init function
 * will have set the input tables in the plan node, but nothing else.
 */
bool WindowFunctionExecutor::p_init(AbstractPlanNode *init_node,
                                    const ExecutorVector& executorVector)
{
    VOLT_TRACE("WindowFunctionExecutor::p_init(start)");
    WindowFunctionPlanNode* node = dynamic_cast<WindowFunctionPlanNode*>(m_abstractNode);
    vassert(node);

    if (!node->isInline()) {
        setTempOutputTable(executorVector);
    }
    /*
     * Initialize the memory pool early, so that we can
     * use it for constructing temp. tuples.
     */
    m_memoryPool.purge();

    vassert( getInProgressPartitionByKeyTuple().isNullTuple());
    vassert( getInProgressOrderByKeyTuple().isNullTuple());
    vassert( getLastPartitionByKeyTuple().isNullTuple());
    vassert( getLastOrderByKeyTuple().isNullTuple());

    m_partitionByKeySchema = TupleSchema::createTupleSchema(m_partitionByExpressions);
    m_orderByKeySchema = TupleSchema::createTupleSchema(m_orderByExpressions);

    /*
     * Initialize all the data for partition by and
     * order by storage once and for all.
     */
    VOLT_TRACE("WindowFunctionExecutor::p_init(end)\n");
    return true;
}

/**
 * Create an instance of a window aggregator for the specified aggregate type.
 * The object is allocated from the provided memory pool.
 */
inline WindowAggregate* getWindowedAggInstance(Pool& memoryPool,
                                               ExpressionType agg_type)
{
    WindowAggregate *answer = NULL;

    switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_RANK:
        answer = new (memoryPool) WindowedRankAgg();
        break;
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_DENSE_RANK:
        answer = new (memoryPool) WindowedDenseRankAgg();
        break;
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_ROW_NUMBER:
        answer = new (memoryPool) WindowedRowNumberAgg();
        break;
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_COUNT:
        answer = new (memoryPool) WindowedCountAgg();
        break;
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_MAX:
        answer = new (memoryPool) WindowedMaxAgg(memoryPool);
        break;
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_MIN:
        answer = new (memoryPool) WindowedMinAgg(memoryPool);
        break;
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_SUM:
        answer = new (memoryPool) WindowedSumAgg();
        break;
    default:
        throwSerializableEEException("Unknown aggregate type %d", agg_type);
    }
    return answer;
}

/*
 * Create an instance of an aggregate calculator for the specified aggregate type.
 * The object is constructed in memory from the provided memory pool.
 */
inline void WindowFunctionExecutor::initAggInstances()
{
    WindowAggregate** aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        aggs[ii] = getWindowedAggInstance(m_memoryPool,
                                          m_aggTypes[ii]);
        vassert(aggs[ii] != NULL);
    }
}

inline void WindowFunctionExecutor::lookaheadOneRowForAggs(const TableTuple &tuple, TableWindow &tableWindow) {
    WindowAggregate **aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        if (aggs[ii]->m_needsLookahead) {
            const AbstractPlanNode::OwningExpressionVector &inputExprs
                = getAggregateInputExpressions()[ii];
            NValueArray vals(inputExprs.size());
            for (int idx = 0; idx < inputExprs.size(); idx += 1) {
                vals[idx] = inputExprs[idx]->eval(&tuple);
            }
            aggs[ii]->lookaheadOneRow(tableWindow, vals);
        }
    }
}

inline void WindowFunctionExecutor::lookaheadNextGroupForAggs(TableWindow &tableWindow) {
    WindowAggregate** aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        aggs[ii]->lookaheadNextGroup(tableWindow);
    }
}

inline void WindowFunctionExecutor::endGroupForAggs(TableWindow &tableWindow, EdgeType edgeType) {
    WindowAggregate** aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        aggs[ii]->endGroup(tableWindow, edgeType);
    }
}

inline void WindowFunctionExecutor::endRowForAggs(TableWindow &tableWindow, EdgeType edgeType) {
    WindowAggregate** aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        aggs[ii]->endRow(tableWindow, edgeType);
    }
}

/*
 *
 * Helper method responsible for inserting the results of the
 * aggregation into a new tuple in the output table as well as passing
 * through any additional columns from the input table.
 */
inline void WindowFunctionExecutor::insertOutputTuple()
{
    TableTuple& tempTuple = m_tmpOutputTable->tempTuple();

    // We copy the aggregate values into the output tuple,
    // then the passthrough columns.
    WindowAggregate** aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < getAggregateCount(); ii++) {
        NValue result = aggs[ii]->finalize(tempTuple.getSchema()->columnType(ii));
        tempTuple.setNValue(ii, result);
    }

    VOLT_TRACE("Setting passthrough columns");
    size_t tupleSize = tempTuple.columnCount();
    for (int ii = getAggregateCount(); ii < tupleSize; ii += 1) {
        AbstractExpression *expr = m_outputColumnExpressions[ii];
        tempTuple.setNValue(ii, expr->eval(&(m_aggregateRow->getPassThroughTuple())));
    }

    m_tmpOutputTable->insertTempTuple(tempTuple);
    VOLT_TRACE("output_table:\n%s", m_tmpOutputTable->debug().c_str());
}

int WindowFunctionExecutor::compareTuples(const TableTuple &tuple1,
                                          const TableTuple &tuple2) const {
    const TupleSchema *schema = tuple1.getSchema();
    vassert(schema == tuple2.getSchema());

    for (int ii = schema->columnCount() - 1; ii >= 0; --ii) {
        int cmp = tuple2.getNValue(ii)
            .compare(tuple1.getNValue(ii));
        if (cmp != 0) {
            return cmp;
        }
    }
    return 0;

}

/**
 * This ensures that the function is called with the
 * given argument.
 */
struct EnsureCleanupOnExit {
    EnsureCleanupOnExit(WindowFunctionExecutor *executor)
        : m_executor(executor) {
    }
    ~EnsureCleanupOnExit() {
        m_executor->p_execute_finish();
    }

    WindowFunctionExecutor *m_executor;
};
/*
 * This function is called straight from AbstractExecutor::execute,
 * which is called from executeExecutors, which is called from the
 * VoltDBEngine::executePlanFragments.  So, this is really the start
 * of execution for this executor.
 *
 * The executor will already have been initialized by p_init.
 */
bool WindowFunctionExecutor::p_execute(const NValueArray& params) {
    VOLT_TRACE("windowFunctionExecutor::p_execute(start)\n");
    // Input table
    Table * input_table = m_abstractNode->getInputTable();
    vassert(input_table);
    VOLT_TRACE("WindowFunctionExecutor: input table\n%s", input_table->debug().c_str());
    m_inputSchema = input_table->schema();
    vassert(m_inputSchema);

    /*
     * Do this after setting the m_inputSchema.
     */
    initWorkingTupleStorage();
    TableWindow tableWindow(input_table);
    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
    m_pmp = &pmp;

    m_aggregateRow
        = new (m_memoryPool, m_aggTypes.size())
        WindowAggregateRow(m_inputSchema, m_memoryPool);

    initAggInstances();

    VOLT_TRACE("Beginning: %s", tableWindow.debug().c_str());

    TableTuple nextTuple(m_inputSchema);

    /*
     * Force a call p_execute_finish when this is all over.
     */
    EnsureCleanupOnExit finishCleanup(this);
    for (EdgeType etype = START_OF_INPUT,
                  nextEtype = INVALID_EDGE_TYPE;
         etype != END_OF_INPUT;
         etype = nextEtype) {
        // Reset the aggregates if this is the
        // start of a partition group.  The start of
        // input is a special form of this.
        if (etype == START_OF_INPUT || etype == START_OF_PARTITION_GROUP) {
            m_aggregateRow->resetAggs();
        }
        // Find the next edge.  This will
        // give the aggs a crack at each row
        // if they want it.
        nextEtype = findNextEdge(etype, tableWindow);
        // Let the aggs know the results
        // of the lookahead.
        lookaheadNextGroupForAggs(tableWindow);
        // Advance to the end of the current group.
        for (int idx = 0; idx < tableWindow.m_orderByGroupSize; idx += 1) {
            VOLT_TRACE("MiddleEdge: Window = %s", tableWindow.debug().c_str());
            tableWindow.m_middleEdge.next(nextTuple);
            m_pmp->countdownProgress();
            m_aggregateRow->recordPassThroughTuple(nextTuple);
            insertOutputTuple();
            endRowForAggs(tableWindow, etype);
        }
        endGroupForAggs(tableWindow, etype);
        VOLT_TRACE("FirstEdge: %s", tableWindow.debug().c_str());
    }
    VOLT_TRACE("WindowFunctionExecutor: finalizing..");

    VOLT_TRACE("WindowFunctionExecutor::p_execute(end)\n");
    return true;
}

WindowFunctionExecutor::EdgeType WindowFunctionExecutor::findNextEdge(EdgeType edgeType, TableWindow &tableWindow)
{
    // This is just an alias for the buffered input tuple.
    TableTuple &nextTuple = getBufferedInputTuple();
    VOLT_TRACE("findNextEdge(start): %s", tableWindow.debug().c_str());
    /*
     * At the start of the input we need to prime the
     * tuple pairs.
     */
    if (edgeType == START_OF_INPUT) {
        if (tableWindow.m_leadingEdge.next(nextTuple)) {
            initPartitionByKeyTuple(nextTuple);
            initOrderByKeyTuple(nextTuple);
            /* First row.  Nothing to compare it with. */
            tableWindow.m_orderByGroupSize = 1;
            lookaheadOneRowForAggs(nextTuple, tableWindow);
        } else {
            /*
             * If there is no first row, then just
             * return false.  The leading edge iterator
             * will never have a next row, so we can
             * ask for its next again and will always get false.
             * We return a zero length group here.
             */
            tableWindow.m_orderByGroupSize = 0;
            return END_OF_INPUT;
        }
    } else {
        /*
         * We've already got a row, so
         * count it.
         */
        tableWindow.m_orderByGroupSize = 1;
        lookaheadOneRowForAggs(nextTuple, tableWindow);
    }
    do {
        VOLT_TRACE("findNextEdge(loopStart): %s", tableWindow.debug().c_str());
        if (tableWindow.m_leadingEdge.next(nextTuple)) {
            initPartitionByKeyTuple(nextTuple);
            initOrderByKeyTuple(nextTuple);
            if (compareTuples(getInProgressPartitionByKeyTuple(),
                              getLastPartitionByKeyTuple()) != 0) {
                VOLT_TRACE("findNextEdge(Partition): %s", tableWindow.debug().c_str());
                return START_OF_PARTITION_GROUP;
            }
            if (compareTuples(getInProgressOrderByKeyTuple(),
                              getLastOrderByKeyTuple()) != 0) {
                VOLT_TRACE("findNextEdge(Group): %s", tableWindow.debug().c_str());
                return START_OF_PARTITION_BY_GROUP;
            }
            tableWindow.m_orderByGroupSize += 1;
            lookaheadOneRowForAggs(nextTuple, tableWindow);
            VOLT_TRACE("findNextEdge(loop): %s", tableWindow.debug().c_str());
        } else {
            VOLT_TRACE("findNextEdge(EOI): %s", tableWindow.debug().c_str());
            return END_OF_INPUT;
        }
    } while (true);
}

void WindowFunctionExecutor::initPartitionByKeyTuple(const TableTuple& nextTuple)
{
    /*
     * The partition by keys should not be null tuples.
     */
    vassert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    vassert( ! getLastPartitionByKeyTuple().isNullTuple());
    /*
     * Swap the data, so that m_inProgressPartitionByKey
     * gets m_lastPartitionByKey's data and vice versa.
     * This just swaps the data pointers.
     */
    swapPartitionByKeyTupleData();
    /*
     * The partition by keys should still not be null tuples.
     */
    vassert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    vassert( ! getLastPartitionByKeyTuple().isNullTuple());
    /*
     * Calculate the partition by key values.  Put them in
     * getInProgressPartitionByKeyTuple().
     */
    for (int ii = 0; ii < m_partitionByExpressions.size(); ii++) {
        getInProgressPartitionByKeyTuple().setNValue(ii, m_partitionByExpressions[ii]->eval(&nextTuple));
    }
}

void WindowFunctionExecutor::initOrderByKeyTuple(const TableTuple& nextTuple)
{
    /*
     * The OrderByKey should not be null tuples.
     */
    vassert( ! getInProgressOrderByKeyTuple().isNullTuple());
    vassert( ! getLastOrderByKeyTuple().isNullTuple());
    /*
     * Swap the data pointers.  No data is moved.
     */
    swapOrderByKeyTupleData();
    /*
     * Still should not be null tuples.
     */
    vassert( ! getInProgressOrderByKeyTuple().isNullTuple());
    vassert( ! getLastOrderByKeyTuple().isNullTuple());
    /*
     * Calculate the order by key values.
     */
    for (int ii = 0; ii < m_orderByExpressions.size(); ii++) {
        getInProgressOrderByKeyTuple().setNValue(ii, m_orderByExpressions[ii]->eval(&nextTuple));
    }
    /*
     * Still should not be null tuples.
     */
    vassert( ! getInProgressOrderByKeyTuple().isNullTuple());
    vassert( ! getLastOrderByKeyTuple().isNullTuple());
}

void WindowFunctionExecutor::swapPartitionByKeyTupleData() {
    vassert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    vassert( ! getLastPartitionByKeyTuple().isNullTuple());
    void* inProgressData = getInProgressPartitionByKeyTuple().address();
    void* nextData = getLastPartitionByKeyTuple().address();
    getInProgressPartitionByKeyTuple().move(nextData);
    getLastPartitionByKeyTuple().move(inProgressData);
    vassert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    vassert( ! getLastPartitionByKeyTuple().isNullTuple());
}

void WindowFunctionExecutor::swapOrderByKeyTupleData() {
    /*
     * Should not be null tuples.
     */
    vassert( ! getInProgressOrderByKeyTuple().isNullTuple());
    vassert( ! getLastOrderByKeyTuple().isNullTuple());
    void* inProgressData = getInProgressOrderByKeyTuple().address();
    void* nextData = getLastOrderByKeyTuple().address();
    getInProgressOrderByKeyTuple().move(nextData);
    getLastOrderByKeyTuple().move(inProgressData);
    /*
     * Still should not be null tuples.
     */
    vassert( ! getInProgressOrderByKeyTuple().isNullTuple());
    vassert( ! getLastOrderByKeyTuple().isNullTuple());
}


void WindowFunctionExecutor::p_execute_finish() {
    VOLT_DEBUG("WindowFunctionExecutor::p_execute_finish() start\n");
    m_pmp = NULL;
    /*
     * The working tuples should not be null.
     */
    vassert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    vassert( ! getInProgressOrderByKeyTuple().isNullTuple());
    vassert( ! getLastPartitionByKeyTuple().isNullTuple());
    vassert( ! getLastOrderByKeyTuple().isNullTuple());
    vassert( ! getBufferedInputTuple().isNullTuple());
    getInProgressPartitionByKeyTuple().move(NULL);
    getInProgressOrderByKeyTuple().move(NULL);
    getLastPartitionByKeyTuple().move(NULL);
    getLastOrderByKeyTuple().move(NULL);
    getBufferedInputTuple().move(NULL);
    /*
     * The working tuples have just been set to null.
     */
    vassert( getInProgressPartitionByKeyTuple().isNullTuple());
    vassert( getInProgressOrderByKeyTuple().isNullTuple());
    vassert( getLastPartitionByKeyTuple().isNullTuple());
    vassert( getLastOrderByKeyTuple().isNullTuple());
    vassert( getBufferedInputTuple().isNullTuple());
    m_memoryPool.purge();
    VOLT_DEBUG("WindowFunctionExecutor::p_execute_finish() end\n");
}

void WindowFunctionExecutor::initWorkingTupleStorage() {
    vassert( getInProgressPartitionByKeyTuple().isNullTuple());
    vassert( getInProgressOrderByKeyTuple().isNullTuple());
    vassert( getLastPartitionByKeyTuple().isNullTuple());
    vassert( getLastOrderByKeyTuple().isNullTuple());
    vassert( getBufferedInputTuple().isNullTuple());

    m_inProgressPartitionByKeyStorage.init(m_partitionByKeySchema, &m_memoryPool);
    m_lastPartitionByKeyStorage.init(m_partitionByKeySchema, &m_memoryPool);

    m_lastOrderByKeyStorage.init(m_orderByKeySchema, &m_memoryPool);
    m_inProgressOrderByKeyStorage.init(m_orderByKeySchema, &m_memoryPool);

    m_bufferedInputStorage.init(m_inputSchema, &m_memoryPool);

    m_inProgressPartitionByKeyStorage.allocateActiveTuple();
    m_lastPartitionByKeyStorage.allocateActiveTuple();

    m_inProgressOrderByKeyStorage.allocateActiveTuple();
    m_lastOrderByKeyStorage.allocateActiveTuple();

    m_bufferedInputStorage.allocateActiveTuple();

    vassert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    vassert( ! getInProgressOrderByKeyTuple().isNullTuple());
    vassert( ! getLastPartitionByKeyTuple().isNullTuple());
    vassert( ! getLastOrderByKeyTuple().isNullTuple());
    vassert( ! getBufferedInputTuple().isNullTuple());

}
} /* namespace voltdb */
