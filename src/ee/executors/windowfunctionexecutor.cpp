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
/*
 * partitionbyexecutor.cpp
 */

#include "executors/windowfunctionexecutor.h"

#include <sstream>
#include <memory>
#include <limits.h>

#include "plannodes/windowfunctionnode.h"
#include "execution/ProgressMonitorProxy.h"
#include "common/ValueFactory.hpp"

namespace voltdb {

/**
 * This class holds all the iterators used when iterating
 * through an input table.
 */
struct TableWindow {
    TableWindow(Table *tbl)
        : m_trailingEdge(tbl->iterator()),
          m_middleEdge(tbl->iterator()),
          m_orderByEdge(tbl->iterator()),
          m_leadingEdge(tbl->iterator()),
          m_partitionSize(0),
          m_orderByGroupSize(0) {}
    std::string debug() {
        std::stringstream stream;
        stream << "Table Window: [Trailing: "
                << m_trailingEdge.getLocation() << ", Middle: "
                << m_middleEdge.getLocation() << ", OrderBy: "
                << m_orderByEdge.getLocation() << ", Leading: "
                << m_leadingEdge.getLocation() << "], "
                << "psize = " << m_partitionSize
                << ", ssize = " << m_orderByGroupSize
                << "\n";
        return stream.str();
    }

    TableIterator m_trailingEdge;
    TableIterator m_middleEdge;
    TableIterator m_orderByEdge;
    TableIterator m_leadingEdge;
    /**
     * This is handy for the aggregators.  It's maintained
     * in findLeadingEdge();
     */
    size_t m_partitionSize;
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
 *
 * The aggregate calculation algorithm is this.
 *   <ol>
 *     <li>Scan the rows looking for the partition by group.  If there
 *         are no partition by expressions, there is one big partition.
 *         <ul>
 *           <li>At each row in the scan, we call lookaheadOneRowInPartitionByGroup
 *               on each aggregate unless m_needsPartitionByLookahead is false.
 *               By setting m_needsPartitionByLookahead to false we can avoid
 *               argument evaluations for individual aggregates.</li>
 *           <li>After the end of the scan we call lookaheadPartitionByGroup
 *               unconditionally.</li>
 *         </ul></li>
 *     <li>Scan the partition again looking for order by boundaries.  If there
 *         are no order by expressions, all rows in the partition are in the
 *         order by group.
 *         <ul>
 *           <li>At each row in the scan we call lookaheadOneRowInOrderByGroup
 *               on each aggregate unless m_needsOrderByLookahead is false.
 *               By setting m_needsOrderByLookahead to false we can avoid
 *               argument evaluation for aggregates which don't need them.</li>
 *           <li>After the end of the order by group is determined, we call
 *               lookaheadOrderByGroup on each aggregate unconditionally.</li>
 *         </ul></li>
 *     <li>For each row in the order by group we call advance on each
 *         aggregate unless m_needsAdvance is set to false.  Since some
 *         aggregates don't need advance, this lets us avoid unnecessary
 *         argument evaluations.</li>
 *     <li>Finally, we output the output row.</li>
 *   </ol>
 */
class WindowAggregate {
public:
    WindowAggregate()
      : m_partitionSize(0),
        m_orderByGroupSize(0),
        m_needsOrderByLookahead(false),
        m_needsPartitionByLookahead(false),
        m_needsAdvance(false) {}
    virtual ~WindowAggregate() {

    }
    void* operator new(size_t size, Pool& memoryPool) { return memoryPool.allocate(size); }
    void operator delete(void*, Pool& memoryPool) { /* NOOP -- on alloc error unroll nothing */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    /**
     * Return true if an aggregate needs to look ahead at
     * each row when looking ahead through an order by group.
     */
    bool needsOrderByLookahead() {
        return m_needsOrderByLookahead;
    }
    /**
     * Return true if an aggregate needs to look ahead at
     * each row when looking ahead through a partition by group.
     */
    bool needsPartitionByLookahead() {
        return m_needsPartitionByLookahead;
    }
    /**
     * Return true if an aggregate needs to
     * each row when scanning an order by group.
     */
    bool needsAdvance() {
        return m_needsAdvance;
    }

    /**
     * Advance an aggregate value for output.
     */
    virtual void advance(const NValueArray &val,
                         bool newOrderByGroup) {}

    /**
     * Do calculations needed when scanning ahead for
     * the end of an order by group.
     */
    virtual void lookaheadOneRowInOrderByGroup(TableWindow *window, NValueArray &argValues) {
    }

    /**
     * Do calculations needed when scanning ahead for
     * the end of a partition by group.
     */
    virtual void lookaheadOneRowInPartitionByGroup(TableWindow *window, NValueArray &argValues) {
    }

    /**
     * Do calculations at the end of a scan of an order by
     * group.
     */
    virtual void lookaheadOrderByGroup(TableWindow *window) {
        m_orderByGroupSize = window->m_orderByGroupSize;
    }
    /**
     * Do calculations at the end of a scan of a partition by
     * group.
     */
    virtual void lookaheadPartitionGroup(TableWindow *window) {
        m_partitionSize = window->m_partitionSize;
    }

    /**
     * Calculate the final value for the output tuple.
     */
    virtual NValue finalize(ValueType type)
    {
        m_value.castAs(type);
        return m_value;
    }

    /**
     * Initialize the aggregate.  This is called at the
     * beginning of each partition by group.
     */
    virtual void resetAgg()
    {
        m_value.setNull();
        m_partitionSize = 0;
        m_orderByGroupSize = 0;
    }
protected:
    NValue m_value;
    size_t m_partitionSize;
    size_t m_orderByGroupSize;
    bool   m_needsOrderByLookahead;
    bool   m_needsPartitionByLookahead;
    bool   m_needsAdvance;
};

/**
 * Dense rank is the easiest.  We just count
 * the number of times the order by expression values
 * change.
 */
class DenseRankAgg : public WindowAggregate {
public:
    DenseRankAgg() {
      m_value = ValueFactory::getBigIntValue(0);
    }

    virtual ~DenseRankAgg() {}

    virtual NValue finalize(ValueType type) {
        size_t increment = orderByPeerIncrement();
        NValue answer = m_value;
        m_value = m_value.op_add(ValueFactory::getBigIntValue(increment));
        return answer;
    }

    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_value = ValueFactory::getBigIntValue(0);
    }
    virtual size_t orderByPeerIncrement() {
        return 1;
    }
};

/**
 * Rank is like dense rank, but we increment
 * the m_rank by the size of the order by group.
 * This size is set in the base class in lookaheadOrderByGroup.
 */
class RankAgg : public DenseRankAgg {
public:
    RankAgg()
    : DenseRankAgg(){}
    ~RankAgg() {}
    size_t orderByPeerIncrement() {
        return m_orderByGroupSize;
    }
};

/**
 * Count is a bit like rank, but we need to contrive
 * to calculate when the argument expression is null,
 * and add the count of non-null rows to the count output
 * before we advance any rows in the order by group.
 */
class CountAgg : public WindowAggregate {
public:
    CountAgg() {
        m_needsOrderByLookahead = true;
    }

    virtual ~CountAgg() {}

    virtual void lookaheadOneRowInOrderByGroup(TableWindow *window, NValueArray &argVals) {
        /*
         * COUNT(*) has no arguments.  If there are arguments,
         * then don't count the row if the argument value is
         * NULL.
         */
        if (argVals.size() == 0 || ! argVals[0].isNull()) {
            m_value = m_value.op_add(argVals[0]);
        }
    }

    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_value = ValueFactory::getBigIntValue(0);
    }
};

#if  0
/*
 * The following are examples of other aggregate definitions.
 */
class MinAgg : public WindowAggregate {
public:
    MinAgg() {
        m_value = ValueFactory::getBigIntValue(LONG_MAX);
        m_needsOrderByLookahead = true;
    }
    /**
     * Calculate the min by looking ahead in the
     * order by group.
     */
    virtual void lookaheadOneRowInOrderByGroup(TableWindow *window, NValueArray &argVals) {
        assert(argVals.size() == 1);
        if ( ! argVals[0].isNull()) {
            if (argVals[0].op_lessThan(m_value)) {
                m_value = argVals[0];
            }
        }
    }
};

class MaxAgg : public WindowAggregate {
public:
    MaxAgg() {
        m_value = ValueFactory::getBigIntValue(LONG_MIN);
        m_needsOrderByLookahead = true;
    }
    /**
     * Calculate the min by looking ahead in the
     * order by group.
     */
    virtual void lookaheadOneRowInOrderByGroup(TableWindow *window, NValueArray &argVals) {
        assert(argVals.size() == 1);
        if ( ! argVals[0].isNull()) {
            if (argVals[0].op_greaterThan(m_value)) {
                m_value = argVals[0];
            }
        }
    }
};

class SumAgg : public WindowAggregate {
public:
    SumAgg() {
        m_needsOrderByLookahead = true;
    }
    /**
     * Calculate the min by looking ahead in the
     * order by group.
     */
    virtual void lookaheadOneRowInOrderByGroup(TableWindow *window, NValueArray &argVals) {
        assert(argVals.size() == 1);
        if ( ! argVals[0].isNull()) {
            m_value = m_value.op_add(argVals[0]);
        }
    }
};

class AvgAgg : public WindowAggregate {
public:
    AvgAgg()
      : m_count(0) {
        m_needsOrderByLookahead = true;
    }
    virtual void lookaheadOneRowInOrderByGroup(TableWindow *window, NValueArray &argVals) {
        assert(argVals.size() == 1);
        if ( ! argVals[0].isNull() ) {
            m_value = m_value.op_add(argVals[0]);
            m_count += 1;
        }
    }
    virtual NValue finalize(ValueType type) {
        if (m_count == 0) {
            throw SQLException("Division by zero");
        }
        return m_value.op_divide(ValueFactory::getDoubleValue(m_count));
    }
private:
    int64_t m_count;
};
#endif

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

TupleSchema* WindowFunctionExecutor::constructSchemaFromExpressionVector
        (const AbstractPlanNode::OwningExpressionVector &exprs) {
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnSizes;
    std::vector<bool> columnAllowNull;
    std::vector<bool> columnInBytes;

    BOOST_FOREACH (AbstractExpression* expr, exprs) {
            columnTypes.push_back(expr->getValueType());
            columnSizes.push_back(expr->getValueSize());
            columnAllowNull.push_back(true);
            columnInBytes.push_back(expr->getInBytes());
    }
    return TupleSchema::createTupleSchema(columnTypes,
                                          columnSizes,
                                          columnAllowNull,
                                          columnInBytes);
}

/**
 * When this function is called, the AbstractExecutor's init function
 * will have set the input tables in the plan node, but nothing else.
 */
bool WindowFunctionExecutor::p_init(AbstractPlanNode *init_node, TempTableLimits *limits) {
    VOLT_TRACE("WindowFunctionExecutor::p_init(start)");
    WindowFunctionPlanNode* node = dynamic_cast<WindowFunctionPlanNode*>(m_abstractNode);
    assert(node);

    if (!node->isInline()) {
        setTempOutputTable(limits);
    }
    /*
     * Initialize the memory pool early, so that we can
     * use it for constructing temp. tuples.
     */
    m_memoryPool.purge();

    assert( getInProgressPartitionByKeyTuple().isNullTuple());
    assert( getInProgressOrderByKeyTuple().isNullTuple());
    assert( getLastPartitionByKeyTuple().isNullTuple());
    assert( getLastOrderByKeyTuple().isNullTuple());

    m_partitionByKeySchema = constructSchemaFromExpressionVector(m_partitionByExpressions);
    m_orderByKeySchema = constructSchemaFromExpressionVector(m_orderByExpressions);

    /*
     * Initialize all the data for partition by and
     * order by storage once and for all.
     */
    VOLT_TRACE("WindowFunctionExecutor::p_init(end)\n");
    return true;
}

/**
 * Create an instance of a window aggregator for the specified aggregate type
 * and "distinct" flag.  The object is allocated from the provided memory pool.
 */
inline WindowAggregate* getWindowedAggInstance(Pool& memoryPool,
                                               ExpressionType agg_type,
                                               bool isDistinct)
{
    switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_RANK:
        return new (memoryPool) RankAgg();
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_DENSE_RANK:
        return new (memoryPool) DenseRankAgg();
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_COUNT:
        return new (memoryPool) CountAgg();
    default:
        {
            char message[128];
            snprintf(message, sizeof(message), "Unknown aggregate type %d", agg_type);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
        }
    }
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
                                          m_aggTypes[ii],
                                          m_distinctAggs[ii]);
    }
}

inline void WindowFunctionExecutor::lookaheadInOrderByGroupForAggs(TableWindow *window,
                                                                   const TableTuple &tuple) {
    WindowAggregate** aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        if (aggs[ii]->needsOrderByLookahead()) {
            const AbstractPlanNode::OwningExpressionVector &inputExprs
                = getAggregateInputExpressions()[ii];
            NValueArray vals(inputExprs.size());
            for (int idx = 0; idx < inputExprs.size(); idx += 1) {
                vals[idx] = inputExprs[idx]->eval(&tuple);
            }
            aggs[ii]->lookaheadOneRowInOrderByGroup(window, vals);
        }
    }
}

inline void WindowFunctionExecutor::lookaheadInPartitionByGroupForAggs(TableWindow *window,
                                                                       const TableTuple &tuple) {
    WindowAggregate** aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        if (aggs[ii]->needsOrderByLookahead()) {
            const AbstractPlanNode::OwningExpressionVector &inputExprs
                = getAggregateInputExpressions()[ii];
            NValueArray vals(inputExprs.size());
            for (int idx = 0; idx < inputExprs.size(); idx += 1) {
                vals[idx] = inputExprs[idx]->eval(&tuple);
            }
            aggs[ii]->lookaheadOneRowInPartitionByGroup(window, vals);
        }
    }
}

inline void WindowFunctionExecutor::advanceAggs(const TableTuple& tuple,
                                                bool newOrderByGroup)
{
    m_aggregateRow->recordPassThroughTuple(tuple);
    WindowAggregate **aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        if (aggs[ii]->needsAdvance()) {
            const AbstractPlanNode::OwningExpressionVector &inputExprs
                = getAggregateInputExpressions()[ii];
            NValueArray vals(inputExprs.size());
            for (int idx = 0; idx < inputExprs.size(); idx += 1) {
                vals[idx] = inputExprs[idx]->eval(&tuple);
            }
            aggs[ii]->advance(vals, newOrderByGroup);
        }
    }
}

/*
 *
 * Helper method responsible for inserting the results of the
 * aggregation into a new tuple in the output table as well as passing
 * through any additional columns from the input table.
 */
inline void WindowFunctionExecutor::insertOutputTuple(WindowAggregateRow* winFunRow)
{
    TableTuple& tempTuple = m_tmpOutputTable->tempTuple();

    // We copy the aggregate values into the output tuple,
    // then the passthrough columns.
    WindowAggregate** aggs = winFunRow->getAggregates();
    for (int ii = 0; ii < getAggregateCount(); ii++) {
        NValue result = aggs[ii]->finalize(tempTuple.getSchema()->columnType(ii));
        tempTuple.setNValue(ii, result);
    }

    VOLT_TRACE("Setting passthrough columns");
    size_t tupleSize = tempTuple.sizeInValues();
    for (int ii = getAggregateCount(); ii < tupleSize; ii += 1) {
        AbstractExpression *expr = m_outputColumnExpressions[ii];
        tempTuple.setNValue(ii, expr->eval(&(winFunRow->getPassThroughTuple())));
    }

    m_tmpOutputTable->insertTempTuple(tempTuple);
    VOLT_TRACE("output_table:\n%s", m_tmpOutputTable->debug().c_str());
}

int WindowFunctionExecutor::compareTuples(const TableTuple &tuple1,
                                          const TableTuple &tuple2) const {
    const TupleSchema *schema = tuple1.getSchema();
    assert (schema == tuple2.getSchema());

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
 * This RAII class sets a pointer to null when it is goes out of scope.
 * We could use boost::scoped_ptr with a specialized destructor, but it
 * seems more obscure than this simple class.
 */
struct ScopedNullingPointer {
    ScopedNullingPointer(ProgressMonitorProxy * & ptr)
        : m_ptr(ptr) { }

    ~ScopedNullingPointer() {
        if (m_ptr) {
            m_ptr = NULL;
        }
    }
    ProgressMonitorProxy    * & m_ptr;
};

void WindowFunctionExecutor::processOneRow(TableWindow* window, bool newOrderByGroup)
{
}

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
    initWorkingTupleStorage();
    // Input table
    Table * input_table = m_abstractNode->getInputTable();
    assert(input_table);
    VOLT_TRACE("WindowFunctionExecutor: input table\n%s", input_table->debug().c_str());

    m_inputSchema = input_table->schema();
    assert(m_inputSchema);

    boost::shared_ptr<TableWindow>  scoped_window(new TableWindow(input_table));
    TableWindow *window = scoped_window.get();
    ProgressMonitorProxy pmp(m_engine, this);
    /*
     * This will set m_pmp to NULL on return, which avoids
     * a reference to the dangling pointer pmp if something
     * throws.
     */
    ScopedNullingPointer np(m_pmp);
    m_pmp = &pmp;

    m_aggregateRow
        = new (m_memoryPool, m_aggTypes.size())
             WindowAggregateRow(m_inputSchema, m_memoryPool);

    initAggInstances();

    TableTuple nextTuple(m_inputSchema);
    while (findLeadingEdge(window)) {
        VOLT_TRACE("LeadingEdge: %s", window->debug().c_str());
        m_aggregateRow->resetAggs();
        while (findOrderByEdge(window)) {
            VOLT_TRACE("OrderByEdge: %s", window->debug().c_str());
            for (int idx = 0; idx < window->m_orderByGroupSize; idx += 1) {
                VOLT_TRACE("middleEdge: Window = %s", window->debug().c_str());
                TableTuple nextTuple(m_inputSchema);
                window->m_middleEdge.next(nextTuple);
                m_pmp->countdownProgress();
                advanceAggs(nextTuple, idx == 0);
                insertOutputTuple(m_aggregateRow);
            }
        }
    }
    p_execute_finish(window);
    VOLT_TRACE("WindowFunctionExecutor: finalizing..");

    cleanupInputTempTable(input_table);
    VOLT_TRACE("WindowFunctionExecutor::p_execute(end)\n");
    return true;
}

bool WindowFunctionExecutor::findLeadingEdge(TableWindow *window)
{
    TableTuple nextTuple(m_inputSchema);

    assert(window->m_middleEdge == window->m_leadingEdge);
    window->m_trailingEdge = window->m_leadingEdge;

    /*
     * This is a theme.  We copy an iterator and increment the
     * copy.  If like the result we copy the copy back into
     * the iterator.  This lets us look ahead one row without
     * completely committing to it.
     */
    TableIterator nextEdge = window->m_leadingEdge;
    /*
     * The first row is slightly special.  We
     * don't have any rows yet, so we need to
     * prime the system.
     */
    // VOLT_TRACE("findLeadingEdge(start): %s", window->debug().c_str());
    if (nextEdge.next(nextTuple)) {
        initPartitionByKeyTuple(nextTuple);
        lookaheadInPartitionByGroupForAggs(window, nextTuple);
        /* First row.  Nothing to compare it with. */
        window->m_partitionSize = 1;
    } else {
       /*
        * If there is no first row, then just
        * return false.  The leading edge iterator
        * will never have a next row, so we can
        * ask for its next again and will always get false.
        */
        window->m_partitionSize = 0;
        return false;
    }
    do {
        // VOLT_TRACE("findLeadingEdge(loopStart): %s", window->debug().c_str());
        window->m_leadingEdge = nextEdge;
        if (nextEdge.next(nextTuple)) {
            initPartitionByKeyTuple(nextTuple);
            if (compareTuples(getInProgressPartitionByKeyTuple(),
                              getLastPartitionByKeyTuple()) != 0) {
                return true;
            } else {
                lookaheadInPartitionByGroupForAggs(window, nextTuple);
                window->m_partitionSize += 1;
            }
            VOLT_TRACE("findLeadingEdge(loop): %s", window->debug().c_str());
        } else {
            return true;
        }
    } while (true);
    // VOLT_TRACE("findLeadingEdge(end): %s", window->debug().c_str());
    return true;
}

bool WindowFunctionExecutor::findOrderByEdge(TableWindow *window)
{
    TableTuple nextTuple(m_inputSchema);

    assert(window->m_middleEdge == window->m_orderByEdge);
    TableIterator nextEdge = window->m_orderByEdge;
    /*
     * The first row is slightly special.  We
     * don't have any rows yet, so we need to
     * prime the system.
     */
    // VOLT_TRACE("findOrderByEdge(start): %s", window->debug().c_str());
    if (nextEdge != window->m_leadingEdge) {
        if ( ! nextEdge.next(nextTuple) ) {
            // How can this be true?
            window->m_orderByGroupSize = 0;
            return false;
        }
        initOrderByKeyTuple(nextTuple);
        /* First row.  Nothing to compare it with. */
        window->m_orderByGroupSize = 1;
        lookaheadInOrderByGroupForAggs(window, nextTuple);
    } else {
       /*
        * If there is no first row, then just
        * return false.  The leading edge iterator
        * will never have a next row, so we can
        * ask for its next again and will always get false.
        */
        window->m_orderByGroupSize = 0;
        return false;
    }
    do {
        window->m_orderByEdge = nextEdge;
        // VOLT_TRACE("findOrderByEdge(loopStart): %s", window->debug().c_str());
        if (nextEdge != window->m_leadingEdge) {
            nextEdge.next(nextTuple);
            initOrderByKeyTuple(nextTuple);
            if (compareTuples(getInProgressOrderByKeyTuple(),
                              getLastOrderByKeyTuple()) != 0) {
                return true;
            } else {
                window->m_orderByGroupSize += 1;
                lookaheadInOrderByGroupForAggs(window, nextTuple);
            }
            VOLT_TRACE("findOrderByEdge(loop): %s", window->debug().c_str());
        } else {
            return true;
        }
    } while (true);
    // VOLT_TRACE("findOrderByEdge(end): %s", window->debug().c_str());
    return true;
}


void WindowFunctionExecutor::initPartitionByKeyTuple(const TableTuple& nextTuple)
{
    /*
     * The partition by keys should not be null tuples.
     */
    assert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    assert( ! getLastPartitionByKeyTuple().isNullTuple());
    /*
     * Swap the data, so that m_inProgressPartitionByKey
     * gets m_lastPartitionByKey's data and vice versa.
     * This just swaps the data pointers.
     */
    swapPartitionByKeyTupleData();
    /*
     * The partition by keys should still not be null tuples.
     */
    assert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    assert( ! getLastPartitionByKeyTuple().isNullTuple());
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
    assert( ! getInProgressOrderByKeyTuple().isNullTuple());
    assert( ! getLastOrderByKeyTuple().isNullTuple());
    /*
     * Swap the data pointers.  No data is moved.
     */
    swapOrderByKeyTupleData();
    /*
     * Still should not be null tuples.
     */
    assert( ! getInProgressOrderByKeyTuple().isNullTuple());
    assert( ! getLastOrderByKeyTuple().isNullTuple());
    /*
     * Calculate the order by key values.
     */
    for (int ii = 0; ii < m_orderByExpressions.size(); ii++) {
        getInProgressOrderByKeyTuple().setNValue(ii, m_orderByExpressions[ii]->eval(&nextTuple));
    }
    /*
     * Still should not be null tuples.
     */
    assert( ! getInProgressOrderByKeyTuple().isNullTuple());
    assert( ! getLastOrderByKeyTuple().isNullTuple());
}

void WindowFunctionExecutor::swapPartitionByKeyTupleData() {
    assert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    assert( ! getLastPartitionByKeyTuple().isNullTuple());
    void* inProgressData = getInProgressPartitionByKeyTuple().address();
    void* nextData = getLastPartitionByKeyTuple().address();
    getInProgressPartitionByKeyTuple().move(nextData);
    getLastPartitionByKeyTuple().move(inProgressData);
    assert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    assert( ! getLastPartitionByKeyTuple().isNullTuple());
}

void WindowFunctionExecutor::swapOrderByKeyTupleData() {
    /*
     * Should not be null tuples.
     */
    assert( ! getInProgressOrderByKeyTuple().isNullTuple());
    assert( ! getLastOrderByKeyTuple().isNullTuple());
    void* inProgressData = getInProgressOrderByKeyTuple().address();
    void* nextData = getLastOrderByKeyTuple().address();
    getInProgressOrderByKeyTuple().move(nextData);
    getLastOrderByKeyTuple().move(inProgressData);
    /*
     * Still should not be null tuples.
     */
    assert( ! getInProgressOrderByKeyTuple().isNullTuple());
    assert( ! getLastOrderByKeyTuple().isNullTuple());
}


void WindowFunctionExecutor::p_execute_finish(TableWindow *window) {
    /*
     * The working tuples should not be null.
     */
    assert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    assert( ! getInProgressOrderByKeyTuple().isNullTuple());
    assert( ! getLastPartitionByKeyTuple().isNullTuple());
    assert( ! getLastOrderByKeyTuple().isNullTuple());
    getInProgressPartitionByKeyTuple().move(NULL);
    getInProgressOrderByKeyTuple().move(NULL);
    getLastPartitionByKeyTuple().move(NULL);
    getLastOrderByKeyTuple().move(NULL);
    /*
     * The working tuples have just been set to null.
     */
    assert( getInProgressPartitionByKeyTuple().isNullTuple());
    assert( getInProgressOrderByKeyTuple().isNullTuple());
    assert( getLastPartitionByKeyTuple().isNullTuple());
    assert( getLastOrderByKeyTuple().isNullTuple());
    m_memoryPool.purge();
}

void WindowFunctionExecutor::initWorkingTupleStorage() {
    assert( getInProgressPartitionByKeyTuple().isNullTuple());
    assert( getInProgressOrderByKeyTuple().isNullTuple());
    assert( getLastPartitionByKeyTuple().isNullTuple());
    assert( getLastOrderByKeyTuple().isNullTuple());

    m_inProgressPartitionByKeyStorage.init(m_partitionByKeySchema, &m_memoryPool);
    m_lastPartitionByKeyStorage.init(m_partitionByKeySchema, &m_memoryPool);

    m_lastOrderByKeyStorage.init(m_orderByKeySchema, &m_memoryPool);
    m_inProgressOrderByKeyStorage.init(m_orderByKeySchema, &m_memoryPool);

    m_inProgressPartitionByKeyStorage.allocateActiveTuple();
    m_lastPartitionByKeyStorage.allocateActiveTuple();

    m_inProgressOrderByKeyStorage.allocateActiveTuple();
    m_lastOrderByKeyStorage.allocateActiveTuple();

    assert( ! getInProgressPartitionByKeyTuple().isNullTuple());
    assert( ! getInProgressOrderByKeyTuple().isNullTuple());
    assert( ! getLastPartitionByKeyTuple().isNullTuple());
    assert( ! getLastOrderByKeyTuple().isNullTuple());

}
} /* namespace voltdb */
