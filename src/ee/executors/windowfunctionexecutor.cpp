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

#include "plannodes/windowfunctionnode.h"
#include "execution/ProgressMonitorProxy.h"
#include <sstream>
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
          m_orderByGroupSize(0),
          m_beforeFirstRow(true),
          m_beforeFirstGroup(true),
          m_afterLastRow(false) {}
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
    /**
     * True only on the first row of a table.  We need
     * this because the last partition by tuple is not
     * primed on the first row, so we can't compare it
     * to the inprogress tuple.
     */
    bool m_beforeFirstRow;
    /**
     * Set to true on the first row of the first group.
     * We need this because we buffer rows, and because
     * we look for partitions before order by rows.
     */
    bool m_beforeFirstGroup;
    /**
     * Set to true if we are looking at the last row
     * of the table.  We need this because we are
     * buffering one row always.
     */
    bool m_afterLastRow;
};

/**
 * A WindowAggregate is the base class of aggregate calculations.
 * Each aggregate function has some
 */
class WindowAggregate {
public:
    virtual ~WindowAggregate() {

    }
    void* operator new(size_t size, Pool& memoryPool) { return memoryPool.allocate(size); }
    void operator delete(void*, Pool& memoryPool) { /* NOOP -- on alloc error unroll nothing */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    virtual void advance(const NValueArray &val,
                         bool newOrderByGroup) = 0;

    virtual void advanceToOrderByEdge(TableWindow *window) {
        m_partitionSize = window->m_partitionSize;
        m_orderByGroupSize = window->m_orderByGroupSize;
    }

    virtual NValue finalize(ValueType type)
    {
        m_value.castAs(type);
        return m_value;
    }

    virtual void resetAgg()
    {
        m_value.setNull();
    }
protected:
    NValue m_value;
    size_t m_partitionSize;
    size_t m_orderByGroupSize;
};

/**
 * Dense rank is the easiest.  We just count
 * the number of times the order by expression values
 * change.
 */
class DenseRankAgg : public WindowAggregate {
public:
    DenseRankAgg()
      : m_rank(0) {
    }

    virtual ~DenseRankAgg() {}
    virtual void advance(const NValueArray &,
                         bool newOrderByGroup) {
    }
    virtual void advanceToOrderByEdge(TableWindow *window) {
        m_rank += orderByPeerIncrement();
        WindowAggregate::advanceToOrderByEdge(window);
    }
    virtual NValue finalize(ValueType type) {
        return ValueFactory::getBigIntValue(m_rank);
    }
    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_rank = 0;
    }
    virtual size_t orderByPeerIncrement() {
        return 1;
    }
protected:
    int m_rank;
};

/**
 * Rank is like dense rank, but we increment
 * the m_rank by the size of the order by group.
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

class CountAgg : public WindowAggregate {
public:
    CountAgg()
      : m_count(0) {}

    virtual ~CountAgg() {}
    virtual void advance(const NValueArray &val,
                         bool newOrderByGroup) {
        assert(val.size() <= 1);
        if ((val.size() == 0) || ( ! val[0].isNull())) {
            m_count += 1;
        }
    }
    virtual NValue finalize(ValueType type) {
        return ValueFactory::getBigIntValue(m_count);
    }
    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_count = 0;
    }
private:
    long m_count;
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

inline void WindowFunctionExecutor::advanceAggsToOrderByEdge(TableWindow *window) {
    WindowAggregate** aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        aggs[ii]->advanceToOrderByEdge(window);
    }
}

inline void WindowFunctionExecutor::advanceAggs(const TableTuple& tuple,
                                                bool newOrderByGroup)
{
    m_aggregateRow->recordPassThroughTuple(tuple);
    WindowAggregate **aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        const AbstractPlanNode::OwningExpressionVector &inputExprs
            = getAggregateInputExpressions()[ii];
        NValueArray vals(inputExprs.size());
        for (int idx = 0; idx < inputExprs.size(); idx += 1) {
            vals[idx] = inputExprs[0]->eval(&tuple);
        }
        aggs[ii]->advance(vals, newOrderByGroup);
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

void WindowFunctionExecutor::processOneRow(TableWindow* window)
{
    VOLT_TRACE("middleEdge: Window = %s", window->debug().c_str());
    TableTuple nextTuple(m_inputSchema);
    window->m_middleEdge.next(nextTuple);
    m_pmp->countdownProgress();
    advanceAggs(nextTuple, window->m_beforeFirstGroup);
    window->m_beforeFirstGroup = false;
    insertOutputTuple(m_aggregateRow);
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

    boost::scoped_ptr<TableWindow>  scoped_window(new TableWindow(input_table));
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

    while (findLeadingEdge(window)) {
        VOLT_TRACE("LeadingEdge: %s", window->debug().c_str());
        m_aggregateRow->resetAggs();
        while (findOrderByEdge(window)) {
            VOLT_TRACE("OrderByEdge: %s", window->debug().c_str());
            advanceAggsToOrderByEdge(window);
            for (int idx = 0; idx < window->m_orderByGroupSize; idx += 1) {
                processOneRow(window);
            }
        }
    }
    p_execute_finish(window);
    VOLT_TRACE("WindowFunctionExecutor: finalizing..");

    cleanupInputTempTable(input_table);
    VOLT_TRACE("WindowFunctionExecutor::p_execute(end)\n");
    return true;
}

bool WindowFunctionExecutor::findLeadingEdge(TableWindow *window) {
    TableTuple nextTuple(m_inputSchema);
    window->m_partitionSize = 0;
    if ( ! window->m_leadingEdge.hasNext()) {
        if ( ! window->m_beforeFirstRow && ! window->m_afterLastRow ) {
            /*
             * If it's not before the first row we have one row
             * in our buffer.  If in addition we can't
             * read the next row, then the partition has
             * size 1.
             */
            window->m_afterLastRow = true;
            window->m_partitionSize += 1;
            return true;
        } else {
            /*
             * If we don't have a next leading edge row,
             * and it is the first row or else we have
             * processed the last row, then we are completely
             * done.
             */
            window->m_trailingEdge = window->m_middleEdge = window->m_orderByEdge = window->m_leadingEdge;
            window->m_partitionSize = 0;
            return false;
        }
    }
    /*
     * Now we know there is a next row for the leading edge.
     */
    do {
        VOLT_TRACE("findLeadingEdge: Window = %s", window->debug().c_str());
        if ( ! window->m_leadingEdge.next(nextTuple)) {
            /*
             * If there is no next row, then the buffered row
             * must be in the same partition as the last row.
             * So, just push past it.
             */
            window->m_partitionSize += 1;
            window->m_beforeFirstRow = false;
            window->m_afterLastRow = true;
            break;
        } else {
            /*
             * If there is a next row, we may have just read
             * the very first row, or we may have a buffered row
             * already. If this is the very first row, just
             * push past it.  Otherwise, compare this next row
             * with the previous row.  If it's different, then
             * we are done with this partition.  But we have
             * a buffered row in the in progress partition.
             */
            initPartitionByKeyTuple(nextTuple);
            if (!window->m_beforeFirstRow
                    && compareTuples(getInProgressPartitionByKeyTuple(),
                                     getLastPartitionByKeyTuple()) != 0) {
                break;
            }
            window->m_beforeFirstRow = false;
        }
    } while (true);
    VOLT_TRACE("findLeadingEdge(end): Window = %s", window->debug().c_str());
    return true;
}

bool WindowFunctionExecutor::findOrderByEdge(TableWindow *window) {
    TableTuple nextTuple(m_inputSchema);

    window->m_orderByGroupSize = (window->m_beforeFirstGroup ? 0 : 1);
    if (window->m_orderByEdge == window->m_leadingEdge) {
        return false;
    }
    do {
        VOLT_TRACE("findOrderByEdge: Window = %s", window->debug().c_str());
        window->m_orderByGroupSize += 1;
        if (window->m_orderByEdge != window->m_leadingEdge) {
            window->m_orderByEdge.next(nextTuple);
            initOrderByKeyTuple(nextTuple);
            if (!window->m_beforeFirstGroup
                    && compareTuples(getInProgressOrderByKeyTuple(),
                                     getLastOrderByKeyTuple()) != 0) {
                break;
            }
            window->m_beforeFirstGroup = false;
        } else {
            break;
        }
    } while (true);
    VOLT_TRACE("findOrderByEdge(end): Window = %s", window->debug().c_str());
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
