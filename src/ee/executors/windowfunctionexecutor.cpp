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

#include "../plannodes/windowfunctionnode.h"
#include "../execution/ProgressMonitorProxy.h"
namespace voltdb {

WindowFunctionExecutor::~WindowFunctionExecutor() {
    // NULL Safe Operation
    TupleSchema::freeTupleSchema(m_partitionByKeySchema);
    TupleSchema::freeTupleSchema(m_orderByKeySchema);
}

TupleSchema* WindowFunctionExecutor::constructSomethingBySchema
        (const AbstractPlanNode::OwningExpressionVector &exprs) {
    std::vector<ValueType> somethingByColumnTypes;
    std::vector<int32_t> somethingByColumnSizes;
    std::vector<bool> somethingByColumnAllowNull;
    std::vector<bool> somethingByColumnInBytes;

    BOOST_FOREACH (AbstractExpression* expr, exprs) {
            somethingByColumnTypes.push_back(expr->getValueType());
            somethingByColumnSizes.push_back(expr->getValueSize());
            somethingByColumnAllowNull.push_back(true);
            somethingByColumnInBytes.push_back(expr->getInBytes());
    }
    return TupleSchema::createTupleSchema(somethingByColumnTypes,
                                          somethingByColumnSizes,
                                          somethingByColumnAllowNull,
                                          somethingByColumnInBytes);
}

/**
 * When this function is called, the AbstractExecutor's init function
 * will have set the input tables in the plan node, but nothing else.
 */
bool WindowFunctionExecutor::p_init(AbstractPlanNode *init_node, TempTableLimits *limits) {
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

    /*
     * Initialize all the data for partition by and
     * order by storage once and for all.
     */
    m_partitionByKeySchema = constructSomethingBySchema(m_partitionByExpressions);
    m_orderByKeySchema = constructSomethingBySchema(m_orderByExpressions);

    m_inProgressPartitionByKeyStorage.init(m_partitionByKeySchema, &m_memoryPool);
    m_lastPartitionByKeyStorage.init(m_partitionByKeySchema, &m_memoryPool);

    m_lastOrderByKeyStorage.init(m_orderByKeySchema, &m_memoryPool);
    m_inProgressOrderByKeyStorage.init(m_orderByKeySchema, &m_memoryPool);

    m_inProgressPartitionByKeyStorage.allocateActiveTuple();
    m_lastPartitionByKeyStorage.allocateActiveTuple();

    m_inProgressOrderByKeyStorage.allocateActiveTuple();
    m_lastOrderByKeyStorage.allocateActiveTuple();

    return true;
}

/**
 * Dense rank is the easiest.  We just count
 * the number of times the order by expression values
 * change.
 */
class DenseRankAgg : public WindowAggregate {
public:
    DenseRankAgg()
      : m_rank(1) {
    }

    virtual ~DenseRankAgg() {}
    virtual void advance(const AbstractPlanNode::OwningExpressionVector &val,
                         const TableTuple &nextTuple,
                         bool newOrderByGroup) {
        assert(val.size() == 0);
        if (newOrderByGroup) {
            m_rank += 1;
        }
    }
    virtual NValue finalize(ValueType type) {
        return ValueFactory::getBigIntValue(m_rank);
    }
    virtual void resetAgg() {
        WindowAggregate::resetAgg();
        m_rank = 1;
    }
protected:
    int m_rank;
};

/**
 * Rank is like dense rank, but we remember how
 * many rows there are between order by expression
 * changes.
 */
class RankAgg : public DenseRankAgg {
public:
    RankAgg()
    : DenseRankAgg(),
      m_numOrderByPeers(0) {}
    ~RankAgg() {}
    void advance(const AbstractPlanNode::OwningExpressionVector & val,
                 const TableTuple &nextTuple,
                 bool newOrderByGroup) {
       assert(val.size() == 0);
       if (newOrderByGroup) {
           m_rank += m_numOrderByPeers;
           m_numOrderByPeers = 1;
       } else {
           m_numOrderByPeers += 1;
       }
    }
private:
    long m_numOrderByPeers;
};
/*
 * Create an instance of a window aggregator for the specified aggregate type
 * and "distinct" flag.  The object is allocated from the provided memory pool.
 */
inline WindowAggregate* getAggInstance(Pool& memoryPool, ExpressionType agg_type, bool isDistinct)
{
    switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_RANK:
        return new (memoryPool) RankAgg();
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_DENSE_RANK:
        return new (memoryPool) DenseRankAgg();
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
inline void WindowFunctionExecutor::initAggInstances(WindowAggregateRow* aggregateRow)
{
    WindowAggregate** aggs = aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        aggs[ii] = getAggInstance(m_memoryPool, m_aggTypes[ii], m_distinctAggs[ii]);
    }
}

inline void WindowFunctionExecutor::advanceAggs(const TableTuple& tuple,
                                                bool newOrderByGroup)
{
    m_aggregateRow->recordPassThroughTuple(tuple);
    WindowAggregate **aggs = m_aggregateRow->getAggregates();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        const WindowFunctionPlanNode::OwningExpressionVector &inputExprs
            = getAggregateInputExpressions()[ii];
        aggs[ii]->advance(inputExprs, tuple, newOrderByGroup);
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
    int ii;
    for (ii = 0; ii < getAggregateCount(); ii++) {
        NValue result = aggs[ii]->finalize(tempTuple.getSchema()->columnType(ii));
        tempTuple.setNValue(ii, result);
    }

    VOLT_TRACE("Setting passthrough columns");
    size_t tupleSize = tempTuple.sizeInValues();
    size_t iii = getAggregateCount();
    std::cout << "tupleSize: " << tupleSize << "\n";
    std::cout << "getAggregateCount() " << iii << "\n";
    for (; ii < tempTuple.sizeInValues(); ii += 1) {
        AbstractExpression *expr = m_outputColumnExpressions[ii-getAggregateCount()];
        NValue nv = expr->eval(&(winFunRow->getPassThroughTuple()));
        tempTuple.setNValue(ii, nv);
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
void WindowFunctionExecutor::p_execute_tuple(const TableTuple& nextTuple, bool firstTuple)
{
    initPartitionByAndOrderByKeyTuple(nextTuple);

    bool newGroup = false;
    /*
     * If this is not the first tuple, see if this is
     * the start of a new group.  Compare the m_inProgressPartitionByKeyTuple
     * with the m_lastPartitionByKeyTuple.
     */
    if (!firstTuple) {
        newGroup = compareTuples(m_inProgressPartitionByKeyTuple,
                                 m_lastPartitionByKeyTuple);
    }
    if (newGroup) {
        m_pmp->countdownProgress();
        m_aggregateRow->resetAggs();
    }
    // Update the aggregation calculation.
    int newOrderByGroup = compareTuples(m_inProgressOrderByKeyTuple,
                                        m_lastOrderByKeyTuple);
    /*
     * The first tuple is not a new order by group.
     */
    advanceAggs(nextTuple, !firstTuple && newOrderByGroup != 0);
    // Output the current row.
    insertOutputTuple(m_aggregateRow);
}

/**
 * This RAII class sets a pointer to null when it is goes out of scope.
 * We could use boost::scoped_ptr with a specialized destructor, but it
 * seems more obscure than this simple class.
 */
template <typename T>
struct ScopedNullingPointer {
    ScopedNullingPointer(T *&ptr, T *value)
        : m_ptr(ptr) {
        ptr = value;
    }
    ~ScopedNullingPointer() {
        if (m_ptr) {
            m_ptr = NULL;
        }
    }
    T *&operator*() {
        return m_ptr;
    }
    T           *&m_ptr;
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
    // Input table
    bool firstTuple = true;
    Table* input_table = m_abstractNode->getInputTable();
    assert(input_table);
    VOLT_TRACE("WindowFunctionExecutor: input table\n%s", input_table->debug().c_str());
    m_inputSchema = input_table->schema();
    TableTuple nextTuple(m_inputSchema);

    ProgressMonitorProxy pmp(m_engine, this);
    /*
     * This will set m_pmp to NULL on return, which avoids
     * a reference to the dangling pointer pmp.  Note that
     * m_pmp is set to &pmp here.
     */
    ScopedNullingPointer<ProgressMonitorProxy> np(m_pmp, &pmp);

    m_aggregateRow
        = new (m_memoryPool, m_aggTypes.size())
             WindowAggregateRow(m_inputSchema, m_memoryPool);

    initAggInstances(m_aggregateRow);
    /*
     * This will not do when we implement proper windowing.
     * We won't be able to use a DeletingAsWeGo iterator,
     * at least not here.
     */
    TableIterator it = input_table->iteratorDeletingAsWeGo();
    /*
     * The first tuple is somewhat special.  We need to do
     * some initialization but we need to see the first tuple.
     */
    while (it.next(nextTuple)) {
        m_pmp->countdownProgress();
        p_execute_tuple(nextTuple, firstTuple);
        firstTuple = false;
    }
    p_execute_finish();
    VOLT_TRACE("WindowFunctionExecutor: finalizing..");

    cleanupInputTempTable(input_table);
    return true;
}

void WindowFunctionExecutor::initPartitionByAndOrderByKeyTuple(const TableTuple& nextTuple)
{
    /*
     * Now the partition by and order by keys for the
     * row before nextTuple are in m_inProgress*KeyTuple,
     * and m_last*TupleKeyStorage has nothing interesting.
     */
    swapPartitionByKeyTupleData();
    swapOrderByKeyTupleData();
    /*
     * Now, after the swap, the partition key and order by key
     * values for the row before nextTuple are in
     * m_last*TupleKeyStorage, and m_inProgress*TupleKeyStorage
     * has nothing interesting for us.  So we will put the
     * key values from nextTuple there.
     */
    assert( ! m_inProgressPartitionByKeyTuple.isNullTuple());
    assert( ! m_inProgressOrderByKeyTuple.isNullTuple());
    /*
     * Calculate the partition by key values.
     */
    for (int ii = 0; ii < m_partitionByExpressions.size(); ii++) {
        m_inProgressPartitionByKeyTuple.setNValue(ii, m_partitionByExpressions[ii]->eval(&nextTuple));
    }
    /*
     * Calculate the order by key values.
     */
    for (int ii = 0; ii < m_orderByExpressions.size(); ii++) {
        m_inProgressOrderByKeyTuple.setNValue(ii, m_orderByExpressions[ii]->eval(&nextTuple));
    }
}

void WindowFunctionExecutor::swapPartitionByKeyTupleData() {
    void* inProgressData = m_inProgressPartitionByKeyTuple.address();
    void* nextData = m_lastPartitionByKeyTuple.address();
    m_inProgressPartitionByKeyTuple.move(nextData);
    m_lastPartitionByKeyTuple.move(inProgressData);
}

void WindowFunctionExecutor::swapOrderByKeyTupleData() {
    void* inProgressData = m_inProgressOrderByKeyTuple.address();
    void* nextData = m_lastOrderByKeyTuple.address();
    m_inProgressOrderByKeyTuple.move(nextData);
    m_lastOrderByKeyTuple.move(inProgressData);
}


void WindowFunctionExecutor::p_execute_finish() {
    m_inProgressPartitionByKeyTuple.move(NULL);
    m_inProgressOrderByKeyTuple.move(NULL);
    m_lastPartitionByKeyTuple.move(NULL);
    m_lastOrderByKeyTuple.move(NULL);
    m_memoryPool.purge();
}
} /* namespace voltdb */
