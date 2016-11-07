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

namespace voltdb {

WindowFunctionExecutor::~WindowFunctionExecutor() {
	// NULL Safe Operation
	TupleSchema::freeTupleSchema(m_partitionBySchema);
}

inline TupleSchema* WindowFunctionExecutor::constructPartitionBySchema() {
    std::vector<ValueType> partitionByColumnTypes;
    std::vector<int32_t> partitionByColumnSizes;
    std::vector<bool> partitionByColumnAllowNull;
    std::vector<bool> partitionByColumnInBytes;

    BOOST_FOREACH (AbstractExpression* expr, m_partitionByExpressions) {
            partitionByColumnTypes.push_back(expr->getValueType());
            partitionByColumnSizes.push_back(expr->getValueSize());
            partitionByColumnAllowNull.push_back(true);
            partitionByColumnInBytes.push_back(expr->getInBytes());
    }
    return TupleSchema::createTupleSchema(partitionByColumnTypes,
                                          partitionByColumnSizes,
                                          partitionByColumnAllowNull,
                                          partitionByColumnInBytes);
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
    m_partitionByKeySchema = constructPartitionBySchema();
    return true;
}

class RankAgg : public WindowAggregate {
	virtual void advance(const AbstractPlanNode::OwningExpressionVector & val) {

    }

};

class DenseRankAgg : public WindowAggregate {
	virtual void advance(const AbstractPlanNode::OwningExpressionVector & val) {

    }
};
/*
 * Create an instance of an windowed aggregator for the specified aggregate type
 * and "distinct" flag.  The object is allocated from the provided memory pool.
 */
inline Agg* getAggInstance(Pool& memoryPool, ExpressionType agg_type, bool isDistinct)
{
    switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_RANK:
    	return new (memoryPool) RankAgg(&memoryPool);
    case EXPRESSION_TYPE_AGGREGATE_WINDOWED_DENSE_RANK:
    	return new (memoryPool) DenseRankAgg(&memoryPool);
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
    WindowAggregate** aggs = aggregateRow->m_aggregates;
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        aggs[ii] = getAggInstance(m_memoryPool, m_aggTypes[ii], m_distinctAggs[ii]);
    }
}

inline void WindowFunctionExecutor::advanceAggs(WindowAggregateRow* aggregateRow, const TableTuple& tuple)
{
    WindowAggregate **aggs = aggregateRow->m_aggregates;
    const std::vector<AbstractPlanNode::OwningExpressionVector> &aggInputExprs = getAggregateInputExpressions();
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        // In particular, COUNT(*) accepts a dummy NValue from a NULL input expression.
        WindowFunctionPlanNode::OwningExpressionVector &inputExpr = getAggregateInputExpressions()[ii];
        aggs[ii]->advance(aggInputExprs[ii]);
    }
}
/// Helper method responsible for inserting the results of the
/// aggregation into a new tuple in the output table as well as passing
/// through any additional columns from the input table.
inline void WindowFunctionExecutor::insertOutputTuple(WindowAggregateRow* winFunRow)
{
    TableTuple& tempTuple = m_tmpOutputTable->tempTuple();

    // We copy the aggregate values into the output tuple,
    // then the passthrough columns.
    WindowAggregate** aggs = winFunRow->m_aggregates;
    for (int ii = 0; ii < getAggregateCount(); ii++) {
        NValue result = aggs[ii]->finalize(tempTuple.getSchema()->columnType(ii));
        tempTuple.setNValue(ii, result);
    }

    VOLT_TRACE("Setting passthrough columns");
    for (int ii = getAggregateCount(); ii < tempTuple.sizeInValues(); ii += 1) {
        tempTuple.setNValue(ii,
                            m_outputColumnExpressions[ii]->eval(&(winFunRow->m_passThroughTuple)));
    }

    m_tmpOutputTable->insertTempTuple(tempTuple);
    VOLT_TRACE("output_table:\n%s", m_tmpOutputTable->debug().c_str());
}

void WindowFunctionExecutor::p_init_tuple(const TableTuple &nextTuple) {
	initPartitionByKeyTuple(nextTuple);

	// Start the aggregation calculation.
	initAggInstances(m_aggregateRow);
	m_aggregateRow->recordPassThroughTuple(m_passThroughTupleSource, nextTuple);
	advanceAggs(m_aggregateRow, nextTuple);
}

void WindowFunctionExecutor::p_execute_tuple(const TableTuple& nextTuple)
{
    TableTuple& nextPartitionByKeyTuple = swapWithInprogressPartitionByKeyTuple();

    initPartitionByKeyTuple(nextTuple);

    for (int ii = m_partitionByKeySchema->columnCount() - 1; ii >= 0; --ii) {
        if (nextPartitionByKeyTuple.getNValue(ii).compare(m_inProgressPartitionByKeyTuple.getNValue(ii)) != 0) {
            VOLT_TRACE("new group!");
            // Output old row.
            insertOutputTuple(m_aggregateRow);
            m_pmp->countdownProgress();
            m_aggregateRow->resetAggs();

            // record the new group scanned tuple
            m_aggregateRow->recordPassThroughTuple(m_passThroughTupleSource, nextTuple);
            break;
        }
    }
    // update the aggregation calculation.
    advanceAggs(m_aggregateRow, nextTuple);
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
    Table* input_table = m_abstractNode->getInputTable();
    assert(input_table);
    VOLT_TRACE("WindowFunctionExecutor: input table\n%s", input_table->debug().c_str());
    /*
     * This will not do when we implement proper windowing.
     */
    TableIterator it = input_table->iteratorDeletingAsWeGo();
    m_inputSchema = input_table->schema();
    TableTuple nextTuple(m_inputSchema);

    ProgressMonitorProxy pmp(m_engine, this);
    /*
     * This will set m_pmp to NULL on return, which avoids
     * a reference to the dangling pointer pmp.  Note that
     * m_pmp is set to &pmp here.
     */
    ScopedNullingPointer<ProgressMonitorProxy> np(m_pmp, &pmp);
    /*
     * Start of code copied from AggregateSerialExecutor::p_execute_init.
     */
    /*
     * Start of code copied from AggregateExecutorBase::p_execute_init.
     */
    m_memoryPool.purge();
    %%%
    m_nextPartitionByKeyStorage.init(m_partitionByKeySchema, &m_memoryPool);
    TableTuple& nextPartitionByKeyTuple = m_nextPartitionByKeyStorage;
    nextPartitionByKeyTuple.move(NULL);


    m_inProgressPartitionByKeyTuple.setSchema(m_partitionByKeySchema);
    // set the schema first because of the NON-null check in MOVE function
    m_inProgressPartitionByKeyTuple.move(NULL);

    char * storage = reinterpret_cast<char*>(m_memoryPool.allocateZeroes(m_inputSchema->tupleLength() + TUPLE_HEADER_SIZE));

    TableTuple nextInputTuple =  TableTuple(storage, m_inputSchema);

    m_aggregateRow = new (m_memoryPool, m_aggTypes.size()) AggregateRow();

    char* storage = reinterpret_cast<char*>(m_memoryPool.allocateZeroes(m_inputSchema->tupleLength() + TUPLE_HEADER_SIZE));
    m_passThroughTupleSource = TableTuple(storage, m_inputSchema);

    /*
     * The first tuple is somewhat special.  We need to do
     * some initialization but we need to see that first tuple.
     */
    if (it.next(nextTuple)) {
    	p_init_tuple(nextTuple);
    	p_execute_tuple(nextTuple);
    	while (it.next(nextTuple)) {
    		m_pmp->countdownProgress();
    		p_execute_tuple(nextTuple);
    	}
    }
    p_execute_finish();
    VOLT_TRACE("WindowFunctionExecutor: finalizing..");

    cleanupInputTempTable(input_table);
    return true;
}

void WindowFunctionExecutor::initPartitionByKeyTuple(const TableTuple& nextTuple)
{
    TableTuple& nextGroupByKeyTuple = m_nextPartitionByKeyStorage;
    if (nextGroupByKeyTuple.isNullTuple()) {
        // Allocate space for the tuple.
        m_nextPartitionByKeyStorage.allocateActiveTuple();
    }
    for (int ii = 0; ii < m_partitionByExpressions.size(); ii++) {
        nextGroupByKeyTuple.setNValue(ii, m_partitionByExpressions[ii]->eval(&nextTuple));
    }
}

TableTuple& WindowFunctionExecutor::swapWithInprogressPartitionByKeyTuple() {
    TableTuple& nextPartitionByKeyTuple = m_nextPartitionByKeyStorage;

    void* recycledStorage = m_inProgressPartitionByKeyTuple.address();
    void* inProgressStorage = nextPartitionByKeyTuple.address();
    m_inProgressPartitionByKeyTuple.move(inProgressStorage);
    nextPartitionByKeyTuple.move(recycledStorage);

    return nextPartitionByKeyTuple;
}


} /* namespace voltdb */
