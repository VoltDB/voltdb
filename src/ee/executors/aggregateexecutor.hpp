/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/***
 *
 *  DO NOT INCLUDE THIS FILE ANYWHERE EXCEPT executors.h.
 *
 ****/
#ifndef HSTOREAGGREGATEEXECUTOR_H
#define HSTOREAGGREGATEEXECUTOR_H

#include "common/Pool.hpp"
#include "common/ValueFactory.hpp"
#include "common/common.h"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "executors/abstractexecutor.h"
#include "expressions/abstractexpression.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"

#include "boost/unordered_map.hpp"
#include "boost/scoped_ptr.hpp"

#include <algorithm>
#include <exception>
#include <limits>
#include <set>
#include <stdint.h>
#include <utility>
#include <vector>
#include <cassert>

namespace voltdb {


typedef std::vector<int> PassThroughColType;

/*
 * Type of the hash set used to check for column aggregate distinctness
 */
typedef boost::unordered_set<NValue,
                             NValue::hash,
                             NValue::equal_to> AggregateNValueSetType;

/*
 * Base class for an individual aggregate that aggregates a specific
 * column for a group
 */
class Agg
{
public:
    Agg(bool isDistinct) : mIsDistinct(isDistinct)
    {}
    virtual ~Agg()
    {
        mDistinctVals.clear();
    }
    virtual void advance(const NValue val) = 0;
    virtual NValue finalize() = 0;
protected:
    bool includeValue(NValue val)
    {
        bool retval = true;
        if (mIsDistinct)
        {
            // find this value in the set.  If it doesn't exist, add
            // it, otherwise indicate it shouldn't be included in the
            // aggregate
            AggregateNValueSetType::iterator setval =
                mDistinctVals.find(val);
            if (setval == mDistinctVals.end())
            {
                mDistinctVals.insert(val);
            }
            else
            {
                retval = false;
            }
        }
        return retval;
    }

    bool mIsDistinct;
    AggregateNValueSetType mDistinctVals;
};

class SumAgg : public Agg
{
  public:
    SumAgg(bool isDistinct) :
        Agg(isDistinct), m_haveAdvanced(false)
    {
        // m_value initialized on first advance
    }

    void advance(const NValue val)
    {
        if (val.isNull() || !includeValue(val)) {
            return;
        }
        if (!m_haveAdvanced) {
            m_value = val;
            m_haveAdvanced = true;
        }
        else {
            m_value = m_value.op_add(val);
        }
    }

    NValue finalize()
    {
        if (!m_haveAdvanced)
        {
            return ValueFactory::getNullValue();
        }
        return m_value;
    }

private:
    NValue m_value;
    bool m_haveAdvanced;
};


class AvgAgg : public Agg {
public:
    AvgAgg(bool isDistinct) :
        Agg(isDistinct), m_count(0)
    {
        // m_value initialized on first advance.
    }

    void advance(const NValue val)
    {
        if (val.isNull() || !includeValue(val)) {
            return;
        }
        if (m_count == 0) {
            m_value = val;
        }
        else {
            m_value = m_value.op_add(val);
        }
        ++m_count;
    }

    NValue finalize()
    {
        if (m_count == 0)
        {
            return ValueFactory::getNullValue();
        }
        const NValue finalizeResult =
            m_value.op_divide(ValueFactory::getBigIntValue(m_count));
        return finalizeResult;
    }

private:
    NValue m_value;
    int64_t m_count;
};

//count always holds integer
class CountAgg : public Agg
{
public:
    CountAgg(bool isDistinct) :
        Agg(isDistinct), m_count(0)
    {}

    void advance(const NValue val)
    {
        if (val.isNull() || !includeValue(val))
        {
            return;
        }
        m_count++;
    }

    NValue finalize()
    {
        return ValueFactory::getBigIntValue(m_count);
    }

private:
    int64_t m_count;
};

class CountStarAgg : public Agg
{
public:
    CountStarAgg(bool isDistinct) :
        Agg(isDistinct), m_count(0)
    {}

    void advance(const NValue val)
    {
        ++m_count;
    }

    NValue finalize()
    {
        return ValueFactory::getBigIntValue(m_count);
    }

private:
    int64_t m_count;
};

class MaxAgg : public Agg
{
public:
    MaxAgg(bool isDistinct) :
        Agg(isDistinct), m_haveAdvanced(false)
    {
        m_value.setNull();
    }

    void advance(const NValue val)
    {
        if (val.isNull() || !includeValue(val))
        {
            return;
        }
        if (!m_haveAdvanced)
        {
            m_value = val;
            m_haveAdvanced = true;
        }
        else
        {
            m_value = m_value.op_max(val);
        }
    }

    NValue finalize()
    {
        if (!m_haveAdvanced) {
            return ValueFactory::getNullValue();
        }
        return m_value;
    }

private:
    NValue m_value;
    bool m_haveAdvanced;
};

class MinAgg : public Agg
{
public:
    MinAgg(bool isDistinct) :
        Agg(isDistinct), m_haveAdvanced(false)
    {
        m_value.setNull();
    }

    void advance(const NValue val)
    {
        if (val.isNull() || !includeValue(val))
        {
            return;
        }
        if (!m_haveAdvanced)
        {
            m_value = val;
            m_haveAdvanced = true;
        }
        else
        {
            m_value = m_value.op_min(val);
        }
    }

    NValue finalize()
    {
        if (!m_haveAdvanced) {
            return ValueFactory::getNullValue();
        }
        return m_value;
    }

private:
    NValue m_value;
    bool m_haveAdvanced;
};

/*
 * Create an instance of an aggregator for the specified aggregate
 * type, column type, and result type. The object is constructed in
 * memory from the provided memrory pool.
 */
inline Agg* getAggInstance(Pool* memoryPool, ExpressionType agg_type,
                           bool isDistinct)
{
    Agg* agg;
    switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_COUNT:
        agg = new (memoryPool->allocate(sizeof(CountAgg))) CountAgg(isDistinct);
        break;
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
        agg = new (memoryPool->allocate(sizeof(CountStarAgg))) CountStarAgg(isDistinct);
        break;
    case EXPRESSION_TYPE_AGGREGATE_SUM:
        agg = new (memoryPool->allocate(sizeof(SumAgg))) SumAgg(isDistinct);
        break;
    case EXPRESSION_TYPE_AGGREGATE_AVG:
        agg = new (memoryPool->allocate(sizeof(AvgAgg))) AvgAgg(isDistinct);
        break;
    case EXPRESSION_TYPE_AGGREGATE_MIN:
        agg = new (memoryPool->allocate(sizeof(MinAgg))) MinAgg(isDistinct);
        break;
    case EXPRESSION_TYPE_AGGREGATE_MAX  :
        agg = new (memoryPool->allocate(sizeof(MaxAgg))) MaxAgg(isDistinct);
        break;
    default: {
        char message[128];
        snprintf(message, 128, "Unknown aggregate type %d", agg_type);
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      message);
    }
    }
    return agg;
}

// Aggregate Struct to keep Executor state in between iteration
namespace detail
{
    struct AggregateExecutorState
    {
        AggregateExecutorState(Table* outputTable) :
           m_outputTable(outputTable),
           m_iterator(outputTable->iterator()),
           m_outputTableSchema(outputTable->schema())
        {}

        Table* m_outputTable;
        TableIterator m_iterator;
        const TupleSchema* m_outputTableSchema;
    };

} //namespace detail

/**
 * The actual executor class templated on the type of aggregation that
 * should be performed. If it is instantiated using
 * PLAN_NODE_TYPE_AGGREGATE then it will do a constant space
 * aggregation that expects the input table to be sorted on the group
 * by key. If it is instantiated using PLAN_NODE_TYPE_HASHAGGREGATE
 * then the input does not need to be sorted and it will hash the
 * group by key to aggregate the tuples.
 */
template<PlanNodeType aggregateType>
class AggregateExecutor : public AbstractExecutor
{
public:
    AggregateExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node) :
        AbstractExecutor(engine, abstract_node), m_groupByKeySchema(NULL),
        m_state()
    { };
    ~AggregateExecutor();

    bool support_pull() const;
    /** Aggregates save tuples in the output table so parent SendExecutor must not save them again*/
    bool parent_send_need_save_tuple_pull() const;

protected:
    bool p_init(AbstractPlanNode* abstract_node,
                TempTableLimits* limits);
    bool p_execute(const NValueArray &params);

    TableTuple p_next_pull();
    void p_pre_execute_pull(const NValueArray& params);
    void p_post_execute_pull();
    void p_reset_state_pull();
    /*
     * List of columns in the output schema that are passing through
     * the value from a column in the input table and not doing any
     * aggregation.
     */
    PassThroughColType m_passThroughColumns;
    Pool m_memoryPool;
    TupleSchema* m_groupByKeySchema;
    boost::scoped_ptr<detail::AggregateExecutorState> m_state;
};


/**
 * A list of aggregates for a specific group.
 */
struct AggregateList
{
    // A tuple from the group of tuples being aggregated. Source of
    // pass through columns.
    TableTuple m_groupTuple;

    // The aggregates for each column for this group
    Agg* m_aggregates[0];
};

/*
 * Type of the hash table used to store aggregates for each group.
 */
typedef boost::unordered_map<TableTuple,
                             AggregateList*,
                             TableTupleHasher,
                             TableTupleEqualityChecker> HashAggregateMapType;

/*
 * Forward Declaration for an aggregator (not an an individual aggregate) that
 * will aggregate some number of tuples and produce the results in the
 * provided output table.
 */
template<PlanNodeType aggregateType>
class Aggregator;

/*
 * Aggregator base to encapsulate common to all aggregators behavior
 */
class AggregatorBase
{
protected:
    AggregatorBase(Pool *memoryPool,
                      TupleSchema *groupByKeySchema,
                      AggregatePlanNode* node,
                      PassThroughColType* passThroughColumns,
                      Table* input_table,
                      Table* output_table,
                      std::vector<ExpressionType> *agg_types,
                      const std::vector<bool>& distinctAggs,
                      const std::vector<AbstractExpression*>& groupByExpressions,
                      std::vector<ValueType> *col_types)
        : m_memoryPool(memoryPool),
          m_groupByKeySchema(groupByKeySchema),
          m_node(node),
          m_passThroughColumns(passThroughColumns),
          m_inputTable(input_table),
          m_outputTable(output_table),
          m_aggTypes(agg_types),
          m_distinctAggs(distinctAggs),
          m_groupByExpressions(groupByExpressions),
          m_colTypes(col_types),
          m_tempInputTable()
    {
        TempTable* tmpOutputTable = dynamic_cast<TempTable*>(m_outputTable);
        assert(tmpOutputTable);
        // m_tempOutputTable is a copy of the inputTable to keep copy
        // of unique tuples from the input table
        m_tempInputTable.reset(TableFactory::getCopiedTempTable(
            m_inputTable->databaseId(), std::string("temp_input"),
            m_inputTable, tmpOutputTable->m_limits));
    }

    /*
     * Helper method responsible for inserting the results of the
     * aggregation into a new tuple in the output table as well as passing
     * through any additional columns from the input table.
     */
    bool insertTuple(Agg** aggs, TableTuple& tuple)
    {
        TableTuple& tmptup = m_outputTable->tempTuple();

        /*
         * This first pass is to add all columns that were aggregated on.
         */
        std::vector<ExpressionType> aggregateTypes = m_node->getAggregates();
        std::vector<int> aggregateOutputColumns = m_node->getAggregateOutputColumns();
        for (int ii = 0; ii < aggregateOutputColumns.size(); ii++)
        {
            if (aggs[ii] != NULL)
            {
                const int columnIndex = aggregateOutputColumns[ii];
                const ValueType columnType = tmptup.getType(columnIndex);
                tmptup.setNValue(columnIndex,
                                 aggs[ii]->finalize().castAs(columnType));
            }
            else
            {
                // (rtb) This is surely not desirable code. However... I
                // think that the planner sometimes outputs aggregate
                // configuration that confuses the aggregate output
                // columns (that are aggregation f()'s) with the group by
                // columns. Maybe this only happens when aggregating DML
                // results?  Need to come back to this; but for now, this
                // arrangement satisfies valgrind (not previously the
                // case) and passes the plans group by test suite (also
                // not previously the case).
                return true;
            }
        }
        VOLT_TRACE("Setting passthrough columns");
        /*
         * Execute a second pass to set the output columns from the input
         * columns that are being passed through.  These are the columns
         * that are not being aggregated on but are still in the SELECT
         * list. These columns may violate the Single-Value rule for GROUP
         * BY (not be on the group by column reference list). This is an
         * intentional optimization to allow values that are not in the
         * GROUP BY to be passed through.
         */
        for (int i = 0; i < m_passThroughColumns->size(); i++)
        {
            int output_col_index = (*m_passThroughColumns)[i];
            tmptup.setNValue(output_col_index,
                             m_node->getOutputSchema()[output_col_index]->
                             getExpression()->eval(&tuple, NULL));
        }
        if (!m_outputTable->insertTuple(tmptup)) {
            VOLT_ERROR("Failed to insert order-by tuple from input table '%s' into"
                       " output table '%s'",
                       m_inputTable->name().c_str(), m_outputTable->name().c_str());
            return false;
        }
        return true;
    }

    bool insertEmptyRecord(Agg** aggregates, TableTuple prevTuple)
    {

        // if no record exists in input_table, we have to output one record
        // only when it doesn't have GROUP BY. See difference of these cases:
        //   SELECT SUM(A) FROM BBB ,   when BBB has no tuple
        //   SELECT SUM(A) FROM BBB GROUP BY C,   when BBB has no tuple
        if (m_groupByExpressions.size() == 0 &&
            m_outputTable->activeTupleCount() == 0)
        {
            VOLT_TRACE("no record. outputting a NULL row..");
            for (int i = 0; i < m_colTypes->size(); i++)
            {
                // It is necessary to look up the mapping between the
                // aggregate index i and the associated output column
                aggregates[i] =
                    getAggInstance(m_memoryPool,
                                   (*m_aggTypes)[i],
                                   m_distinctAggs[i]);
            }
            return insertTuple(aggregates, prevTuple);
        }
        return true;
    }

    void purgeAggs(Agg** aggs, size_t size)
    {
        for (int ii = 0; ii < size; ii++)
        {
            if (aggs[ii] != NULL)
            {
                aggs[ii]->~Agg();
            }
        }
    }

    Pool* m_memoryPool;
    TupleSchema *m_groupByKeySchema;
    AggregatePlanNode* m_node;
    PassThroughColType* m_passThroughColumns;
    Table* m_inputTable;
    Table* m_outputTable;
    std::vector<ExpressionType>* m_aggTypes;
    const std::vector<bool>& m_distinctAggs;
    const std::vector<AbstractExpression*>& m_groupByExpressions;
    std::vector<ValueType>* m_colTypes;
    boost::scoped_ptr<TempTable> m_tempInputTable;
};


/*
 * Specialization of an Aggregator that uses a hash map to aggregate
 * tuples from the input table.
 */
template<>
class Aggregator<PLAN_NODE_TYPE_HASHAGGREGATE> : public AggregatorBase
{
public:
    Aggregator(Pool *memoryPool,
                      TupleSchema *groupByKeySchema,
                      AggregatePlanNode* node,
                      PassThroughColType* passThroughColumns,
                      Table* input_table,
                      Table* output_table,
                      std::vector<ExpressionType> *agg_types,
                      const std::vector<bool>& distinctAggs,
                      const std::vector<AbstractExpression*>& groupByExpressions,
                      std::vector<ValueType> *col_types) :
        AggregatorBase(memoryPool, groupByKeySchema, node, passThroughColumns,
            input_table, output_table, agg_types, distinctAggs,
            groupByExpressions, col_types),
        m_aggregates(),
        m_groupByKeyTuple(groupByKeySchema)
    {
        m_groupByKeyTuple.
            moveNoHeader(static_cast<char*>
                         (memoryPool->
                          allocate(m_groupByKeySchema->tupleLength())));
    }

    ~Aggregator()
    {
        for (HashAggregateMapType::const_iterator iter = m_aggregates.begin();
             iter != m_aggregates.end();
             iter++)
        {
            this->purgeAggs(iter->second->m_aggregates, m_node->getAggregateOutputColumns().size());
        }
    }

    bool nextTuple(TableTuple nextTuple, TableTuple)
    {
        AggregateList *aggregateList;

        // configure a tuple and search for the required group.
        for (int i = 0; i < m_groupByExpressions.size(); i++)
        {
            m_groupByKeyTuple.setNValue(i,
                                      m_groupByExpressions[i]->eval(&nextTuple,
                                                                    NULL));
        }
        HashAggregateMapType::const_iterator keyIter =
            m_aggregates.find(m_groupByKeyTuple);

        // Group not found. Make a new entry in the hash for this new group.
        if (keyIter == m_aggregates.end())
        {
            aggregateList =
                static_cast<AggregateList*>
                (m_memoryPool->allocate(sizeof(AggregateList) +
                                        (sizeof(void*) * m_colTypes->size())));
            // Save the nextTuple in the temp table to make a deep copy
            aggregateList->m_groupTuple = m_tempInputTable->insertTupleNonVirtual(nextTuple);
            for (int i = 0; i < m_colTypes->size(); i++)
            {
                // Map aggregate index i to the corresponding output
                // column.
                aggregateList->m_aggregates[i] =
                    getAggInstance(m_memoryPool, (*m_aggTypes)[i],
                                   m_distinctAggs[i]);
            }
            m_aggregates.
                insert(HashAggregateMapType::value_type(m_groupByKeyTuple,
                                                        aggregateList));
            m_groupByKeyTuple.
                moveNoHeader(m_memoryPool->
                             allocate(m_groupByKeySchema->tupleLength()));
        }
        // otherwise, the list is the second item of the pair...
        else
        {
            aggregateList = keyIter->second;
        }

        // update the aggregation calculation.
        for (int i = 0; i < m_colTypes->size(); i++)
        {
            aggregateList->m_aggregates[i]->
                advance(m_node->getAggregateInputExpressions()[i]->
                        eval(&nextTuple, NULL));
        }

        return true;
    }

    bool finalize(TableTuple prevTuple)
    {
        for (HashAggregateMapType::const_iterator iter = m_aggregates.begin();
             iter != m_aggregates.end();
             iter++)
        {
            if (!this->insertTuple(iter->second->m_aggregates, iter->second->m_groupTuple))
            {
                return false;
            }
        }

        Agg** aggregates =
            static_cast<Agg**>(m_memoryPool->allocate(sizeof(void*)));
        return this->insertEmptyRecord(aggregates, prevTuple);
    }

private:
    HashAggregateMapType m_aggregates;
    TableTuple m_groupByKeyTuple;
};

/*
 * Specialization of an aggregator that expects the input table to be
 * sorted on the group by key.
 */
template<>
class Aggregator<PLAN_NODE_TYPE_AGGREGATE> : public AggregatorBase
{
public:
    inline Aggregator(Pool* memoryPool,
                      TupleSchema* groupByKeySchema,
                      AggregatePlanNode* node,
                      PassThroughColType *passThroughColumns,
                      Table* input_table,
                      Table* output_table,
                      std::vector<ExpressionType>* agg_types,
                      const std::vector<bool>& distinctAggs,
                      const std::vector<AbstractExpression*>& groupByExpressions,
                      std::vector<ValueType>* col_types) :
        AggregatorBase(memoryPool, groupByKeySchema, node, passThroughColumns,
            input_table, output_table, agg_types, distinctAggs,
            groupByExpressions, col_types),
        m_aggs(static_cast<Agg**>(memoryPool->allocate(sizeof(void*) *
                                                       agg_types->size())))
    {
        ::memset(m_aggs, 0, sizeof(void*) * agg_types->size());
    }

    ~Aggregator()
    {
        this->purgeAggs(m_aggs, m_aggTypes->size());
    }

    inline bool nextTuple(TableTuple nextTuple, TableTuple prevTuple)
    {
        bool startNewAgg = false;
        if (prevTuple.address() == NULL)
        {
            startNewAgg = true;
        }
        else
        {
            for (int i = 0; i < m_groupByExpressions.size(); i++)
            {
                bool cmp =
                    m_groupByExpressions[i]->eval(&nextTuple, NULL).
                    op_notEquals(m_groupByExpressions[i]->eval(&prevTuple,
                                                               NULL)).
                    isTrue();
                if (cmp)
                {
                    startNewAgg = true;
                    break;
                }
            }
        }
        if (startNewAgg)
        {
           VOLT_TRACE("new group!");
            if (!prevTuple.isNullTuple() && !this->insertTuple(m_aggs, prevTuple))
            {
                return false;
            }
            for (int i = 0; i < m_colTypes->size(); i++)
            {
                //is_ints and ret_types are all referring to all
                //output columns some of which are not aggregates.  It
                //is necessary to look up the mapping between the
                //aggregate index i and the output column associated
                //with it
                if (m_aggs[i] != NULL)
                {
                    m_aggs[i]->~Agg();
                }
                m_aggs[i] =
                    getAggInstance(m_memoryPool, (*m_aggTypes)[i],
                                   m_distinctAggs[i]);
            }
        }
        for (int i = 0; i < m_colTypes->size(); i++)
        {
            m_aggs[i]->
                advance(m_node->getAggregateInputExpressions()[i]->
                        eval(&nextTuple, NULL));
        }
        return true;
    }

    inline bool finalize(TableTuple prevTuple)
    {
        if (!prevTuple.isNullTuple() && !this->insertTuple(m_aggs, prevTuple))
        {
            return false;
        }
        return this->insertEmptyRecord(m_aggs, prevTuple);
    }

private:
    Agg** m_aggs;
};

template<PlanNodeType aggregateType>
bool
AggregateExecutor<aggregateType>::p_init(AbstractPlanNode *abstract_node,
                                         TempTableLimits* limits)
{
    AggregatePlanNode* node = dynamic_cast<AggregatePlanNode*>(abstract_node);
    assert(node);
    assert(limits);

    assert(node->getInputTables().size() == 1);
    int columnCount = (int)node->getOutputSchema().size();
    assert(columnCount >= 1);

    assert(node->getChildren()[0] != NULL);
    for (int i = 0; i < node->getAggregateInputExpressions().size(); i++)
    {
        VOLT_DEBUG("\nAGG INPUT EXPRESSIONS: %s\n",
                   node->getAggregateInputExpressions()[i]->debug().c_str());
    }

    /*
     * Find the difference between the set of aggregate output columns
     * (output columns resulting from an aggregate) and output columns.
     * Columns that are not the result of aggregates are being passed
     * through from the input table. Do this extra work here rather then
     * serialize yet more data.
     */
    std::vector<bool>
        outputColumnsResultingFromAggregates(node->getOutputSchema().size(),
                                             false);
    std::vector<int> aggregateOutputColumns =
        node->getAggregateOutputColumns();

    for (int ii = 0; ii < aggregateOutputColumns.size(); ii++)
    {
        outputColumnsResultingFromAggregates[aggregateOutputColumns[ii]] =
            true;
    }

    /*
     * Now collect the indices in the output table of the pass
     * through columns.
     */
    for (int ii = 0; ii < outputColumnsResultingFromAggregates.size();
         ii++)
    {
        if (outputColumnsResultingFromAggregates[ii] == false)
        {
            m_passThroughColumns.push_back(ii);
        }
    }

    TupleSchema* schema = node->generateTupleSchema(true);
    std::string* columnNames = new std::string[columnCount];
    for (int ctr = 0; ctr < columnCount; ctr++)
    {
        columnNames[ctr] = node->getOutputSchema()[ctr]->getColumnName();
    }
    node->setOutputTable(TableFactory::getTempTable(node->databaseId(),
                                                    "temp",
                                                    schema,
                                                    columnNames,
                                                    limits));
    delete[] columnNames;

    std::vector<ValueType> groupByColumnTypes;
    std::vector<int32_t> groupByColumnSizes;
    std::vector<bool> groupByColumnAllowNull;
    for (int ii = 0; ii < node->getGroupByExpressions().size(); ii++)
    {
        AbstractExpression* expr = node->getGroupByExpressions()[ii];
        groupByColumnTypes.push_back(expr->getValueType());
        groupByColumnSizes.push_back(expr->getValueSize());
        groupByColumnAllowNull.push_back(true);
    }
    m_groupByKeySchema =
        TupleSchema::createTupleSchema(groupByColumnTypes,
                                       groupByColumnSizes,
                                       groupByColumnAllowNull,
                                       true);

    return true;
}

//
// Assumes that the input table is sorted on the group-by keys.
//
template<PlanNodeType aggregateType>
bool AggregateExecutor<aggregateType>::p_execute(const NValueArray &params)
{
    m_memoryPool.purge();
    VOLT_DEBUG("started AGGREGATE");
    AggregatePlanNode* node = dynamic_cast<AggregatePlanNode*>(getPlanNode());
    assert(node);
    Table* output_table = node->getOutputTable();
    assert(output_table);
    Table* input_table = node->getInputTables()[0];
    assert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());

    std::vector<ExpressionType> agg_types = node->getAggregates();
    std::vector<ValueType> col_types(node->getAggregateInputExpressions().size());
    for (int i = 0; i < col_types.size(); i++)
    {
        col_types[i] =
            node->getAggregateInputExpressions()[i]->getValueType();
    }

    TableIterator it = input_table->iterator();

    const std::vector<bool> distinctAggs =
        node->getDistinctAggregates();
    std::vector<AbstractExpression*> groupByExpressions =
        node->getGroupByExpressions();
    TableTuple prev(input_table->schema());

    Aggregator<aggregateType> aggregator(&m_memoryPool, m_groupByKeySchema,
                                         node, &m_passThroughColumns,
                                         input_table, output_table,
                                         &agg_types, distinctAggs,
                                         groupByExpressions, &col_types);

    VOLT_TRACE("looping..");
    for (TableTuple cur(input_table->schema()); it.next(cur);
         prev.move(cur.address()))
    {
        if (!aggregator.nextTuple( cur, prev))
        {
            return false;
        }
    }
    VOLT_TRACE("finalizing..");
    if (!aggregator.finalize(prev))
        return false;

    VOLT_TRACE("finished");
    VOLT_TRACE("output table\n%s", output_table->debug().c_str());

    return true;
}

template<PlanNodeType aggregateType>
AggregateExecutor<aggregateType>::~AggregateExecutor()
{
    if (m_groupByKeySchema != NULL) {
        TupleSchema::freeTupleSchema(m_groupByKeySchema);
    }
}

template<PlanNodeType aggregateType>
inline
bool AggregateExecutor<aggregateType>::support_pull() const
{
    return true;
}

template<PlanNodeType aggregateType>
inline
bool AggregateExecutor<aggregateType>::parent_send_need_save_tuple_pull() const
{
    return false;
}



template<PlanNodeType aggregateType>
inline
TableTuple AggregateExecutor<aggregateType>::p_next_pull()
{
    TableTuple tuple(m_state->m_outputTableSchema);
    if (m_state->m_iterator.next(tuple))
    {
        return tuple;
    }
    else
    {
        return TableTuple (m_state->m_outputTableSchema);
    }
}

template<PlanNodeType aggregateType>
void AggregateExecutor<aggregateType>::p_pre_execute_pull(const NValueArray& params)
{
    m_memoryPool.purge();
    VOLT_DEBUG("started AGGREGATE");
    AggregatePlanNode* node = dynamic_cast<AggregatePlanNode*>(getPlanNode());
    assert(node);
    Table* output_table = node->getOutputTable();
    assert(output_table);
    Table* input_table = node->getInputTables()[0];
    assert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());

    assert(node->getChildren().size() == 1);
    AbstractExecutor* child_executor = node->getChildren()[0]->getExecutor();
    assert(child_executor);

    std::vector<ExpressionType> agg_types = node->getAggregates();
    std::vector<ValueType> col_types(node->getAggregateInputExpressions().size());
    for (int i = 0; i < col_types.size(); i++)
    {
        col_types[i] =
            node->getAggregateInputExpressions()[i]->getValueType();
    }

    const std::vector<bool> distinctAggs =
        node->getDistinctAggregates();
    std::vector<AbstractExpression*> groupByExpressions =
        node->getGroupByExpressions();
    TableTuple prev(input_table->schema());

    Aggregator<aggregateType> aggregator(&m_memoryPool, m_groupByKeySchema,
                                         node, &m_passThroughColumns,
                                         input_table, output_table,
                                         &agg_types, distinctAggs,
                                         groupByExpressions, &col_types);

    VOLT_TRACE("looping..");
    while (true)
    {
        TableTuple cur = child_executor->next_pull();
        if (cur.isNullTuple())
        {
            break;
        }
        if (!aggregator.nextTuple( cur, prev))
        {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "Aggregator failed to process tuple");
        }
        prev.move(cur.address());
    }
    VOLT_TRACE("finalizing..");
    if (!aggregator.finalize(prev))
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "Aggregator failed to finalize.");

    }

    m_state.reset(new detail::AggregateExecutorState(output_table));
}

template<PlanNodeType aggregateType>
void AggregateExecutor<aggregateType>::p_post_execute_pull()
{
    VOLT_TRACE("finished");
    VOLT_TRACE("output table\n%s", m_state->m_outputTable->debug().c_str());
}

template<PlanNodeType aggregateType>
inline
void AggregateExecutor<aggregateType>::p_reset_state_pull()
{
    m_state->m_iterator = m_state->m_outputTable->iterator();
}

}

#endif
