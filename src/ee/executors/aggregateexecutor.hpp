/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
        AbstractExecutor(engine, abstract_node), m_groupByKeySchema(NULL)
    { };
    ~AggregateExecutor();

protected:
    bool p_init(AbstractPlanNode* abstract_node,
                TempTableLimits* limits);
    bool p_execute(const NValueArray &params);

    /*
     * List of columns in the output schema that are passing through
     * the value from a column in the input table and not doing any
     * aggregation.
     */
    PassThroughColType m_passThroughColumns;
    Pool m_memoryPool;
    TupleSchema* m_groupByKeySchema;
};

/*
 * Interface for an aggregator (not an an individual aggregate) that
 * will aggregate some number of tuples and produce the results in the
 * provided output table.
 */
template<PlanNodeType aggregateType>
class Aggregator {
public:
    Aggregator(Pool* memoryPool,
               TupleSchema* groupByKeySchema,
               AggregatePlanNode* node,
               PassThroughColType* passThroughColumns,
               Table* input_table,
               Table* output_table,
               std::vector<ExpressionType>* agg_types,
               const std::vector<AbstractExpression*>& groupByExpressions,
               std::vector<ValueType>* col_types);
    bool nextTuple(TableTuple nextTuple, TableTuple prevTuple);
    bool finalize(TableTuple prevTuple);
    void purgeAggs();
};

/*
 * Helper method responsible for inserting the results of the
 * aggregation into a new tuple in the output table as well as passing
 * through any additional columns from the input table.
 */
inline bool
helper(AggregatePlanNode* node, Agg** aggs,
       Table* output_table, Table* input_table, TableTuple prev,
       PassThroughColType* passThroughColumns)
{
    TableTuple& tmptup = output_table->tempTuple();

    /*
     * This first pass is to add all columns that were aggregated on.
     */
    std::vector<ExpressionType> aggregateTypes = node->getAggregates();
    std::vector<int> aggregateOutputColumns = node->getAggregateOutputColumns();
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
    for (int i = 0; i < passThroughColumns->size(); i++)
    {
        int output_col_index = (*passThroughColumns)[i];
        tmptup.setNValue(output_col_index,
                         node->getOutputSchema()[output_col_index]->
                         getExpression()->eval(&prev, NULL));
    }

    if (!output_table->insertTuple(tmptup)) {
        VOLT_ERROR("Failed to insert order-by tuple from input table '%s' into"
                   " output table '%s'",
                   input_table->name().c_str(), output_table->name().c_str());
        return false;
    }
    return true;
}

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
 * Specialization of an Aggregator that uses a hash map to aggregate
 * tuples from the input table.
 */
template<>
class Aggregator<PLAN_NODE_TYPE_HASHAGGREGATE>
{
public:
    inline Aggregator(Pool *memoryPool,
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
          groupByKeyTuple(groupByKeySchema)
    {
        groupByKeyTuple.
            moveNoHeader(static_cast<char*>
                         (memoryPool->
                          allocate(groupByKeySchema->tupleLength())));
    }

    ~Aggregator()
    {
        purgeAggs();
    }

    inline bool nextTuple(TableTuple nextTuple, TableTuple)
    {
        AggregateList *aggregateList;

        // configure a tuple and search for the required group.
        for (int i = 0; i < m_groupByExpressions.size(); i++)
        {
            groupByKeyTuple.setNValue(i,
                                      m_groupByExpressions[i]->eval(&nextTuple,
                                                                    NULL));
        }
        HashAggregateMapType::const_iterator keyIter =
            m_aggregates.find(groupByKeyTuple);

        // Group not found. Make a new entry in the hash for this new group.
        if (keyIter == m_aggregates.end())
        {
            aggregateList =
                static_cast<AggregateList*>
                (m_memoryPool->allocate(sizeof(AggregateList) +
                                        (sizeof(void*) * m_colTypes->size())));
            aggregateList->m_groupTuple = nextTuple;
            for (int i = 0; i < m_colTypes->size(); i++)
            {
                // Map aggregate index i to the corresponding output
                // column.
                aggregateList->m_aggregates[i] =
                    getAggInstance(m_memoryPool, (*m_aggTypes)[i],
                                   m_distinctAggs[i]);
            }
            m_aggregates.
                insert(HashAggregateMapType::value_type(groupByKeyTuple,
                                                        aggregateList));
            groupByKeyTuple.
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

    inline bool finalize(TableTuple prevTuple)
    {
        for (HashAggregateMapType::const_iterator iter = m_aggregates.begin();
             iter != m_aggregates.end();
             iter++)
        {
            if (!helper(m_node, iter->second->m_aggregates, m_outputTable,
                        m_inputTable, iter->second->m_groupTuple,
                        m_passThroughColumns))
            {
                return false;
            }
        }

        // if no record exists in input_table, we have to output one record
        // only when it doesn't have GROUP BY. See difference of these cases:
        //   SELECT SUM(A) FROM BBB ,   when BBB has no tuple
        //   SELECT SUM(A) FROM BBB GROUP BY C,   when BBB has no tuple
        if (m_groupByExpressions.size() == 0 &&
            m_outputTable->activeTupleCount() == 0)
        {
            VOLT_TRACE("no record. outputting a NULL row..");
            Agg** aggregates =
                static_cast<Agg**>(m_memoryPool->allocate(sizeof(void*)));
            for (int i = 0; i < m_colTypes->size(); i++)
            {
                // It is necessary to look up the mapping between the
                // aggregate index i and the associated output column
                aggregates[i] =
                    getAggInstance(m_memoryPool,
                                   (*m_aggTypes)[i],
                                   m_distinctAggs[i]);
            }
            if (!helper(m_node, aggregates, m_outputTable, m_inputTable,
                        prevTuple, m_passThroughColumns))
            {
                return false;
            }
        }
        return true;
    }

    void purgeAggs()
    {
        for (HashAggregateMapType::const_iterator iter = m_aggregates.begin();
             iter != m_aggregates.end();
             iter++)
        {
            Agg** aggs = iter->second->m_aggregates;
            int num_aggs = static_cast<int>(m_node->getAggregateOutputColumns().size());
            for (int ii = 0; ii < num_aggs; ii++)
            {
                if (aggs[ii] != NULL)
                {
                    aggs[ii]->~Agg();
                }
            }
        }
    }

private:
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
    HashAggregateMapType m_aggregates;
    TableTuple groupByKeyTuple;
};

/*
 * Specialization of an aggregator that expects the input table to be
 * sorted on the group by key.
 */
template<>
class Aggregator<PLAN_NODE_TYPE_AGGREGATE>
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
        m_memoryPool(memoryPool),
        m_groupByKeySchema(groupByKeySchema),
        m_node(node),
        m_passThroughColumns(passThroughColumns),
        m_inputTable(input_table),
        m_outputTable(output_table),
        m_aggTypes(agg_types),
        m_distinctAggs(distinctAggs),
        m_groupByExpressions(groupByExpressions),
        m_colTypes(col_types),
        m_aggs(static_cast<Agg**>(memoryPool->allocate(sizeof(void*) *
                                                       agg_types->size())))
    {
        ::memset(m_aggs, 0, sizeof(void*) * agg_types->size());
    }

    ~Aggregator()
    {
        purgeAggs();
    }

    inline bool nextTuple(TableTuple nextTuple, TableTuple prevTuple)
    {
        bool m_startNewAgg = false;
        if (prevTuple.address() == NULL)
        {
            m_startNewAgg = true;
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
                    m_startNewAgg = true;
                    break;
                }
            }
        }
        if (m_startNewAgg)
        {
            VOLT_TRACE("new group!");
            if (!prevTuple.isNullTuple() &&
                !helper(m_node, m_aggs, m_outputTable, m_inputTable,
                        prevTuple, m_passThroughColumns))
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
        if (!prevTuple.isNullTuple() &&
            !helper(m_node, m_aggs, m_outputTable, m_inputTable,
                    prevTuple, m_passThroughColumns))
        {
            return false;
        }

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
                //is_ints and ret_types are all referring to all
                //output columns some of which are not aggregates.  It
                //is necessary to look up the mapping between the
                //aggregate index i and the output column associated
                //with it
                m_aggs[i] =
                    getAggInstance(m_memoryPool, (*m_aggTypes)[i],
                                   m_distinctAggs[i]);
            }
            if (!helper(m_node, m_aggs, m_outputTable, m_inputTable,
                        prevTuple, m_passThroughColumns))
            {
                return false;
            }
        }
        return true;
    }

    void purgeAggs()
    {
        for (int ii = 0; ii < m_aggTypes->size(); ii++)
        {
            if (m_aggs[ii] != NULL)
            {
                m_aggs[ii]->~Agg();
            }
        }
    }

private:
    Pool* m_memoryPool;
    TupleSchema* m_groupByKeySchema;
    AggregatePlanNode* m_node;
    PassThroughColType* m_passThroughColumns;
    Table* m_inputTable;
    Table* m_outputTable;
    std::vector<ExpressionType>* m_aggTypes;
    const std::vector<bool>& m_distinctAggs;
    const std::vector<AbstractExpression*>& m_groupByExpressions;
    std::vector<ValueType>* m_colTypes;
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
    AggregatePlanNode* node = dynamic_cast<AggregatePlanNode*>(m_abstractNode);
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
}

#endif
