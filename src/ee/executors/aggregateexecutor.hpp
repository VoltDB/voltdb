/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

#include "executors/abstractexecutor.h"

#include "common/Pool.hpp"
#include "common/ValueFactory.hpp"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/SerializableEEException.h"
#include "expressions/abstractexpression.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/projectionnode.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"

#include "boost/foreach.hpp"
#include "boost/unordered_map.hpp"

#include "expressions/expressionutil.h"

#include <algorithm>
#include <limits>
#include <set>
#include <stdint.h>
#include <utility>

namespace voltdb {
/*
 * Type of the hash set used to check for column aggregate distinctness
 */
typedef boost::unordered_set<NValue,
                             NValue::hash,
                             NValue::equal_to> AggregateNValueSetType;

/**
 * Mix-in class to tweak some Aggs' behavior when the DISTINCT flag was specified,
 * It tracks and de-dupes repeated input values.
 * It is specified as a parameter class that determines the type of the ifDistinct data member.
 */
struct Distinct : public AggregateNValueSetType {
    bool excludeValue(const NValue& val)
    {
        // find this value in the set.  If it doesn't exist, add
        // it, otherwise indicate it shouldn't be included in the
        // aggregate
        iterator setval = find(val);
        if (setval == end())
        {
            insert(val);
            return false; // Include value just this once.
        }
        return true; // Never again this value;
    }
};

/**
 * Mix-in class to tweak some Aggs' behavior when the DISTINCT flag was NOT specified,
 * It "does nothing", by-passing the tracking and de-duping of repeated input values.
 * It is specified as a parameter class that determines the type of the ifDistinct data member.
 */
struct NotDistinct {
    void clear() { }
    bool excludeValue(const NValue& val)
    {
        return false; // Include value any number of times
    }
};

/*
 * Base class for an individual aggregate that aggregates a specific
 * column for a group
 */
class Agg
{
public:
    void* operator new(size_t size, Pool& memoryPool) { return memoryPool.allocate(size); }
    void operator delete(void*, Pool& memoryPool) { /* NOOP -- on alloc error unroll nothing */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    Agg() : m_haveAdvanced(false)
    {
        m_value.setNull();
    }
    virtual ~Agg()
    {
        /* do nothing */
    }
    virtual void advance(const NValue& val) = 0;
    virtual NValue finalize() { return m_value; }
    virtual void purgeAgg() {};

protected:
    bool m_haveAdvanced;
    NValue m_value;
};

// Parameter D is either Distinct of NotDistinct.
template<class D>
class SumAgg : public Agg
{
  public:
    SumAgg() {}

    virtual void advance(const NValue& val)
    {
        if (val.isNull() || ifDistinct.excludeValue(val)) {
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

    virtual void purgeAgg() { ifDistinct.clear(); }

private:
    D ifDistinct;
};


// Parameter D is either Distinct of NotDistinct.
template<class D>
class AvgAgg : public Agg
{
public:
    AvgAgg() : m_count(0) {}

    virtual void advance(const NValue& val)
    {
        if (val.isNull() || ifDistinct.excludeValue(val)) {
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

    virtual NValue finalize()
    {
        if (m_count == 0)
        {
            return ValueFactory::getNullValue();
        }
        const NValue finalizeResult =
            m_value.op_divide(ValueFactory::getBigIntValue(m_count));
        return finalizeResult;
    }

    virtual void purgeAgg() { ifDistinct.clear(); }

private:
    D ifDistinct;
    int64_t m_count;
};

//count always holds integer
// Parameter D is either Distinct of NotDistinct.
template<class D>
class CountAgg : public Agg
{
public:
    CountAgg() : m_count(0) {}

    virtual void advance(const NValue& val)
    {
        if (val.isNull() || ifDistinct.excludeValue(val)) {
            return;
        }
        m_count++;
    }

    virtual NValue finalize()
    {
        return ValueFactory::getBigIntValue(m_count);
    }

    virtual void purgeAgg() { ifDistinct.clear(); }

private:
    D ifDistinct;
    int64_t m_count;
};

class CountStarAgg : public Agg
{
public:
    CountStarAgg() : m_count(0) {}

    virtual void advance(const NValue& val)
    {
        ++m_count;
    }

    virtual NValue finalize()
    {
        return ValueFactory::getBigIntValue(m_count);
    }

private:
    int64_t m_count;
};

class MaxAgg : public Agg
{
public:
    MaxAgg() {}

    virtual void advance(const NValue& val)
    {
        if (val.isNull())
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
};

class MinAgg : public Agg
{
public:
    MinAgg() { }

    virtual void advance(const NValue& val)
    {
        if (val.isNull())
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
};

/*
 * Create an instance of an aggregator for the specified aggregate type and "distinct" flag.
 * The object is allocated from the provided memory pool.
 */
inline Agg* getAggInstance(Pool& memoryPool, ExpressionType agg_type, bool isDistinct)
{
    switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
        return new (memoryPool) CountStarAgg();
    case EXPRESSION_TYPE_AGGREGATE_MIN:
        return new (memoryPool) MinAgg();
    case EXPRESSION_TYPE_AGGREGATE_MAX  :
        return new (memoryPool) MaxAgg();
    case EXPRESSION_TYPE_AGGREGATE_COUNT:
        if (isDistinct) {
            return new (memoryPool) CountAgg<Distinct>();
        }
        return new (memoryPool) CountAgg<NotDistinct>();
    case EXPRESSION_TYPE_AGGREGATE_SUM:
        if (isDistinct) {
            return new (memoryPool) SumAgg<Distinct>();
        }
        return new (memoryPool) SumAgg<NotDistinct>();
    case EXPRESSION_TYPE_AGGREGATE_AVG:
        if (isDistinct) {
            return new (memoryPool) AvgAgg<Distinct>();
        }
        return new (memoryPool) AvgAgg<NotDistinct>();
    default:
    {
        char message[128];
        snprintf(message, sizeof(message), "Unknown aggregate type %d", agg_type);
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
    }
}


/**
 * A list of aggregates for a specific group.
 */
struct AggregateList
{
    void* operator new(size_t size, Pool& memoryPool, size_t nAggs)
    { return memoryPool.allocate(size + (sizeof(void*) * nAggs)); }
    void operator delete(void*, Pool& memoryPool, size_t nAggs) { /* NOOP -- on alloc error unroll */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    // A tuple from the group of tuples being aggregated. Source of
    // pass through columns.
    TableTuple m_passThroughTuple;

    // The aggregates for each column for this group
    Agg* m_aggregates[0];
};


/**
 * The base class for aggregate executors regardless of the type of grouping that should be performed.
 */
class AggregateExecutorBase : public AbstractExecutor
{
public:
    AggregateExecutorBase(VoltDBEngine* engine, AbstractPlanNode* abstract_node) :
        AbstractExecutor(engine, abstract_node), m_groupByKeySchema(NULL)
    { }
    ~AggregateExecutorBase()
    {
        if (m_groupByKeySchema != NULL) {
            TupleSchema::freeTupleSchema(m_groupByKeySchema);
        }
    }

protected:
    virtual bool p_init(voltdb::AbstractPlanNode*, voltdb::TempTableLimits* limits)
    {
        AggregatePlanNode* node = dynamic_cast<AggregatePlanNode*>(m_abstractNode);
        assert(node);
        assert(node->getChildren().size() == 1);
        assert(node->getChildren()[0] != NULL);

        m_inputExpressions = node->getAggregateInputExpressions();
        for (int i = 0; i < m_inputExpressions.size(); i++)
        {
            VOLT_DEBUG("\nAGG INPUT EXPRESSION: %s\n",
                       m_inputExpressions[i] ? m_inputExpressions[i]->debug().c_str() : "null");
        }

        /*
         * Find the difference between the set of aggregate output columns
         * (output columns resulting from an aggregate) and output columns.
         * Columns that are not the result of aggregates are being passed
         * through from the input table. Do this extra work here rather then
         * serialize yet more data.
         */
        std::vector<bool> outputColumnsResultingFromAggregates(node->getOutputSchema().size(), false);
        std::vector<int> aggregateOutputColumns = node->getAggregateOutputColumns();
        BOOST_FOREACH(int aOC, aggregateOutputColumns) {
            outputColumnsResultingFromAggregates[aOC] = true;
        }

        /*
         * Now collect the indices in the output table of the pass
         * through columns.
         */
        for (int ii = 0; ii < outputColumnsResultingFromAggregates.size(); ii++) {
            if (outputColumnsResultingFromAggregates[ii] == false) {
                m_passThroughColumns.push_back(ii);
            }
        }

        setTempOutputTable(limits);

        m_aggTypes = node->getAggregates();
        m_distinctAggs = node->getDistinctAggregates();
        m_groupByExpressions = node->getGroupByExpressions();
        node->collectOutputExpressions(m_outputColumnExpressions);
        m_aggregateOutputColumns = node->getAggregateOutputColumns();

        std::vector<ValueType> groupByColumnTypes;
        std::vector<int32_t> groupByColumnSizes;
        std::vector<bool> groupByColumnAllowNull;
        for (int ii = 0; ii < m_groupByExpressions.size(); ii++)
        {
            AbstractExpression* expr = m_groupByExpressions[ii];
            groupByColumnTypes.push_back(expr->getValueType());
            groupByColumnSizes.push_back(expr->getValueSize());
            groupByColumnAllowNull.push_back(true);
        }
        m_groupByKeySchema = TupleSchema::createTupleSchema(groupByColumnTypes,
                                                            groupByColumnSizes,
                                                            groupByColumnAllowNull,
                                                            true);
        return true;
    }

    virtual void executeAggBase(const NValueArray& params)
    {
        m_memoryPool.purge();
        VOLT_DEBUG("started AGGREGATE");
        assert(dynamic_cast<AggregatePlanNode*>(m_abstractNode));
        assert(m_tmpOutputTable);

        // substitute params
        BOOST_FOREACH(AbstractExpression* inputExpression, m_inputExpressions) {
            if (inputExpression) {
                inputExpression->substitute(params);
            }
        }
        BOOST_FOREACH(AbstractExpression* groupByExpression, m_groupByExpressions) {
            groupByExpression->substitute(params);
        }
        BOOST_FOREACH(AbstractExpression* outputColumnExpression, m_outputColumnExpressions) {
            outputColumnExpression->substitute(params);
        }
        m_groupByKeyTuple.allocateTupleNoHeader(m_groupByKeySchema, &m_memoryPool);
    }

    void initGroupByKeyTuple(const TableTuple& nxtTuple)
    {
        // TODO: Here is where an inline projection executor could be used to initialize both a group key tuple
        // and an agg input tuple from the same raw input tuple.
        // configure a tuple
        for (int ii = 0; ii < m_groupByExpressions.size(); ii++) {
            m_groupByKeyTuple.setNValue(ii, m_groupByExpressions[ii]->eval(&nxtTuple));
        }
    }

    /// Helper method responsible for inserting the results of the
    /// aggregation into a new tuple in the output table as well as passing
    /// through any additional columns from the input table.
    void insertOutputTuple(Agg** aggs, const TableTuple &passThroughTuple)
    {
        TempTable* output_table = m_tmpOutputTable;
        TableTuple& tmptup = output_table->tempTuple();

        /*
         * This first pass is to add all columns that were aggregated on.
         */
        for (int ii = 0; ii < m_aggregateOutputColumns.size(); ii++) {
            const int columnIndex = m_aggregateOutputColumns[ii];
            const ValueType columnType = tmptup.getType(columnIndex);
            tmptup.setNValue(columnIndex, aggs[ii]->finalize().castAs(columnType));
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
        BOOST_FOREACH(int output_col_index,m_passThroughColumns) {
            tmptup.setNValue(output_col_index, m_outputColumnExpressions[output_col_index]->eval(&passThroughTuple, NULL));
        }

        output_table->insertTupleNonVirtual(tmptup);
        purgeRowOfAggs(aggs);
    }

    void advanceAggs(Agg** aggs, const TableTuple& nxtTuple)
    {
        for (int ii = 0; ii < m_aggTypes.size(); ii++) {
            // In particular, COUNT(*) accepts a dummy NValue from a NULL input expression.
            AbstractExpression* inputExpr = m_inputExpressions[ii];
            aggs[ii]->advance(inputExpr ? inputExpr->eval(&nxtTuple) : NValue());
        }
    }

    /*
     * Create an instance of an aggregator for the specified aggregate type.
     * The object is constructed in memory from the provided memory pool.
     */
    void initAggInstances(Agg** aggs)
    {
        for (int ii = 0; ii < m_aggTypes.size(); ii++) {
            aggs[ii] = getAggInstance(m_memoryPool, m_aggTypes[ii], m_distinctAggs[ii]);
        }
    }

    void purgeRowOfAggs(Agg** aggs)
    {
        for (int ii = 0; ii < m_aggTypes.size(); ii++) {
            aggs[ii]->purgeAgg();
        }
    }

    /*
     * List of columns in the output schema that are passing through
     * the value from a column in the input table and not doing any
     * aggregation.
     */
    std::vector<int> m_passThroughColumns;
    Pool m_memoryPool;
    TupleSchema* m_groupByKeySchema;
    TupleSchema* m_aggSchema;
    std::vector<ExpressionType> m_aggTypes;
    std::vector<bool> m_distinctAggs;
    std::vector<AbstractExpression*> m_groupByExpressions;
    std::vector<AbstractExpression*> m_inputExpressions;
    std::vector<AbstractExpression*> m_outputColumnExpressions;
    std::vector<int> m_aggregateOutputColumns;
    PoolBackedTempTuple m_groupByKeyTuple;

    // Inline Projection
    ProjectionPlanNode* m_projectionNode;
    AbstractExpression** m_projectionExpressions;
    bool* m_needsSubstituteProject;
    int* m_projectionAllTupleArray;

    // arrange the memory mgmt aids at the bottom to try to maximize
    // cache hits (by keeping them out of the way of useful runtime data)
    boost::shared_array<bool> m_needsSubstituteProjectPtr;
    boost::shared_array<int> m_projectionAllTupleArrayPtr;
};


/**
 * The concrete executor class for PLAN_NODE_TYPE_HASHAGGREGATE
 * in which the input does not need to be sorted and execution will hash the group by key to aggregate the tuples.
 */
class AggregateHashExecutor : public AggregateExecutorBase
{
public:
    AggregateHashExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node) :
        AggregateExecutorBase(engine, abstract_node) { }
    ~AggregateHashExecutor() { }

private:
    virtual bool p_execute(const NValueArray& params);

    /*
     * Type of the hash table used to store aggregate lists for each group.
     */
    typedef boost::unordered_map<TableTuple,
                                 AggregateList*,
                                 TableTupleHasher,
                                 TableTupleEqualityChecker> HashAggregateMapType;
    HashAggregateMapType m_hash;
};

bool AggregateHashExecutor::p_execute(const NValueArray& params)
{
    executeAggBase(params);

    m_groupByKeyTuple.allocateTupleNoHeader(m_groupByKeySchema, &m_memoryPool);
    m_hash.clear();

    VOLT_TRACE("looping..");
    Table* input_table = m_abstractNode->getInputTables()[0];
    assert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());
    TableIterator it = input_table->iterator();
    TableTuple nxtTuple(input_table->schema());
    while (it.next(nxtTuple)) {
        initGroupByKeyTuple(nxtTuple);
        AggregateList *aggregateList;
        // Search for the matching group.
        HashAggregateMapType::const_iterator keyIter = m_hash.find(m_groupByKeyTuple);

        // Group not found. Make a new entry in the hash for this new group.
        if (keyIter == m_hash.end()) {
            aggregateList = new (m_memoryPool, m_aggTypes.size()) AggregateList();
            aggregateList->m_passThroughTuple = nxtTuple;
            initAggInstances(aggregateList->m_aggregates);
            m_hash.insert(HashAggregateMapType::value_type(m_groupByKeyTuple, aggregateList));
            // The map is referencing the current key tuple for use by the new group,
            // so allocate a new tuple to hold the next candidate key
            m_groupByKeyTuple.reallocateTupleNoHeader();
        } else {
            // otherwise, the list is the second item of the pair...
            aggregateList = keyIter->second;
        }
        // update the aggregation calculation.
        advanceAggs(aggregateList->m_aggregates, nxtTuple);
    }

    VOLT_TRACE("finalizing..");
    for (HashAggregateMapType::const_iterator iter = m_hash.begin(); iter != m_hash.end(); iter++) {
        insertOutputTuple(iter->second->m_aggregates, iter->second->m_passThroughTuple);
    }
    m_hash.clear();
    return true;
}


/**
 * The concrete executor class for PLAN_NODE_TYPE_AGGREGATE
 * a constant space aggregation that expects the input table to be sorted on the group by key
 * at least to the extent that rows with equal keys arrive sequentially (not interspersed with other key values).
 */
class AggregateSerialExecutor : public AggregateExecutorBase
{
public:
    AggregateSerialExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node) :
        AggregateExecutorBase(engine, abstract_node) { }
    ~AggregateSerialExecutor() { }

private:
    virtual bool p_execute(const NValueArray& params);
    /** Serial aggregates need only one row of Aggs and the "previous" group key tuple that defines
     * their associated group keys -- so group transitions can be detected.
     * In the case of table aggregates that have no grouping keys, the previous tuple has no effect and is tracked
     * for nothing.
     * TODO: A separate instantiation for that case could be made much simpler/faster. */
    Agg** m_aggs;
    TableTuple m_prevGroupByKeyTuple;
    TableTuple m_prevPassThroughTuple;
};

bool AggregateSerialExecutor::p_execute(const NValueArray& params)
{
    executeAggBase(params);

    m_aggs = static_cast<Agg**>(m_memoryPool.allocateZeroes(sizeof(void*) * m_aggTypes.size()));
    initAggInstances(m_aggs);
    m_prevGroupByKeyTuple = TableTuple(m_groupByKeySchema);

    VOLT_TRACE("looping..");
    Table* input_table = m_abstractNode->getInputTables()[0];
    assert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());
    TableIterator it = input_table->iterator();
    TableTuple nxtTuple(input_table->schema());
    m_prevPassThroughTuple = nxtTuple;
    while (it.next(nxtTuple)) {
        initGroupByKeyTuple(nxtTuple);
        bool startNewAgg = false;
        if (m_prevGroupByKeyTuple.isNullTuple()) {
            startNewAgg = true;
        } else {
            // Test for repetition of equal GROUP BY keys.
            // Testing keys from last to first might be faster --
            // if the GROUP BY keys are listed in major-to-minor sort order,
            // the last one will be the most likely to have changed.
            for (int ii = m_groupByKeySchema->columnCount() - 1; ii >= 0; --ii) {
                startNewAgg = m_groupByKeyTuple.getNValue(ii).op_notEquals(m_prevGroupByKeyTuple.getNValue(ii)).isTrue();
                if (startNewAgg) {
                    break;
                }
            }
        }
        if (startNewAgg) {
            VOLT_TRACE("new group!");
            if ( ! m_prevGroupByKeyTuple.isNullTuple()) {
                insertOutputTuple(m_aggs, m_prevPassThroughTuple);
            }
            initAggInstances(m_aggs);
            m_prevPassThroughTuple.move(nxtTuple.address());
            m_prevGroupByKeyTuple.move(m_groupByKeyTuple.address());
        }
        // update the aggregation calculation.
        advanceAggs(m_aggs, nxtTuple);
    }

    VOLT_TRACE("finalizing..");
    // if no record exists in input_table, we have to output one record
    // only when it doesn't have a GROUP BY. See difference of these cases:
    //   SELECT SUM(A) FROM BBB,            when BBB has no tuple, produces one output row.
    //   SELECT SUM(A) FROM BBB GROUP BY C, when BBB has no tuple, produces no output row.
    if (m_prevGroupByKeyTuple.isNullTuple()) {
        if (m_groupByKeySchema->columnCount() != 0) {
            // Tear down the aggs pre-built for the first group -- they weren't needed after all.
            purgeRowOfAggs(m_aggs);
            return true;
        }
        VOLT_TRACE("no record. outputting a NULL row..");
    }
    insertOutputTuple(m_aggs, m_prevPassThroughTuple);
    return true;
}

}

#endif
