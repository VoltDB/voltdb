/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "executors/aggregateexecutor.h"

#include "common/ValueFactory.hpp"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/SerializableEEException.h"
#include "expressions/abstractexpression.h"
#include "plannodes/aggregatenode.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"

#include "boost/foreach.hpp"
#include "boost/unordered_map.hpp"

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

    virtual NValue finalize()
    {
        ifDistinct.clear();
        return m_value;
    }

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
        ifDistinct.clear();
        const NValue finalizeResult = m_value.op_divide(ValueFactory::getBigIntValue(m_count));
        return finalizeResult;
    }

    virtual void resetAgg()
    {
        m_haveAdvanced = false;
        m_count = 0;
    }

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
        ifDistinct.clear();
        return ValueFactory::getBigIntValue(m_count);
    }

    virtual void resetAgg()
    {
        m_haveAdvanced = false;
        m_count = 0;
    }

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

    virtual void resetAgg()
    {
        m_haveAdvanced = false;
        m_count = 0;
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

bool AggregateExecutorBase::p_init(AbstractPlanNode*, TempTableLimits* limits)
{
    AggregatePlanNode* node = dynamic_cast<AggregatePlanNode*>(m_abstractNode);
    assert(node);

    m_inputExpressions = node->getAggregateInputExpressions();
    for (int i = 0; i < m_inputExpressions.size(); i++) {
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
    m_aggregateOutputColumns = node->getAggregateOutputColumns();
    BOOST_FOREACH(int aOC, m_aggregateOutputColumns) {
        outputColumnsResultingFromAggregates[aOC] = true;
    }
    for (int ii = 0; ii < outputColumnsResultingFromAggregates.size(); ii++) {
        if (outputColumnsResultingFromAggregates[ii] == false) {
            m_passThroughColumns.push_back(ii);
        }
    }

    if (!node->isInline()) {
        setTempOutputTable(limits);
    }

    m_aggTypes = node->getAggregates();
    m_distinctAggs = node->getDistinctAggregates();
    m_groupByExpressions = node->getGroupByExpressions();
    node->collectOutputExpressions(m_outputColumnExpressions);

    // FIXME(xin):
    // Group by columns are just pass throught columns, why not right?
    // m_passThroughColumns.size() == m_groupByExpressions.size() is not true, any case?

    m_prePredicate = node->getPrePredicate();
    m_postPredicate = node->getPostPredicate();

    std::vector<ValueType> groupByColumnTypes;
    std::vector<int32_t> groupByColumnSizes;
    std::vector<bool> groupByColumnAllowNull;
    std::vector<bool> groupByColumnInBytes;
    for (int ii = 0; ii < m_groupByExpressions.size(); ii++) {
        AbstractExpression* expr = m_groupByExpressions[ii];
        groupByColumnTypes.push_back(expr->getValueType());
        groupByColumnSizes.push_back(expr->getValueSize());
        groupByColumnAllowNull.push_back(true);
        groupByColumnInBytes.push_back(expr->getInBytes());
    }
    m_groupByKeySchema = TupleSchema::createTupleSchema(groupByColumnTypes,
                                                        groupByColumnSizes,
                                                        groupByColumnAllowNull,
                                                        groupByColumnInBytes);
    return true;
}

inline void AggregateExecutorBase::executeAggBase(const NValueArray& params)
{
    m_memoryPool.purge();
    VOLT_DEBUG("started AGGREGATE");
    assert(dynamic_cast<AggregatePlanNode*>(m_abstractNode));
    assert(m_tmpOutputTable);
}

inline void AggregateExecutorBase::initGroupByKeyTuple(PoolBackedTupleStorage &nextGroupByKeyStorage,
                                                       const TableTuple& nxtTuple)
{
    TableTuple& nextGroupByKeyTuple = nextGroupByKeyStorage;
    if (nextGroupByKeyTuple.isNullTuple()) {
        nextGroupByKeyStorage.allocateActiveTuple();
    }
    // TODO: Here is where an inline projection executor could be used to initialize both a group key tuple
    // and an agg input tuple from the same raw input tuple.
    // configure a tuple
    for (int ii = 0; ii < m_groupByExpressions.size(); ii++) {
        nextGroupByKeyTuple.setNValue(ii, m_groupByExpressions[ii]->eval(&nxtTuple));
    }
}

/// Helper method responsible for inserting the results of the
/// aggregation into a new tuple in the output table as well as passing
/// through any additional columns from the input table.
inline bool AggregateExecutorBase::insertOutputTuple(AggregateRow* aggregateRow)
{
    TableTuple& tempTuple = m_tmpOutputTable->tempTuple();

    // This first pass is to add all columns that were aggregated on.
    Agg** aggs = aggregateRow->m_aggregates;
    for (int ii = 0; ii < m_aggregateOutputColumns.size(); ii++) {
        const int columnIndex = m_aggregateOutputColumns[ii];
        tempTuple.setNValue(columnIndex, aggs[ii]->finalize().castAs(tempTuple.getSchema()->columnType(columnIndex)));
    }

    VOLT_TRACE("Setting passthrough columns");
    BOOST_FOREACH(int output_col_index, m_passThroughColumns) {
        tempTuple.setNValue(output_col_index,
                         m_outputColumnExpressions[output_col_index]->eval(&(aggregateRow->m_passThroughTuple)));
    }

    bool inserted = false;
    if (m_postPredicate == NULL || m_postPredicate->eval(&tempTuple, NULL).isTrue()) {
        m_tmpOutputTable->insertTupleNonVirtual(tempTuple);
        inserted = true;
    }

    VOLT_TRACE("output_table:\n%s", m_tmpOutputTable->debug().c_str());
    return inserted;
}

inline void AggregateExecutorBase::advanceAggs(AggregateRow* aggregateRow, const TableTuple& tuple)
{
    Agg** aggs = aggregateRow->m_aggregates;
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        // In particular, COUNT(*) accepts a dummy NValue from a NULL input expression.
        AbstractExpression* inputExpr = m_inputExpressions[ii];
        aggs[ii]->advance(inputExpr ? inputExpr->eval(&tuple) : NValue());
    }
}

/*
 * Create an instance of an aggregator for the specified aggregate type.
 * The object is constructed in memory from the provided memory pool.
 */
inline void AggregateExecutorBase::initAggInstances(AggregateRow* aggregateRow)
{
    Agg** aggs = aggregateRow->m_aggregates;
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        aggs[ii] = getAggInstance(m_memoryPool, m_aggTypes[ii], m_distinctAggs[ii]);
    }
}

void AggregateHashExecutor::p_execute_init(const NValueArray& params,
        ProgressMonitorProxy* pmp, const TupleSchema * schema)
{
    VOLT_TRACE("hash aggregate executor init..");
    executeAggBase(params);
    m_pmp = pmp;

    m_nextGroupByKeyStorage.init(m_groupByKeySchema, &m_memoryPool);
    m_inputSchema = schema;
}

bool AggregateHashExecutor::p_execute(const NValueArray& params)
{
    // Input table
    Table* input_table = m_abstractNode->getInputTables()[0];
    assert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());

    const TupleSchema * inputSchema = input_table->schema();
    assert(inputSchema);
    TableIterator it = input_table->iterator();
    TableTuple nextTuple(inputSchema);

    ProgressMonitorProxy pmp(m_engine, this);
    p_execute_init(params, &pmp, inputSchema);

    VOLT_TRACE("looping..");
    while (it.next(nextTuple)) {
        p_execute_tuple(nextTuple);
    }

    p_execute_finish();
    return true;
}

void AggregateHashExecutor::p_execute_tuple(const TableTuple& nextTuple) {
    m_pmp->countdownProgress();
    initGroupByKeyTuple(m_nextGroupByKeyStorage, nextTuple);
    AggregateRow* aggregateRow;
    TableTuple& nextGroupByKeyTuple = m_nextGroupByKeyStorage;
    // Search for the matching group.
    HashAggregateMapType::const_iterator keyIter = m_hash.find(nextGroupByKeyTuple);

    // Group not found. Make a new entry in the hash for this new group.
    if (keyIter == m_hash.end()) {
        VOLT_TRACE("hash aggregate: new group..");
        aggregateRow = new (m_memoryPool, m_aggTypes.size()) AggregateRow();
        m_hash.insert(HashAggregateMapType::value_type(nextGroupByKeyTuple, aggregateRow));
        initAggInstances(aggregateRow);

        char* storage = reinterpret_cast<char*>(
                m_memoryPool.allocateZeroes(m_inputSchema->tupleLength() + TUPLE_HEADER_SIZE));
        TableTuple passThroughTupleSource = TableTuple (storage, m_inputSchema);

        aggregateRow->recordPassThroughTuple(passThroughTupleSource, nextTuple);
        // The map is referencing the current key tuple for use by the new group,
        // so force a new tuple allocation to hold the next candidate key.
        nextGroupByKeyTuple.move(NULL);
    } else {
        // otherwise, the agg row is the second item of the pair...
        aggregateRow = keyIter->second;
    }
    // update the aggregation calculation.
    advanceAggs(aggregateRow, nextTuple);
}

void AggregateHashExecutor::p_execute_finish() {
    VOLT_TRACE("finalizing..");
    for (HashAggregateMapType::const_iterator iter = m_hash.begin(); iter != m_hash.end(); iter++) {
        AggregateRow *aggregateRow = iter->second;
        if (insertOutputTuple(aggregateRow)) {
            m_pmp->countdownProgress();
        }
        delete aggregateRow;
    }

    // Clean up
    TableTuple& nextGroupByKeyTuple = m_nextGroupByKeyStorage;
    nextGroupByKeyTuple.move(NULL);

    m_hash.clear();
}

inline void AggregateSerialExecutor::getNextGroupByValues(const TableTuple& nextTuple)
{
    for (int ii = 0; ii < m_groupByExpressions.size(); ii++) {
        m_nextGroupByValues[ii] = m_groupByExpressions[ii]->eval(&nextTuple);
    }
}

void AggregateSerialExecutor::p_execute_init(const NValueArray& params,
        ProgressMonitorProxy* pmp, const TupleSchema * inputSchema) {
    executeAggBase(params);
    assert(m_prePredicate == NULL || m_abstractNode->getInputTables()[0]->activeTupleCount() <= 1);
    m_aggregateRow = new (m_memoryPool, m_aggTypes.size()) AggregateRow();

    m_noInputRows = true;
    m_failPrePredicateOnFirstRow = false;

    size_t numGroupby = m_groupByExpressions.size();
    NValue nullValue = ValueFactory::getNullValue();
    for (int i = 0; i < numGroupby; i++) {
        m_inProgressGroupByValues.push_back(nullValue);
        m_nextGroupByValues.push_back(nullValue);
    }

    m_pmp = pmp;

    char* storage = reinterpret_cast<char*>(
            m_memoryPool.allocateZeroes(inputSchema->tupleLength() + TUPLE_HEADER_SIZE));
    m_passThroughTupleSource = TableTuple (storage, inputSchema);
}

bool AggregateSerialExecutor::p_execute(const NValueArray& params)
{
    // Input table
    Table* input_table = m_abstractNode->getInputTables()[0];
    assert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());
    TableIterator it = input_table->iterator();
    TableTuple nextTuple(input_table->schema());

    ProgressMonitorProxy pmp(m_engine, this);
    p_execute_init(params, &pmp, input_table->schema());

    while (it.next(nextTuple)) {
        m_pmp->countdownProgress();
        p_execute_tuple(nextTuple);
    }
    p_execute_finish();
    VOLT_TRACE("finalizing..");

    return true;
}

void AggregateSerialExecutor::p_execute_tuple(const TableTuple& nextTuple) {
    // Use the first input tuple to "prime" the system.
    if (m_noInputRows) {
        // ENG-1565: for this special case, can have only one input row, apply the predicate here
        if (m_prePredicate == NULL || m_prePredicate->eval(&nextTuple, NULL).isTrue()) {
            getNextGroupByValues(nextTuple);
            m_inProgressGroupByValues = m_nextGroupByValues;

            // Start the aggregation calculation.
            initAggInstances(m_aggregateRow);
            m_aggregateRow->recordPassThroughTuple(m_passThroughTupleSource, nextTuple);
            advanceAggs(m_aggregateRow, nextTuple);
        } else {
            m_failPrePredicateOnFirstRow = true;
        }
        m_noInputRows = false;
        return;
    }

    getNextGroupByValues(nextTuple);
    for (int ii = m_groupByKeySchema->columnCount() - 1; ii >= 0; --ii) {
        if (m_nextGroupByValues.at(ii).compare(m_inProgressGroupByValues.at(ii)) != 0) {
            VOLT_TRACE("new group!");
            // Output old row.
            if (insertOutputTuple(m_aggregateRow)) {
                m_pmp->countdownProgress();
            }
            m_aggregateRow->resetAggs();

            // swap inProgressGroupByValues
            m_inProgressGroupByValues = m_nextGroupByValues;

            // record the new group scanned tuple
            m_aggregateRow->recordPassThroughTuple(m_passThroughTupleSource, nextTuple);
            break;
        }
    }

    // update the aggregation calculation.
    advanceAggs(m_aggregateRow, nextTuple);
}

void AggregateSerialExecutor::p_execute_finish()
{
    if (m_noInputRows || m_failPrePredicateOnFirstRow) {
        VOLT_TRACE("finalizing after no input rows..");
        // No input rows means either no group rows (when grouping) or an empty table row (otherwise).
        // Note the difference between these two cases:
        //   SELECT SUM(A) FROM BBB,            when BBB has no tuple, produces one output row.
        //   SELECT SUM(A) FROM BBB GROUP BY C, when BBB has no tuple, produces no output row.
        if (m_groupByKeySchema->columnCount() == 0) {
            VOLT_TRACE("no input row, but output an empty result row for the whole table.");
            initAggInstances(m_aggregateRow);
            if (insertOutputTuple(m_aggregateRow)) {
                m_pmp->countdownProgress();
            }
        }
    } else {
        // There's one last group (or table) row in progress that needs to be output.
        if (insertOutputTuple(m_aggregateRow)) {
            m_pmp->countdownProgress();
        }
    }

    // clean up the member variables
    delete m_aggregateRow;
    m_nextGroupByValues.clear();
    m_inProgressGroupByValues.clear();
}

}
