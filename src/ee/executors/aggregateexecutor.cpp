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


AggregateRow* AggregateExecutorBase::allocateAggregateRow() {
    AggregateRow *aggregateRow = new (m_memoryPool, m_aggTypes.size()) AggregateRow();
    aggregateRow->m_filled = false;

    return aggregateRow;
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

    setTempOutputTable(limits);

    m_aggTypes = node->getAggregates();
    m_distinctAggs = node->getDistinctAggregates();
    m_groupByExpressions = node->getGroupByExpressions();
    node->collectOutputExpressions(m_outputColumnExpressions);
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
    if (m_prePredicate != NULL && m_prePredicate->hasParameter()) {
        m_prePredicate->substitute(params);
    }
    if (m_postPredicate != NULL && m_postPredicate->hasParameter()) {
        m_postPredicate->substitute(params);
    }
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

inline std::vector<NValue> AggregateExecutorBase::getNextGroupByValues(const TableTuple& nextTuple)
{
    std::vector<NValue> nextGroupByValues;
    for (int ii = 0; ii < m_groupByExpressions.size(); ii++) {
        nextGroupByValues.push_back(m_groupByExpressions[ii]->eval(&nextTuple));
    }
    return nextGroupByValues;
}

/// Helper method responsible for inserting the results of the
/// aggregation into a new tuple in the output table as well as passing
/// through any additional columns from the input table.
inline bool AggregateExecutorBase::insertOutputTuple(AggregateRow* aggregateRow)
{
    TempTable* output_table = m_tmpOutputTable;
    TableTuple& tmptup = output_table->tempTuple();
    // This first pass is to add all columns that were aggregated on.
    Agg** aggs = aggregateRow->m_aggregates;
    for (int ii = 0; ii < m_aggregateOutputColumns.size(); ii++) {
        const int columnIndex = m_aggregateOutputColumns[ii];
        tmptup.setNValue(columnIndex, aggs[ii]->finalize().castAs(tmptup.getSchema()->columnType(columnIndex)));
    }
    VOLT_TRACE("Setting passthrough columns");
    for (int ii = 0; ii < m_passThroughColumns.size(); ii++) {
        NValue val = aggregateRow->m_passThroughValues.at(ii);
        int colIndex = m_passThroughColumns[ii];
        tmptup.setNValue(colIndex, val);
        VOLT_TRACE("Passthrough columns: %d", colIndex);
    }

    bool inserted = false;
    if (m_postPredicate == NULL || m_postPredicate->eval(&tmptup, NULL).isTrue()) {
        output_table->insertTupleNonVirtual(tmptup);
        inserted = true;
    }

    VOLT_TRACE("output_table:\n%s", output_table->debug().c_str());
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

typedef boost::unordered_map<TableTuple,
                             AggregateRow*,
                             TableTupleHasher,
                             TableTupleEqualityChecker> HashAggregateMapType;

bool AggregateHashExecutor::p_execute(const NValueArray& params)
{
    executeAggBase(params);
    ProgressMonitorProxy pmp(m_engine, this);

    VOLT_TRACE("looping..");
    HashAggregateMapType hash;
    PoolBackedTupleStorage m_nextGroupByKeyStorage(m_groupByKeySchema, &m_memoryPool);
    TableTuple& nextGroupByKeyTuple = m_nextGroupByKeyStorage;

    // Input table
    Table* input_table = m_abstractNode->getInputTables()[0];
    assert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());

    TableIterator it = input_table->iterator();
    TableTuple nextTuple(input_table->schema());

    while (it.next(nextTuple)) {
        pmp.countdownProgress();
        initGroupByKeyTuple(m_nextGroupByKeyStorage, nextTuple);
        AggregateRow *aggregateRow;
        // Search for the matching group.
        HashAggregateMapType::const_iterator keyIter = hash.find(nextGroupByKeyTuple);

        // Group not found. Make a new entry in the hash for this new group.
        if (keyIter == hash.end()) {
            aggregateRow = new (m_memoryPool, m_aggTypes.size()) AggregateRow();
            aggregateRow->m_filled = false;
            hash.insert(HashAggregateMapType::value_type(nextGroupByKeyTuple, aggregateRow));
            initAggInstances(aggregateRow);
            aggregateRow->recordPassThroughTuple(nextTuple, m_passThroughColumns, m_outputColumnExpressions);
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

    VOLT_TRACE("finalizing..");
    for (HashAggregateMapType::const_iterator iter = hash.begin(); iter != hash.end(); iter++) {
        AggregateRow *aggregateRow = iter->second;
        if (insertOutputTuple(aggregateRow)) {
            pmp.countdownProgress();
        }
        delete aggregateRow;
    }

    return true;
}

bool AggregateSerialExecutor::p_execute(const NValueArray& params)
{
    executeAggBase(params);
    assert(m_prePredicate == NULL || m_abstractNode->getInputTables()[0]->activeTupleCount() <= 1);

    // Input table
    Table* input_table = m_abstractNode->getInputTables()[0];
    assert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());
    TableIterator it = input_table->iterator();
    TableTuple nextTuple(input_table->schema());

    AggregateRow *aggregateRow = new (m_memoryPool, m_aggTypes.size()) AggregateRow();
    aggregateRow->m_filled = false;
    boost::scoped_ptr<AggregateRow> will_finally_delete_aggregate_row(aggregateRow);
    AggSerialInfo info;
    ProgressMonitorProxy pmp(m_engine, this);

    while (it.next(nextTuple)) {
        pmp.countdownProgress();
        p_execute_tuple(nextTuple,aggregateRow, &info, &pmp);
    }
    VOLT_TRACE("finalizing..");
    return p_execute_finish(aggregateRow, &info, &pmp);
}

void AggregateSerialExecutor::p_execute_tuple(
        TableTuple& nextTuple, AggregateRow* aggregateRow, AggSerialInfo* info, ProgressMonitorProxy* pmpPtr) {

    // Use the first input tuple to "prime" the system.
    if(info->m_noInputRows) {
        // ENG-1565: for this special case, can have only one input row, apply the predicate here
        if (m_prePredicate == NULL || m_prePredicate->eval(&nextTuple, NULL).isTrue()) {
            m_inProgressGroupByValues = getNextGroupByValues(nextTuple);
            // Start the aggregation calculation.
            initAggInstances(aggregateRow);
            aggregateRow->recordPassThroughTuple(nextTuple, m_passThroughColumns, m_outputColumnExpressions);
            advanceAggs(aggregateRow, nextTuple);
        } else {
            info->m_failOnPrePredicate = true;
        }
        info->m_noInputRows = false;
        return;
    }

    std::vector<NValue> nextGroupByValues = getNextGroupByValues(nextTuple);
    for (int ii = m_groupByKeySchema->columnCount() - 1; ii >= 0; --ii) {
        if (nextGroupByValues.at(ii).compare(m_inProgressGroupByValues.at(ii)) != 0) {
            VOLT_TRACE("new group!");
            // Output old row.
            if (insertOutputTuple(aggregateRow)) {
                (*pmpPtr).countdownProgress();
            }
            // Recycle the aggs to start a new row.
            aggregateRow->resetAggs();

            // swap inProgressGroupByValues
            m_inProgressGroupByValues = nextGroupByValues;
            break;
        }
    }
    // update the aggregation calculation.
    aggregateRow->recordPassThroughTuple(nextTuple, m_passThroughColumns, m_outputColumnExpressions);
    advanceAggs(aggregateRow, nextTuple);
}

bool AggregateSerialExecutor::p_execute_finish(AggregateRow* aggregateRow, AggSerialInfo* info, ProgressMonitorProxy* pmpPtr)
{
    if (info->zeroInputRowCase()) {
        VOLT_TRACE("finalizing after no input rows..");
        // No input rows means either no group rows (when grouping) or an empty table row (otherwise).
        // Note the difference between these two cases:
        //   SELECT SUM(A) FROM BBB,            when BBB has no tuple, produces one output row.
        //   SELECT SUM(A) FROM BBB GROUP BY C, when BBB has no tuple, produces no output row.
        if (m_groupByKeySchema->columnCount() == 0) {
            VOLT_TRACE("no input row, but output an empty result row for the whole table.");
            initAggInstances(aggregateRow);
            if (insertOutputTuple(aggregateRow)) {
                (*pmpPtr).countdownProgress();
            }
        }
        return true;
    }

    // There's one last group (or table) row in progress that needs to be output.
    if (insertOutputTuple(aggregateRow)) {
        (*pmpPtr).countdownProgress();
    }
    return true;
}

}
