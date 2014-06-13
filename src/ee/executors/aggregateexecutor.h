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

#ifndef HSTOREAGGREGATEEXECUTOR_H
#define HSTOREAGGREGATEEXECUTOR_H

#include "executors/abstractexecutor.h"

#include "common/Pool.hpp"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "execution/ProgressMonitorProxy.h"

namespace voltdb {

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
    virtual void resetAgg()
    {
        m_haveAdvanced = false;
        m_value.setNull();
    }
protected:
    bool m_haveAdvanced;
    NValue m_value;
};

/**
 * A collection of aggregates in progress for a specific group.
 */
struct AggregateRow
{
    void* operator new(size_t size, Pool& memoryPool, size_t nAggs)
    {
        // allocate nAggs +1 for null terminator: see resetAggs, and destructor.
        // Would it be cleaner to have a count data member? Not by much.
        return memoryPool.allocateZeroes(size + (sizeof(void*) * (nAggs + 1)));
    }
    void operator delete(void*, Pool& memoryPool, size_t nAggs) { /* NOOP -- on alloc error unroll */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    ~AggregateRow()
    {
        // Stop at the terminating null agg pointer that has been allocated as an extra and ignored since.
        for (int ii = 0; m_aggregates[ii] != NULL; ++ii) {
            // All the aggs inherit no-op delete operators, so, "delete" is really just destructor invocation.
            // The destructor being invoked is the implicit specialization of Agg's destructor.
            // The compiler generates it to invoke the destructor (if any) of the distinct value set (if any).
            // It must be called because the pooled Agg object only embeds the (boost) set's "head".
            // The "body" is allocated by boost via its defaulted stl allocator as the set grows
            // -- AND it is not completely deallocated when "clear" is called.
            // This AggregateRow destructor would not be required at all if distinct was based on a home-grown
            // pool-aware hash, incapable of leaking outside the pool.
            delete m_aggregates[ii];
        }
    }

    void resetAggs()
    {
        // Stop at the terminating null agg pointer that has been allocated as an extra and ignored since.
        for (int ii = 0; m_aggregates[ii] != NULL; ++ii) {
            m_aggregates[ii]->resetAgg();
        }
    }

    void recordPassThroughTuple(TableTuple &passThroughTupleSource, const TableTuple &tuple)
    {
        passThroughTupleSource.copy(tuple);
        m_passThroughTuple = passThroughTupleSource;
    }

    // A tuple from the group of tuples being aggregated. Source of pass through columns.
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
        AbstractExecutor(engine, abstract_node), m_groupByKeySchema(NULL),
        m_prePredicate(NULL), m_postPredicate(NULL)
    { }
    ~AggregateExecutorBase()
    {
        if (m_groupByKeySchema != NULL) {
            TupleSchema::freeTupleSchema(m_groupByKeySchema);
        }
    }

    void setAggregateOutputTable(TempTable* newTempTable) {
        // inlined aggregate will not allocate its own output table, but will use Scan's output table instead

        // These two schemas should be equal
        m_tmpOutputTable = newTempTable;
    }

    void executeAggBase(const NValueArray& params);

protected:
    virtual bool p_init(AbstractPlanNode*, TempTableLimits*);

    void initGroupByKeyTuple(PoolBackedTupleStorage &groupByKeyTuple, const TableTuple& nxtTuple);

    /// Helper method responsible for inserting the results of the
    /// aggregation into a new tuple in the output table as well as passing
    /// through any additional columns from the input table.
    bool insertOutputTuple(AggregateRow* aggregateRow);

    void advanceAggs(AggregateRow* aggregateRow, const TableTuple& tuple);

    /*
     * Create an instance of an aggregator for the specified aggregate type.
     * The object is constructed in memory from the provided memory pool.
     */
    void initAggInstances(AggregateRow* aggregateRow);

    /*
     * List of columns in the output schema that are passing through
     * the value from a column in the input table and not doing any
     * aggregation.
     */
    std::vector<int> m_passThroughColumns;
    std::vector<int> m_aggregateOutputColumns;
    Pool m_memoryPool;
    TupleSchema* m_groupByKeySchema;
    TupleSchema* m_aggSchema;
    std::vector<ExpressionType> m_aggTypes;
    std::vector<bool> m_distinctAggs;
    std::vector<AbstractExpression*> m_groupByExpressions;
    std::vector<AbstractExpression*> m_inputExpressions;
    std::vector<AbstractExpression*> m_outputColumnExpressions;
    AbstractExpression* m_prePredicate;    // ENG-1565: for enabling max() using index purpose only
    AbstractExpression* m_postPredicate;

    ProgressMonitorProxy* m_pmp;
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

protected:
    virtual bool p_execute(const NValueArray& params);
};

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

    void p_execute_init(const NValueArray& params, ProgressMonitorProxy* pmp, const TupleSchema * schema);

    void p_execute_tuple(const TableTuple& nextTuple);

    void p_execute_finish();
protected:
    virtual bool p_execute(const NValueArray& params);

    void getNextGroupByValues(const TableTuple& nextTuple);

    std::vector<NValue> m_inProgressGroupByValues;
    std::vector<NValue> m_nextGroupByValues;

    AggregateRow * m_aggregateRow;

    TableTuple m_passThroughTupleSource;

    // State variables for iteration on input table
    bool m_noInputRows;
    bool m_failPrePredicateOnFirstRow;
};

}
#endif
