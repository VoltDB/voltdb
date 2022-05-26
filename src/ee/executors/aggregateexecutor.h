/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#pragma once

#include "executors/abstractexecutor.h"

#include "common/Pool.hpp"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "execution/ProgressMonitorProxy.h"
#include "executors/executorutil.h"

namespace voltdb {

/*
 * Base class for an individual aggregate that aggregates a specific
 * column for a group
 */
class Agg {
protected:
    NValue m_value;
    /**
     * Potentially, putting these two bool member variables will save memory.
     */
    bool m_haveAdvanced = false;
    bool m_inlineCopiedToNonInline = false;
public:
    void* operator new(size_t size, Pool& memoryPool) { return memoryPool.allocate(size); }
    void operator delete(void*, Pool& memoryPool) { /* NOOP -- on alloc error unroll nothing */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    Agg() {
        m_value.setNull();
    }
    virtual ~Agg() {}
    virtual void advance(const NValue& val) = 0;
    virtual NValue finalize(ValueType type) {
        m_value.castAs(type);
        return m_value;
    }
    virtual void resetAgg() {
        m_haveAdvanced = false;
        m_value.setNull();
        m_inlineCopiedToNonInline = false;
    }
};

/**
 * A collection of aggregates in progress for a specific group.
 */
struct AggregateRow {
    // A tuple from the group of tuples being aggregated. Source of pass through columns.
    TableTuple m_passThroughTuple;
    // The aggregates for each column for this group
    Agg* m_aggregates[0];

    void* operator new(size_t size, Pool& memoryPool, size_t nAggs) {
        // allocate nAggs +1 for null terminator: see resetAggs, and destructor.
        // Would it be cleaner to have a count data member? Not by much.
        return memoryPool.allocateZeroes(size + sizeof(void*) * (nAggs + 1));
    }
    void operator delete(void*, Pool& memoryPool, size_t nAggs) { /* NOOP -- on alloc error unroll */ }
    void operator delete(void*) { /* NOOP -- deallocate wholesale with pool */ }

    ~AggregateRow() {
        // Stop at the terminating null agg pointer that has been allocated as an extra and ignored since.
        for (int ii = 0; m_aggregates[ii] != nullptr; ++ii) {
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

    void resetAggs() {
        // Stop at the terminating null agg pointer that has been allocated as an extra and ignored since.
        for (int ii = 0; m_aggregates[ii] != nullptr; ++ii) {
            m_aggregates[ii]->resetAgg();
        }
    }

    void recordPassThroughTuple(TableTuple &passThroughTupleSource, const TableTuple &tuple) {
        passThroughTupleSource.copy(tuple);
        m_passThroughTuple = passThroughTupleSource;
    }
};

/**
 * The base class for aggregate executors regardless of the type of grouping that should be performed.
 */
class AggregateExecutorBase : public AbstractExecutor {
    TupleSchema* constructGroupBySchema(bool partial);
public:
    AggregateExecutorBase(VoltDBEngine* engine, AbstractPlanNode* abstract_node) :
        AbstractExecutor(engine, abstract_node) {}
    ~AggregateExecutorBase() {
        // NULL safe operation
        TupleSchema::freeTupleSchema(m_groupByKeySchema);
        TupleSchema::freeTupleSchema(m_groupByKeyPartialHashSchema);
    }

    /**
     * Initiate the member variables for execute the aggregate.
     * Inlined aggregate will not allocate its own output table,
     * but will use other's output table instead.
     */
    virtual TableTuple p_execute_init(const NValueArray& params, ProgressMonitorProxy* pmp,
            const TupleSchema * schema, AbstractTempTable* newTempTable = NULL,
            CountingPostfilter* parentPredicate = NULL);

    /**
     * Evaluate a tuple. As a side effect, signals when LIMIT has been met, the caller may stop executing.
     */
    virtual void p_execute_tuple(const TableTuple& nextTuple) = 0;

    /**
     * Last method to insert the results to output table and clean up memory or variables.
     */
    virtual void p_execute_finish();

    void cleanupMemoryPool() override {
        AggregateExecutorBase::p_execute_finish();
    }

protected:
    bool p_init(AbstractPlanNode*, const ExecutorVector& executorVector) override;
    void initCountingPredicate(const NValueArray& params, CountingPostfilter* parentPredicate);

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


    void initGroupByKeyTuple(const TableTuple& nextTuple);

    /**
     * Swap the current group by key tuple with the in-progress group by key tuple.
     * Return the new group by key tuple associated with in-progress tuple address.
     * This function is only used in serial or partial aggregation.
     */
    TableTuple& swapWithInprogressGroupByKeyTuple();

    /*
     * List of columns in the output schema that are passing through
     * the value from a column in the input table and not doing any
     * aggregation.
     */
    std::vector<int> m_passThroughColumns;
    std::vector<int> m_aggregateOutputColumns;
    Pool m_memoryPool;
    TupleSchema* m_groupByKeySchema = nullptr;
    std::vector<ExpressionType> m_aggTypes;
    std::vector<int> m_aggregateIds;        // TODO: these are added for UDAF: should be stored elsewhere.
    std::vector<bool> m_isWorker;
    std::vector<bool> m_isPartition;
    std::vector<bool> m_distinctAggs;
    std::vector<AbstractExpression*> m_groupByExpressions;
    std::vector<AbstractExpression*> m_inputExpressions;
    std::vector<AbstractExpression*> m_outputColumnExpressions;
    AbstractExpression* m_prePredicate = nullptr;    // ENG-1565: for enabling max() using index purpose only
    AbstractExpression* m_postPredicate = nullptr;

    ProgressMonitorProxy* m_pmp = nullptr;
    PoolBackedTupleStorage m_nextGroupByKeyStorage;
    const TupleSchema* m_inputSchema = nullptr;

    // used for serial/partial aggregation only
    TableTuple m_inProgressGroupByKeyTuple;
    // used for partial aggregation.
    std::vector<int> m_partialSerialGroupByColumns;
    std::vector<int> m_partialHashGroupByColumns;
    TupleSchema* m_groupByKeyPartialHashSchema = nullptr;

    // used for inline limit for serial/partial aggregate
    CountingPostfilter m_postfilter;
};

using HashAggregateMapType =
std::unordered_map<TableTuple, AggregateRow*, TableTupleHasher, TableTupleEqualityChecker>;


/**
 * The concrete executor class for PlanNodeType::HashAggregate
 * in which the input does not need to be sorted and execution will hash the group by key to aggregate the tuples.
 */
class AggregateHashExecutor : public AggregateExecutorBase {
    HashAggregateMapType m_hash;
    bool p_execute(const NValueArray& params) override;
public:
    AggregateHashExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node) :
        AggregateExecutorBase(engine, abstract_node) { }

    // empty destructor defined in .cpp file because of it is called virtually (not inline)
    // same reason for serial and partial
    ~AggregateHashExecutor();
    TableTuple p_execute_init(const NValueArray& params, ProgressMonitorProxy* pmp,
            const TupleSchema * schema, AbstractTempTable* newTempTable  = nullptr,
            CountingPostfilter* parentPredicate = nullptr) override;
    void p_execute_tuple(const TableTuple& nextTuple) override;
    void p_execute_finish() override;
};

/**
 * The concrete executor class for PlanNodeType::Aggregate
 * a constant space aggregation that expects the input table to be sorted on the group by key
 * at least to the extent that rows with equal keys arrive sequentially (not interspersed with other key values).
 */
class AggregateSerialExecutor : public AggregateExecutorBase {
    bool p_execute(const NValueArray& params) override;
public:
    AggregateSerialExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node) :
        AggregateExecutorBase(engine, abstract_node) { }
    ~AggregateSerialExecutor();
    TableTuple p_execute_init(const NValueArray& params, ProgressMonitorProxy* pmp,
            const TupleSchema * schema, AbstractTempTable* newTempTable  = nullptr,
            CountingPostfilter* parentPredicate = nullptr) override;
    void p_execute_tuple(const TableTuple& nextTuple) override;
    void p_execute_finish() override;

protected:
    AggregateRow* m_aggregateRow = nullptr;
    // State variables for iteration on input table
    bool m_noInputRows = true;
    bool m_failPrePredicateOnFirstRow = false;

    TableTuple m_passThroughTupleSource;
};


class AggregatePartialExecutor : public AggregateExecutorBase {
    bool m_atTheFirstRow = true;
    PoolBackedTupleStorage m_nextPartialGroupByKeyStorage;
    HashAggregateMapType m_hash;

    virtual bool p_execute(const NValueArray& params);
    void initPartialHashGroupByKeyTuple(const TableTuple& nextTuple);
public:
    AggregatePartialExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node) :
        AggregateExecutorBase(engine, abstract_node) { }
    ~AggregatePartialExecutor();
    TableTuple p_execute_init(const NValueArray& params, ProgressMonitorProxy* pmp,
            const TupleSchema * schema, AbstractTempTable* newTempTable  = nullptr,
            CountingPostfilter* parentPredicate = nullptr);
    void p_execute_tuple(const TableTuple& nextTuple);
    void p_execute_finish();
};


inline AggregateExecutorBase* getInlineAggregateExecutor(const AbstractPlanNode* node) {
    AbstractPlanNode* aggNode = nullptr;
    AggregateExecutorBase* aggExec = nullptr;
    if (nullptr != (aggNode = node->getInlinePlanNode(PlanNodeType::PartialAggregate)) ) {
        VOLT_TRACE("init inline partial aggregation stuff...");
        aggExec = dynamic_cast<AggregatePartialExecutor*>(aggNode->getExecutor());
        vassert(aggExec != nullptr);
    } else if (nullptr != (aggNode = node->getInlinePlanNode(PlanNodeType::Aggregate)) ) {
        VOLT_TRACE("init inline serial aggregation stuff...");
        aggExec = dynamic_cast<AggregateSerialExecutor*>(aggNode->getExecutor());
        vassert(aggExec != nullptr);
    } else if (nullptr != (aggNode = node->getInlinePlanNode(PlanNodeType::HashAggregate)) ) {
        VOLT_TRACE("init inline hash aggregation stuff...");
        aggExec = dynamic_cast<AggregateHashExecutor*>(aggNode->getExecutor());
        vassert(aggExec != nullptr);
    }
    return aggExec;
}

}
