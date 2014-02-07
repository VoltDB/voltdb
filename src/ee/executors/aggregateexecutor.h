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

namespace voltdb {
struct AggregateRow;

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

protected:
    virtual bool p_init(AbstractPlanNode*, TempTableLimits*);

    void executeAggBase(const NValueArray& params);

    void initGroupByKeyTuple(PoolBackedTupleStorage &groupByKeyTuple, const TableTuple& nxtTuple);

    /// Helper method responsible for inserting the results of the
    /// aggregation into a new tuple in the output table as well as passing
    /// through any additional columns from the input table.
    bool insertOutputTuple(AggregateRow* aggregateRow);

    void advanceAggs(AggregateRow* aggregateRow);

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
    Pool m_memoryPool;
    TupleSchema* m_groupByKeySchema;
    TupleSchema* m_aggSchema;
    std::vector<ExpressionType> m_aggTypes;
    std::vector<bool> m_distinctAggs;
    std::vector<AbstractExpression*> m_groupByExpressions;
    std::vector<AbstractExpression*> m_inputExpressions;
    std::vector<AbstractExpression*> m_outputColumnExpressions;
    std::vector<int> m_aggregateOutputColumns;
    AbstractExpression* m_prePredicate;    // ENG-1565: for enabling max() using index purpose only
    AbstractExpression* m_postPredicate;
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

private:
    virtual bool p_execute(const NValueArray& params);
};

}
#endif
