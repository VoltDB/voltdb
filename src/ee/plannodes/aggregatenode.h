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

#ifndef HSTOREAGGREGATENODE_H
#define HSTOREAGGREGATENODE_H

#include "plannodes/abstractplannode.h"

namespace voltdb {

class AggregatePlanNode : public AbstractPlanNode
{
public:
    AggregatePlanNode(PlanNodeType type)
        : m_aggregates()
        , m_distinctAggregates()
        , m_aggregateOutputColumns()
        , m_aggregateInputExpressions()
        , m_groupByExpressions()
        , m_partialGroupByColumns()
        , m_type(type)
        , m_prePredicate()
        , m_postPredicate()
    {
    }

    ~AggregatePlanNode();
    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;

    const std::vector<ExpressionType> getAggregates() const { return m_aggregates; }

    const std::vector<int> getAggregateIds() const { return m_aggregateIds; }

    const std::vector<bool> getIsWorker() const { return m_isWorker; }

    const std::vector<bool> getPartition() const { return m_isPartition; }

    const std::vector<bool>& getDistinctAggregates() const { return m_distinctAggregates; }

    /*
     * Returns a list of output column indices that map from each
     * aggregation to an output column. These are serialized as
     * integers and not by name as is usually done to make the plan
     * node format more human readable.
     */
    const std::vector<int>& getAggregateOutputColumns() const
    { return m_aggregateOutputColumns; }

    const std::vector<int>& getPartialGroupByColumns() const
    { return m_partialGroupByColumns; }

    const std::vector<AbstractExpression*>& getAggregateInputExpressions() const
    { return m_aggregateInputExpressions; }

    const std::vector<AbstractExpression*>& getGroupByExpressions() const
    { return m_groupByExpressions; }

    AbstractExpression* getPrePredicate() const
    { return m_prePredicate.get(); }

    AbstractExpression* getPostPredicate() const
    { return m_postPredicate.get(); }

    void collectOutputExpressions(std::vector<AbstractExpression*>& outputColumnExpressions) const;

protected:
    void loadFromJSONObject(PlannerDomValue obj);

    std::vector<ExpressionType> m_aggregates;
    std::vector<int> m_aggregateIds;
    std::vector<bool> m_isWorker;
    std::vector<bool> m_isPartition;
    std::vector<bool> m_distinctAggregates;
    std::vector<int> m_aggregateOutputColumns;
    OwningExpressionVector m_aggregateInputExpressions;

    //
    // What columns to group by on
    //
    OwningExpressionVector m_groupByExpressions;

    std::vector<int> m_partialGroupByColumns;

    PlanNodeType m_type; //AGGREGATE, PARTIALAGGREGATE, HASHAGGREGATE

    // ENG-1565: for accelerating min() / max() using index purpose only
    boost::scoped_ptr<AbstractExpression> m_prePredicate;

    boost::scoped_ptr<AbstractExpression> m_postPredicate;
};

} // namespace voltdb

#endif
