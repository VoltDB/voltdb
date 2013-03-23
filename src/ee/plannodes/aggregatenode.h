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

#ifndef HSTOREAGGREGATENODE_H
#define HSTOREAGGREGATENODE_H

#include "plannodes/abstractplannode.h"

namespace voltdb
{

class AggregatePlanNode : public AbstractPlanNode
{
public:
    AggregatePlanNode(PlanNodeType type) : m_type(type) { }
    ~AggregatePlanNode();

    virtual PlanNodeType getPlanNodeType() const { return m_type; }

    const std::vector<ExpressionType> getAggregates() const { return m_aggregates; }

    const std::vector<bool>& getDistinctAggregates() const { return m_distinctAggregates; }

    /*
     * Returns a list of output column indices that map from each
     * aggregation to an output column. These are serialized as
     * integers and not by name as is usually done to make the plan
     * node format more human readable.
     */
    const std::vector<int>& getAggregateOutputColumns() const
    { return m_aggregateOutputColumns; }

    const std::vector<AbstractExpression*>& getAggregateInputExpressions() const
    { return m_aggregateInputExpressions; }

    const std::vector<AbstractExpression*>& getGroupByExpressions() const
    { return m_groupByExpressions; }

    void collectOutputExpressions(std::vector<AbstractExpression*>& outputColumnExpressions) const;

    std::string debugInfo(const std::string &spacer) const;

    //
    // Public methods used only for tests
    //
    void setAggregates(std::vector<ExpressionType> &aggregates);
    void setAggregateOutputColumns(std::vector<int> outputColumns);

protected:
    virtual void loadFromJSONObject(PlannerDomValue obj);

    std::vector<ExpressionType> m_aggregates;
    std::vector<bool> m_distinctAggregates;
    std::vector<int> m_aggregateOutputColumns;
    std::vector<AbstractExpression*> m_aggregateInputExpressions;
    std::vector<AbstractExpression*> m_outputColumnExpressions;

    //
    // What columns to group by on
    //
    std::vector<AbstractExpression*> m_groupByExpressions;

    PlanNodeType m_type; //AGGREGATE OR HASHAGGREGATE
};

}

#endif
