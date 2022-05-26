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

#ifndef HSTOREJOINNODE_H
#define HSTOREJOINNODE_H

#include "abstractplannode.h"

namespace voltdb {

class AbstractExpression;

class AbstractJoinPlanNode : public AbstractPlanNode
{
public:
    AbstractJoinPlanNode();
    ~AbstractJoinPlanNode();
    std::string debugInfo(const std::string& spacer) const;

    JoinType getJoinType() const { return m_joinType; }
    AbstractExpression* getPreJoinPredicate() const { return m_preJoinPredicate.get(); }
    AbstractExpression* getJoinPredicate() const { return m_joinPredicate.get(); }
    AbstractExpression* getWherePredicate() const { return m_wherePredicate.get(); }
    const TupleSchema* getTupleSchemaPreAgg() const { return m_tupleSchemaPreAgg; }
    void getOutputColumnExpressions(std::vector<AbstractExpression*>& outputExpressions) const;

protected:
    void loadFromJSONObject(PlannerDomValue obj);

    // This is the outer-table-only join expression. If the outer tuple fails it,
    // it may still be part of the result set (pending other filtering)
    // but can't be joined with any tuple from the inner table.
    // In a left outer join, the failed outer tuple STILL gets null-padded in the output table.
    boost::scoped_ptr<AbstractExpression> m_preJoinPredicate;

    // This is the predicate to figure out whether a joined tuple should
    // be put into the output table
    boost::scoped_ptr<AbstractExpression> m_joinPredicate;

    // The additional filtering criteria specified by the WHERE clause
    // in case of outer joins. The predicated is applied to the whole
    // joined tuple after it's assembled
    boost::scoped_ptr<AbstractExpression> m_wherePredicate;

    // Currently either inner or left outer.
    JoinType m_joinType;

    // output schema pre inline aggregation
    std::vector<SchemaColumn*> m_outputSchemaPreAgg;

    TupleSchema* m_tupleSchemaPreAgg;
};

} // namespace voltdb

#endif
