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

#include "abstractjoinnode.h"

#include "expressions/abstractexpression.h"

#include <stdexcept>

using namespace std;
using namespace voltdb;

AbstractJoinPlanNode::AbstractJoinPlanNode(CatalogId id)
    : AbstractPlanNode(id), m_predicate(NULL)
{
}

AbstractJoinPlanNode::AbstractJoinPlanNode()
    : AbstractPlanNode(), m_predicate(NULL)
{
}

AbstractJoinPlanNode::~AbstractJoinPlanNode()
{
    delete m_predicate;
}

JoinType AbstractJoinPlanNode::getJoinType() const
{
    return m_joinType;
}

void AbstractJoinPlanNode::setJoinType(JoinType join_type)
{
    m_joinType = join_type;
}

void AbstractJoinPlanNode::setPredicate(AbstractExpression* predicate)
{
    assert(!m_predicate);
    if (m_predicate != predicate)
    {
        delete m_predicate;
    }
    m_predicate = predicate;
}

AbstractExpression* AbstractJoinPlanNode::getPredicate() const
{
    return m_predicate;
}

string AbstractJoinPlanNode::debugInfo(const string& spacer) const
{
    ostringstream buffer;
    buffer << spacer << "JoinType[" << m_joinType << "]\n";
    if (m_predicate != NULL)
    {
        buffer << m_predicate->debug(spacer);
    }
    return (buffer.str());
}

void
AbstractJoinPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    m_joinType = stringToJoin(obj.valueForKey("JOIN_TYPE").asStr());

    if (obj.hasNonNullKey("PREDICATE")) {
        m_predicate = AbstractExpression::buildExpressionTree(obj.valueForKey("PREDICATE"));
    }
    else {
        m_predicate = NULL;
    }
}
