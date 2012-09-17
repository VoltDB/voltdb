/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

#include "distinctnode.h"

#include "storage/table.h"

#include <sstream>
#include <stdexcept>

using namespace json_spirit;
using namespace voltdb;
using namespace std;

DistinctPlanNode::DistinctPlanNode(CatalogId id) :
    AbstractPlanNode(id), m_distinctExpressions()
{
}

DistinctPlanNode::DistinctPlanNode() : AbstractPlanNode()
{
}

DistinctPlanNode::~DistinctPlanNode()
{
    if (!isInline()) {
        delete getOutputTable();
        setOutputTable(NULL);
    }
    std::vector<AbstractExpression*>::iterator it = m_distinctExpressions.begin();
    while (it != m_distinctExpressions.end()) {
       AbstractExpression* expr = *(it++);
       delete expr;
    }
}

PlanNodeType
DistinctPlanNode::getPlanNodeType() const
{
    return PLAN_NODE_TYPE_DISTINCT;
}

const vector<AbstractExpression*>&
DistinctPlanNode::getDistinctExpressions() const
{
    return m_distinctExpressions;
}

string
DistinctPlanNode::debugInfo(const string &spacer) const
{
    ostringstream buffer;
    buffer << spacer << "DistinctExpressions[\n";
    for (std::vector<AbstractExpression*>::const_iterator it = m_distinctExpressions.begin();
        it != m_distinctExpressions.end(); ++it) {
        buffer << (*it)->debug() << '\n';
    }
    buffer << "]\n";
    return buffer.str();
}

void
DistinctPlanNode::loadFromJSONObject(json_spirit::Object& obj)
{
    Value distinctExpressionsValue = find_value(obj, "DISTINCT_EXPRESSIONS");
    if (distinctExpressionsValue == Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "DistinctPlanNode::loadFromJSONObject:"
                                      " Can't find DISTINCT_EXPRESSIONS value");
    }
    Array distinctExpressionsArray = distinctExpressionsValue.get_array();
    m_distinctExpressions.reserve(distinctExpressionsArray.size());
    for (int ii = 0; ii < distinctExpressionsArray.size(); ii++)
    {
        json_spirit::Object distinctExpressionObject = distinctExpressionsArray[ii].get_obj();
        m_distinctExpressions.push_back(AbstractExpression::buildExpressionTree(distinctExpressionObject));
    }
}
