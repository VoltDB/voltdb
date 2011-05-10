/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#include "aggregatenode.h"

#include "common/types.h"
#include "storage/table.h"

#include <sstream>
#include <stdexcept>

using namespace json_spirit;
using namespace std;
using namespace voltdb;

AggregatePlanNode::AggregatePlanNode(CatalogId id)
  : AbstractPlanNode(id)
{
    // Do nothing
}

AggregatePlanNode::AggregatePlanNode(PlanNodeType type)
  : AbstractPlanNode(), m_type(type)
{
    // Do nothing
}

AggregatePlanNode::~AggregatePlanNode()
{
    if (!isInline())
    {
        delete getOutputTable();
        setOutputTable(NULL);
    }
    for (int i = 0; i < m_aggregateInputExpressions.size(); i++)
    {
        delete m_aggregateInputExpressions[i];
    }
    for (int i = 0; i < m_groupByExpressions.size(); i++)
    {
        delete m_groupByExpressions[i];
    }
}

PlanNodeType
AggregatePlanNode::getPlanNodeType() const
{
    return m_type;
}

vector<ExpressionType>
AggregatePlanNode::getAggregates()
{
    return m_aggregates;
}

const vector<ExpressionType>
AggregatePlanNode::getAggregates() const
{
    return m_aggregates;
}

const vector<bool>&
AggregatePlanNode::getDistinctAggregates() const
{
    return m_distinctAggregates;
}

const vector<AbstractExpression*>&
AggregatePlanNode::getGroupByExpressions() const
{
    return m_groupByExpressions;
}

string AggregatePlanNode::debugInfo(const string &spacer) const {
    ostringstream buffer;
    buffer << spacer << "\nAggregates["
           << (int) m_aggregates.size() << "]: {";
    for (int ctr = 0, cnt = (int) m_aggregates.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << "type="
               << expressionToString(m_aggregates[ctr]) << "\n";
        buffer << spacer << "distinct="
               << m_distinctAggregates[ctr] << "\n";
        buffer << spacer << "outcol="
               << m_aggregateOutputColumns[ctr] << "\n";
        buffer << spacer << "expr="
               << m_aggregateInputExpressions[ctr]->debug(spacer) << "\n";
    }
    buffer << spacer << "}";

    buffer << spacer << "\nGroupByExpressions[";
    string add = "";
    for (int ctr = 0, cnt = (int) m_groupByExpressions.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << m_groupByExpressions[ctr]->debug(spacer);
        add = ", ";
    }
    buffer << "]\n";

    return buffer.str();
}

void
AggregatePlanNode::loadFromJSONObject(Object &obj)
{
    Value aggregateColumnsValue = find_value(obj, "AGGREGATE_COLUMNS");
    if (aggregateColumnsValue == Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AggregatePlanNode::loadFromJSONObject:"
                                      " Can't find AGGREGATE_COLUMNS value");
    }
    Array aggregateColumnsArray = aggregateColumnsValue.get_array();
    for (int ii = 0; ii < aggregateColumnsArray.size(); ii++)
    {
        Value aggregateColumnValue = aggregateColumnsArray[ii];
        Object aggregateColumn = aggregateColumnValue.get_obj();
        bool containsType = false;
        bool containsDistinct = false;
        bool containsOutputColumn = false;
        bool containsExpression = false;
        for (int zz = 0; zz < aggregateColumn.size(); zz++)
        {
            if (aggregateColumn[zz].name_ == "AGGREGATE_TYPE")
            {
                containsType = true;
                string aggregateColumnTypeString =
                    aggregateColumn[zz].value_.get_str();
                m_aggregates.
                    push_back(stringToExpression(aggregateColumnTypeString));
            }
            else if (aggregateColumn[zz].name_ == "AGGREGATE_DISTINCT")
            {
                containsDistinct = true;
                bool distinct = false;
                if (aggregateColumn[zz].value_.get_int() == 1)
                {
                    distinct = true;
                }
                m_distinctAggregates.push_back(distinct);
            }
            else if (aggregateColumn[zz].name_ == "AGGREGATE_OUTPUT_COLUMN")
            {
                containsOutputColumn = true;
                m_aggregateOutputColumns.
                    push_back(aggregateColumn[zz].value_.get_int());
            }
            else if (aggregateColumn[zz].name_ == "AGGREGATE_EXPRESSION")
            {
                containsExpression = true;
                m_aggregateInputExpressions.
                    push_back(AbstractExpression::buildExpressionTree(aggregateColumn[zz].value_.get_obj()));
            }
        }
        assert(containsType && containsDistinct &&
               containsOutputColumn && containsExpression);
    }

    Value groupByExpressionsValue = find_value(obj, "GROUPBY_EXPRESSIONS");
    if (!(groupByExpressionsValue == Value::null))
    {
        Array groupByExpressionsArray = groupByExpressionsValue.get_array();
        for (int ii = 0; ii < groupByExpressionsArray.size(); ii++)
        {
            Value groupByExpressionValue = groupByExpressionsArray[ii];
            m_groupByExpressions.push_back(AbstractExpression::buildExpressionTree(groupByExpressionValue.get_obj()));
        }
    }
}

// definitions of public test methods

void
AggregatePlanNode::setAggregates(vector<ExpressionType> &aggregates)
{
    m_aggregates = aggregates;
}

void
AggregatePlanNode::setAggregateOutputColumns(vector<int> outputColumns)
{
    m_aggregateOutputColumns = outputColumns;
}
