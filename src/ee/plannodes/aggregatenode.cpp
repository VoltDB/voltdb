/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

#include "PlanColumn.h"
#include "expressions/expressionutil.h"
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
}

PlanNodeType
AggregatePlanNode::getPlanNodeType() const
{
    return m_type;
}

vector<string>&
AggregatePlanNode::getOutputColumnNames()
{
    return m_outputColumnNames;
}

const vector<string>&
AggregatePlanNode::getOutputColumnNames() const
{
    return m_outputColumnNames;
}

vector<ValueType>&
AggregatePlanNode::getOutputColumnTypes()
{
    return m_outputColumnTypes;
}

const vector<ValueType>&
AggregatePlanNode::getOutputColumnTypes() const
{
    return m_outputColumnTypes;
}

vector<int32_t>&
AggregatePlanNode::getOutputColumnSizes()
{
    return m_outputColumnSizes;
}

const vector<int32_t>&
AggregatePlanNode::getOutputColumnSizes() const
{
    return m_outputColumnSizes;
}

vector<int>&
AggregatePlanNode::getOutputColumnInputGuids()
{
    return m_outputColumnGuids;
}

const vector<int>&
AggregatePlanNode::getOutputColumnInputGuids() const
{
    return m_outputColumnGuids;
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

void
AggregatePlanNode::setAggregateColumns(vector<int> columns)
{
    m_aggregateColumns = columns;
}

vector<int>
AggregatePlanNode::getAggregateColumns() const
{
    return m_aggregateColumns;
}

vector<string>
AggregatePlanNode::getAggregateColumnNames() const
{
    return m_aggregateColumnNames;
}

vector<int>
AggregatePlanNode::getAggregateColumnGuids() const
{
    return m_aggregateColumnGuids;
}

void
AggregatePlanNode::setGroupByColumns(vector<int> &columns)
{
    m_groupByColumns = columns;
}

vector<int>&
AggregatePlanNode::getGroupByColumns()
{
    return m_groupByColumns;
}

const vector<int>&
AggregatePlanNode::getGroupByColumns() const
{
    return m_groupByColumns;
}

vector<int>&
AggregatePlanNode::getGroupByColumnGuids()
{
    return m_groupByColumnGuids;
}

const vector<int>&
AggregatePlanNode::getGroupByColumnGuids() const
{
    return m_groupByColumnGuids;
}

vector<string>&
AggregatePlanNode::getGroupByColumnNames()
{
    return m_groupByColumnNames;
}

const vector<string>&
AggregatePlanNode::getGroupByColumnNames() const
{
    return m_groupByColumnNames;
}

int
AggregatePlanNode::getColumnIndexFromGuid(
    int guid, const catalog::Database *db) const
{
    for (int i = 0; i < m_outputColumnGuids.size(); i++)
    {
        if (guid == m_outputColumnGuids[i])
        {
            return i;
        }
    }
    return -1;
}



string AggregatePlanNode::debugInfo(const string &spacer) const {
    ostringstream buffer;
    buffer << spacer << "\nAggregateColumns["
           << (int) m_aggregateColumns.size() << "]: {";
    for (int ctr = 0, cnt = (int) m_aggregateColumns.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << m_aggregateColumns[ctr];
    }
    buffer << spacer << "}";
    buffer << spacer << "\nAggregateTypes["
           << (int) m_aggregateColumns.size() << "]: {";
    for (int ctr = 0, cnt = (int) m_aggregateColumns.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << expressionutil::getTypeName(m_aggregates[ctr]);
    }
    buffer << spacer << "}";

    buffer << spacer << "}";
    buffer << spacer << "\nAggregateColumnNames["
           << (int) m_aggregateColumnNames.size() << "]: {";
    for (int ctr = 0, cnt = (int) m_aggregateColumnNames.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << m_aggregateColumnNames[ctr];
    }
    buffer << spacer << "}";

    buffer << spacer << "\nGroupByColumns[";
    string add = "";
    for (int ctr = 0, cnt = (int) m_groupByColumns.size();
         ctr < cnt; ctr++)
    {
        buffer << add << m_groupByColumns[ctr];
        add = ", ";
    }
    buffer << "]\n";

    buffer << spacer << "OutputColumns[" << m_outputColumnGuids.size()
           << "]:\n";
    for (int ctr = 0, cnt = (int) m_outputColumnGuids.size();
         ctr < cnt; ctr++)
    {
        buffer << spacer << "   [" << ctr << "] "
               << m_outputColumnGuids[ctr] << " : ";
        buffer << "name=" << m_outputColumnNames[ctr] << " : ";
        buffer << "size=" << m_outputColumnSizes[ctr] << " : ";
        buffer << "type=" << m_outputColumnTypes[ctr] << "\n";
    }
    return buffer.str();
}

void
AggregatePlanNode::loadFromJSONObject(Object &obj,
                                      const catalog::Database *catalog_db)
{
    Value outputColumnsValue = find_value(obj, "OUTPUT_COLUMNS");
    if (outputColumnsValue == Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AggregatePlanNode::loadFromJSONObject:"
                                      " Can't find OUTPUT_COLUMNS value");
    }
    Array outputColumnsArray = outputColumnsValue.get_array();

    for (int ii = 0; ii < outputColumnsArray.size(); ii++)
    {
        Value outputColumnValue = outputColumnsArray[ii];
        PlanColumn outputColumn = PlanColumn(outputColumnValue.get_obj());
        m_outputColumnGuids.push_back(outputColumn.getGuid());
        m_outputColumnNames.push_back(outputColumn.getName());
        m_outputColumnTypes.push_back(outputColumn.getType());
        m_outputColumnSizes.push_back(outputColumn.getSize());
    }

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
        bool containsName = false;
        bool containsGuid = false;
        bool containsOutputColumn = false;
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
            else if (aggregateColumn[zz].name_ == "AGGREGATE_NAME")
            {
                containsName = true;
                m_aggregateColumnNames.
                    push_back(aggregateColumn[zz].value_.get_str());
            }
            else if (aggregateColumn[zz].name_ == "AGGREGATE_GUID")
            {
                containsGuid = true;
                m_aggregateColumnGuids.
                    push_back(aggregateColumn[zz].value_.get_int());
            }
            else if (aggregateColumn[zz].name_ == "AGGREGATE_OUTPUT_COLUMN")
            {
                containsOutputColumn = true;
                m_aggregateOutputColumns.
                    push_back(aggregateColumn[zz].value_.get_int());
            }
        }
        assert(containsName && containsType && containsOutputColumn);
    }

    Value groupByColumnsValue = find_value(obj, "GROUPBY_COLUMNS");
    if (!(groupByColumnsValue == Value::null))
    {
        Array groupByColumnsArray = groupByColumnsValue.get_array();
        for (int ii = 0; ii < groupByColumnsArray.size(); ii++)
        {
            Value groupByColumnValue = groupByColumnsArray[ii];
            PlanColumn groupByColumn = PlanColumn(groupByColumnValue.get_obj());
            m_groupByColumnGuids.push_back(groupByColumn.getGuid());
            m_groupByColumnNames.push_back(groupByColumn.getName());
        }
    }
}

// definitions of public test methods

void
AggregatePlanNode::setOutputColumnNames(vector<string> &names)
{
     m_outputColumnNames = names;
}

void
AggregatePlanNode::setOutputColumnTypes(vector<ValueType> &types)
{
    m_outputColumnTypes = types;
}

void
AggregatePlanNode::setOutputColumnSizes(vector<int32_t> &sizes)
{
    m_outputColumnSizes = sizes;
}

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

void
AggregatePlanNode::setAggregateColumnNames(vector<string> column_names)
{
    m_aggregateColumnNames = column_names;
}
