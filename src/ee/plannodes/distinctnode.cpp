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

#include "distinctnode.h"

#include "storage/table.h"

#include <sstream>
#include <stdexcept>

using namespace voltdb;
using namespace std;

DistinctPlanNode::DistinctPlanNode(CatalogId id) : AbstractPlanNode(id)
{
    m_distinctColumnIdx = -1;
}

DistinctPlanNode::DistinctPlanNode() : AbstractPlanNode()
{
    m_distinctColumnIdx = -1;
}

DistinctPlanNode::~DistinctPlanNode()
{
    if (!isInline()) {
        delete getOutputTable();
        setOutputTable(NULL);
    }
}

PlanNodeType
DistinctPlanNode::getPlanNodeType() const
{
    return PLAN_NODE_TYPE_DISTINCT;
}

void
DistinctPlanNode::setDistinctColumn(int column)
{
    m_distinctColumnIdx = column;
}

int
DistinctPlanNode::getDistinctColumn() const
{
    assert(m_distinctColumnIdx >= 0);
    return m_distinctColumnIdx;
}

void
DistinctPlanNode::setDistinctColumnName(string columnName)
{
    m_distinctColumnName = columnName;
}

string
DistinctPlanNode::getDistinctColumnName() const
{
    return m_distinctColumnName;
}

int
DistinctPlanNode::getDistinctColumnGuid() const
{
    return m_distinctColumnGuid;
}

string
DistinctPlanNode::debugInfo(const string &spacer) const
{
    ostringstream buffer;
    buffer << spacer << "DistinctColumn[" << this->m_distinctColumnIdx << "]\n";
    return buffer.str();
}

void
DistinctPlanNode::loadFromJSONObject(json_spirit::Object& obj,
                                     const catalog::Database* catalog_db)
{
    json_spirit::Value distinctColumnGuidValue =
        json_spirit::find_value( obj, "DISTINCT_COLUMN_GUID");
    if (distinctColumnGuidValue == json_spirit::Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "DistinctPlanNode::loadFromJSONObject: "
                                      "Can't find DISTINCT_COLUMN_GUID value");
    }
    m_distinctColumnGuid = distinctColumnGuidValue.get_int();

    json_spirit::Value distinctColumnNameValue =
        json_spirit::find_value( obj, "DISTINCT_COLUMN_NAME");
    if (distinctColumnNameValue == json_spirit::Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "DistinctPlanNode::loadFromJSONObject: "
                                      "Can't find DISTINCT_COLUMN_NAME value");
    }
    m_distinctColumnName = distinctColumnNameValue.get_str();
}
