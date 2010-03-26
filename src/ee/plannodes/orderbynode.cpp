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

#include "orderbynode.h"

#include "common/types.h"
#include "storage/table.h"

#include <sstream>
#include <stdexcept>
#include <string>

using namespace std;
using namespace voltdb;

OrderByPlanNode::OrderByPlanNode(CatalogId id) : AbstractPlanNode(id)
{
    //DO NOTHING
}

OrderByPlanNode::OrderByPlanNode() : AbstractPlanNode()
{
    //DO NOTHING
}

OrderByPlanNode::~OrderByPlanNode()
{
    delete getOutputTable();
    setOutputTable(NULL);
}

PlanNodeType
OrderByPlanNode::getPlanNodeType() const
{
    return PLAN_NODE_TYPE_ORDERBY;
}

void
OrderByPlanNode::setSortColumns(vector<int>& cols)
{
    m_sortColumns = cols;
}


vector<int>&
OrderByPlanNode::getSortColumns()
{
    return m_sortColumns;
}

const vector<int>&
OrderByPlanNode::getSortColumns() const
{
    return m_sortColumns;
}

vector<int>&
OrderByPlanNode::getSortColumnGuids()
{
    return m_sortColumnGuids;
}

void
OrderByPlanNode::setSortColumnNames(vector<string>& column_names)
{
    m_sortColumnNames = column_names;
}

vector<string>&
OrderByPlanNode::getSortColumnNames()
{
    return m_sortColumnNames;
}

const vector<string>&
OrderByPlanNode::getSortColumnNames() const
{
    return m_sortColumnNames;
}

void
OrderByPlanNode::setSortDirections(vector<SortDirectionType>& dirs)
{
    m_sortDirections = dirs;
}

vector<SortDirectionType>&
OrderByPlanNode::getSortDirections()
{
    return m_sortDirections;
}

const vector<SortDirectionType>&
OrderByPlanNode::getDirections() const
{
    return m_sortDirections;
}

string
OrderByPlanNode::debugInfo(const string& spacer) const
{
    ostringstream buffer;
    buffer << spacer << "SortColumns[" << m_sortColumns.size() << "]\n";
    for (int ctr = 0, cnt = (int)m_sortColumns.size(); ctr < cnt; ctr++)
    {
        buffer << spacer << "  [" << ctr << "] " << m_sortColumns[ctr]
               << "::" << m_sortDirections[ctr] << "\n";
    }
    return buffer.str();

}

void
OrderByPlanNode::loadFromJSONObject(json_spirit::Object& obj,
                                    const catalog::Database* catalog_db)
{
    json_spirit::Value sortColumnsValue =
        json_spirit::find_value(obj, "SORT_COLUMNS");
    if (sortColumnsValue == json_spirit::Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "OrderByPlanNode::loadFromJSONObject:"
                                      " Can't find SORT_COLUMNS value");
    }
    json_spirit::Array sortColumnsArray = sortColumnsValue.get_array();

    for (int ii = 0; ii < sortColumnsArray.size(); ii++)
    {
        json_spirit::Object sortColumn = sortColumnsArray[ii].get_obj();
        bool hasName = false, hasDirection = false;
        for (int zz = 0; zz < sortColumn.size(); zz++)
        {
            if (sortColumn[zz].name_ == "COLUMN_NAME")
            {
                hasName = true;
                m_sortColumnNames.push_back(sortColumn[zz].value_.get_str());
            }
            else if (sortColumn[zz].name_ == "COLUMN_GUID")
            {
                hasName = true;
                m_sortColumnGuids.push_back(sortColumn[zz].value_.get_int());
            }
            else if (sortColumn[zz].name_ == "SORT_DIRECTION")
            {
                hasDirection = true;
                string sortDirectionTypeString = sortColumn[zz].value_.get_str();
                m_sortDirections.
                    push_back(stringToSortDirection(sortDirectionTypeString));
            }
        }
        assert (hasName && hasDirection);
    }
}
