/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

#include "PlanColumn.h"

using namespace json_spirit;
using namespace std;
using namespace voltdb;

PlanColumn::PlanColumn(Object& colObject) : m_colObject(colObject)
{
    bool contains_guid = false;
    bool contains_name = false;
    bool contains_type = false;
    bool contains_size = false;
    bool contains_input_column_name = false;
    for (int attr = 0; attr < m_colObject.size(); attr++)
    {
        if (m_colObject[attr].name_ == "GUID")
        {
            contains_guid = true;
            m_guid = m_colObject[attr].value_.get_int();
        }
        else if (m_colObject[attr].name_ == "NAME")
        {
            contains_name = true;
            m_name = m_colObject[attr].value_.get_str();
        }
        else if (m_colObject[attr].name_ == "TYPE")
        {
            contains_type = true;
            string m_colObjectTypeString =
                m_colObject[attr].value_.get_str();
            m_type = stringToValue(m_colObjectTypeString);
        }
        else if (m_colObject[attr].name_ == "SIZE")
        {
            contains_size = true;
            m_size = m_colObject[attr].value_.get_int();
        }
        else if (m_colObject[attr].name_ == "INPUT_COLUMN_NAME")
        {
            contains_input_column_name = true;
            m_inputColumnName = m_colObject[attr].value_.get_str();
        }
    }

    if (!contains_input_column_name)
    {
        m_inputColumnName = "";
    }

    assert(contains_name && contains_type && contains_size);
}

int
PlanColumn::getGuid() const
{
    return m_guid;
}

string
PlanColumn::getName() const
{
    return m_name;
}

ValueType
PlanColumn::getType() const
{
    return m_type;
}

int32_t
PlanColumn::getSize() const
{
    return m_size;
}

string
PlanColumn::getInputColumnName() const
{
    return m_inputColumnName;
}

AbstractExpression*
PlanColumn::getExpression()
{
    AbstractExpression* expression = NULL;
    // lazy vector search

    Value columnExpressionValue = find_value(m_colObject, "EXPRESSION");
    if (columnExpressionValue == Value::null)
    {
        throw runtime_error("PlanColumn::getExpression: "
                            "Can't find EXPRESSION value");
    }

    Object columnExpressionObject = columnExpressionValue.get_obj();
    expression = AbstractExpression::buildExpressionTree(columnExpressionObject);

    // Hacky...shouldn't be calling this if we don't have an expression, though.
    assert(expression != NULL);
    return expression;
}
