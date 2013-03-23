/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include "SchemaColumn.h"

using namespace std;
using namespace voltdb;

SchemaColumn::SchemaColumn(PlannerDomValue colObject)
{
    bool contains_table_name = false;
    bool contains_column_name = false;
    bool contains_column_alias = false;
    bool contains_type = false;
    bool contains_size = false;

    if (colObject.hasKey("TABLE_NAME")) {
        contains_table_name = true;
        m_tableName = colObject.valueForKey("TABLE_NAME").asStr();
    }

    if (colObject.hasKey("COLUMN_NAME")) {
        contains_column_name = true;
        m_columnName = colObject.valueForKey("COLUMN_NAME").asStr();
    }

    if (colObject.hasKey("COLUMN_ALIAS")) {
        contains_column_alias = true;
        m_columnAlias = colObject.valueForKey("COLUMN_ALIAS").asStr();
    }

    if (colObject.hasKey("TYPE")) {
        contains_type = true;
        string colObjectTypeString = colObject.valueForKey("TYPE").asStr();
        m_type = stringToValue(colObjectTypeString);
    }

    if (colObject.hasKey("SIZE")) {
        contains_size = true;
        m_size = colObject.valueForKey("SIZE").asInt();
    }

    m_expression = NULL;
    // lazy vector search

    PlannerDomValue columnExpressionValue = colObject.valueForKey("EXPRESSION");

    m_expression = AbstractExpression::buildExpressionTree(columnExpressionValue);

    if(!(contains_table_name && contains_column_name &&
         contains_column_alias && contains_type && contains_size)) {
        throw runtime_error("SchemaColumn::constructor missing configuration data.");
    }
}

SchemaColumn::~SchemaColumn()
{
    delete m_expression;
}

string
SchemaColumn::getTableName() const
{
    return m_tableName;
}

string
SchemaColumn::getColumnName() const
{
    return m_columnName;
}

string
SchemaColumn::getColumnAlias() const
{
    return m_columnAlias;
}

ValueType
SchemaColumn::getType() const
{
    return m_type;
}

int32_t
SchemaColumn::getSize() const
{
    return m_size;
}

AbstractExpression*
SchemaColumn::getExpression()
{
    return m_expression;
}
