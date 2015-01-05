/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

SchemaColumn::SchemaColumn(PlannerDomValue colObject, int idx)
{
    if (colObject.hasKey("TABLE_NAME")) {
        m_tableName = colObject.valueForKey("TABLE_NAME").asStr();
    }

    if (colObject.hasKey("COLUMN_NAME")) {
        m_columnName = colObject.valueForKey("COLUMN_NAME").asStr();
    }
    else {
//        throw runtime_error("SchemaColumn::constructor missing column name.");
        char tmpName[6]; // 1024
        std::snprintf(tmpName, sizeof(tmpName), "C%d", idx);
        m_columnName = std::string(tmpName);
    }

    if (colObject.hasKey("COLUMN_ALIAS")) {
        m_columnAlias = colObject.valueForKey("COLUMN_ALIAS").asStr();
    }

    if (colObject.hasKey("TYPE")) {
        string colObjectTypeString = colObject.valueForKey("TYPE").asStr();
        m_type = stringToValue(colObjectTypeString);
    }

    if (colObject.hasKey("SIZE")) {
        m_size = colObject.valueForKey("SIZE").asInt();
    }

    m_expression = NULL;
    // lazy vector search
    if (colObject.hasKey("EXPRESSION")) {
        PlannerDomValue columnExpressionValue = colObject.valueForKey("EXPRESSION");

        m_expression = AbstractExpression::buildExpressionTree(columnExpressionValue);
        assert(m_expression);
    }
}

SchemaColumn::~SchemaColumn()
{
    delete m_expression;
}

string
SchemaColumn::getColumnName() const
{
    return m_columnName;
}

AbstractExpression*
SchemaColumn::getExpression()
{
    return m_expression;
}
