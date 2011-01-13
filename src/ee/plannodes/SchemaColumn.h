/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
#ifndef VOLTDBSCHEMACOLUMN_H
#define VOLTDBSCHEMACOLUMN_H

#include "common/types.h"
#include "expressions/abstractexpression.h"

#include "boost/shared_ptr.hpp"
#include "json_spirit/json_spirit.h"

#include <string>

namespace voltdb
{

/**
 * Convenience class to deserialize a SchemaColumn object from the JSON
 * and provide common accessors to the contents.  Currently relies on
 * colObject to remain valid; SchemaColumns should not be passed around,
 * stored, or expected to be valid outside the scope of the initial
 * JSON deserialization.
 */
class SchemaColumn
{
public:
    SchemaColumn(json_spirit::Object& colObject);
    ~SchemaColumn();

    std::string getTableName() const;
    std::string getColumnName() const;
    std::string getColumnAlias() const;
    ValueType getType() const;
    int32_t getSize() const;

    // SchemaColumn retains responsibility for the deletion of
    // the expression
    AbstractExpression* getExpression();

private:
    const json_spirit::Object& m_colObject;

    std::string m_tableName;
    std::string m_columnName;
    std::string m_columnAlias;
    AbstractExpression* m_expression;
    ValueType m_type;
    int32_t m_size;
};

}

#endif
