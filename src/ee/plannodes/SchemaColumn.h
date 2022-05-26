/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#pragma once

#include "common/types.h"
#include "common/PlannerDomValue.h"
#include "expressions/abstractexpression.h"

#include "boost/shared_ptr.hpp"

namespace voltdb {

/**
 * Convenience class to deserialize a SchemaColumn object from the JSON
 * and provide common accessors to the contents.  Currently relies on
 * colObject to remain valid; SchemaColumns should not be passed around,
 * stored, or expected to be valid outside the scope of the initial
 * JSON deserialization.
 */
class SchemaColumn {
public:
    SchemaColumn(PlannerDomValue colObject, int idx);
    ~SchemaColumn();

    std::string getColumnName() const;

    // SchemaColumn retains responsibility for the deletion of
    // the expression
    AbstractExpression* getExpression();

private:
    std::string m_tableName;
    std::string m_columnName;
    std::string m_columnAlias;
    AbstractExpression* m_expression;
    ValueType m_type;
    int32_t m_size;
};

}

