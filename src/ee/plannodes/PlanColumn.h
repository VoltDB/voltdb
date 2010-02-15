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
#ifndef VOLTDBPLANCOLUMN_H
#define VOLTDBPLANCOLUMN_H

#include "common/types.h"
#include "expressions/abstractexpression.h"

#include "boost/shared_ptr.hpp"
#include "json_spirit/json_spirit.h"

#include <string>

namespace voltdb
{

/**
 * Convenience class to deserialize a PlanColumn object from the JSON
 * and provide common accessors to the contents.  Currently relies on
 * colObject to remain valid; PlanColumns should not be passed around,
 * stored, or expected to be valid outside the scope of the initial
 * JSON deserialization.
 */
class PlanColumn
{
public:
    PlanColumn(json_spirit::Object& colObject);

    int getGuid() const;
    std::string getName() const;
    ValueType getType() const;
    uint16_t getSize() const;
    std::string getInputColumnName() const;

    // getExpression lazily evaluates the expression in the JSON
    // object because some expressions (namely aggregates) are currently
    // unhappy, so we only actually do this from places where we know it will
    // succeed.
    AbstractExpression* getExpression();

private:
    const json_spirit::Object& m_colObject;

    int m_guid;
    std::string m_name;
    ValueType m_type;
    uint16_t m_size;
    std::string m_inputColumnName;
};

}

#endif
