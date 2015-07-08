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

#include <vector>

#include "common/NValue.hpp"
#include "common/types.h"
#include "common/TupleSchema.h"

#ifndef TUPLESCHEMABUILDER_H_
#define TUPLESCHEMABUILDER_H_

namespace voltdb {

class TupleSchemaBuilder {
public:

    explicit TupleSchemaBuilder(size_t numCols)
        : m_types(numCols)
        , m_sizes(numCols)
        , m_allowNullFlags(numCols)
        , m_inBytesFlags(numCols)
    {
    }

    void setColumnAtIndex(size_t index,
                          ValueType valueType,
                          int32_t colSize,
                          bool allowNull,
                          bool inBytes)
    {
        assert(index < m_types.size());
        m_types[index] = valueType;
        m_sizes[index] = colSize;
        m_allowNullFlags[index] = allowNull;
        m_inBytesFlags[index] = inBytes;
    }

    void setColumnAtIndex(size_t index,
                          ValueType valueType,
                          int32_t colSize,
                          bool allowNull)
    {
        setColumnAtIndex(index,
                         valueType,
                         colSize,
                         allowNull,
                         false); // size not in bytes
    }

    void setColumnAtIndex(size_t index,
                          ValueType valueType,
                          int32_t colSize)
    {
        setColumnAtIndex(index,
                         valueType,
                         colSize,
                         true,   // allow nulls
                         false); // size not in bytes
    }

    void setColumnAtIndex(size_t index,
                          ValueType valueType)
    {
        // sizes for variable length types
        // must be explicitly specified
        assert (valueType != VALUE_TYPE_VARCHAR
                && valueType != VALUE_TYPE_VARBINARY);

        setColumnAtIndex(index, valueType,
                         NValue::getTupleStorageSize(valueType),
                         true,   // allow nulls
                         false); // size not in bytes
    }

    TupleSchema* build() const
    {
        return TupleSchema::createTupleSchema(m_types,
                                              m_sizes,
                                              m_allowNullFlags,
                                              m_inBytesFlags);
    }

private:
    std::vector<ValueType> m_types;
    std::vector<int32_t> m_sizes;
    std::vector<bool> m_allowNullFlags;
    std::vector<bool> m_inBytesFlags;
};

} // end namespace voltdb

#endif // TUPLESCHEMABUILDER_H_
