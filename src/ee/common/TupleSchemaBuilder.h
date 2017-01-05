/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

/** A helper class to create TupleSchema objects.
 * Example:
 *
 *   TupleSchemaBuilder builder(3); // 3 columns
 *   builder.setColumnAtIndex(0, VALUE_TYPE_BIGINT);
 *   builder.setColumnAtIndex(1, VALUE_TYPE_VARCHAR, 32);
 *   builder.setColumnAtIndex(2, VALUE_TYPE_INTEGER);
 *   TupleSchema *schema = builder.build();
 */
class TupleSchemaBuilder {
public:

    /** Create a builder that will build a schema with the given
     * number of columns. */
    explicit TupleSchemaBuilder(size_t numCols)
        : m_types(numCols)
        , m_sizes(numCols)
        , m_allowNullFlags(numCols)
        , m_inBytesFlags(numCols)
        , m_hiddenTypes(0)
        , m_hiddenSizes(0)
        , m_hiddenAllowNullFlags(0)
        , m_hiddenInBytesFlags(0)
    {
    }

    /** Create a builder that will build a schema with the given
     * number of columns and hidden columns. */
    TupleSchemaBuilder(size_t numCols, size_t numHiddenCols)
        : m_types(numCols)
        , m_sizes(numCols)
        , m_allowNullFlags(numCols)
        , m_inBytesFlags(numCols)
        , m_hiddenTypes(numHiddenCols)
        , m_hiddenSizes(numHiddenCols)
        , m_hiddenAllowNullFlags(numHiddenCols)
        , m_hiddenInBytesFlags(numHiddenCols)
    {
    }

    /** Set the attributes of the index-th column for the schema to be
     *  built. */
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

    /** Set the attributes of the index-th hidden column for the
     *  schema to be built. */
    void setHiddenColumnAtIndex(size_t index,
                                ValueType valueType,
                                int32_t colSize,
                                bool allowNull,
                                bool inBytes)
    {
        assert(index < m_hiddenTypes.size());
        m_hiddenTypes[index] = valueType;
        m_hiddenSizes[index] = colSize;
        m_hiddenAllowNullFlags[index] = allowNull;
        m_hiddenInBytesFlags[index] = inBytes;
    }

    /** Finally, build the schema with the attributes specified. */
    TupleSchema* build() const
    {
        return TupleSchema::createTupleSchema(m_types,
                                              m_sizes,
                                              m_allowNullFlags,
                                              m_inBytesFlags,
                                              m_hiddenTypes,
                                              m_hiddenSizes,
                                              m_hiddenAllowNullFlags,
                                              m_hiddenInBytesFlags);
    }

    /* Below are convenience methods for setting column attributes,
     * with reasonable defaults:
     *   - Size attribute is implied for non-variable-length types
     *   - nullability is true by default
     *   - inBytes flag is false by default
     */

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
        assert (! isVariableLengthType(valueType));

        setColumnAtIndex(index, valueType,
                         NValue::getTupleStorageSize(valueType),
                         true,   // allow nulls
                         false); // size not in bytes
    }

    void setHiddenColumnAtIndex(size_t index,
                                ValueType valueType,
                                int32_t colSize,
                                bool allowNull)
    {
        setHiddenColumnAtIndex(index,
                               valueType,
                               colSize,
                               allowNull,
                               false);  // size not in bytes
    }

    void setHiddenColumnAtIndex(size_t index,
                                ValueType valueType,
                                int32_t colSize)
    {
        setHiddenColumnAtIndex(index,
                               valueType,
                               colSize,
                               true,    // allow nulls
                               false);  // size not in bytes
    }

    void setHiddenColumnAtIndex(size_t index,
                                ValueType valueType)
    {
        // sizes for variable length types
        // must be explicitly specified
        assert (! isVariableLengthType(valueType));

        setHiddenColumnAtIndex(index,
                               valueType,
                               NValue::getTupleStorageSize(valueType),
                               true,    // allow nulls
                               false);  // size not in bytes
    }

private:
    std::vector<ValueType> m_types;
    std::vector<int32_t> m_sizes;
    std::vector<bool> m_allowNullFlags;
    std::vector<bool> m_inBytesFlags;

    std::vector<ValueType> m_hiddenTypes;
    std::vector<int32_t> m_hiddenSizes;
    std::vector<bool> m_hiddenAllowNullFlags;
    std::vector<bool> m_hiddenInBytesFlags;

};

} // end namespace voltdb

#endif // TUPLESCHEMABUILDER_H_
