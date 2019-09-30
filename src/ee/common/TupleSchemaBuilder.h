/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
#include <vector>

#include "common/NValue.hpp"
#include "common/types.h"
#include "common/TupleSchema.h"

namespace voltdb {

/** A helper class to create TupleSchema objects.
 * Example:
 *
 *   TupleSchemaBuilder builder(3); // 3 columns
 *   builder.setColumnAtIndex(0, tBIGINT);
 *   builder.setColumnAtIndex(1, tVARCHAR, 32);
 *   builder.setColumnAtIndex(2, tINTEGER);
 *   TupleSchema *schema = builder.build();
 */
class TupleSchemaBuilder {
    std::vector<ValueType> m_types;
    std::vector<int32_t> m_sizes;
    std::vector<bool> m_allowNullFlags;
    std::vector<bool> m_inBytesFlags;
    std::vector<HiddenColumn::Type> m_hiddenTypes;
public:
    /** Create a builder that will build a schema with the given
     * number of columns and hidden columns. */
    TupleSchemaBuilder(size_t numCols, size_t numHiddenCols = 0)
        : m_types(numCols), m_sizes(numCols), m_allowNullFlags(numCols),
        m_inBytesFlags(numCols), m_hiddenTypes(numHiddenCols) {}

    /** Set the attributes of the index-th column for the schema to be
     *  built. */
    TupleSchemaBuilder& setColumnAtIndex(
            size_t index,
            ValueType valueType,
            int32_t colSize,
            bool allowNull = true,
            bool inBytes = false) {
        vassert(index < m_types.size());
        m_types[index] = valueType;
        m_sizes[index] = colSize;
        m_allowNullFlags[index] = allowNull;
        m_inBytesFlags[index] = inBytes;
        return *this;
    }
    TupleSchemaBuilder& setColumnAtIndex(size_t index, ValueType valueType) {
        return setColumnAtIndex(index, valueType, NValue::getTupleStorageSize(valueType));
    }

    /** Set the attributes of the index-th hidden column for the
      *  schema to be built. */
     TupleSchemaBuilder& setHiddenColumnAtIndex(size_t index, HiddenColumn::Type columnType) {
         vassert(index < m_hiddenTypes.size());
         m_hiddenTypes[index] = columnType;
         return *this;
     }
    /** Finally, build the schema with the attributes specified. */
    ScopedTupleSchema build() const {
        return {TupleSchema::createTupleSchema(
                m_types, m_sizes, m_allowNullFlags, m_inBytesFlags, m_hiddenTypes)};
    }

    /** A special build method for index keys, which use "headerless" tuples */
    ScopedTupleSchema buildKeySchema() const {
        return {TupleSchema::createKeySchema(m_types, m_sizes, m_inBytesFlags)};
    }
};

} // end namespace voltdb

