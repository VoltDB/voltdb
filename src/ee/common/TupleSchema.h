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

#ifndef TUPLESCHEMA_H_
#define TUPLESCHEMA_H_

#include <cassert>
#include <cstring>
#include <stdint.h>
#include <string>
#include "common/types.h"
#include <iostream>
#include <vector>

#define UNINLINEABLE_OBJECT_LENGTH 64

namespace voltdb {

/**
 * Represents the shcema of a tuple or table row. Used to define table rows, as
 * well as index keys. Note: due to arbitrary size embedded array data, this class
 * cannot be created on the stack; all constructors are private.
 */
class TupleSchema {
public:
    /** Static factory method to create a TupleSchema object with a fixed number of columns */
    static TupleSchema* createTupleSchema(const std::vector<ValueType> columnTypes, const std::vector<int32_t> columnSizes, const std::vector<bool> allowNull, bool allowInlinedStrings);
    /** Static factory method fakes a copy constructor */
    static TupleSchema* createTupleSchema(const TupleSchema *schema);

    /**
     * Static factory method to create a TupleSchema object by copying the
     * specified columns of the given schema.
     */
    static TupleSchema* createTupleSchema(const TupleSchema *schema,
                                          const std::vector<uint16_t> set);

    /**
     * Static factory method to create a TupleSchema object by joining the two
     * given TupleSchema objects. The result contains the first TupleSchema
     * followed by the second one.
     *
     * This method has the same limitation that the number of columns of a
     * schema has to be less than or equal to 64.
     */
    static TupleSchema* createTupleSchema(const TupleSchema *first,
                                          const TupleSchema *second);

    /**
     * Static factory method to create a TupleSchema object by including the
     * specified columns of the two given TupleSchema objects. The result
     * contains only those columns specified in the bitmasks.
     */
    static TupleSchema* createTupleSchema(const TupleSchema *first,
                                          const std::vector<uint16_t> firstSet,
                                          const TupleSchema *second,
                                          const std::vector<uint16_t> secondSet);

    /** Static factory method to destroy a TupleSchema object. Set to null after this call */
    static void freeTupleSchema(TupleSchema *schema);

    /** Get the type of the column at a given index.
        Behavior unpredicatble if invalid index. */
    inline ValueType columnType(int index) const;
    /** Get the nullability of the column at a given index.
        Behavior unpredicatble if invalid index. */
    inline bool columnAllowNull(int index) const;
    inline bool columnIsInlined(const int index) const;
    /** Get the oftset in the tuples bytes of the column at a given index.
        Behavior unpredicatble if invalid index. */
    inline uint32_t columnOffset(int index) const;
    /** Get the length in bytes of the column at a given index.
        Behavior unpredictable if invalid index. Length for inlined
        strings will be the maximum length specified or 8 if string is
        not inlined. */
    inline uint32_t columnLength(int index) const;

    /** Return the number of columns in the schema for the tuple. */
    inline uint16_t columnCount() const;
    /** Return the number of bytes used by one tuple. */
    inline uint32_t tupleLength() const;

    /** Returns a flag indicating whether strings will be inlined in this schema **/
    bool allowInlinedObjects() const;

    /** Get a string representation of this schema for debugging */
    std::string debug() const;

    uint16_t getUninlinedObjectColumnCount() const ;

    uint16_t getUninlinedObjectColumnInfoIndex(const int objectColumnIndex) const;

    bool equals(const TupleSchema *other) const;

private:
    // holds per column info
    struct ColumnInfo {
        uint32_t offset;
        uint32_t length;   // does not include length prefix for ObjectTypes
        char type;
        char allowNull;
        bool inlined;      // Stored inside the tuple or outside the tuple.
    };

    /*
     * Report the actual length in bytes of a column. For inlined strings this will include the two byte length prefix and null terminator.
     */
    uint32_t columnLengthPrivate(const int index) const;

    const ColumnInfo* getColumnInfo(int columnIndex) const;
    ColumnInfo* getColumnInfo(int columnIndex);

    void setUninlinedObjectColumnInfoIndex(uint16_t objectColumnIndex, uint16_t objectColumnInfoIndex);

    /** Set the type and column size for a column. The columns can be set in any order,
        but it's important to set this data for all columns before any use. Note, the "length"
        param may not be read in some places for some types (like integers), so make sure it
        is correct, or the code will act all wonky. */
    void setColumnMetaData(uint16_t index, ValueType type, int32_t length, bool allowNull, uint16_t &uninlinedObjectColumnIndex);

    /*
     * Returns the number of string columns that can't be inlined.
     */
    static uint16_t countUninlineableObjectColumns(
            std::vector<ValueType> columnTypes,
            std::vector<int32_t> columnSizes,
            bool allowInlinedObjects);

    // can't (shouldn't) call constructors or destructor
    // prevents TupleSchema from being created on the stack
    TupleSchema() {}
    TupleSchema(const TupleSchema &ts) {};
    ~TupleSchema() {}
    //Whether this schema allows strings to be inlined
    bool m_allowInlinedObjects;
    // number of columns
    uint16_t m_columnCount;
    uint16_t m_uninlinedObjectColumnCount;

    /*
     * Data storage for column info and for indices of string columns
     */
    char m_data[0];
};

///////////////////////////////////
// METHOD IMPLEMENTATIONS
///////////////////////////////////

inline ValueType TupleSchema::columnType(int index) const {
    assert(index < m_columnCount);
    assert(index > -1);
    const ColumnInfo *columnInfo = getColumnInfo(index);
    return static_cast<ValueType>(columnInfo->type);
}

inline bool TupleSchema::columnAllowNull(int index) const {
    assert(index < m_columnCount);
    const ColumnInfo *columnInfo = getColumnInfo(index);
    return columnInfo->allowNull;
}

inline bool TupleSchema::columnIsInlined(const int index) const {
    assert(index < m_columnCount);
    const ColumnInfo *columnInfo = getColumnInfo(index);
    return columnInfo->inlined;
}

inline uint32_t TupleSchema::columnOffset(int index) const {
    assert(index < m_columnCount);
    const ColumnInfo *columnInfo = getColumnInfo(index);
    return columnInfo->offset;
}

/** Return the column length in DDL terms (do not include length-prefix bytes) */
inline uint32_t TupleSchema::columnLength(const int index) const {
    assert(index < m_columnCount);
    const ColumnInfo *columnInfo = getColumnInfo(index);
    return columnInfo->length;
}

inline uint32_t TupleSchema::columnLengthPrivate(const int index) const {
    assert(index < m_columnCount);
    const ColumnInfo *columnInfo = getColumnInfo(index);
    const ColumnInfo *columnInfoPlusOne = getColumnInfo(index + 1);
    // calculate the real column length in raw bytes
    return static_cast<uint32_t>(columnInfoPlusOne->offset - columnInfo->offset);
}

inline uint16_t TupleSchema::columnCount() const {
    return m_columnCount;
}

inline uint32_t TupleSchema::tupleLength() const {
    // index "m_count" has the offset for the end of the tuple
    // index "m_count-1" has the offset for the last column
    return getColumnInfo(m_columnCount)->offset;
}

inline bool TupleSchema::allowInlinedObjects() const {
    return m_allowInlinedObjects;
}

inline const TupleSchema::ColumnInfo* TupleSchema::getColumnInfo(int columnIndex) const {
    return &reinterpret_cast<const ColumnInfo*>(m_data + (sizeof(uint16_t) * m_uninlinedObjectColumnCount))[columnIndex];
}

inline TupleSchema::ColumnInfo* TupleSchema::getColumnInfo(int columnIndex) {
    return &reinterpret_cast<ColumnInfo*>(m_data + (sizeof(uint16_t) * m_uninlinedObjectColumnCount))[columnIndex];
}

inline uint16_t TupleSchema::getUninlinedObjectColumnCount() const { return m_uninlinedObjectColumnCount; }

inline uint16_t TupleSchema::getUninlinedObjectColumnInfoIndex(const int objectColumnIndex) const {
    return reinterpret_cast<const uint16_t*>(m_data)[objectColumnIndex];
}

inline void TupleSchema::setUninlinedObjectColumnInfoIndex(uint16_t objectColumnIndex, uint16_t objectColumnInfoIndex) {
    reinterpret_cast<uint16_t*>(m_data)[objectColumnIndex] = objectColumnInfoIndex;
}
} // namespace voltdb

#endif // TUPLESCHEMA_H_
