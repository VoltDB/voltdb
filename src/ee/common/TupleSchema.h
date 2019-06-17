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

#ifndef TUPLESCHEMA_H_
#define TUPLESCHEMA_H_

#include <common/debuglog.h>
#include <cstring>
#include <stdint.h>
#include <string>
#include <iostream>
#include <vector>

#include "common/FatalException.hpp"
#include "common/types.h"

#define UNINLINEABLE_OBJECT_LENGTH 64
#define UNINLINEABLE_CHARACTER_LENGTH 16
#define MAX_BYTES_PER_UTF8_CHARACTER 4

namespace voltdb {

class AbstractExpression;
/**
 * Represents the schema of a tuple or table row. Used to define table rows, as
 * well as index keys. Note: due to arbitrary size embedded array data, this class
 * cannot be created on the stack; all constructors are private.
 *
 * Consider using the helper class TupleSchemaBuilder to create
 * TupleSchema objects.
 */
class TupleSchema {
public:
    // holds per column info
    struct ColumnInfo {
        uint32_t offset;
        uint32_t length;   // does not include length prefix for ObjectTypes
        char type;
        char allowNull;
        bool inlined;      // Stored inside the tuple or outside the tuple.

        bool inBytes;

        inline const ValueType getVoltType() const {
            return static_cast<ValueType>(type);
        }

        std::string debug() const;
    };

    // This needs to keep in synch with the VoltType.MAX_VALUE_LENGTH defined in java.
    enum class_constants { COLUMN_MAX_VALUE_LENGTH = 1048576 };

    /** Static factory method to create a TupleSchema a fixed number
     *  of all visible columns */
    static TupleSchema* createTupleSchema(const std::vector<ValueType>& columnTypes,
                                          const std::vector<int32_t>&   columnSizes,
                                          const std::vector<bool>&      allowNull,
                                          const std::vector<bool>&      columnInBytes);

    /** Static factory method to create a TupleSchema that contains hidden columns */
    static TupleSchema* createTupleSchema(const std::vector<ValueType>& columnTypes,
                                          const std::vector<int32_t>&   columnSizes,
                                          const std::vector<bool>&      allowNull,
                                          const std::vector<bool>&      columnInBytes,
                                          const std::vector<ValueType>& hiddenColumnTypes,
                                          const std::vector<int32_t>&   hiddenColumnSizes,
                                          const std::vector<bool>&      hiddenAllowNull,
                                          const std::vector<bool>&      hiddenColumnInBytes);

    /** Static factory method to create a TupleSchema that contains hidden columns */
    static TupleSchema* createTupleSchema(const std::vector<ValueType>& columnTypes,
                                          const std::vector<int32_t>&   columnSizes,
                                          const std::vector<bool>&      allowNull,
                                          const std::vector<bool>&      columnInBytes,
                                          const std::vector<ValueType>& hiddenColumnTypes,
                                          const std::vector<int32_t>&   hiddenColumnSizes,
                                          const std::vector<bool>&      hiddenAllowNull,
                                          const std::vector<bool>&      hiddenColumnInBytes,
                                          const bool isTableWithStream);

    /** Static factory method to create a TupleSchema for index keys */
    static TupleSchema* createKeySchema(const std::vector<ValueType>&   columnTypes,
                                        const std::vector<int32_t>&     columnSizes,
                                        const std::vector<bool>&        columnInBytes);

    /** A simplified factory method for ease of testing */
    static TupleSchema* createTupleSchemaForTest(const std::vector<ValueType>& columnTypes,
                                                 const std::vector<int32_t>&   columnSizes,
                                                 const std::vector<bool>&     allowNull);

    static TupleSchema* createTupleSchema(const std::vector<AbstractExpression *> &exprs);

    /** Static factory method fakes a copy constructor (will also
     *  duplicate hidden columns) */
    static TupleSchema* createTupleSchema(const TupleSchema *schema);

    /**
     * Static factory method to create a TupleSchema object by copying the
     * specified columns of the given schema.
     * (Hidden column will be omitted.)
     */
    static TupleSchema* createTupleSchema(const TupleSchema *schema,
                                          const std::vector<uint16_t>& set);

    /**
     * Static factory method to create a TupleSchema object by joining the two
     * given TupleSchema objects. The result contains the first TupleSchema
     * followed by the second one.
     *
     * This method has the same limitation that the number of columns of a
     * schema has to be less than or equal to 64.
     *
     * Hidden column will be omitted from the created schema.
     */
    static TupleSchema* createTupleSchema(const TupleSchema *first,
                                          const TupleSchema *second);

    /**
     * Static factory method to create a TupleSchema object by including the
     * specified columns of the two given TupleSchema objects. The result
     * contains only those columns specified in the bitmasks.
     *
     * Hidden columns will be omitted from the created schema.
     */
    static TupleSchema* createTupleSchema(const TupleSchema *first,
                                          const std::vector<uint16_t>& firstSet,
                                          const TupleSchema *second,
                                          const std::vector<uint16_t>& secondSet);

    /** Static factory method to destroy a TupleSchema object. Set to null after this call */
    static void freeTupleSchema(TupleSchema *schema);

    /** Return the number of (visible) columns in the schema for the tuple. */
    inline uint16_t columnCount() const;

    /** Return the number of hidden columns in the schema for the tuple. */
    inline uint16_t hiddenColumnCount() const;

    /** Return true if there is a hidden column on the table with stream. */
    inline bool isTableWithStream() const;

    /** Return true if tuples with this schema do not have an accessible header byte. */
    inline bool isHeaderless() const {
        return m_isHeaderless;
    }

    /** Return the number of bytes used by one tuple. */
    inline uint32_t tupleLength() const;

    size_t getMaxSerializedTupleSize(bool includeHiddenColumns = false) const;

    /** Get a string representation of this schema for debugging */
    std::string debug() const;

    /** Returns the number of variable-length columns that are too long
     * to be inlined into tuple storage. */
    uint16_t getUninlinedObjectColumnCount() const ;

    /** Returns the index of the n-th uninlined column in the
     * column info array. */
    uint16_t getUninlinedObjectColumnInfoIndex(const int objectColumnIndex) const;

    /** Returns the number of variable-length hidden columns that are too long
     * to be inlined into tuple storage.
     *
     * For now this will always return 0, as uninlined hidden columns are not yet
     * supported.  This method exists for debug assertions and to call out places
     * where we'd need to make changes should we ever support this.
     */
    uint16_t getUninlinedObjectHiddenColumnCount() const {
        return m_uninlinedObjectHiddenColumnCount;
    }

    /** Returns true if other TupleSchema is equal to this one.  Both
     *  visible and hidden columns must match for schemas to be
     *  equal. */
    bool equals(const TupleSchema *other) const;

    /* Returns true if number of columns and their data types are the
     * same.  Includes hidden columns. */
    bool isCompatibleForMemcpy(const TupleSchema *other) const;

    /** Returns column info object for columnIndex-th (visible) column.  */
    const ColumnInfo* getColumnInfo(int columnIndex) const;
    ColumnInfo* getColumnInfo(int columnIndex);

    /** Returns the value type for idx-th (visible) column.  */
    ValueType columnType(int idx) const {
        const TupleSchema::ColumnInfo *columnInfo = getColumnInfo(idx);
        return columnInfo->getVoltType();
    }

    /** Returns the inlined-ness for idx-th (visible) column.  */
    bool columnIsInlined(int idx) const {
        const TupleSchema::ColumnInfo *columnInfo = getColumnInfo(idx);
        return columnInfo->inlined;
    }

    /** Returns column info object for columnIndex-th hidden column.  */
    const ColumnInfo* getHiddenColumnInfo(int columnIndex) const;
    ColumnInfo* getHiddenColumnInfo(int columnIndex);

    /** Returns the offset of the first hidden column in the tuple.
     * In debug builds, asserts if there are no hidden columns. */
    size_t offsetOfHiddenColumns() const;

    /** Returns the length of all the hidden columns in the tuple, so
     * that hidden columns can be memcpy'd from one tuple to another.
     * In debug builds, asserts if there are no hidden columns. */
    size_t lengthOfAllHiddenColumns() const;

    uint16_t totalColumnCount() const;

private:

    /** These methods are like their public counterparts, but accepts
     *  indexes >= m_columnCount, in order to access hidden columns or
     *  the terminating ColumnInfo object. */
    ColumnInfo* getColumnInfoPrivate(int columnIndex);
    const ColumnInfo* getColumnInfoPrivate(int columnIndex) const;

    /*
     * Report the actual length in bytes of a column. For inlined strings this will include the two byte length prefix and null terminator.
     */
    uint32_t columnLengthPrivate(const int index) const;

    void setUninlinedObjectColumnInfoIndex(uint16_t objectColumnIndex, uint16_t objectColumnInfoIndex);

    /** Set the type and column size for a column. The columns can be set in any order,
        but it's important to set this data for all columns before any use. Note, the "length"
        param may not be read in some places for some types (like integers), so make sure it
        is correct, or the code will act all wonky. */
    void setColumnMetaData(uint16_t index, ValueType type, int32_t length, bool allowNull,
            uint16_t &uninlinedObjectColumnIndex, bool inBytes);

    /*
     * Returns the number of string columns that can't be inlined.
     */
    static uint16_t countUninlineableObjectColumns(
            std::vector<ValueType> columnTypes,
            std::vector<int32_t> columnSizes,
            std::vector<bool> columnInBytes);

    // can't (shouldn't) call constructors or destructor
    // prevents TupleSchema from being created on the stack
    TupleSchema() {}
    TupleSchema(const TupleSchema &ts) {};
    ~TupleSchema() {}

    static const uint16_t m_uninlinedObjectHiddenColumnCount = 0;

    // number of columns
    uint16_t m_columnCount;
    uint16_t m_uninlinedObjectColumnCount;

    // number of hidden columns
    // currently unlined values in hidden columns are not possible
    uint16_t m_hiddenColumnCount;

    // Whether or not the tuples using this schema have a header byte
    bool m_isHeaderless;

    // has a hidden column for table with stream
    bool m_isTableWithStream;

    /*
     * Data storage for:
     *   - An array of int16_t, containing the 0-based ordinal position
     *       of each non-inlined column
     *   - An array of ColumnInfo objects, in this order:
     *       - normal, visible columns
     *       - hidden columns (must be accessed via getHiddenColumnInfo)
     *       - A terminating ColumnInfo containing the offset of the first byte
     *         after the tuple (i.e., the tuple length)
     */
    char m_data[0];
};

///////////////////////////////////
// METHOD IMPLEMENTATIONS
///////////////////////////////////

inline uint32_t TupleSchema::columnLengthPrivate(const int index) const {
    vassert(index < totalColumnCount());
    const ColumnInfo *columnInfo = getColumnInfoPrivate(index);
    const ColumnInfo *columnInfoPlusOne = getColumnInfoPrivate(index + 1);
    // calculate the real column length in raw bytes
    return static_cast<uint32_t>(columnInfoPlusOne->offset - columnInfo->offset);
}

inline uint16_t TupleSchema::columnCount() const {
    return m_columnCount;
}

inline uint16_t TupleSchema::hiddenColumnCount() const {
    return m_hiddenColumnCount;
}

inline bool TupleSchema::isTableWithStream() const {
    return m_isTableWithStream;
}

inline uint16_t TupleSchema::totalColumnCount() const {
    return static_cast<uint16_t>(m_columnCount + m_hiddenColumnCount);
}

inline uint32_t TupleSchema::tupleLength() const {
    // index "m_columnCount + m_hiddenColumnCount" has the offset for the end of the tuple
    // index "m_columnCount + m_hiddenColumnCount - 1" has the offset for the last hidden column
    // index "m_columnCount - 1" has the offset for the last visible column
    return getColumnInfoPrivate(totalColumnCount())->offset;
}

inline size_t TupleSchema::offsetOfHiddenColumns() const {
    vassert(hiddenColumnCount() > 0);
    return getColumnInfoPrivate(columnCount())->offset;
}

inline size_t TupleSchema::lengthOfAllHiddenColumns() const {
    vassert(hiddenColumnCount() > 0);
    return tupleLength() - offsetOfHiddenColumns();
}


inline const TupleSchema::ColumnInfo* TupleSchema::getColumnInfoPrivate(int columnIndex) const {
    return &reinterpret_cast<const ColumnInfo*>(m_data + (sizeof(uint16_t) * m_uninlinedObjectColumnCount))[columnIndex];
}

inline TupleSchema::ColumnInfo* TupleSchema::getColumnInfoPrivate(int columnIndex) {
    return &reinterpret_cast<ColumnInfo*>(m_data + (sizeof(uint16_t) * m_uninlinedObjectColumnCount))[columnIndex];
}

inline const TupleSchema::ColumnInfo* TupleSchema::getColumnInfo(int columnIndex) const {
    vassert(columnIndex < totalColumnCount());
    return getColumnInfoPrivate(columnIndex);
}

inline TupleSchema::ColumnInfo* TupleSchema::getColumnInfo(int columnIndex) {
    vassert(columnIndex < totalColumnCount());
    return getColumnInfoPrivate(columnIndex);
}

inline const TupleSchema::ColumnInfo* TupleSchema::getHiddenColumnInfo(int hiddenColumnIndex) const {
    vassert(hiddenColumnIndex < m_hiddenColumnCount);
    return getColumnInfoPrivate(m_columnCount + hiddenColumnIndex);
}

inline TupleSchema::ColumnInfo* TupleSchema::getHiddenColumnInfo(int hiddenColumnIndex) {
    vassert(hiddenColumnIndex < m_hiddenColumnCount);
    return getColumnInfoPrivate(m_columnCount + hiddenColumnIndex);
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
