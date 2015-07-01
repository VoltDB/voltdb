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

#include <sstream>
#include <cstdio>
#include "common/TupleSchema.h"
#include "common/NValue.hpp"

namespace voltdb {

TupleSchema* TupleSchema::createTupleSchemaForTest(const std::vector<ValueType> columnTypes,
                                            const std::vector<int32_t> columnSizes,
                                            const std::vector<bool> allowNull)
{
    const std::vector<bool> columnInBytes (allowNull.size(), false);
    return TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);
}

TupleSchema* TupleSchema::createTupleSchema(const std::vector<ValueType> columnTypes,
                                            const std::vector<int32_t> columnSizes,
                                            const std::vector<bool> allowNull,
                                            const std::vector<bool> columnInBytes)
{
    const uint16_t uninlineableObjectColumnCount =
      TupleSchema::countUninlineableObjectColumns(columnTypes, columnSizes, columnInBytes);
    const uint16_t columnCount = static_cast<uint16_t>(columnTypes.size());
    // big enough for any data members plus big enough for tupleCount + 1 "ColumnInfo"
    //  fields. We need CI+1 because we get the length of a column by offset subtraction
    // Also allocate space for an int16_t for each uninlineable object column so that
    // the indices of uninlineable columns can be stored at the front and aid in iteration
    int memSize = (int)(sizeof(TupleSchema) +
                        (sizeof(ColumnInfo) * (columnCount + 1)) +
                        (uninlineableObjectColumnCount * sizeof(int16_t)));

    // allocate the set amount of memory and cast it to a tuple pointer
    TupleSchema *retval = reinterpret_cast<TupleSchema*>(new char[memSize]);

    // clear all the offset values
    memset(retval, 0, memSize);
    retval->m_columnCount = columnCount;
    retval->m_uninlinedObjectColumnCount = uninlineableObjectColumnCount;

    uint16_t uninlinedObjectColumnIndex = 0;
    for (uint16_t ii = 0; ii < columnCount; ii++) {
        const ValueType type = columnTypes[ii];
        const uint32_t length = columnSizes[ii];
        const bool columnAllowNull = allowNull[ii];
        const bool inBytes = columnInBytes[ii];
        retval->setColumnMetaData(ii, type, length, columnAllowNull, uninlinedObjectColumnIndex, inBytes);
    }

    return retval;
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *schema) {
    // big enough for any data members plus big enough for tupleCount + 1 "ColumnInfo"
    //  fields. We need CI+1 because we get the length of a column by offset subtraction
    int memSize =
            (int)(sizeof(TupleSchema) +
                    (sizeof(ColumnInfo) * (schema->m_columnCount + 1)) +
                    (schema->m_uninlinedObjectColumnCount * sizeof(uint16_t)));

    // allocate the set amount of memory and cast it to a tuple pointer
    TupleSchema *retval = reinterpret_cast<TupleSchema*>(new char[memSize]);

    // clear all the offset values
    memcpy(retval, schema, memSize);

    return retval;
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *schema,
                                            const std::vector<uint16_t> set) {
    return createTupleSchema(schema, set, NULL, std::vector<uint16_t>());
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *first,
                                            const TupleSchema *second) {
    assert(first);
    assert(second);

    std::vector<uint16_t> firstSet;
    std::vector<uint16_t> secondSet;

    for (uint16_t i = 0; i < first->columnCount(); i++) {
        firstSet.push_back(i);
    }
    for (uint16_t i = 0; i < second->columnCount(); i++) {
        secondSet.push_back(i);
    }

    return createTupleSchema(first, firstSet, second, secondSet);
}

TupleSchema*
TupleSchema::createTupleSchema(const TupleSchema *first,
                               const std::vector<uint16_t> firstSet,
                               const TupleSchema *second,
                               const std::vector<uint16_t> secondSet) {
    assert(first);

    const std::vector<uint16_t>::size_type offset = firstSet.size();
    const std::vector<uint16_t>::size_type combinedColumnCount = firstSet.size()
        + secondSet.size();
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(combinedColumnCount, true);
    std::vector<bool> columnInBytes(combinedColumnCount, false);
    std::vector<uint16_t>::const_iterator iter;
    for (iter = firstSet.begin(); iter != firstSet.end(); iter++) {
        const TupleSchema::ColumnInfo *columnInfo = first->getColumnInfo(*iter);
        columnTypes.push_back(columnInfo->getVoltType());
        columnLengths.push_back(columnInfo->length);
        columnAllowNull[*iter] = columnInfo->allowNull;
        columnInBytes[*iter] = columnInfo->inBytes;
    }
    for (iter = secondSet.begin(); second && iter != secondSet.end(); iter++) {
        const TupleSchema::ColumnInfo *columnInfo = second->getColumnInfo(*iter);
        columnTypes.push_back(columnInfo->getVoltType());
        columnLengths.push_back(columnInfo->length);
        columnAllowNull[offset + *iter] = columnInfo->allowNull;
        columnInBytes[offset + *iter] = columnInfo->inBytes;
    }

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes,
                                                         columnLengths,
                                                         columnAllowNull,
                                                         columnInBytes);

    // Remember to set the inlineability of each column correctly.
    for (iter = firstSet.begin(); iter != firstSet.end(); iter++) {
        ColumnInfo *info = schema->getColumnInfo(*iter);
        info->inlined = first->getColumnInfo(*iter)->inlined;
    }
    for (iter = secondSet.begin(); second && iter != secondSet.end(); iter++) {
        ColumnInfo *info = schema->getColumnInfo((int)offset + *iter);
        info->inlined = second->getColumnInfo(*iter)->inlined;
    }

    return schema;
}

void TupleSchema::freeTupleSchema(TupleSchema *schema) {
    delete[] reinterpret_cast<char*>(schema);
}

void TupleSchema::setColumnMetaData(uint16_t index, ValueType type, const int32_t length, bool allowNull,
                                    uint16_t &uninlinedObjectColumnIndex, bool inBytes)
{
    assert(length <= COLUMN_MAX_VALUE_LENGTH);
    uint32_t offset = 0;

    // set the type
    ColumnInfo *columnInfo = getColumnInfo(index);
    columnInfo->type = static_cast<char>(type);
    columnInfo->allowNull = (char)(allowNull ? 1 : 0);
    columnInfo->length = length;
    columnInfo->inBytes = inBytes;

    if ((type == VALUE_TYPE_VARCHAR && inBytes) || type == VALUE_TYPE_VARBINARY) {
        if (length == 0) {
            throwFatalLogicErrorStreamed("Zero length for object type " << valueToString((ValueType)type));
        }
        if (length < UNINLINEABLE_OBJECT_LENGTH) {
            /*
             * Inline the string if it is less then UNINLINEABLE_OBJECT_LENGTH bytes.
             */
            columnInfo->inlined = true;
            // One byte to store the size
            offset = static_cast<uint32_t>(length + SHORT_OBJECT_LENGTHLENGTH);
        } else {
            /*
             * Set the length to the size of a String pointer since it won't be inlined.
             */
            offset = static_cast<uint32_t>(NValue::getTupleStorageSize(type));
            columnInfo->inlined = false;
            setUninlinedObjectColumnInfoIndex(uninlinedObjectColumnIndex++, index);
        }
    } else if (type == VALUE_TYPE_VARCHAR) {
        if (length == 0) {
            throwFatalLogicErrorStreamed("Zero length for object type " << valueToString((ValueType)type));
        }
        if (length < UNINLINEABLE_CHARACTER_LENGTH) {
            /*
             * Inline the string if it is less then UNINLINEABLE_CHARACTER_LENGTH characters.
             */
            columnInfo->inlined = true;
            // One byte to store the size
            offset = static_cast<uint32_t>(length * 4 + SHORT_OBJECT_LENGTHLENGTH);
        } else {
            /*
             * Set the length to the size of a String pointer since it won't be inlined.
             */
            offset = static_cast<uint32_t>(NValue::getTupleStorageSize(type));
            columnInfo->inlined = false;
            setUninlinedObjectColumnInfoIndex(uninlinedObjectColumnIndex++, index);
        }
    } else {
        // All values are inlined if they aren't strings.
        columnInfo->inlined = true;
        // don't trust the planner since it can be avoided
        offset = static_cast<uint32_t>(NValue::getTupleStorageSize(type));
    }
    // make the column offsets right for all columns past this one
    int oldsize = columnLengthPrivate(index);
    ColumnInfo *nextColumnInfo = NULL;
    for (int i = index + 1; i <= m_columnCount; i++) {
        nextColumnInfo = getColumnInfo(i);
        nextColumnInfo->offset = static_cast<uint32_t>(nextColumnInfo->offset + offset - oldsize);
    }
    assert(index == 0 ? columnInfo->offset == 0 : true);
}

std::string TupleSchema::debug() const {
    std::ostringstream buffer;

    buffer << "Schema has " << columnCount() << " columns, length = " << tupleLength()
           <<  ", uninlinedObjectColumns "  << m_uninlinedObjectColumnCount << std::endl;

    for (uint16_t i = 0; i < columnCount(); i++) {
        const TupleSchema::ColumnInfo *columnInfo = getColumnInfo(i);

        buffer << " column " << i << ": type = " << getTypeName(columnInfo->getVoltType());
        buffer << ", length = " << columnInfo->length << ", nullable = ";
        buffer << (columnInfo->allowNull ? "true" : "false") << ", isInlined = " << columnInfo->inlined <<  std::endl;
    }

    std::string ret(buffer.str());
    return ret;
}

bool TupleSchema::isCompatibleForCopy(const TupleSchema *other) const
{
    if (this == other) {
        return true;
    }
    if (other->m_columnCount != m_columnCount ||
        other->m_uninlinedObjectColumnCount != m_uninlinedObjectColumnCount ||
        other->tupleLength() != tupleLength()) {
        return false;
    }

    for (int ii = 0; ii < m_columnCount; ii++) {
        const ColumnInfo *columnInfo = getColumnInfo(ii);
        const ColumnInfo *ocolumnInfo = other->getColumnInfo(ii);
        if (columnInfo->offset != ocolumnInfo->offset ||
                columnInfo->type != ocolumnInfo->type ||
                columnInfo->inlined != ocolumnInfo->inlined) {
            return false;
        }
    }

    return true;
}

bool TupleSchema::equals(const TupleSchema *other) const
{
    // First check for structural equality.
    if ( ! isCompatibleForCopy(other)) {
        return false;
    }
    // Finally, rule out behavior differences.
    for (int ii = 0; ii < m_columnCount; ii++) {
        const ColumnInfo *columnInfo = getColumnInfo(ii);
        const ColumnInfo *ocolumnInfo = other->getColumnInfo(ii);
        if (columnInfo->allowNull != ocolumnInfo->allowNull) {
            return false;
        }
        // The declared column length for an out-of-line object is a behavior difference
        // that has no effect on tuple format.
        if (( ! columnInfo->inlined) &&
                (columnInfo->length != ocolumnInfo->length)) {
            return false;
        }
    }
    return true;
}

/*
 * Returns the number of string columns that can't be inlined.
 */
uint16_t TupleSchema::countUninlineableObjectColumns(
        const std::vector<ValueType> columnTypes,
        const std::vector<int32_t> columnSizes,
        const std::vector<bool> columnInBytes)
{
    const uint16_t numColumns = static_cast<uint16_t>(columnTypes.size());
    uint16_t numUninlineableObjects = 0;
    for (int ii = 0; ii < numColumns; ii++) {
        ValueType vt = columnTypes[ii];
        if (vt == VALUE_TYPE_VARCHAR || vt == VALUE_TYPE_VARBINARY) {
            if (columnInBytes[ii] || vt == VALUE_TYPE_VARBINARY) {
                if (columnSizes[ii] >= UNINLINEABLE_OBJECT_LENGTH) {
                    numUninlineableObjects++;
                }
            } else if (columnSizes[ii] >= UNINLINEABLE_CHARACTER_LENGTH) {
                numUninlineableObjects++;
            }
        }
    }
    return numUninlineableObjects;
}

} // namespace voltdb
