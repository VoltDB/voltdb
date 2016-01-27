/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

static inline int memSizeForTupleSchema(uint16_t columnCount,
                                        uint16_t uninlineableObjectColumnCount,
                                        uint16_t hiddenColumnCount) {
    // We must allocate enough memory for any data members plus enough
    // for columnCount + hiddenColumnCount + 1 "ColumnInfo" fields.
    // We use the last ColumnInfo object as a placeholder to store the
    // offset of the end of a tuple (that is, the offset of the first
    // byte after the tuple).
    //
    // Also allocate space for an int16_t for each uninlineable object
    // column so that the indices of uninlineable columns can be
    // stored at the front and aid in iteration.
    return static_cast<int>(sizeof(TupleSchema) +
                            (uninlineableObjectColumnCount * sizeof(int16_t)) +
                            (sizeof(TupleSchema::ColumnInfo) * (hiddenColumnCount +
                                                                columnCount + 1)));
}

static inline bool isInlineable(ValueType vt, int32_t length, bool inBytes) {
    switch (vt) {
    case VALUE_TYPE_VARCHAR:
        if (inBytes) {
            return length < UNINLINEABLE_OBJECT_LENGTH;
        }
        else {
            return length < UNINLINEABLE_CHARACTER_LENGTH;
        }

    case VALUE_TYPE_VARBINARY:
        return length < UNINLINEABLE_OBJECT_LENGTH;

    case VALUE_TYPE_GEOGRAPHY:
        return false; // never inlined

    default:
        return true;
    }
}

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
    const std::vector<ValueType> hiddenTypes(0);
    const std::vector<int32_t> hiddenSizes(0);
    const std::vector<bool> hiddenAllowNull(0);
    const std::vector<bool> hiddenColumnInBytes(0);
    return TupleSchema::createTupleSchema(columnTypes,
                                          columnSizes,
                                          allowNull,
                                          columnInBytes,
                                          hiddenTypes,
                                          hiddenSizes,
                                          hiddenAllowNull,
                                          hiddenColumnInBytes);
}

TupleSchema* TupleSchema::createTupleSchema(const std::vector<ValueType> columnTypes,
                                            const std::vector<int32_t>   columnSizes,
                                            const std::vector<bool>      allowNull,
                                            const std::vector<bool>      columnInBytes,
                                            const std::vector<ValueType> hiddenColumnTypes,
                                            const std::vector<int32_t>   hiddenColumnSizes,
                                            const std::vector<bool>      hiddenAllowNull,
                                            const std::vector<bool>      hiddenColumnInBytes)
{
    const uint16_t uninlineableObjectColumnCount =
      TupleSchema::countUninlineableObjectColumns(columnTypes, columnSizes, columnInBytes);
    const uint16_t columnCount = static_cast<uint16_t>(columnTypes.size());
    const uint16_t hiddenColumnCount = static_cast<uint16_t>(hiddenColumnTypes.size());
    int memSize = memSizeForTupleSchema(columnCount,
                                        uninlineableObjectColumnCount,
                                        hiddenColumnCount);

    // allocate the set amount of memory and cast it to a tuple pointer
    TupleSchema *retval = reinterpret_cast<TupleSchema*>(new char[memSize]);

    // clear all the offset values
    memset(retval, 0, memSize);
    retval->m_columnCount = columnCount;
    retval->m_uninlinedObjectColumnCount = uninlineableObjectColumnCount;
    retval->m_hiddenColumnCount = hiddenColumnCount;

    uint16_t uninlinedObjectColumnIndex = 0;
    for (uint16_t ii = 0; ii < columnCount; ii++) {
        const ValueType type = columnTypes[ii];
        const uint32_t length = columnSizes[ii];
        const bool columnAllowNull = allowNull[ii];
        const bool inBytes = columnInBytes[ii];
        retval->setColumnMetaData(ii, type, length, columnAllowNull, uninlinedObjectColumnIndex, inBytes);
    }

    for (uint16_t ii = 0; ii < hiddenColumnCount; ++ii) {
        const ValueType type = hiddenColumnTypes[ii];
        const uint32_t length = hiddenColumnSizes[ii];
        const bool columnAllowNull = hiddenAllowNull[ii];
        const bool inBytes = hiddenColumnInBytes[ii];

        // We can't allow uninlineable data in hidden columns yet
        if (! isInlineable(type, length, inBytes)) {
            throwFatalLogicErrorStreamed("Attempt to create uninlineable hidden column");
        }

        retval->setColumnMetaData(static_cast<uint16_t>(columnCount + ii),
                                  type,
                                  length,
                                  columnAllowNull,
                                  uninlinedObjectColumnIndex,
                                  inBytes);
    }

    return retval;
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *schema) {
    int memSize = memSizeForTupleSchema(schema->m_columnCount,
                                        schema->m_uninlinedObjectColumnCount,
                                        schema->m_hiddenColumnCount);

    // allocate the set amount of memory and cast it to a tuple pointer
    TupleSchema *retval = reinterpret_cast<TupleSchema*>(new char[memSize]);

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
    ColumnInfo *columnInfo = getColumnInfoPrivate(index);
    columnInfo->type = static_cast<char>(type);
    columnInfo->allowNull = (char)(allowNull ? 1 : 0);
    columnInfo->length = length;
    columnInfo->inBytes = inBytes;

    if (isVariableLengthType(type)) {
        if (length == 0) {
            throwFatalLogicErrorStreamed("Zero length for object type " << valueToString((ValueType)type));
        }

        if (isInlineable(type, length, inBytes)) {
            columnInfo->inlined = true;

            // If the length was specified in characters, convert to bytes.
            int32_t factor = (type == VALUE_TYPE_VARCHAR && !inBytes) ? MAX_BYTES_PER_UTF8_CHARACTER : 1;

            // inlined variable length columns have a size prefix (1 byte)
            offset = static_cast<uint32_t>(SHORT_OBJECT_LENGTHLENGTH + (length * factor));
        } else {
            columnInfo->inlined = false;

            // Set the length to the size of a String pointer since it won't be inlined.
            offset = static_cast<uint32_t>(NValue::getTupleStorageSize(type));

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
    for (int i = index + 1; i <= totalColumnCount(); i++) {
        nextColumnInfo = getColumnInfoPrivate(i);
        nextColumnInfo->offset = static_cast<uint32_t>(nextColumnInfo->offset + offset - oldsize);
    }
    assert(index == 0 ? columnInfo->offset == 0 : true);
}

std::string TupleSchema::ColumnInfo::debug() const {
    std::ostringstream buffer;
    buffer << "type = " << getTypeName(getVoltType()) << ", "
           << "offset = " << offset << ", "
           << "length = " << length << ", "
           << "nullable = " << (allowNull ? "true" : "false") << ", "
           << "isInlined = " << inlined;
    return buffer.str();
}

size_t TupleSchema::getMaxSerializedTupleSize(bool includeHiddenColumns) const {
    size_t bytes = sizeof(int32_t); // placeholder for tuple length
    int serializeColumnCount = m_columnCount;
    if (includeHiddenColumns) {
        serializeColumnCount += m_hiddenColumnCount;
    }

    for (int i = 0;i < serializeColumnCount; ++i) {
        const TupleSchema::ColumnInfo* columnInfo = getColumnInfoPrivate(i);
        int32_t factor = (columnInfo->type == VALUE_TYPE_VARCHAR && !columnInfo->inBytes) ? MAX_BYTES_PER_UTF8_CHARACTER : 1;
        if (isVariableLengthType((ValueType)columnInfo->type)) {
            bytes += sizeof(int32_t); // value length placeholder for variable length columns
        }
        bytes += columnInfo->length * factor;
    }

    return bytes;
}

std::string TupleSchema::debug() const {
    std::ostringstream buffer;

    buffer << "Schema has "
           << columnCount() << " columns, "
           << hiddenColumnCount() << " hidden columns, "
           << "length = " << tupleLength() << ", "
           <<  "uninlinedObjectColumns "  << m_uninlinedObjectColumnCount << std::endl;

    for (uint16_t i = 0; i < columnCount(); i++) {
        const TupleSchema::ColumnInfo *columnInfo = getColumnInfo(i);
        buffer << " column " << i << ": " << columnInfo->debug() << std::endl;
    }

    for (uint16_t i = 0; i < hiddenColumnCount(); i++) {
        const TupleSchema::ColumnInfo *columnInfo = getHiddenColumnInfo(i);
        buffer << " hidden column " << i << ": " << columnInfo->debug() << std::endl;
    }

    buffer << " terminator column info: "
           << getColumnInfoPrivate(totalColumnCount())->debug() << std::endl;

    std::string ret(buffer.str());
    return ret;
}

bool TupleSchema::isCompatibleForMemcpy(const TupleSchema *other) const
{
    if (this == other) {
        return true;
    }
    if (other->m_columnCount != m_columnCount ||
        other->m_hiddenColumnCount != m_hiddenColumnCount ||
        other->m_uninlinedObjectColumnCount != m_uninlinedObjectColumnCount ||
        other->tupleLength() != tupleLength()) {
        return false;
    }

    for (int ii = 0; ii < totalColumnCount(); ii++) {
        const ColumnInfo *columnInfo = getColumnInfoPrivate(ii);
        const ColumnInfo *ocolumnInfo = other->getColumnInfoPrivate(ii);
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
    if ( ! isCompatibleForMemcpy(other)) {
        return false;
    }

    // Finally, rule out behavior differences.
    for (int ii = 0; ii < totalColumnCount(); ii++) {
        const ColumnInfo *columnInfo = getColumnInfoPrivate(ii);
        const ColumnInfo *ocolumnInfo = other->getColumnInfoPrivate(ii);
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
 * Returns the number of variable-length columns that can't be inlined.
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
        if (! isInlineable(vt, columnSizes[ii], columnInBytes[ii])) {
            numUninlineableObjects++;
        }
    }
    return numUninlineableObjects;
}

} // namespace voltdb
