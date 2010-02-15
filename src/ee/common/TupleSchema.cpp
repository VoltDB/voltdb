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

#include <sstream>
#include <cstdio>
#include "common/TupleSchema.h"
#include "common/NValue.hpp"

namespace voltdb {

TupleSchema* TupleSchema::createTupleSchema(const std::vector<ValueType> columnTypes,
                                            const std::vector<uint16_t> columnSizes,
                                            const std::vector<bool> allowNull,
                                            bool allowInlinedStrings)
{
    const uint16_t uninlineableStringColumnCount =
      TupleSchema::countUninlineableStringColumns(columnTypes, columnSizes, allowInlinedStrings);
    const uint16_t columnCount = static_cast<uint16_t>(columnTypes.size());
    // big enough for any data members plus big enough for tupleCount + 1 "ColumnInfo"
    //  fields. We need CI+1 because we get the length of a column by offset subtraction
    int memSize = (int)(sizeof(TupleSchema) +
                        (sizeof(ColumnInfo) * (columnCount + 1)) +
                        (uninlineableStringColumnCount * sizeof(int16_t)));

    // allocate the set amount of memory and cast it to a tuple pointer
    TupleSchema *retval = reinterpret_cast<TupleSchema*>(new char[memSize]);

    // clear all the offset values
    memset(retval, 0, memSize);
    retval->m_allowInlinedStrings = allowInlinedStrings;
    retval->m_columnCount = columnCount;
    retval->m_uninlinedStringColumnCount = uninlineableStringColumnCount;

    uint16_t uninlinedStringColumnIndex = 0;
    for (uint16_t ii = 0; ii < columnCount; ii++) {
        const ValueType type = columnTypes[ii];
        const uint16_t length = columnSizes[ii];
        const bool columnAllowNull = allowNull[ii];
        retval->setColumnMetaData(ii, type, length, columnAllowNull, uninlinedStringColumnIndex);
    }

    return retval;
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *schema) {
    // big enough for any data members plus big enough for tupleCount + 1 "ColumnInfo"
    //  fields. We need CI+1 because we get the length of a column by offset subtraction
    int memSize = (int)(sizeof(TupleSchema) + (sizeof(ColumnInfo) * (schema->m_columnCount + 1)) + (schema->m_uninlinedStringColumnCount * sizeof(uint16_t)));

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
    std::vector<uint16_t> columnLengths;
    std::vector<bool> columnAllowNull(combinedColumnCount, true);
    std::vector<uint16_t>::const_iterator iter;
    for (iter = firstSet.begin(); iter != firstSet.end(); iter++) {
        columnTypes.push_back(first->columnType(*iter));
        columnLengths.push_back(first->columnLength(*iter));
        columnAllowNull[*iter] = first->columnAllowNull(*iter);
    }
    for (iter = secondSet.begin(); second && iter != secondSet.end(); iter++) {
        columnTypes.push_back(second->columnType(*iter));
        columnLengths.push_back(second->columnLength(*iter));
        columnAllowNull[offset + *iter] = second->columnAllowNull(*iter);
    }

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes,
                                                         columnLengths,
                                                         columnAllowNull,
                                                         true);

    // Remember to set the inlineability of each column correctly.
    for (iter = firstSet.begin(); iter != firstSet.end(); iter++) {
        ColumnInfo *info = schema->getColumnInfo(*iter);
        info->inlined = first->columnIsInlined(*iter);
    }
    for (iter = secondSet.begin(); second && iter != secondSet.end(); iter++) {
        ColumnInfo *info = schema->getColumnInfo((int)offset + *iter);
        info->inlined = second->columnIsInlined(*iter);
    }

    return schema;
}

void TupleSchema::freeTupleSchema(TupleSchema *schema) {
    delete[] reinterpret_cast<char*>(schema);
}

void TupleSchema::setColumnMetaData(uint16_t index, ValueType type, const uint16_t length, bool allowNull,
                                    uint16_t &uninlinedStringColumnIndex)
{
    assert(length < 32768);
    int16_t offset = 0;

    // set the type
    ColumnInfo *columnInfo = getColumnInfo(index);
    columnInfo->type = static_cast<char>(type);
    columnInfo->allowNull = (char)(allowNull ? 1 : 0);
    columnInfo->length = length;
    if (type == VALUE_TYPE_VARCHAR ) {
        if (length < 255 && m_allowInlinedStrings) {
            /*
             * Inline the string if it is less then 255 chars.
             */
            columnInfo->inlined = true;
            // Two bytes to store the size, one for the null terminator
            offset = static_cast<uint16_t>(length + 3);
        } else {
            /*
             * Set the length to the size of a String pointer since it won't be inlined.
             */
            offset = static_cast<uint16_t>(NValue::getTupleStorageSize(VALUE_TYPE_VARCHAR));
            columnInfo->inlined = false;
            setUninlinedStringColumnInfoIndex(uninlinedStringColumnIndex++, index);
        }
    } else {
        // All values are inlined if they aren't strings.
        columnInfo->inlined = true;
        // don't trust the planner since it can be avoided
        offset = static_cast<uint16_t>(NValue::getTupleStorageSize(type));
    }
    // make the column offsets right for all columns past this one
    int oldsize = columnLengthPrivate(index);
    ColumnInfo *nextColumnInfo = NULL;
    for (int i = index + 1; i <= m_columnCount; i++) {
        nextColumnInfo = getColumnInfo(i);
        nextColumnInfo->offset = static_cast<uint16_t>(nextColumnInfo->offset + offset - oldsize);
    }
    assert(index == 0 ? columnInfo->offset == 0 : true);
}

std::string TupleSchema::debug() const {
    std::ostringstream buffer;

    buffer << "Schema has " << columnCount() << " columns, allowInlinedStrings = " << allowInlinedStrings()
           << ", length = " << tupleLength() <<  ", uninlinedStringColumns "  << m_uninlinedStringColumnCount
           << std::endl;

    for (uint16_t i = 0; i < columnCount(); i++) {
        buffer << " column " << i << ": type = " << getTypeName(columnType(i));
        buffer << ", length = " << columnLength(i) << ", nullable = ";
        buffer << (columnAllowNull(i) ? "true" : "false") << ", isInlined = " << columnIsInlined(i) <<  std::endl;
    }

    std::string ret(buffer.str());
    return ret;
}

bool TupleSchema::equals(const TupleSchema *other) const {
    if (other->m_columnCount != m_columnCount ||
        other->m_uninlinedStringColumnCount != m_uninlinedStringColumnCount ||
        other->m_allowInlinedStrings != m_allowInlinedStrings) {
        return false;
    }

    for (int ii = 0; ii < m_columnCount; ii++) {
        const ColumnInfo *columnInfo = getColumnInfo(ii);
        const ColumnInfo *ocolumnInfo = other->getColumnInfo(ii);
        if (columnInfo->allowNull != ocolumnInfo->allowNull ||
                columnInfo->offset != ocolumnInfo->offset ||
                columnInfo->type != ocolumnInfo->type) {
            return false;
        }
    }

    return true;
}

/*
 * Returns the number of string columns that can't be inlined.
 */
uint16_t TupleSchema::countUninlineableStringColumns(
        const std::vector<ValueType> columnTypes,
        const std::vector<uint16_t> columnSizes,
        bool allowInlineStrings) {
    const uint16_t numColumns = static_cast<uint16_t>(columnTypes.size());
    uint16_t numUninlineableStrings = 0;
    for (int ii = 0; ii < numColumns; ii++) {
        if (columnTypes[ii] == VALUE_TYPE_VARCHAR) {
            if (!allowInlineStrings) {
                numUninlineableStrings++;
            } else if (columnSizes[ii] >= 255) {
                numUninlineableStrings++;
            }
        }
    }
    return numUninlineableStrings;
}

} // namespace voltdb
