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

#include <sstream>
#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "expressions/abstractexpression.h"
#include "plannodes/abstractplannode.h"

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
    return static_cast<int>(
            sizeof(TupleSchema) +
            (uninlineableObjectColumnCount * sizeof(int16_t)) +
            (sizeof(TupleSchema::ColumnInfo) * columnCount) +
            (sizeof(TupleSchema::HiddenColumnInfo) * hiddenColumnCount) +
            sizeof(TupleSchema::ColumnInfoBase));
}

static inline bool isInlineable(ValueType vt, int32_t length, bool inBytes) {
    switch (vt) {
        case ValueType::tVARCHAR:
            if (inBytes) {
                return length < UNINLINEABLE_OBJECT_LENGTH;
            } else {
                return length < UNINLINEABLE_CHARACTER_LENGTH;
            }
        case ValueType::tVARBINARY:
            return length < UNINLINEABLE_OBJECT_LENGTH;

        case ValueType::tGEOGRAPHY:
            return false; // never inlined

        default:
            return true;
    }
}

TupleSchema* TupleSchema::createKeySchema(const std::vector<ValueType>& columnTypes,
        const std::vector<int32_t>& columnSizes, const std::vector<bool>& columnInBytes) {
    std::vector<bool> allowNull(columnTypes.size(), true);
    TupleSchema* schema = createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);
    schema->m_isHeaderless = true;
    return schema;
}

TupleSchema* TupleSchema::createTupleSchemaForTest(const std::vector<ValueType>& columnTypes,
        const std::vector<int32_t>& columnSizes, const std::vector<bool>& allowNull) {
    const std::vector<bool> columnInBytes (allowNull.size(), false);
    return TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);
}

TupleSchema* TupleSchema::createTupleSchema(const std::vector<ValueType>& columnTypes,
        const std::vector<int32_t>& columnSizes,
        const std::vector<bool>& allowNull,
        const std::vector<bool>& columnInBytes) {
    const std::vector<HiddenColumn::Type> hiddenTypes(0);
    return TupleSchema::createTupleSchema(columnTypes,
                                          columnSizes,
                                          allowNull,
                                          columnInBytes,
                                          hiddenTypes);
}

TupleSchema* TupleSchema::createTupleSchema(const std::vector<ValueType>& columnTypes,
        const std::vector<int32_t>&   columnSizes,
        const std::vector<bool>&      allowNull,
        const std::vector<bool>&      columnInBytes,
        const std::vector<HiddenColumn::Type>& hiddenColumnTypes) {
    const uint16_t uninlineableObjectColumnCount =
      TupleSchema::countUninlineableObjectColumns(columnTypes, columnSizes, columnInBytes);
    const uint16_t columnCount = static_cast<uint16_t>(columnTypes.size());
    const uint16_t hiddenColumnCount = static_cast<uint16_t>(hiddenColumnTypes.size());
    vassert(hiddenColumnCount < UNSET_HIDDEN_COLUMN);
    int memSize = memSizeForTupleSchema(columnCount,
                                        uninlineableObjectColumnCount,
                                        hiddenColumnCount);

    // allocate the set amount of memory and cast it to a tuple pointer
    char *data = new char[memSize];
    // clear all the offset values
    ::memset(data, 0, memSize);
    TupleSchema *retval = reinterpret_cast<TupleSchema *>(data);
    ::memset(retval->m_hiddenColumnIndexes, UNSET_HIDDEN_COLUMN, sizeof(retval->m_hiddenColumnIndexes));

    retval->m_columnCount = columnCount;
    retval->m_uninlinedObjectColumnCount = uninlineableObjectColumnCount;
    retval->m_hiddenColumnCount = hiddenColumnCount;
    retval->m_isHeaderless = false;
    uint16_t uninlinedObjectColumnIndex = 0;
    for (uint16_t ii = 0; ii < columnCount; ii++) {
        const ValueType type = columnTypes[ii];
        const uint32_t length = columnSizes[ii];
        const bool columnAllowNull = allowNull[ii];
        const bool inBytes = columnInBytes[ii];
        retval->setColumnMetaData(ii, type, length, columnAllowNull, uninlinedObjectColumnIndex, inBytes);
    }

    for (uint16_t ii = 0; ii < hiddenColumnCount; ++ii) {
        retval->setHiddenColumnMetaData(ii, hiddenColumnTypes[ii]);
    }

    return retval;
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *schema) {
    int memSize = memSizeForTupleSchema(schema->m_columnCount,
                                        schema->m_uninlinedObjectColumnCount,
                                        schema->m_hiddenColumnCount);

    // allocate the set amount of memory and cast it to a tuple pointer
    char *data = new char[memSize];
    ::memcpy(data, schema, memSize);
    return reinterpret_cast<TupleSchema*>(data);
}

TupleSchema* TupleSchema::createTupleSchema(
        const TupleSchema *schema, const std::vector<uint16_t>& set) {
    return createTupleSchema(schema, set, NULL, std::vector<uint16_t>());
}

TupleSchema* TupleSchema::createTupleSchema(const TupleSchema *first,
                                            const TupleSchema *second) {
    vassert(first);
    vassert(second);

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
        const std::vector<uint16_t>& firstSet,
        const TupleSchema *second,
        const std::vector<uint16_t>& secondSet) {
    vassert(first);

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

TupleSchema* TupleSchema::createTupleSchema(
        const std::vector<AbstractExpression *> &exprs) {
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnSizes;
    std::vector<bool> columnAllowNull;
    std::vector<bool> columnInBytes;

    for (auto b = exprs.begin(), e = exprs.end(); b != e; b++) {
        const AbstractExpression *expr = *b;
        columnTypes.push_back(expr->getValueType());
        columnSizes.push_back(expr->getValueSize());
        columnAllowNull.push_back(true);
        columnInBytes.push_back(expr->getInBytes());
    }
    return TupleSchema::createTupleSchema(columnTypes,
                                          columnSizes,
                                          columnAllowNull,
                                          columnInBytes);
}

void TupleSchema::freeTupleSchema(TupleSchema *schema) {
    delete[] reinterpret_cast<char*>(schema);
}

void TupleSchema::setColumnMetaData(uint16_t index, ValueType type, const int32_t length, bool allowNull,
                                    uint16_t &uninlinedObjectColumnIndex, bool inBytes) {
    vassert(length <= COLUMN_MAX_VALUE_LENGTH);
    uint32_t offset = 0;

    // set the type
    ColumnInfo *columnInfo = getColumnInfo(index);
    columnInfo->length = length;
    columnInfo->inBytes = inBytes;

    if (isVariableLengthType(type)) {
        if (length == 0) {
            throwFatalLogicErrorStreamed("Zero length for object type " << valueToString((ValueType)type));
        }

        if (isInlineable(type, length, inBytes)) {
            columnInfo->inlined = true;

            // If the length was specified in characters, convert to bytes.
            int32_t factor = (type == ValueType::tVARCHAR && !inBytes) ? MAX_BYTES_PER_UTF8_CHARACTER : 1;

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
    setColumnMetaDataCommon(index, columnInfo, type, offset, allowNull);
}

void TupleSchema::setHiddenColumnMetaData(uint16_t index, HiddenColumn::Type columnType) {
    HiddenColumnInfo *columnInfo = getHiddenColumnInfo(index);
    columnInfo->columnType = columnType;
    uint16_t absoluteIndex = columnCount() + index;
    vassert(m_hiddenColumnIndexes[columnType] == UNSET_HIDDEN_COLUMN);
    m_hiddenColumnIndexes[columnType] = index;
    switch (columnType) {
        default:
            vassert(false);
            return;
        case HiddenColumn::XDCR_TIMESTAMP:
        case HiddenColumn::VIEW_COUNT:
            setColumnMetaDataCommon(absoluteIndex, columnInfo, ValueType::tBIGINT, 8, false);
            return;
        case HiddenColumn::MIGRATE_TXN:
            setColumnMetaDataCommon(absoluteIndex, columnInfo, ValueType::tBIGINT, 8, true);
            return;
    }
}

void inline TupleSchema::setColumnMetaDataCommon(uint16_t absoluteIndex, ColumnInfoBase *columnInfo, ValueType type, int32_t length, bool allowNull) {
    columnInfo->type = static_cast<char>(type);
    columnInfo->allowNull = allowNull;

    // Set offset of next column appropriately
    getColumnInfoPrivate(absoluteIndex + 1)->offset = columnInfo->offset + length;
    vassert(absoluteIndex == 0 ? columnInfo->offset == 0 : true);
}

std::string TupleSchema::HiddenColumnInfo::debug() const {
    std::ostringstream buffer;
    buffer << "type = " << getTypeName(getVoltType()) << ", "
           << "offset = " << offset << ", "
           << "nullable = " << (allowNull ? "true" : "false") << ", "
           << "column type = " << columnType;
    return buffer.str();
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

    for (int i = 0;i < columnCount(); ++i) {
        const TupleSchema::ColumnInfo* columnInfo = getColumnInfo(i);
        int32_t factor =
            columnInfo->type == static_cast<int>(ValueType::tVARCHAR) && !columnInfo->inBytes ?
            MAX_BYTES_PER_UTF8_CHARACTER : 1;
        if (isVariableLengthType((ValueType)columnInfo->type)) {
            bytes += sizeof(int32_t); // value length placeholder for variable length columns
        }
        bytes += columnInfo->length * factor;
    }

    if (includeHiddenColumns) {
        for (int i = 0; i < hiddenColumnCount(); ++i) {
            const TupleSchema::HiddenColumnInfo* columnInfo = getHiddenColumnInfo(i);
            bytes += NValue::getTupleStorageSize(columnInfo->getVoltType());
        }
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
        const TupleSchema::HiddenColumnInfo *columnInfo = getHiddenColumnInfo(i);
        buffer << " hidden column " << i << ": " << columnInfo->debug() << std::endl;
    }

    buffer << " terminator column offset: "
           << getColumnInfoPrivate(totalColumnCount())->offset << std::endl;

    std::string ret(buffer.str());
    return ret;
}

bool TupleSchema::isCompatibleForMemcpy(const TupleSchema *other, const bool includeHidden) const {
    if (this == other) {
        return true;
    }
    if (other->m_columnCount != m_columnCount || other->m_uninlinedObjectColumnCount != m_uninlinedObjectColumnCount) {
        return false;
    }

    if (includeHidden) {
        if (other->m_hiddenColumnCount != m_hiddenColumnCount || other->tupleLength() != tupleLength()) {
            return false;
        }
    } else if (other->visibleTupleLength() != visibleTupleLength()) {
        return false;
    }

    for (int ii = 0; ii < columnCount(); ii++) {
        const ColumnInfo *columnInfo = getColumnInfo(ii);
        const ColumnInfo *ocolumnInfo = other->getColumnInfo(ii);
        if (columnInfo->offset != ocolumnInfo->offset ||
                columnInfo->type != ocolumnInfo->type ||
                columnInfo->inlined != ocolumnInfo->inlined) {
            return false;
        }
    }

    if (includeHidden) {
        for (int ii = 0; ii < hiddenColumnCount(); ii++) {
            const HiddenColumnInfo *columnInfo = getHiddenColumnInfo(ii);
            const HiddenColumnInfo *ocolumnInfo = other->getHiddenColumnInfo(ii);
            if (columnInfo->offset != ocolumnInfo->offset ||
                    columnInfo->type != ocolumnInfo->type) {
                return false;
            }
        }
    }

    return true;
}

bool TupleSchema::equals(const TupleSchema *other) const {
    // First check for structural equality.
    if ( ! isCompatibleForMemcpy(other)) {
        return false;
    }

    // Finally, rule out behavior differences.
    for (int ii = 0; ii < columnCount(); ii++) {
        const ColumnInfo *columnInfo = getColumnInfo(ii);
        const ColumnInfo *ocolumnInfo = other->getColumnInfo(ii);
        if (columnInfo->allowNull != ocolumnInfo->allowNull) {
            return false;
        }
        // The declared column length for an out-of-line object is a behavior difference
        // that has no effect on tuple format.
        if (! columnInfo->inlined && columnInfo->length != ocolumnInfo->length) {
            return false;
        }
    }

    for (int ii = 0; ii < hiddenColumnCount(); ii++) {
        const HiddenColumnInfo *columnInfo = getHiddenColumnInfo(ii);
        const HiddenColumnInfo *ocolumnInfo = other->getHiddenColumnInfo(ii);
        if (columnInfo->columnType != ocolumnInfo->columnType) {
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
        const std::vector<bool> columnInBytes) {
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
