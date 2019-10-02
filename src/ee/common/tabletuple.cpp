/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <cstdlib>
#include <sstream>
#include "common/tabletuple.h"

namespace voltdb {

std::string TableTuple::debug(const std::string& tableName, bool skipNonInline) const {
    vassert(m_schema);
    vassert(m_data);

    std::ostringstream buffer;
    if (tableName.empty()) {
       buffer << "TableTuple(no table) ->";
    } else {
       buffer << "TableTuple(" << tableName << ") ->";
    }

    if (isActive() == false) {
       buffer << " <DELETED> ";
    }
    for (int ctr = 0; ctr < m_schema->columnCount(); ctr++) {
        buffer << "(";
        const TupleSchema::ColumnInfo *colInfo = m_schema->getColumnInfo(ctr);
        if (isVariableLengthType(colInfo->getVoltType()) && !colInfo->inlined && skipNonInline) {
            StringRef* sr = *reinterpret_cast<StringRef**>(getWritableDataPtr(colInfo));
            buffer << "<non-inlined value @" << static_cast<void*>(sr) << ">";
        } else {
            buffer << getNValue(ctr).debug();
        }
        buffer << ")";
    }

    if (m_schema->hiddenColumnCount() > 0) {
        buffer << " hidden->";

        for (int ctr = 0; ctr < m_schema->hiddenColumnCount(); ctr++) {
            buffer << "(";
            const TupleSchema::HiddenColumnInfo* colInfo = m_schema->getHiddenColumnInfo(ctr);
            vassert(!isVariableLengthType(colInfo->getVoltType()));
            buffer << getHiddenNValue(ctr).debug() << ")";
        }
    }

    buffer << " @" << static_cast<const void*>(address());

    return buffer.str();
}

TableTuple& TableTuple::setAllNulls() {
    vassert(m_schema);
    vassert(m_data);

    for (int ii = 0; ii < m_schema->columnCount(); ++ii) {
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(ii);
        NValue value = NValue::getNullValue(columnInfo->getVoltType());
        setNValue(columnInfo, value, false);
    }

    for (int jj = 0; jj < m_schema->hiddenColumnCount(); ++jj) {
        const TupleSchema::HiddenColumnInfo *hiddenColumnInfo = m_schema->getHiddenColumnInfo(jj);
        NValue value = NValue::getNullValue(hiddenColumnInfo->getVoltType());
        setHiddenNValue(hiddenColumnInfo, value);
    }
    return *this;
}

std::string TableTuple::debugNoHeader() const {
    vassert(m_schema);
    vassert(m_data);
    return debug("");
}

void TableTuple::deserializeFrom(voltdb::SerializeInputBE &tupleIn, Pool *dataPool,
        const LoadTableCaller &caller) {
    vassert(m_schema);
    vassert(m_data);

    const int32_t columnCount  = m_schema->columnCount();
    const int32_t hiddenColumnCount  = m_schema->hiddenColumnCount();
    tupleIn.readInt();

    // ENG-14346, we may throw SQLException because of a too-wide VARCHAR column.
    // In some systems, the uninitialized StringRef* is not NULL, which may result in
    // unexpected errors during cleanup.
    // This can only happen in the loadTable path, because we check the value length
    // in Java for the normal transaction path.
    // We explicitly initialize the StringRefs for those non-inlined columns here to
    // prevent any surprises.
    uint16_t nonInlinedColCount = m_schema->getUninlinedObjectColumnCount();
    for (uint16_t i = 0; i < nonInlinedColCount; i++) {
        uint16_t idx = m_schema->getUninlinedObjectColumnInfoIndex(i);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        char *dataPtr = getWritableDataPtr(columnInfo);
        *reinterpret_cast<StringRef**>(dataPtr) = NULL;
    }

    for (int j = 0; j < columnCount; ++j) {
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(j);

        /**
         * Hack hack. deserializeFrom is only called when we serialize
         * and deserialize tables. The serialization format for
         * Strings/Objects in a serialized table happens to have the
         * same in memory representation as the Strings/Objects in a
         * tabletuple. The goal here is to wrap the serialized
         * representation of the value in an NValue and then serialize
         * that into the tuple from the NValue. This makes it possible
         * to push more value specific functionality out of
         * TableTuple. The memory allocation will be performed when
         * serializing to tuple storage.
         */
        char *dataPtr = getWritableDataPtr(columnInfo);
        NValue::deserializeFrom(tupleIn, dataPool, dataPtr, columnInfo->getVoltType(),
                columnInfo->inlined, static_cast<int32_t>(columnInfo->length), columnInfo->inBytes);
    }

    for (int j = 0; j < hiddenColumnCount; ++j) {
        const TupleSchema::HiddenColumnInfo *columnInfo = m_schema->getHiddenColumnInfo(j);

        if (caller.useDefaultValue(columnInfo->columnType)) {
            VOLT_DEBUG("Using default value for caller %d and hidden column %d", caller.getId(), columnInfo->columnType);
            setHiddenNValue(columnInfo, HiddenColumn::getDefaultValue(columnInfo->columnType));
        } else { // tupleIn may not have hidden column
            if (!tupleIn.hasRemaining()) {
                throwSerializableEEException(
                        "TableTuple::deserializeFrom table tuple doesn't have enough space to deserialize the hidden column "
                        "(index=%d) hidden column count=%d\n", j, m_schema->hiddenColumnCount());
            }
            char *dataPtr = getWritableDataPtr(columnInfo);
            NValue::deserializeFrom(tupleIn, dataPool, dataPtr, columnInfo->getVoltType(), false, -1, false);
        }
    }
}

void TableTuple::deserializeFromDR(voltdb::SerializeInputLE &tupleIn,  Pool *dataPool) {
    vassert(m_schema);
    vassert(m_data);
    const int32_t columnCount  = m_schema->columnCount();
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;
    const uint8_t *nullArray = reinterpret_cast<const uint8_t*>(tupleIn.getRawPointer(nullMaskLength));

    for (int j = 0; j < columnCount; j++) {
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(j);

        const uint32_t index = j >> 3;
        const uint32_t bit = j % 8;
        const uint8_t mask =  0x80u >> bit;
        const bool isNull = nullArray[index] & mask;

        if (isNull) {
            NValue value = NValue::getNullValue(columnInfo->getVoltType());
            setNValue(j, value);
        } else {
            char *dataPtr = getWritableDataPtr(columnInfo);
            NValue::deserializeFrom<TUPLE_SERIALIZATION_DR, BYTE_ORDER_LITTLE_ENDIAN>(
                    tupleIn, dataPool, dataPtr,
                    columnInfo->getVoltType(), columnInfo->inlined,
                    static_cast<int32_t>(columnInfo->length), columnInfo->inBytes);
        }
    }

    int32_t hiddenColumnCount = m_schema->hiddenColumnCount();

    for (int i = 0; i < hiddenColumnCount; i++) {
        const TupleSchema::HiddenColumnInfo * hiddenColumnInfo = m_schema->getHiddenColumnInfo(i);
        if (hiddenColumnInfo->columnType == HiddenColumn::Type::MIGRATE_TXN) {
            // Set the hidden column for persistent table to null
            NValue value = NValue::getNullValue(hiddenColumnInfo->getVoltType());
            setHiddenNValue(i, value);
        } else {
            char *dataPtr = getWritableDataPtr(hiddenColumnInfo);
            NValue::deserializeFrom<TUPLE_SERIALIZATION_DR, BYTE_ORDER_LITTLE_ENDIAN>(
                    tupleIn, dataPool, dataPtr, hiddenColumnInfo->getVoltType(), false, -1, false);
        }
    }
}

void TableTuple::serializeTo(voltdb::SerializeOutput &output, const HiddenColumnFilter *filter) const {
    size_t start = output.reserveBytes(4);

    for (int j = 0; j < m_schema->columnCount(); ++j) {
        //int fieldStart = output.position();
        NValue value = getNValue(j);
        value.serializeTo(output);
    }

    if (filter) {
        for (int j = 0; j < m_schema->hiddenColumnCount(); ++j) {
            if (filter->include(j)) {
                NValue value = getHiddenNValue(j);
                value.serializeTo(output);
            }
        }
    }
    // write the length of the tuple
    output.writeIntAt(start, static_cast<int32_t>(output.position() - start - sizeof(int32_t)));
}

bool TableTuple::equalsNoSchemaCheck(const TableTuple &other,
        const HiddenColumnFilter *hiddenColumnFilter) const {
    for (int ii = 0; ii < m_schema->columnCount(); ii++) {
        const NValue lhs = getNValue(ii);
        const NValue rhs = other.getNValue(ii);
        if (lhs.op_notEquals(rhs).isTrue()) {
            return false;
        }
    }
    if (hiddenColumnFilter != NULL) {
        for (int ii = 0; ii < m_schema->hiddenColumnCount(); ii++) {
            if (hiddenColumnFilter->include(ii)) {
                const NValue lhs = getHiddenNValue(ii);
                const NValue rhs = other.getHiddenNValue(ii);
                if (lhs.op_notEquals(rhs).isTrue()) {
                    return false;
                }
            }
        }
    }
    return true;
}

void TableTuple::relocateNonInlinedFields(std::ptrdiff_t offset) {
    uint16_t nonInlinedColCount = m_schema->getUninlinedObjectColumnCount();
    for (uint16_t i = 0; i < nonInlinedColCount; i++) {
        uint16_t idx = m_schema->getUninlinedObjectColumnInfoIndex(i);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        vassert(isVariableLengthType(columnInfo->getVoltType()) && !columnInfo->inlined);

        char **dataPtr = reinterpret_cast<char**>(getWritableDataPtr(columnInfo));
        if (*dataPtr != nullptr) {
            *dataPtr += offset;
            NValue value = getNValue(idx);
            value.relocateNonInlined(offset);
        }
    }
}

void TableTuple::freeObjectColumns() const {
    const uint16_t unlinlinedColumnCount = m_schema->getUninlinedObjectColumnCount();
    std::vector<char*> oldObjects;
    for (int ii = 0; ii < unlinlinedColumnCount; ii++) {
        int idx = m_schema->getUninlinedObjectColumnInfoIndex(ii);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        char** dataPtr = reinterpret_cast<char**>(getWritableDataPtr(columnInfo));
        oldObjects.push_back(*dataPtr);
    }
    NValue::freeObjectsFromTupleStorage(oldObjects);
}


}
