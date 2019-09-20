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

std::string TableTuple::debugNoHeader() const {
    vassert(m_schema);
    vassert(m_data);
    return debug("");
}

/**
 * Release to the heap any memory allocated for any uninlined columns.
 */
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

/*
 * With a persistent update the copy should only do an allocation for
 * a string if the source and destination pointers are different.
 */
void TableTuple::copyForPersistentUpdate(const TableTuple &source,
        std::vector<char*> &oldObjects, std::vector<char*> &newObjects) {
    vassert(m_schema);
    vassert(m_schema->equals(source.m_schema));
    const int columnCount = m_schema->columnCount();
    const uint16_t uninlineableObjectColumnCount = m_schema->getUninlinedObjectColumnCount();
    /*
     * The source and target tuple have the same policy WRT to
     * inlining strings because a TableTuple used for updating a
     * persistent table uses the same schema as the persistent table.
     */
    if (uninlineableObjectColumnCount > 0) {
        uint16_t uninlineableObjectColumnIndex = 0;
        uint16_t nextUninlineableObjectColumnInfoIndex = m_schema->getUninlinedObjectColumnInfoIndex(0);
        /*
         * Copy each column doing an allocation for string
         * copies. Compare the source and target pointer to see if it
         * is changed in this update. If it is changed then free the
         * old string and copy/allocate the new one from the source.
         */
        for (uint16_t ii = 0; ii < columnCount; ii++) {
            if (ii == nextUninlineableObjectColumnInfoIndex) {
                const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(ii);
                char *       *mPtr = reinterpret_cast<char**>(getWritableDataPtr(columnInfo));
                const TupleSchema::ColumnInfo *sourceColumnInfo = source.getSchema()->getColumnInfo(ii);
                char * const *oPtr = reinterpret_cast<char* const*>(source.getDataPtr(sourceColumnInfo));
                if (*mPtr != *oPtr) {
                    // Make a copy of the input string. Don't want to delete the old string
                    // because it's either from the temp pool or persistently referenced elsewhere.
                    oldObjects.push_back(*mPtr);
                    // TODO: Here, it's known that the column is an object type, and yet
                    // setNValueAllocateForObjectCopies is called to figure this all out again.
                    setNValueAllocateForObjectCopies(ii, source.getNValue(ii));
                    // Yes, uses the same old pointer as two statements ago to get a new value. Neat.
                    newObjects.push_back(*mPtr);
                }
                uninlineableObjectColumnIndex++;
                if (uninlineableObjectColumnIndex < uninlineableObjectColumnCount) {
                    nextUninlineableObjectColumnInfoIndex =
                      m_schema->getUninlinedObjectColumnInfoIndex(uninlineableObjectColumnIndex);
                } else {
                    // This is completely optional -- the value from here on has to be one that can't
                    // be reached by incrementing from the current value.
                    // Zero works, but then again so does the current value.
                    nextUninlineableObjectColumnInfoIndex = 0;
                }
            } else {
                // TODO: Here, it's known that the column value is some kind of scalar or inline, yet
                // setNValueAllocateForObjectCopies is called to figure this all out again.
                // This seriously complicated function is going to boil down to an incremental
                // memcpy of a few more bytes of the tuple.
                // Solution? It would likely be faster even for object-heavy tuples to work in three passes:
                // 1) collect up all the "changed object pointer" offsets.
                // 2) do the same wholesale tuple memcpy as in the no-objects "else" clause, below,
                // 3) replace the object pointer at each "changed object pointer offset"
                //    with a pointer to an object copy of its new referent.
                setNValueAllocateForObjectCopies(ii, source.getNValue(ii));
            }
        }

        // Copy any hidden columns that follow normal visible ones.
        if (m_schema->hiddenColumnCount() > 0) {
            // If we ever add support for uninlined hidden columns,
            // we'll need to do update this code.
            vassert(m_schema->getUninlinedObjectHiddenColumnCount() == 0);
            ::memcpy(m_data + TUPLE_HEADER_SIZE + m_schema->offsetOfHiddenColumns(),
                     source.m_data + TUPLE_HEADER_SIZE + m_schema->offsetOfHiddenColumns(),
                     m_schema->lengthOfAllHiddenColumns());
        }

        // This obscure assignment is propagating the tuple flags rather than leaving it to the caller.
        // TODO: It would be easier for the caller to simply set the values it wants upon return.
        m_data[0] = source.m_data[0];
    } else {
        // copy the tuple flags and the data (all inline/scalars)
        ::memcpy(m_data, source.m_data, m_schema->tupleLength() + TUPLE_HEADER_SIZE);
    }
}

void TableTuple::deserializeFrom(SerializeInputBE& tupleIn, Pool* dataPool,
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
            VOLT_DEBUG("Using default value for caller %d and hidden column %d",
                    caller.getId(), columnInfo->columnType);
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


void TableTuple::deserializeFromDR(SerializeInputLE &tupleIn,  Pool *dataPool) {
    vassert(m_schema);
    vassert(m_data);
    const int32_t columnCount  = m_schema->columnCount();
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;
    const uint8_t *nullArray = reinterpret_cast<const uint8_t*>(tupleIn.getRawPointer(nullMaskLength));

    for (int j = 0; j < columnCount; j++) {
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(j);

        const uint32_t index = j >> 3;
        const uint32_t bit = j % 8;
        const uint8_t mask = (uint8_t) (0x80u >> bit);
        const bool isNull = (nullArray[index] & mask);

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
        if (hiddenColumnInfo->columnType == HiddenColumn::MIGRATE_TXN) {
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

void TableTuple::serializeTo(SerializeOutput &output, const HiddenColumnFilter *filter) const {
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
    output.writeIntAt(start, output.position() - start - sizeof(int32_t));
}

bool TableTuple::equalsNoSchemaCheck(const TableTuple &other,
        const HiddenColumnFilter *hiddenColumnFilter) const {
//    if (address() == other.address()) {
//        return true;
//    }
    for (int ii = 0; ii < m_schema->columnCount(); ii++) {
        const NValue lhs = getNValue(ii);
        const NValue rhs = other.getNValue(ii);
        if (lhs.op_notEquals(rhs).isTrue()) {
            return false;
        }
    }
    if (hiddenColumnFilter != nullptr) {
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

void TableTuple::setAllNulls() {
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
}

void TableTuple::relocateNonInlinedFields(std::ptrdiff_t offset) {
    uint16_t nonInlinedColCount = m_schema->getUninlinedObjectColumnCount();
    for (uint16_t i = 0; i < nonInlinedColCount; i++) {
        uint16_t idx = m_schema->getUninlinedObjectColumnInfoIndex(i);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        vassert(isVariableLengthType(columnInfo->getVoltType()) && !columnInfo->inlined);

        char **dataPtr = reinterpret_cast<char**>(getWritableDataPtr(columnInfo));
        if (*dataPtr != nullptr) {
            (*dataPtr) += offset;
            NValue value = getNValue(idx);
            value.relocateNonInlined(offset);
        }
    }
}

int TableTuple::compare(const TableTuple &other) const {
    const int columnCount = m_schema->columnCount();
    int diff;
    for (int ii = 0; ii < columnCount; ii++) {
        const NValue lhs = getNValue(ii);
        const NValue rhs = other.getNValue(ii);
        diff = lhs.compare(rhs);
        if (diff) {
            return diff;
        }
    }
    return VALUE_COMPARE_EQUAL;
}

/**
 * Compare two tuples. Null value in the rhs tuple will be treated as maximum.
 */
int TableTuple::compareNullAsMax(const TableTuple &other) const {
    const int columnCount = m_schema->columnCount();
    assert(columnCount == other.m_schema->columnCount());
    int diff;
    for (int ii = 0; ii < columnCount; ii++) {
        const NValue& lhs = getNValue(ii);
        const NValue& rhs = other.getNValue(ii);
        diff = lhs.compareNullAsMax(rhs);
        if (diff) {
            return diff;
        }
    }
    return VALUE_COMPARE_EQUAL;
}

}
