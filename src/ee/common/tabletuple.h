/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#pragma once

#include "common/common.h"
#include "common/LoadTableCaller.h"
#include "common/HiddenColumnFilter.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/FatalException.hpp"
#include "common/ExportSerializeIo.h"
#include "common/serializeio.h"

#include <common/debuglog.h>
#include <ostream>
#include <iostream>
#include <vector>
#include <json/json.h>

class CopyOnWriteTest_TestTableTupleFlags;
class TableTupleTest_HeaderDefaults;

namespace voltdb {

#define TUPLE_HEADER_SIZE 1

// Boolean status bits appear in the tuple header, which is the first
// byte of tuple storage.
//
// The default status bits are all zeros:
//   not active
//   not dirty
//   not pending delete
//   not pending delete on undo release
//   inlined variable length data IS volatile
//   non-inlined variable length data IS NOT volatile
#define ACTIVE_MASK                              1
#define DIRTY_MASK                               2
#define PENDING_DELETE_MASK                      4
#define PENDING_DELETE_ON_UNDO_RELEASE_MASK      8
#define INLINED_NONVOLATILE_MASK                16
#define NONINLINED_VOLATILE_MASK                32

class TableColumn;
class TupleIterator;
class ElasticScanner;
class StandAloneTupleStorage;
class SetAndRestorePendingDeleteFlag;


class TableTuple {
    // friend access is intended to allow write access to the tuple flags -- try not to abuse it...
    friend class Table;
    friend class TempTable;
    friend class LargeTempTable;
    friend class LargeTempTableBlock;
    friend class PersistentTable;
    friend class ElasticScanner;
    friend class PoolBackedTupleStorage;
    friend class CopyOnWriteIterator;
    friend class CopyOnWriteContext;
    friend class ::CopyOnWriteTest_TestTableTupleFlags;
    friend class ::TableTupleTest_HeaderDefaults;
    friend class StandAloneTupleStorage; // ... OK, this friend can also update m_schema.
    friend class SetAndRestorePendingDeleteFlag;

public:
    TableTuple() = default;

    /** Copy constructor */
    TableTuple(const TableTuple &rhs) = default;

    /** Setup the tuple given a schema */
    explicit TableTuple(const TupleSchema *schema);

    /** Setup the tuple given the specified data location and schema **/
    TableTuple(char *data, const voltdb::TupleSchema *schema);

    /** Assignment operator */
    TableTuple& operator=(const TableTuple&) = default;

    /**
     * Set the tuple to point toward a given address in a table's
     * backing store
     */
    inline void move(void *address) {
#ifndef  NDEBUG
        if (m_schema == NULL && address != NULL) {
            StackTrace::printStackTrace();
        }
#endif
        vassert(m_schema != NULL || address == NULL);
        m_data = reinterpret_cast<char*> (address);
    }

    /**
     * Set the tuple to point toward a given address and initialize the tuple header to be used as a new tuple
     */
    inline void moveAndInitialize(void *address) {
        move(address);
        resetHeader();
    }

    inline void moveNoHeader(void *address) {
        vassert(m_schema);
        // isActive() and all the other methods expect a header
        m_data = reinterpret_cast<char*> (address) - TUPLE_HEADER_SIZE;
    }

    // Used to wrap read only tuples in indexing code. TODO Remove
    // constedeness from indexing code so this cast isn't necessary.
    inline void moveToReadOnlyTuple(const void *address) {
        vassert(m_schema);
        vassert(address);
        //Necessary to move the pointer back TUPLE_HEADER_SIZE
        // artificially because Tuples used as keys for indexes do not
        // have the header.
        m_data = reinterpret_cast<char*>(const_cast<void*>(address)) - TUPLE_HEADER_SIZE;
    }

    /** Get the address of this tuple in the table's backing store */
    inline char* address() const {
        return m_data;
    }

    /** Return the number of columns in this tuple */
    inline int columnCount() const {
        return m_schema->columnCount();
    }

    bool areAllCollumnsVarAndNull() const {
        int cols = columnCount();
        for (int i = 0; i < cols; ++i) {
            if (!isVarLengthType(i) || !isNull(i)) {
                return false;
            }
        }
        return true;
    }

    /**
        Determine the maximum number of bytes when serialized for Export.
        Excludes the bytes required by the row header (which includes
        the null bit indicators) and ignores the width of metadata cols.
    */
    size_t maxExportSerializationSize() const {
        size_t bytes = 0;
        int cols = columnCount();
        for (int i = 0; i < cols; ++i) {
            bytes += maxExportSerializedColumnSize(i);
        }
        return bytes;
    }

    size_t maxDRSerializationSize() const {
        size_t bytes = maxExportSerializationSize();

        int hiddenCols = m_schema->hiddenColumnCount();
        HiddenColumnFilter filter = HiddenColumnFilter::create(HiddenColumnFilter::EXCLUDE_MIGRATE, m_schema);
        for (int i = 0; i < hiddenCols; ++i) {
            if (filter.include(i)) {
                bytes += maxExportSerializedHiddenColumnSize(i);
            }
        }

        return bytes;
    }

    // return the number of bytes when serialized for regular usage (other
    // than export and DR).
    size_t serializationSize() const {
        size_t bytes = sizeof(int32_t);
        for (int colIdx = 0; colIdx < columnCount(); ++colIdx) {
            bytes += maxSerializedColumnSize(colIdx);
        }
        return bytes;
    }

    /** Return the amount of memory needed to store the non-inlined
        objects in this tuple in persistent, relocatable storage.
        Note that this tuple may be in a temp table, or in a
        persistent table, or not in a table at all. */
    size_t getNonInlinedMemorySizeForPersistentTable() const {
        size_t bytes = 0;
        uint16_t nonInlinedColCount = m_schema->getUninlinedObjectColumnCount();
        for (uint16_t i = 0; i < nonInlinedColCount; i++) {
            uint16_t idx = m_schema->getUninlinedObjectColumnInfoIndex(i);
            const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
            voltdb::ValueType columnType = columnInfo->getVoltType();
            if (isVariableLengthType(columnType) && !columnInfo->inlined) {
                bytes += getNValue(idx).getAllocationSizeForObjectInPersistentStorage();
            }
        }

        return bytes;
    }

    /** Return the amount of memory needed to store the non-inlined
        objects in this tuple in temporary storage.  Note that this
        tuple may be in a temp table, or in a persistent table, or not
        in a table at all. */
    size_t getNonInlinedMemorySizeForTempTable() const {
        size_t bytes = 0;
        uint16_t nonInlinedColCount = m_schema->getUninlinedObjectColumnCount();
        for (uint16_t i = 0; i < nonInlinedColCount; i++) {
            uint16_t idx = m_schema->getUninlinedObjectColumnInfoIndex(i);
            const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
            voltdb::ValueType columnType = columnInfo->getVoltType();
            if (isVariableLengthType(columnType) && !columnInfo->inlined) {
                bytes += getNValue(idx).getAllocationSizeForObjectInTempStorage();
            }
        }

        return bytes;
    }

    /* Utility function to shrink and set given NValue based. Uses data from it's column information to compute
     * the length to shrink the NValue to. This function operates is intended only to be used on variable length
     * columns ot type varchar and varbinary.
     */
    void shrinkAndSetNValue(const int idx, const voltdb::NValue& value) {
        vassert(m_schema);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        vassert(columnInfo);
        const ValueType valueType = columnInfo->getVoltType();
        // shrink is permissible only on variable length column and currently only for varchar and varbinary
        vassert(valueType == ValueType::tVARBINARY || valueType == ValueType::tVARCHAR);
        bool isColumnLngthInBytes = valueType == ValueType::tVARBINARY ? true : columnInfo->inBytes;
        uint32_t columnLength = columnInfo->length;

        // For the given NValue, compute the shrink length in bytes to shrink the nvalue based on
        // current column length. Use the computed shrink length to create new NValue based so that
        // it can fits in current tuple's column

        int32_t nValueLength = 0;
        const char* candidateValueBuffPtr = ValuePeeker::peekObject_withoutNull(value, &nValueLength);
        // compute length for shrinked candidate key
        int32_t neededLength;
        if (isColumnLngthInBytes) {
            neededLength = columnLength;
        } else {
            // column length is defined in characters. Obtain the number of bytes needed for those many characters
            neededLength = static_cast<int32_t> (NValue::getIthCharPosition(
                        candidateValueBuffPtr, nValueLength, columnLength + 1) - candidateValueBuffPtr);
        }
        // create new nvalue using the computed length
        NValue shrinkedNValue = ValueFactory::getTempStringValue(candidateValueBuffPtr, neededLength);
        setNValue(columnInfo, shrinkedNValue, false);
    }

    /*
     * This will put the NValue into this tuple at the idx-th field.
     *
     * If the NValue refers to inlined storage (points to storage
     * interior to some tuple memory), and the storage is not inlined
     * in this tuple, then this will allocate the un-inlined value in
     * the temp string pool.  So, don't use this to update a tuple in
     * a persistent table!
     */
    void setNValue(const int idx, voltdb::NValue const& value) {
        vassert(m_schema);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        setNValue(columnInfo, value, false);
    }


    void setHiddenNValue(const TupleSchema::HiddenColumnInfo *columnInfo,
            voltdb::NValue const& value) {
        char *dataPtr = getWritableDataPtr(columnInfo);
        value.serializeToTupleStorage(dataPtr, false, -1, false, false);
    }

    void setHiddenNValue(const int idx, voltdb::NValue const& value) {
        vassert(m_schema);
        const TupleSchema::HiddenColumnInfo *columnInfo = m_schema->getHiddenColumnInfo(idx);
        setHiddenNValue(columnInfo, value);
    }

    /*
     * Like the above method except for "hidden" fields, not
     * accessible in the normal codepath.
     */


    /*
     * Copies range of NValues from one tuple to another.
     */
    void setNValues(int beginIdx, TableTuple const& lhs, int begin, int end);

    /*
     * Version of setNValue that will allocate space to copy
     * strings that can't be inlined rather then copying the
     * pointer. Used when setting an NValue that will go into
     * permanent storage in a persistent table.  It is also possible
     * to provide NULL for stringPool in which case the strings will
     * be allocated in persistent, relocatable storage.
     * The POOL argument may either be a Pool instance or an instance
     * of a LargeTempTableBlock (Large temp table blocks store
     * non-inlined data in the same buffer as tuples).
     */
    template<class POOL>
    void setNValueAllocateForObjectCopies(const int idx, voltdb::NValue const& value, POOL *dataPool) {
        vassert(m_schema);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        setNValue(columnInfo, value, true, dataPool);
    }

    /** This method behaves very much like the method above except it
        will copy non-inlined objects referenced in the tuple to
        persistent, relocatable storage. */
    void setNValueAllocateForObjectCopies(const int idx, voltdb::NValue const& value) {
        setNValueAllocateForObjectCopies(idx, value, static_cast<Pool*>(NULL));
    }

    /** How long is a tuple? */
    inline int tupleLength() const {
        return m_schema->tupleLength() + TUPLE_HEADER_SIZE;
    }

    /** Is the tuple deleted or active? */
    inline bool isActive() const {
        return *(reinterpret_cast<const char*> (m_data)) & ACTIVE_MASK;
    }

    /** Is the tuple deleted or active? */
    inline bool isDirty() const {
        return *(reinterpret_cast<const char*> (m_data)) & DIRTY_MASK;
    }

    inline bool isPendingDelete() const {
        return *(reinterpret_cast<const char*> (m_data)) & PENDING_DELETE_MASK;
    }

    inline bool isPendingDeleteOnUndoRelease() const {
        return *(reinterpret_cast<const char*> (m_data)) & PENDING_DELETE_ON_UNDO_RELEASE_MASK;
    }

    /** Is variable-length data stored inside the tuple volatile (could data
        change, or could storage be freed)? */
    inline bool inlinedDataIsVolatile() const {
        // This is a little counter-intuitive: If this bit is set to
        // zero, then the inlined variable length data should be
        // considered volatile.
        return !(*(reinterpret_cast<const char*> (m_data)) & INLINED_NONVOLATILE_MASK);
    }

    /** Is variable-length data stored outside the tuple volatile
        (could data change, or could storage be freed)? */
    inline bool nonInlinedDataIsVolatile() const {
        return *(reinterpret_cast<const char*> (m_data)) & NONINLINED_VOLATILE_MASK;
    }

    /** Is the column value null? */
    inline bool isNull(const int idx) const {
        return getNValue(idx).isNull();
    }

    /** Is the hidden column value null? */
    inline bool isHiddenNull(const int idx) const {
        return getHiddenNValue(idx).isNull();
    }

    inline bool isNullTuple() const {
        return m_data == NULL;
    }

    /** Get the value of a specified column (const). */
    inline const NValue getNValue(const int idx) const {
        vassert(m_schema);
        vassert(m_data);
        vassert(idx < m_schema->totalColumnCount());   // column index might point to a hidden column of migrating table

        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        const voltdb::ValueType columnType = columnInfo->getVoltType();
        const char* dataPtr = getDataPtr(columnInfo);
        const bool isInlined = columnInfo->inlined;
        const bool isVolatile = inferVolatility(columnInfo);

        return NValue::initFromTupleStorage(dataPtr, columnType, isInlined, isVolatile);
    }

    /** Like the above method but for hidden columns. */
    inline const NValue getHiddenNValue(const int idx) const {
        vassert(m_schema);
        vassert(m_data);
        vassert(idx < m_schema->hiddenColumnCount());

        const TupleSchema::HiddenColumnInfo *columnInfo = m_schema->getHiddenColumnInfo(idx);
        const voltdb::ValueType columnType = columnInfo->getVoltType();
        const char* dataPtr = getDataPtr(columnInfo);

        return NValue::initFromTupleStorage(dataPtr, columnType, false, false);
    }

    inline const voltdb::TupleSchema* getSchema() const {
        return m_schema;
    }

    inline void setSchema(const TupleSchema * schema) {
        m_schema = schema;
    }

    /** Print out a human readable description of this tuple */
    std::string debug(const std::string& tableName, bool skipNonInline = false) const;

    std::string debug() const {
        return debugNoHeader();
    }

    std::string debugNoHeader() const;

    std::string debugSkipNonInlineData() const {
        return debug("", true);
    }

    std::string toJsonArray() const {
        int totalColumns = columnCount();
        Json::Value array;
        for (int i = 0; i < totalColumns; i++) {
           array.append({getNValue(i).toString()});
        }
        return writeJson(array);
    }

    std::string toJsonString(const std::vector<std::string>& columnNames) const {
        Json::Value object;
        for (int i = 0; i < columnCount(); i++) {
            object[columnNames[i]] = getNValue(i).toString();
        }
        return writeJson(object);
    }

    /** Copy values from one tuple into another.  Any non-inlined
        objects will be copied into the provided instance of Pool, or
        into persistent, relocatable storage if no pool is provided.
        Note that the POOL argument may also be an instance or
        LargeTempTableBlock. */
    template<class POOL>
    void copyForPersistentInsert(const TableTuple &source, POOL *pool);

    /** Similar to the above method except that any non-inlined objects
        will be allocated in persistent, relocatable storage. */
    void copyForPersistentInsert(const TableTuple &source) {
        copyForPersistentInsert(source, static_cast<Pool*>(NULL));
    }

    // The vector "output" arguments detail the non-inline object memory management
    // required of the upcoming release or undo.
    void copyForPersistentUpdate(const TableTuple &source,
                                 std::vector<char*> &oldObjects, std::vector<char*> &newObjects);
    void copy(const TableTuple &source);

    /** this does set NULL in addition to clear string count.*/
    void setAllNulls();

    /** When a large temp table block is reloaded from disk, we need to
        update all addresses pointing to non-inline data. */
    void relocateNonInlinedFields(std::ptrdiff_t offset);

    bool equals(const TableTuple &other) const;
    bool equalsNoSchemaCheck(const TableTuple &other, const HiddenColumnFilter *hiddenColumnFilter = NULL) const;

    int compare(const TableTuple &other) const;
    int compareNullAsMax(const TableTuple &other) const;

    void deserializeFrom(voltdb::SerializeInputBE &tupleIn, Pool *stringPool,
            const LoadTableCaller &caller);
    void deserializeFromDR(voltdb::SerializeInputLE &tupleIn, Pool *stringPool);
    void serializeTo(voltdb::SerializeOutput& output, const HiddenColumnFilter *filter = NULL) const;
    size_t serializeToExport(voltdb::ExportSerializeOutput &io,
                          int colOffset, uint8_t *nullArray) const;
    void serializeToDR(voltdb::ExportSerializeOutput &io, int colOffset, uint8_t *nullArray);

    void freeObjectColumns() const;
    size_t hashCode(size_t seed) const;
    size_t hashCode() const;

private:
    static string writeJson(Json::Value const& val) {
       // ENG-15989: FastWriter is not thread-safe, and therefore cannot be made static.
       Json::FastWriter writer;
       writer.omitEndingLineFeed();
       return writer.write(val);
    }
    inline void setActiveTrue() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) |= static_cast<char>(ACTIVE_MASK);
    }

    inline void setActiveFalse() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) &= static_cast<char>(~ACTIVE_MASK);
    }

    inline void setPendingDeleteOnUndoReleaseTrue() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) |= static_cast<char>(PENDING_DELETE_ON_UNDO_RELEASE_MASK);
    }
    inline void setPendingDeleteOnUndoReleaseFalse() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) &= static_cast<char>(~PENDING_DELETE_ON_UNDO_RELEASE_MASK);
    }

    inline void setPendingDeleteTrue() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) |= static_cast<char>(PENDING_DELETE_MASK);
    }
    inline void setPendingDeleteFalse() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) &= static_cast<char>(~PENDING_DELETE_MASK);
    }

    inline void setDirtyTrue() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) |= static_cast<char>(DIRTY_MASK);
    }
    inline void setDirtyFalse() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) &= static_cast<char>(~DIRTY_MASK);
    }

    /** Mark inlined variable length data in the tuple as subject to
        change or deallocation. */
    inline void setInlinedDataIsVolatileTrue() {
        // This is a little counter-intuitive: If this bit is set to
        // zero, then the inlined variable length data should be
        // considered volatile.
        *(reinterpret_cast<char*> (m_data)) &= static_cast<char>(~INLINED_NONVOLATILE_MASK);
    }

    /** Mark inlined variable length data in the tuple as not subject
        to change or deallocation. */
    inline void setInlinedDataIsVolatileFalse() {
        // Set the bit to 1, indicating that inlined variable-length
        // data is NOT volatile.
        *(reinterpret_cast<char*> (m_data)) |= static_cast<char>(INLINED_NONVOLATILE_MASK);
    }

    /** Mark non-inlined variable length data referenced from the
        tuple as subject to change or deallocation. */
    inline void setNonInlinedDataIsVolatileTrue() {
        *(reinterpret_cast<char*> (m_data)) |= static_cast<char>(NONINLINED_VOLATILE_MASK);
    }

    /** Mark non-inlined variable length data referenced from the
        tuple as not subject to change or deallocation. */
    inline void setNonInlinedDataIsVolatileFalse() {
        *(reinterpret_cast<char*> (m_data)) &= static_cast<char>(~NONINLINED_VOLATILE_MASK);
    }

    inline bool inferVolatility(const TupleSchema::ColumnInfo *colInfo) const {
        if (! isVariableLengthType(colInfo->getVoltType())) {
            // NValue has 16 bytes of storage which can contain all
            // the fixed-length types.
            return false;
        } else if (m_schema->isHeaderless()) {
            // For index keys, there is no header byte to check status.
            return false;
        } else if (colInfo->inlined) {
            return inlinedDataIsVolatile();
        } else {
            return nonInlinedDataIsVolatile();
        }
    }

    inline void resetHeader() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) = 0;
    }

    /** The types of the columns in the tuple */
    const TupleSchema *m_schema = nullptr;

    /**
     * The column data, padded at the front by 8 bytes
     * representing whether the tuple is active or deleted
     */
    char *m_data = nullptr;

    inline char* getWritableDataPtr(const TupleSchema::ColumnInfoBase* colInfo) const {
        vassert(m_schema);
        vassert(m_data);
        return &m_data[TUPLE_HEADER_SIZE + colInfo->offset];
    }

    inline const char* getDataPtr(const TupleSchema::ColumnInfoBase* colInfo) const {
        vassert(m_schema);
        vassert(m_data);
        return &m_data[TUPLE_HEADER_SIZE + colInfo->offset];
    }

    inline size_t serializeColumnToExport(ExportSerializeOutput &io,
            int offset, const NValue &value, uint8_t *nullArray) const {
        // NULL doesn't produce any bytes for the NValue
        // Handle it here to consolidate manipulation of
        // the null array.
        size_t sz = 0;
        if (value.isNull()) {
            // turn on offset'th bit of nullArray
            int byte = offset >> 3;
            int bit = offset % 8;
            int mask = 0x80 >> bit;
            nullArray[byte] = (uint8_t)(nullArray[byte] | mask);
        } else {
            sz = value.serializeToExport_withoutNull(io);
        }
        return sz;
    }

    inline void serializeHiddenColumnsToDR(ExportSerializeOutput &io) const {
        // Exclude the hidden column for persistent table with stream
        uint16_t hiddenColumnCount = m_schema->hiddenColumnCount();
        uint8_t migrateColumn = m_schema->getHiddenColumnIndex(HiddenColumn::MIGRATE_TXN);
        for (int colIdx = 0; colIdx < hiddenColumnCount; colIdx++) {
            if (colIdx != migrateColumn) {
                getHiddenNValue(colIdx).serializeToExport_withoutNull(io);
            }
        }
    }

    inline size_t maxExportSerializedColumnSize(int colIndex) const {
        return maxExportSerializedColumnSizeCommon(colIndex, false);
    }

    inline size_t maxExportSerializedHiddenColumnSize(int colIndex) const {
        return maxExportSerializedColumnSizeCommon(colIndex, true);
    }

    // exclude hidden column since it should nver be var-length type
    inline bool isVarLengthType(int colIndex) const {
        const TupleSchema::ColumnInfoBase *columnInfo = m_schema->getColumnInfo(colIndex);
        ValueType columnType = columnInfo->getVoltType();
        return columnType ==  ValueType::tVARCHAR || columnType ==  ValueType::tVARBINARY || columnType == ValueType::tGEOGRAPHY;
    }

    inline size_t maxExportSerializedColumnSizeCommon(int colIndex, bool isHidden) const {
        const TupleSchema::ColumnInfoBase *columnInfo;
        if (isHidden) {
            columnInfo = m_schema->getHiddenColumnInfo(colIndex);
        } else {
            columnInfo = m_schema->getColumnInfo(colIndex);
        }
        voltdb::ValueType columnType = columnInfo->getVoltType();
        switch (columnType) {
            case ValueType::tTINYINT:
              return sizeof (int8_t);
            case ValueType::tSMALLINT:
              return sizeof (int16_t);
            case ValueType::tINTEGER:
              return sizeof (int32_t);
            case ValueType::tBIGINT:
            case ValueType::tTIMESTAMP:
            case ValueType::tDOUBLE:
              return sizeof (int64_t);
            case ValueType::tDECIMAL:
              //1-byte scale, 1-byte precision, 16 bytes all the time right now
              return 18;
            case ValueType::tVARCHAR:
            case ValueType::tVARBINARY:
            case ValueType::tGEOGRAPHY:
        {
            bool isNullCol = isHidden ? isHiddenNull(colIndex) : isNull(colIndex);
            if (isNullCol) {
                return 0;
              }
              // 32 bit length preceding value and
              // actual character data without null string terminator.
                  const NValue value = isHidden ? getHiddenNValue(colIndex) : getNValue(colIndex);
            int32_t length;
            ValuePeeker::peekObject_withoutNull(value, &length);
            return sizeof(int32_t) + length;
              }
            case ValueType::tPOINT:
              return sizeof (GeographyPointValue);
          default:
            // let caller handle this error
            throwDynamicSQLException(
                    "Unknown ValueType %s found during Export serialization.",
                    valueToString(columnType).c_str() );
            return 0;
        }
    }

    inline size_t maxSerializedColumnSize(int colIndex) const {
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(colIndex);
        voltdb::ValueType columnType = columnInfo->getVoltType();

        if (isVariableLengthType(columnType)) {
            // Null variable length value doesn't take any bytes in
            // export table.
            if (isNull(colIndex)) {
                return sizeof(int32_t);
            }
        } else if (columnType == ValueType::tDECIMAL) {
            // Other than export and DR table, decimal column in regular table
            // doesn't contain scale and precision bytes.
            return 16;
        }
        return maxExportSerializedColumnSize(colIndex);
    }

    /** Write the given NValue into this tuple at the location
        specified by columnInfo.  If allocation of objects is
        requested, then use the provided pool.  If no pool is
        provided, then objects will be copied into persistent,
        relocatable storage.
        Note that the POOL argument may be an instance of
        LargeTempTableBlock which stores tuple data and non-inlined
        objects in the same buffer. */
    template<class POOL>
    void setNValue(const TupleSchema::ColumnInfo *columnInfo, voltdb::NValue value,
            bool allocateObjects, POOL* tempPool) {
        vassert(m_data);
        ValueType columnType = columnInfo->getVoltType();
        value = value.castAs(columnType);
        bool isInlined = columnInfo->inlined;
        bool isInBytes = columnInfo->inBytes;
        char *dataPtr = getWritableDataPtr(columnInfo);
        int32_t columnLength = columnInfo->length;

        // If the nvalue is not to be inlined, we will be storing a
        // pointer in this tuple, and this pointer may be pointing to
        // volatile storage (i.e., a large temp table block).
        //
        // So, if the NValue is volatile, not inlined, and
        // allocateObjects has not been set, mark this tuple as having
        // volatile non-inlined data.
        if (value.getVolatile() && !isInlined && !allocateObjects) {
            setNonInlinedDataIsVolatileTrue();
        }

        value.serializeToTupleStorage(dataPtr, isInlined, columnLength, isInBytes,
                allocateObjects, tempPool);
    }

    /** This method is similar to the above method except no pool is
        provided, so if allocation is requested it will be done in
        persistent, relocatable storage. */
    void setNValue(const TupleSchema::ColumnInfo *columnInfo,
            voltdb::NValue const& value, bool allocateObjects) {
        setNValue<Pool>(columnInfo, value, allocateObjects, nullptr);
    }
};

/**
 * Convenience class for Tuples that get their (inline) storage from a pool.
 * The pool is specified on initial allocation and retained for later reallocations.
 * The tuples can be used like normal tuples except for allocation/reallocation.
 * The caller takes responsibility for consistently using the specialized methods below for that.
 */
class PoolBackedTupleStorage {
    TableTuple m_tuple{};
    Pool* m_pool = nullptr;
public:
    void init(const TupleSchema* schema, Pool* pool) {
        m_tuple.setSchema(schema);
        m_pool = pool;
    }

    void allocateActiveTuple() {
        char* storage = reinterpret_cast<char*>(m_pool->allocateZeroes(
                    m_tuple.getSchema()->tupleLength() + TUPLE_HEADER_SIZE));
        m_tuple.moveAndInitialize(storage);
        m_tuple.setActiveTrue();
        m_tuple.setInlinedDataIsVolatileTrue();
    }

    /** Operator conversion to get an access to the underline tuple.
     * To prevent clients from repointing the tuple to some other backing
     * storage via move()or address() calls the tuple is returned by value
     */
    operator TableTuple& () {
        return m_tuple;
    }
};

// A small class to hold together a standalone tuple (not backed by any table)
// and the associated tuple storage memory to keep the actual data.
// This class will also make a copy of the tuple schema passed in and delete the
// copy in its destructor (since instances of TupleSchema for persistent tables can
// go away in the event of TRUNCATE TABLE).
class StandAloneTupleStorage {
    std::unique_ptr<char[]> m_tupleStorage{};
        TableTuple m_tuple{};
        TupleSchema* m_tupleSchema = nullptr;
    public:
        /** Creates an uninitialized tuple */
        StandAloneTupleStorage() = default;

        /** Allocates enough memory for a given schema
         * and initialies tuple to point to this memory
         */
        explicit StandAloneTupleStorage(const TupleSchema* schema) :
            m_tupleStorage(), m_tuple(), m_tupleSchema(nullptr) {
            init(schema);
        }

        ~StandAloneTupleStorage() {
            TupleSchema::freeTupleSchema(m_tupleSchema);
        }

        /** Allocates enough memory for a given schema
         * and initializes tuple to point to this memory
         */
        void init(const TupleSchema* schema) {
            vassert(schema != NULL);

            // TupleSchema can go away, so copy it here and keep it with our tuple.
            if (m_tupleSchema != NULL) {
                TupleSchema::freeTupleSchema(m_tupleSchema);
            }
            m_tupleSchema = TupleSchema::createTupleSchema(schema);

            // note: apparently array new of the form
            //   new char[N]()
            // will zero-initialize the allocated memory.
            m_tupleStorage.reset(new char[m_tupleSchema->tupleLength() + TUPLE_HEADER_SIZE]());
            m_tuple.m_schema = m_tupleSchema;
            m_tuple.move(m_tupleStorage.get());
            m_tuple.setAllNulls();
            m_tuple.setActiveTrue();
            m_tuple.setInlinedDataIsVolatileTrue();
        }

        /** Get the tuple that this object is wrapping.
         * Returned const ref to avoid corrupting the tuples data and schema pointers
         */
        TableTuple& tuple() {
            return m_tuple;
        }
};

inline TableTuple::TableTuple(const TupleSchema *schema) : m_schema(schema), m_data(NULL) {
    vassert(m_schema);
}

/** Setup the tuple given the specified data location and schema **/
inline TableTuple::TableTuple(char *data, const voltdb::TupleSchema *schema) :
            m_schema(schema), m_data(data){
    vassert(data);
    vassert(schema);
}

/** Multi column version. */
inline void TableTuple::setNValues(int beginIdx, TableTuple const& lhs, int begin, int end) {
    vassert(m_schema);
    vassert(lhs.getSchema());
    vassert(beginIdx + end - begin <= columnCount());
    while (begin != end) {
        setNValue(beginIdx++, lhs.getNValue(begin++));
    }
}

/*
 * With a persistent insert the copy should do an allocation for all non-inlined strings
 */
template<class POOL>
inline void TableTuple::copyForPersistentInsert(const voltdb::TableTuple &source, POOL *pool) {
    copy(source);

    const uint16_t uninlineableObjectColumnCount = m_schema->getUninlinedObjectColumnCount();
    if (uninlineableObjectColumnCount > 0) {

        /*
         * Copy each uninlined string column doing an allocation for string copies.
         */
        for (uint16_t i = 0; i < uninlineableObjectColumnCount; i++) {
            const uint16_t uinlineableObjectColumnIndex =
                    m_schema->getUninlinedObjectColumnInfoIndex(i);
            setNValueAllocateForObjectCopies(uinlineableObjectColumnIndex,
                    source.getNValue(uinlineableObjectColumnIndex),
                    pool);
        }
        m_data[0] = source.m_data[0];
    }
}

/*
 * With a persistent update the copy should only do an allocation for
 * a string if the source and destination pointers are different.
 */
inline void TableTuple::copyForPersistentUpdate(const TableTuple &source,
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

inline void TableTuple::copy(const TableTuple &source) {
    vassert(m_schema);
    vassert(source.m_schema);
    vassert(source.m_data);
    vassert(m_data);

#ifndef NDEBUG
    if(! m_schema->isCompatibleForMemcpy(source.m_schema, false)) {
        std::ostringstream message;
        message << "src  tuple: " << source.debug("") << std::endl;
        message << "src schema: " << source.m_schema->debug() << std::endl;
        message << "dest schema: " << m_schema->debug() << std::endl;
        throwFatalException("%s", message.str().c_str());
    }
#endif
    if (m_schema == source.m_schema) {
        // identical schema so do full raw copy
        ::memcpy(m_data, source.m_data, tupleLength());
    } else {
        // copy the visible data AND the isActive flag
        ::memcpy(m_data, source.m_data, m_schema->visibleTupleLength() + TUPLE_HEADER_SIZE);

        // copy or initialize the hidden columns
        for (int i = 0; i < m_schema->hiddenColumnCount(); ++i) {
            HiddenColumn::Type type = m_schema->getHiddenColumnInfo(i)->columnType;
            uint8_t index = source.m_schema->getHiddenColumnIndex(type);
            NValue value =
                    index == TupleSchema::UNSET_HIDDEN_COLUMN ?
                            HiddenColumn::getDefaultValue(type) : source.getHiddenNValue(index);
            setHiddenNValue(i, value);
        }
    }
}

inline void TableTuple::deserializeFrom(voltdb::SerializeInputBE &tupleIn, Pool *dataPool,
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

inline void TableTuple::deserializeFromDR(voltdb::SerializeInputLE &tupleIn,  Pool *dataPool) {
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

inline void TableTuple::serializeTo(voltdb::SerializeOutput &output, const HiddenColumnFilter *filter) const {
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

inline size_t TableTuple::serializeToExport(ExportSerializeOutput &io,
        int colOffset, uint8_t *nullArray) const {
    size_t sz = 0;
    for (int i = 0; i < columnCount(); i++) {
        sz += serializeColumnToExport(io, colOffset + i, getNValue(i), nullArray);
    }
    return sz;
}

inline void TableTuple::serializeToDR(ExportSerializeOutput &io, int colOffset, uint8_t *nullArray) {
    serializeToExport(io, colOffset, nullArray);
    serializeHiddenColumnsToDR(io);
}

inline bool TableTuple::equals(const TableTuple &other) const {
    if (!m_schema->equals(other.m_schema)) {
        return false;
    }
    return equalsNoSchemaCheck(other);
}

inline bool TableTuple::equalsNoSchemaCheck(const TableTuple &other,
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

inline void TableTuple::setAllNulls() {
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

inline void TableTuple::relocateNonInlinedFields(std::ptrdiff_t offset) {
    uint16_t nonInlinedColCount = m_schema->getUninlinedObjectColumnCount();
    for (uint16_t i = 0; i < nonInlinedColCount; i++) {
        uint16_t idx = m_schema->getUninlinedObjectColumnInfoIndex(i);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        vassert(isVariableLengthType(columnInfo->getVoltType()) && !columnInfo->inlined);

        char **dataPtr = reinterpret_cast<char**>(getWritableDataPtr(columnInfo));
        if (*dataPtr != NULL) {
            (*dataPtr) += offset;
            NValue value = getNValue(idx);
            value.relocateNonInlined(offset);
        }
    }
}

inline int TableTuple::compare(const TableTuple &other) const {
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
inline int TableTuple::compareNullAsMax(const TableTuple &other) const {
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

inline size_t TableTuple::hashCode(size_t seed) const {
    const int columnCount = m_schema->columnCount();
    for (int i = 0; i < columnCount; i++) {
        const NValue value = getNValue(i);
        value.hashCombine(seed);
    }
    return seed;
}

inline size_t TableTuple::hashCode() const {
    size_t seed = 0;
    return hashCode(seed);
}

/**
 * Release to the heap any memory allocated for any uninlined columns.
 */
inline void TableTuple::freeObjectColumns() const {
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

/**
 * Hasher for use with boost::unordered_map and similar
 */
struct TableTupleHasher : std::unary_function<TableTuple, std::size_t> {
    /** Generate a 64-bit number for the key value */
    size_t operator()(TableTuple const& tuple) const {
        return tuple.hashCode();
    }
};

/**
 * Equality operator for use with boost::unrodered_map and similar
 */
class TableTupleEqualityChecker {
public:
    bool operator()(const TableTuple& lhs, const TableTuple& rhs) const {
        return lhs.equalsNoSchemaCheck(rhs);
    }
};

}

