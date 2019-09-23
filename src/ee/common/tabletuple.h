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

#include <common/debuglog.h>
#include <ostream>
#include <iostream>
#include <vector>
#include <json/json.h>

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

class TableTuple {
    /** The types of the columns in the tuple */
    const TupleSchema *m_schema = nullptr;
    /**
     * The column data, padded at the front by 8 bytes
     * representing whether the tuple is active or deleted
     */
    char *m_data = nullptr;
public:
    TableTuple() = default;

    /** Copy constructor */
    TableTuple(const TableTuple &rhs) = default;

    /** Setup the tuple given a schema */
    explicit TableTuple(const TupleSchema *schema);

    /** Setup the tuple given the specified data location and schema **/
    TableTuple(char *data, const TupleSchema *schema);

    /** Assignment operator */
    TableTuple& operator=(const TableTuple&) = default;

    /**
     * Set the tuple to point toward a given address in a table's
     * backing store
     */
    void move(void *address) {
        vassert(m_schema != nullptr || address == nullptr);
        m_data = reinterpret_cast<char*>(address);
    }

    void resetHeader() {
        // treat the first "value" as a boolean flag
        *m_data = 0;
    }

    void setActiveTrue() {
        // treat the first "value" as a boolean flag
        *m_data |= static_cast<char>(ACTIVE_MASK);
    }

    void setActiveFalse() {
        *m_data &= static_cast<char>(~ACTIVE_MASK);
    }

    /** Mark inlined variable length data in the tuple as subject to
        change or deallocation. */
    void setInlinedDataIsVolatileTrue() {
        // This is a little counter-intuitive: If this bit is set to
        // zero, then the inlined variable length data should be
        // considered volatile.
        *m_data &= static_cast<char>(~INLINED_NONVOLATILE_MASK);
    }

    /** Mark inlined variable length data in the tuple as not subject
        to change or deallocation. */
    void setInlinedDataIsVolatileFalse() {
        // Set the bit to 1, indicating that inlined variable-length
        // data is NOT volatile.
        *m_data |= static_cast<char>(INLINED_NONVOLATILE_MASK);
    }

    /** Mark non-inlined variable length data referenced from the
        tuple as subject to change or deallocation. */
    void setNonInlinedDataIsVolatileTrue() {
        *m_data |= static_cast<char>(NONINLINED_VOLATILE_MASK);
    }

    /** Mark non-inlined variable length data referenced from the
        tuple as not subject to change or deallocation. */
    void setNonInlinedDataIsVolatileFalse() {
        *m_data &= static_cast<char>(~NONINLINED_VOLATILE_MASK);
    }

    void setPendingDeleteTrue() {
        // treat the first "value" as a boolean flag
        *m_data |= static_cast<char>(PENDING_DELETE_MASK);
    }
    void setPendingDeleteFalse() {
        // treat the first "value" as a boolean flag
        *m_data &= static_cast<char>(~PENDING_DELETE_MASK);
    }

    void setPendingDeleteOnUndoReleaseTrue() {
        // treat the first "value" as a boolean flag
        *m_data |= static_cast<char>(PENDING_DELETE_ON_UNDO_RELEASE_MASK);
    }
    void setPendingDeleteOnUndoReleaseFalse() {
        // treat the first "value" as a boolean flag
        *m_data &= static_cast<char>(~PENDING_DELETE_ON_UNDO_RELEASE_MASK);
    }

    void setDirtyTrue() {
        // treat the first "value" as a boolean flag
        *m_data |= static_cast<char>(DIRTY_MASK);
    }
    void setDirtyFalse() {
        // treat the first "value" as a boolean flag
        *m_data &= static_cast<char>(~DIRTY_MASK);
    }

    void moveNoHeader(void const*address) {
        vassert(m_schema);
        // isActive() and all the other methods expect a header
        m_data = reinterpret_cast<char*>(const_cast<void*>(address)) -
                TUPLE_HEADER_SIZE;
    }

    /** Get the address of this tuple in the table's backing store */
    char* address() const {
        return m_data;
    }

    /** Return the number of columns in this tuple */
    int columnCount() const {
        return m_schema->columnCount();
    }

    /**
        Determine the maximum number of bytes when serialized for Export.
        Excludes the bytes required by the row header (which includes
        the null bit indicators) and ignores the width of metadata cols.
    */
    size_t maxExportSerializationSize() const;
    size_t maxDRSerializationSize() const;

    // return the number of bytes when serialized for regular usage (other
    // than export and DR).
    size_t serializationSize() const;

    /** Return the amount of memory needed to store the non-inlined
        objects in this tuple in persistent, relocatable storage.
        Note that this tuple may be in a temp table, or in a
        persistent table, or not in a table at all. */
    size_t getNonInlinedMemorySizeForPersistentTable() const;

    /** Return the amount of memory needed to store the non-inlined
        objects in this tuple in temporary storage.  Note that this
        tuple may be in a temp table, or in a persistent table, or not
        in a table at all. */
    size_t getNonInlinedMemorySizeForTempTable() const;

    /* Utility function to shrink and set given NValue based. Uses data from it's column information to compute
     * the length to shrink the NValue to. This function operates is intended only to be used on variable length
     * columns ot type varchar and varbinary.
     */
    void shrinkAndSetNValue(const int idx, const NValue& value);

    /*
     * This will put the NValue into this tuple at the idx-th field.
     *
     * If the NValue refers to inlined storage (points to storage
     * interior to some tuple memory), and the storage is not inlined
     * in this tuple, then this will allocate the un-inlined value in
     * the temp string pool.  So, don't use this to update a tuple in
     * a persistent table!
     */
    void setNValue(const int idx, NValue const& value) {
        vassert(m_schema);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        setNValue(columnInfo, value, false);
    }

    void setHiddenNValue(const TupleSchema::HiddenColumnInfo *columnInfo,
            NValue const& value) {
        char *dataPtr = getWritableDataPtr(columnInfo);
        value.serializeToTupleStorage(dataPtr, false, -1, false, false);
    }

    void setHiddenNValue(const int idx, NValue const& value) {
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
    void setNValueAllocateForObjectCopies(const int idx, NValue const& value, POOL *dataPool) {
        vassert(m_schema);
        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        setNValue(columnInfo, value, true, dataPool);
    }

    /** This method behaves very much like the method above except it
        will copy non-inlined objects referenced in the tuple to
        persistent, relocatable storage. */
    void setNValueAllocateForObjectCopies(const int idx, NValue const& value) {
        setNValueAllocateForObjectCopies(idx, value, static_cast<Pool*>(NULL));
    }

    /** How long is a tuple? */
    int tupleLength() const {
        return m_schema->tupleLength() + TUPLE_HEADER_SIZE;
    }

    /** Is the tuple deleted or active? */
    bool isActive() const {
        return *m_data & ACTIVE_MASK;
    }

    /** Is the tuple deleted or active? */
    bool isDirty() const {
        return *m_data & DIRTY_MASK;
    }

    bool isPendingDelete() const {
        return *m_data & PENDING_DELETE_MASK;
    }

    bool isPendingDeleteOnUndoRelease() const {
        return *m_data & PENDING_DELETE_ON_UNDO_RELEASE_MASK;
    }

    /** Is variable-length data stored inside the tuple volatile (could data
        change, or could storage be freed)? */
    bool inlinedDataIsVolatile() const {
        // This is a little counter-intuitive: If this bit is set to
        // zero, then the inlined variable length data should be
        // considered volatile.
        return !(*m_data & INLINED_NONVOLATILE_MASK);
    }

    /** Is variable-length data stored outside the tuple volatile
        (could data change, or could storage be freed)? */
    bool nonInlinedDataIsVolatile() const {
        return *m_data & NONINLINED_VOLATILE_MASK;
    }

    /** Is the column value null? */
    bool isNull(const int idx) const {
        return getNValue(idx).isNull();
    }

    /** Is the hidden column value null? */
    bool isHiddenNull(const int idx) const {
        return getHiddenNValue(idx).isNull();
    }

    bool isNullTuple() const {
        return m_data == nullptr;
    }

    /** Get the value of a specified column (const). */
    const NValue getNValue(const int idx) const {
        vassert(m_schema);
        vassert(m_data);
        vassert(idx < m_schema->totalColumnCount());   // column index might point to a hidden column of migrating table

        const TupleSchema::ColumnInfo *columnInfo = m_schema->getColumnInfo(idx);
        const ValueType columnType = columnInfo->getVoltType();
        const char* dataPtr = getDataPtr(columnInfo);
        const bool isInlined = columnInfo->inlined;
        const bool isVolatile = inferVolatility(columnInfo);
        return NValue::initFromTupleStorage(dataPtr, columnType, isInlined, isVolatile);
    }

    /** Like the above method but for hidden columns. */
    const NValue getHiddenNValue(const int idx) const {
        vassert(m_schema);
        vassert(m_data);
        vassert(idx < m_schema->hiddenColumnCount());
        const TupleSchema::HiddenColumnInfo *columnInfo = m_schema->getHiddenColumnInfo(idx);
        const ValueType columnType = columnInfo->getVoltType();
        const char* dataPtr = getDataPtr(columnInfo);
        return NValue::initFromTupleStorage(dataPtr, columnType, false, false);
    }

    const TupleSchema* getSchema() const {
        return m_schema;
    }

    void setSchema(const TupleSchema* schema) {
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

    std::string toJsonArray() const;
    std::string toJsonString(const std::vector<std::string>& columnNames) const;

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
        copyForPersistentInsert(source, static_cast<Pool*>(nullptr));
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
    bool equalsNoSchemaCheck(const TableTuple &other,
            const HiddenColumnFilter *hiddenColumnFilter = nullptr) const;

    int compare(const TableTuple &other) const;
    int compareNullAsMax(const TableTuple &other) const;

    void deserializeFrom(SerializeInputBE &tupleIn, Pool *stringPool,
            const LoadTableCaller &caller);
    void deserializeFromDR(SerializeInputLE &tupleIn, Pool *stringPool);
    void serializeTo(SerializeOutput& output, const HiddenColumnFilter *filter = NULL) const;
    size_t serializeToExport(ExportSerializeOutput &io, int colOffset, uint8_t *nullArray) const;
    void serializeToDR(ExportSerializeOutput &io, int colOffset, uint8_t *nullArray);

    void freeObjectColumns() const;
    size_t hashCode(size_t seed = 0) const;

private:
    static string writeJson(Json::Value const& val) {
       // ENG-15989: FastWriter is not thread-safe, and therefore cannot be made static.
       Json::FastWriter writer;
       writer.omitEndingLineFeed();
       return writer.write(val);
    }

    bool inferVolatility(const TupleSchema::ColumnInfo *colInfo) const {
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

    char* getWritableDataPtr(const TupleSchema::ColumnInfoBase* colInfo) const {
        vassert(m_schema);
        vassert(m_data);
        return &m_data[TUPLE_HEADER_SIZE + colInfo->offset];
    }

    const char* getDataPtr(const TupleSchema::ColumnInfoBase* colInfo) const {
        vassert(m_schema);
        vassert(m_data);
        return &m_data[TUPLE_HEADER_SIZE + colInfo->offset];
    }

    size_t serializeColumnToExport(ExportSerializeOutput &io,
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

    void serializeHiddenColumnsToDR(ExportSerializeOutput &io) const;

    size_t maxExportSerializedColumnSize(int colIndex) const {
        return maxExportSerializedColumnSizeCommon(colIndex, false);
    }

    size_t maxExportSerializedHiddenColumnSize(int colIndex) const {
        return maxExportSerializedColumnSizeCommon(colIndex, true);
    }

    size_t maxExportSerializedColumnSizeCommon(int colIndex, bool isHidden) const;
    size_t maxSerializedColumnSize(int colIndex) const;

    /** Write the given NValue into this tuple at the location
        specified by columnInfo.  If allocation of objects is
        requested, then use the provided pool.  If no pool is
        provided, then objects will be copied into persistent,
        relocatable storage.
        Note that the POOL argument may be an instance of
        LargeTempTableBlock which stores tuple data and non-inlined
        objects in the same buffer. */
    template<class POOL>
    void setNValue(const TupleSchema::ColumnInfo *columnInfo, NValue value,
            bool allocateObjects, POOL* tempPool) {
        vassert(m_data);
        value = value.castAs(columnInfo->getVoltType());
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
            NValue const& value, bool allocateObjects) {
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
        m_tuple.move(storage);
        m_tuple.resetHeader();
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
        vassert(schema != nullptr);

        // TupleSchema can go away, so copy it here and keep it with our tuple.
        if (m_tupleSchema != nullptr) {
            TupleSchema::freeTupleSchema(m_tupleSchema);
        }
        m_tupleSchema = TupleSchema::createTupleSchema(schema);

        // note: apparently array new of the form
        //   new char[N]()
        // will zero-initialize the allocated memory.
        m_tupleStorage.reset(new char[m_tupleSchema->tupleLength() + TUPLE_HEADER_SIZE]());
        m_tuple.setSchema(m_tupleSchema);
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
    TableTuple const& tuple() const {
        return m_tuple;
    }
};

inline TableTuple::TableTuple(const TupleSchema *schema) : m_schema(schema) {
    vassert(m_schema);
}

/** Setup the tuple given the specified data location and schema **/
inline TableTuple::TableTuple(char *data, const TupleSchema *schema) :
            m_schema(schema), m_data(data) {
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
inline void TableTuple::copyForPersistentInsert(const TableTuple &source, POOL *pool) {
    vassert(m_schema);
    vassert(source.m_schema);
    vassert(source.m_data);
    vassert(m_data);

    const uint16_t uninlineableObjectColumnCount = m_schema->getUninlinedObjectColumnCount();

#ifndef NDEBUG
    if(! m_schema->isCompatibleForMemcpy(source.m_schema)) {
        std::ostringstream message;
        message << "src  tuple: " << source.debug("") << std::endl;
        message << "src schema: " << source.m_schema->debug() << std::endl;
        message << "dest schema: " << m_schema->debug() << std::endl;
        throwFatalException("%s", message.str().c_str());
    }
#endif
    // copy the data AND the isActive flag
    ::memcpy(m_data, source.m_data, m_schema->tupleLength() + TUPLE_HEADER_SIZE);
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


inline void TableTuple::copy(const TableTuple &source) {
    vassert(m_schema);
    vassert(source.m_schema);
    vassert(source.m_data);
    vassert(m_data);

#ifndef NDEBUG
    if(! m_schema->isCompatibleForMemcpy(source.m_schema)) {
        std::ostringstream message;
        message << "src  tuple: " << source.debug("") << std::endl;
        message << "src schema: " << source.m_schema->debug() << std::endl;
        message << "dest schema: " << m_schema->debug() << std::endl;
        throwFatalException("%s", message.str().c_str());
    }
#endif
    // copy the data AND the isActive flag
    ::memcpy(m_data, source.m_data, m_schema->tupleLength() + TUPLE_HEADER_SIZE);
}

inline void TableTuple::serializeToDR(ExportSerializeOutput &io, int colOffset, uint8_t *nullArray) {
    serializeToExport(io, colOffset, nullArray);
    serializeHiddenColumnsToDR(io);
}

inline bool TableTuple::equals(const TableTuple &other) const {
    if (!m_schema->equals(other.m_schema)) {
        return false;
    } else {
        return equalsNoSchemaCheck(other);
    }
}

inline size_t TableTuple::hashCode(size_t seed) const {
    const int columnCount = m_schema->columnCount();
    for (int i = 0; i < columnCount; i++) {
        const NValue value = getNValue(i);
        value.hashCombine(seed);
    }
    return seed;
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

