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

#include <sstream>
#include <cstdio>
#include <boost/foreach.hpp>
#include <boost/scoped_array.hpp>

#include "table.h"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "common/Pool.hpp"
#include "common/FatalException.hpp"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/persistenttable.h"

using std::string;

namespace voltdb {

Table::Table(int tableAllocationTargetSize) :
    m_tableAllocationTargetSize(tableAllocationTargetSize)
{
}

Table::~Table() {
#ifdef VOLT_POOL_CHECKING
    auto engine = ExecutorContext::getEngine();
    bool shutdown = engine == nullptr ? true : engine->isDestroying();
    if (shutdown) {
       m_tlPool.shutdown();
    }
#endif
    // not all tables are reference counted but this should be invariant
    vassert(m_refcount == 0);

    // clear the schema
    if (m_ownsTupleSchema) {
        TupleSchema::freeTupleSchema(m_schema);
    }

    // clear any cached column serializations
    if (m_columnHeaderData)
        delete[] m_columnHeaderData;
}

void Table::initializeWithColumns(TupleSchema *schema, const std::vector<string> &columnNames, bool ownsTupleSchema, int32_t compactionThreshold) {

    // copy the tuple schema
    if (m_ownsTupleSchema) {
        TupleSchema::freeTupleSchema(m_schema);
    }
    m_ownsTupleSchema = ownsTupleSchema;
    m_schema  = schema;

    m_columnCount = schema->columnCount();

    m_tupleLength = m_schema->tupleLength() + TUPLE_HEADER_SIZE;
#ifdef MEMCHECK
    m_tuplesPerBlock = 1;
    m_tableAllocationSize = m_tupleLength;
#else
    m_tuplesPerBlock = m_tableAllocationTargetSize / m_tupleLength;
#ifdef USE_MMAP
    if (m_tuplesPerBlock < 1) {
        m_tuplesPerBlock = 1;
        m_tableAllocationSize = nexthigher(m_tupleLength);
    } else {
        m_tableAllocationSize = nexthigher(m_tableAllocationTargetSize);
    }
#else
    if (m_tuplesPerBlock < 1) {
        m_tuplesPerBlock = 1;
        m_tableAllocationSize = m_tupleLength;
    } else {
        m_tableAllocationSize = m_tableAllocationTargetSize;
    }
#endif
#endif

    // initialize column names
    m_columnNames.resize(m_columnCount);
    for (int i = 0; i < m_columnCount; ++i)
        m_columnNames[i] = columnNames[i];

    m_allowNulls.resize(m_columnCount);
    for (int i = m_columnCount - 1; i >= 0; --i) {
        TupleSchema::ColumnInfo const* columnInfo = m_schema->getColumnInfo(i);
        m_allowNulls[i] = columnInfo->allowNull;
    }
    // initialize the temp tuple
    m_tempTupleMemory.reset(new char[m_schema->tupleLength() + TUPLE_HEADER_SIZE]);
    m_tempTuple = TableTuple(m_tempTupleMemory.get(), m_schema);
    ::memset(m_tempTupleMemory.get(), 0, m_tempTuple.tupleLength());
    // default value of hidden dr timestamp is null
    if (m_schema->hiddenColumnCount() > 0) {
        for (int i = 0; i < m_schema->hiddenColumnCount(); i++) {
           m_tempTuple.setHiddenNValue(i, NValue::getNullValue(ValueType::tBIGINT));
        }
    }
    m_tempTuple.setActiveTrue();

    // set the data to be empty
    m_tupleCount = 0;

    m_compactionThreshold = compactionThreshold;
}

// ------------------------------------------------------------------
// COLUMNS
// ------------------------------------------------------------------

int Table::columnIndex(const std::string &name) const {
    for (int ctr = 0, cnt = m_columnCount; ctr < cnt; ctr++) {
        if (m_columnNames[ctr].compare(name) == 0) {
            return ctr;
        }
    }
    return -1;
}

bool Table::checkNulls(TableTuple& tuple) const {
    vassert(m_columnCount == tuple.columnCount());
    for (int i = m_columnCount - 1; i >= 0; --i) {
        if (( ! m_allowNulls[i]) && tuple.isNull(i)) {
            VOLT_TRACE ("%d th attribute was NULL. It is non-nillable attribute.", i);
            return false;
        }
    }
    return true;
}

// ------------------------------------------------------------------
// UTILITY
// ------------------------------------------------------------------

std::string Table::debug(const std::string &spacer) const {
    VOLT_DEBUG("tabledebug start");
    std::ostringstream buffer;
    std::string infoSpacer = spacer + "  |";

    buffer << infoSpacer << tableType() << "(" << name() << "):\n";
    buffer << infoSpacer << "\tAllocated Tuples:  " << allocatedTupleCount() << "\n";
    buffer << infoSpacer << "\tNumber of Columns: " << columnCount() << "\n";

    //
    // Columns
    //
    buffer << infoSpacer << "===========================================================\n";
    buffer << infoSpacer << "\tCOLUMNS\n";
    buffer << infoSpacer << m_schema->debug();
    //buffer << infoSpacer << " - TupleSchema needs a \"debug\" method. Add one for output here.\n";

#ifdef VOLT_TRACE_ENABLED
    //
    // Tuples
    //
    if (tableType().compare("LargeTempTable") != 0
        && tableType().compare("StreamedTable") != 0) {
        buffer << infoSpacer << "===========================================================\n";
        buffer << infoSpacer << "\tDATA\n";

        TableIterator iter = const_cast<Table*>(this)->iterator();
        TableTuple tuple(m_schema);
        if (this->activeTupleCount() == 0) {
            buffer << infoSpacer << "\t<NONE>\n";
        } else {
            std::string lastTuple = "";
            while (iter.next(tuple)) {
                if (tuple.isActive()) {
                    buffer << infoSpacer << "\t" << tuple.debug(this->name().c_str()) << "\n";
                }
            }
        }
        buffer << infoSpacer << "===========================================================\n";
    }
#endif
    std::string ret(buffer.str());
    VOLT_DEBUG("tabledebug end");

    return ret;
}

// ------------------------------------------------------------------
// Serialization Methods
// ------------------------------------------------------------------

/*
 * Warn: Iterate all tuples to get accurate size, don't use it on
 * performance critical path if table is large.
 */
size_t Table::getAccurateSizeToSerialize() {
    // column header size
    size_t bytes = getColumnHeaderSizeToSerialize();

    // tuples
    bytes += sizeof(int32_t);  // tuple count
    int64_t written_count = 0;
    TableIterator titer = iterator();
    TableTuple tuple(m_schema);
    while (titer.next(tuple)) {
        bytes += tuple.serializationSize();  // tuple size
        ++written_count;
    }
    vassert(written_count == m_tupleCount);

    return bytes;
}

size_t Table::getColumnHeaderSizeToSerialize() {
    // use a cache if possible
    if (m_columnHeaderData) {
        vassert(m_columnHeaderSize != -1);
        return m_columnHeaderSize;
    }

    size_t bytes = 0;

    // column header size, status code, column count
    bytes += sizeof(int32_t) + sizeof(int8_t) + sizeof(int16_t);
    // column types
    bytes += sizeof(int8_t) * m_columnCount;
    // column names
    bytes += sizeof(int32_t) * m_columnCount;
    for (int i = 0; i < m_columnCount; ++i) {
        bytes += columnName(i).size();
    }

    return m_columnHeaderSize = bytes;
}

void Table::serializeColumnHeaderTo(SerializeOutput &serialOutput) {
    serializeColumnHeaderTo(serialOutput, nullptr);
}

void Table::serializeColumnHeaderTo(SerializeOutput &serialOutput, HiddenColumnFilter::Type hiddenColumnFilter) {
    HiddenColumnFilter filter = HiddenColumnFilter::create(hiddenColumnFilter, m_schema);
    serializeColumnHeaderTo(serialOutput, &filter);
}

// Serialize scheam to serialOutput. If hiddenColumnFilter is not null include hidden columns which should be included
void Table::serializeColumnHeaderTo(SerializeOutput &serialOutput, HiddenColumnFilter *hiddenColumnFilter) {
    /* NOTE:
       VoltDBEngine uses a binary template to create tables of single integers.
       It's called m_templateSingleLongTable and if you are seeing a serialization
       bug in tables of single integers, make sure that's correct.
    */

    // use a cache if we are not including hidden columns
    if (m_columnHeaderData && !(hiddenColumnFilter && hiddenColumnFilter->getHiddenColumnCount())) {
        vassert(m_columnHeaderSize != -1);
        serialOutput.writeBytes(m_columnHeaderData, m_columnHeaderSize);
        return;
    }

    // skip header position
    std::size_t const start = serialOutput.position();

    // skip header position
    serialOutput.writeInt(-1);

    //status code
    serialOutput.writeByte(-128);

    // column counts as a short
    serialOutput.writeShort(static_cast<int16_t>(m_columnCount +
            (hiddenColumnFilter ? hiddenColumnFilter->getHiddenColumnCount() : 0)));

    // write an array of column types as bytes
    for (int i = 0; i < m_columnCount; ++i) {
        const ValueType type = m_schema->columnType(i);
        serialOutput.writeByte(static_cast<int8_t>(type));
    }

    if (hiddenColumnFilter) {
        for (short i = 0; i < m_schema->hiddenColumnCount(); ++i) {
            if (hiddenColumnFilter->include(i)) {
                const ValueType type = m_schema->getHiddenColumnInfo(i)->getVoltType();
                serialOutput.writeByte(static_cast<int8_t>(type));
            }
        }
    }

    // write the array of column names as voltdb strings
    // NOTE: strings are ASCII only in metadata (UTF-8 in table storage)
    for (int i = 0; i < m_columnCount; ++i) {
        // column name: write (offset, length) for column definition, and string to string table
        const string& name = columnName(i);
        // column names can't be null, so length must be >= 0
        int32_t length = static_cast<int32_t>(name.size());
        vassert(length >= 0);

        // this is standard string serialization for voltdb
        serialOutput.writeInt(length);
        serialOutput.writeBytes(name.data(), length);
    }

    if (hiddenColumnFilter) {
        for (short i = 0; i < m_schema->hiddenColumnCount(); ++i) {
            if (hiddenColumnFilter->include(i)) {
                HiddenColumn::Type type = m_schema->getHiddenColumnInfo(i)->columnType;
                const char *name = HiddenColumn::getName(type);

                int32_t length = static_cast<int32_t>(::strlen(name));
                serialOutput.writeInt(length);
                serialOutput.writeBytes(name, length);
            }
        }

        int32_t nonInclusiveHeaderSize = serialOutput.position() - start - sizeof(int32_t);
        serialOutput.writeIntAt(start, nonInclusiveHeaderSize);
    } else {
        // write the header size which is a non-inclusive int
        getColumnHeaderSizeToSerialize();
        vassert(static_cast<int32_t>(serialOutput.position() - start) == m_columnHeaderSize);
        int32_t nonInclusiveHeaderSize = static_cast<int32_t>(m_columnHeaderSize - sizeof(int32_t));
        serialOutput.writeIntAt(start, nonInclusiveHeaderSize);

        // cache the results
        m_columnHeaderData = new char[m_columnHeaderSize];
        memcpy(m_columnHeaderData, static_cast<const char*>(serialOutput.data()) + start, m_columnHeaderSize);
    }
}

void Table::serializeTo(SerializeOutput &serialOutput) {
    // The table is serialized as:
    // [(int) total size]
    // [(int) header size] [num columns] [column types] [column names]
    // [(int) num tuples] [tuple data]

    /* NOTE:
       VoltDBEngine uses a binary template to create tables of single integers.
       It's called m_templateSingleLongTable and if you are seeing a serialization
       bug in tables of single integers, make sure that's correct.
    */

    // a placeholder for the total table size
    std::size_t pos = serialOutput.position();
    serialOutput.writeInt(-1);

    serializeColumnHeaderTo(serialOutput);

    // active tuple counts
    serialOutput.writeInt(static_cast<int32_t>(m_tupleCount));
    int64_t written_count = 0;
    TableIterator titer = iterator();
    TableTuple tuple(m_schema);
    while (titer.next(tuple)) {
        tuple.serializeTo(serialOutput);
        ++written_count;
    }
    vassert(written_count == m_tupleCount);

    // length prefix is non-inclusive
    int32_t sz = static_cast<int32_t>(serialOutput.position() - pos - sizeof(int32_t));
    vassert(sz > 0);
    serialOutput.writeIntAt(pos, sz);
}

void Table::serializeToWithoutTotalSize(SerializeOutput &serialOutput) {
    serializeColumnHeaderTo(serialOutput);

    // active tuple counts
    serialOutput.writeInt(static_cast<int32_t>(m_tupleCount));
    int64_t written_count = 0;
    TableIterator titer = iterator();
    TableTuple tuple(m_schema);
    while (titer.next(tuple)) {
        tuple.serializeTo(serialOutput);
        ++written_count;
    }
    vassert(written_count == m_tupleCount);
}

/**
 * Serialized the table, but only includes the tuples specified (columns data and all).
 * Used by the exception stuff Ariel put in.
 */
void Table::serializeTupleTo(SerializeOutput &serialOutput, voltdb::TableTuple *tuples, int numTuples) {
    //vassert(m_schema->equals(tuples[0].getSchema()));

    std::size_t pos = serialOutput.position();
    serialOutput.writeInt(-1);

    vassert(!tuples[0].isNullTuple());

    serializeColumnHeaderTo(serialOutput);

    serialOutput.writeInt(static_cast<int32_t>(numTuples));
    for (int ii = 0; ii < numTuples; ii++) {
        tuples[ii].serializeTo(serialOutput);
    }

    serialOutput.writeIntAt(pos, static_cast<int32_t>(serialOutput.position() - pos - sizeof(int32_t)));
}

bool Table::equals(voltdb::Table *other) {
    if (columnCount() != other->columnCount()) {
        return false;
    }

    if (activeTupleCount() != other->activeTupleCount()) {
        return false;
    }

    if (databaseId() != other->databaseId()) {
        return false;
    }

    if (name() != other->name()) {
        return false;
    }

    if (tableType() != other->tableType()) {
        return false;
    }

    const voltdb::TupleSchema *otherSchema = other->schema();
    if ( ! m_schema->equals(otherSchema)) {
        return false;
    }

    voltdb::TableIterator firstTI = iterator();
    voltdb::TableIterator secondTI = other->iterator();
    voltdb::TableTuple firstTuple(m_schema);
    voltdb::TableTuple secondTuple(otherSchema);
    while (firstTI.next(firstTuple)) {
        if ( ! secondTI.next(secondTuple)) {
            return false;
        }

        if ( ! firstTuple.equals(secondTuple)) {
            return false;
        }
    }
    return true;
}

void Table::loadTuplesFromNoHeader(SerializeInputBE &serialInput,
                                   Pool *stringPool) {
    int tupleCount = serialInput.readInt();
    vassert(tupleCount >= 0);

    int32_t serializedTupleCount = 0;
    size_t tupleCountPosition = 0;
    TableTuple target(m_schema);
    for (int i = 0; i < tupleCount; ++i) {
        nextFreeTuple(&target);
        target.setActiveTrue();
        target.setDirtyFalse();
        target.setPendingDeleteFalse();
        target.setPendingDeleteOnUndoReleaseFalse();

        target.deserializeFrom(serialInput, stringPool, LoadTableCaller::get(LoadTableCaller::INTERNAL));

        processLoadedTuple(target, NULL, serializedTupleCount, tupleCountPosition);
    }
}

void Table::loadTuplesFrom(SerializeInputBE &serialInput,
                           Pool *stringPool) {
    /*
     * directly receives a VoltTable buffer.
     * [00 01]   [02 03]   [04 .. 0x]
     * rowstart  colcount  colcount * 1 byte (column types)
     *
     * [0x+1 .. 0y]
     * colcount * strings (column names)
     *
     * [0y+1 0y+2 0y+3 0y+4]
     * rowcount
     *
     * [0y+5 .. end]
     * rowdata
     */

    // todo: just skip ahead to this position
    serialInput.readInt(); // rowstart

    serialInput.readByte();

    int16_t colcount = serialInput.readShort();
    vassert(colcount >= 0);

    // Store the following information so that we can provide them to the user
    // on failure
    ValueType types[colcount];
    boost::scoped_array<std::string> names(new std::string[colcount]);

    // skip the column types
    for (int i = 0; i < colcount; ++i) {
        types[i] = (ValueType) serialInput.readEnumInSingleByte();
    }

    // skip the column names
    for (int i = 0; i < colcount; ++i) {
        names[i] = serialInput.readTextString();
    }

    // Check if the column count matches what the temp table is expecting
    int16_t expectedColumnCount = static_cast<int16_t>(m_schema->columnCount() + m_schema->hiddenColumnCount());
    if (colcount != expectedColumnCount) {
        std::stringstream message(std::stringstream::in
                                  | std::stringstream::out);
        message << "Column count mismatch. Expecting "
                << expectedColumnCount
                << ", but " << colcount << " given" << std::endl;
        message << "Expecting the following columns:" << std::endl;
        message << debug() << std::endl;
        message << "The following columns are given:" << std::endl;
        for (int i = 0; i < colcount; i++) {
            message << "column " << i << ": " << names[i]
                    << ", type = " << getTypeName(types[i]) << std::endl;
        }
        throw SerializableEEException(message.str().c_str());
    }

    loadTuplesFromNoHeader(serialInput, stringPool);
}

}
