/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
#include <cassert>
#include <cstdio>
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
    m_tempTuple(),
    m_schema(NULL),
    m_tupleCount(0),
    m_usedTuples(0),
    m_allocatedTuples(0),
    m_columnCount(0),
    m_tuplesPerBlock(0),
    m_columnHeaderData(NULL),
    m_columnHeaderSize(-1),
    m_columnNames(NULL),
    m_databaseId(-1),
    m_name(""),
    m_ownsTupleSchema(true),
    m_tableAllocationTargetSize(tableAllocationTargetSize),
    m_tempTableMemoryInBytes(NULL)
{
}

Table::~Table() {
    // clear the schema
    if (m_ownsTupleSchema) {
        TupleSchema::freeTupleSchema(m_schema);
    }

    m_schema = NULL;
    delete[] m_columnNames;
    m_columnNames = NULL;

    delete[] reinterpret_cast<char*>(m_tempTuple.m_data);
    m_tempTuple.m_data = NULL;

    /*
     * The memcheck build uses the heap to allocate each tuple in order to
     * detect errors while accessing tuples as well as tuples storage pointers
     * leaking from a table after the tuple has been deleted.
     */
#ifdef MEMCHECK_NOFREELIST
    for (std::set<void*>::iterator iter = m_allocatedTuplePointers.begin();
            iter != m_allocatedTuplePointers.end(); ++iter) {
        assert(m_deletedTuplePointers.find(*iter) == m_deletedTuplePointers.end());
        delete[] reinterpret_cast<char*>(*iter);
    }
//    for (std::set<void*>::iterator iter = m_deletedTuplePointers.begin();
//            iter != m_deletedTuplePointers.end(); ++iter) {
//        assert(m_allocatedTuplePointers.find(*iter) == m_allocatedTuplePointers.end());
//        delete[] reinterpret_cast<char*>(*iter);
//    }
    m_allocatedTuplePointers.clear();
    m_deletedTuplePointers.clear();
    m_data.clear();
#else
    // clear the tuple memory
    for (std::vector<char*>::iterator iter = m_data.begin(); iter != m_data.end(); ++iter)
        delete[] reinterpret_cast<char*>(*iter);
#endif

    // clear any cached column serializations
    if (m_columnHeaderData)
        delete[] m_columnHeaderData;
    m_columnHeaderData = NULL;
}

void Table::initializeWithColumns(TupleSchema *schema, const std::string* columnNames, bool ownsTupleSchema) {
    m_ownsTupleSchema = ownsTupleSchema;

    // copy the tuple schema
    TupleSchema::freeTupleSchema(m_schema);
    m_schema  = schema;

    m_columnCount = schema->columnCount();
#ifdef MEMCHECK
    m_tuplesPerBlock = 1;
#else
    m_tuplesPerBlock = m_tableAllocationTargetSize / (m_schema->tupleLength() + TUPLE_HEADER_SIZE);
#endif

    // initialize column names
    delete[] m_columnNames;
    m_columnNames = new std::string[m_columnCount];
    for (int i = 0; i < m_columnCount; ++i)
        m_columnNames[i] = columnNames[i];

    // initialize the temp tuple
    char *m_tempTupleMemory = m_tempTuple.m_data;
    delete[] reinterpret_cast<char*>(m_tempTupleMemory);
    m_tempTupleMemory = new char[m_schema->tupleLength() + TUPLE_HEADER_SIZE];
    m_tempTuple = TableTuple(m_tempTupleMemory, m_schema);
    ::memset(m_tempTupleMemory, 0, m_tempTuple.tupleLength());
    m_tempTuple.setDeletedFalse();

    // set the data to be empty
    m_tupleCount = 0;
    m_usedTuples = 0;
#ifdef MEMCHECK_NOFREELIST
    m_deletedTupleCount = 0;
#else
    m_holeFreeTuples.clear();//Why clear it. Shouldn't it be empty? Won't this leak?
#endif

    m_tupleLength = m_schema->tupleLength() + TUPLE_HEADER_SIZE;

    // note that any allocated memory in m_data is left alone
    // as is m_allocatedTuples

    onSetColumns(); // for more initialization
}

// ------------------------------------------------------------------
// TUPLES
// ------------------------------------------------------------------
TableIterator Table::tableIterator() {
    return TableIterator(this);
}

void Table::nextFreeTuple(TableTuple *tuple) {
    // First check whether we have any in our list
    // In the memcheck it uses the heap instead of a free list to help Valgrind.
#ifndef MEMCHECK_NOFREELIST
    if (!m_holeFreeTuples.empty()) {
        VOLT_TRACE("GRABBED FREE TUPLE!\n");
        char* ret = m_holeFreeTuples.back();
        m_holeFreeTuples.pop_back();
        assert (m_columnCount == tuple->sizeInValues());
        tuple->move(ret);
        return;
    }
#endif

    // if there are no tuples free, we need to grab another chunk of memory
    // Allocate a new set of tuples
    if (m_usedTuples >= m_allocatedTuples) {
        allocateNextBlock();
    }

    // get free tuple
    assert (m_usedTuples < m_allocatedTuples);
    assert (m_columnCount == tuple->sizeInValues());
    tuple->move(dataPtrForTuple((int) m_usedTuples));
    ++m_usedTuples;
    //cout << "table::nextFreeTuple(" << reinterpret_cast<const void *>(this) << ") m_usedTuples == " << m_usedTuples << endl;
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

// ------------------------------------------------------------------
// UTILITY
// ------------------------------------------------------------------

std::string Table::debug() {
    VOLT_DEBUG("tabledebug start");
    std::ostringstream buffer;

    buffer << tableType() << "(" << name() << "):\n";
    buffer << "\tAllocated Tuples:  " << m_allocatedTuples << "\n";
#ifdef MEMCHECK_NOFREELIST
    buffer << "\tDeleted Tuples:    " << m_deletedTupleCount << "\n";
#else
    buffer << "\tDeleted Tuples:    " << m_holeFreeTuples.size() << "\n";
#endif
    buffer << "\tNumber of Columns: " << columnCount() << "\n";

    //
    // Columns
    //
    buffer << "===========================================================\n";
    buffer << "\tCOLUMNS\n";
    buffer << m_schema->debug();
    //buffer << " - TupleSchema needs a \"debug\" method. Add one for output here.\n";

    //
    // Tuples
    //
    buffer << "===========================================================\n";
    buffer << "\tDATA\n";

    TableIterator iter(this);
    TableTuple tuple(m_schema);
    if (this->activeTupleCount() == 0) {
        buffer << "\t<NONE>\n";
    } else {
        std::string lastTuple = "";
        while (iter.next(tuple)) {
            if (tuple.isActive()) {
                buffer << "\t" << tuple.debug(this->name().c_str()) << "\n";
            }
        }
    }
    buffer << "===========================================================\n";

    std::string ret(buffer.str());
    VOLT_DEBUG("tabledebug end");

    return ret;
}

// ------------------------------------------------------------------
// Serialization Methods
// ------------------------------------------------------------------
int Table::getApproximateSizeToSerialize() const {
    // HACK to get this over quick
    // just max table serialization to 10MB
    return 1024 * 1024 * 10;
}

bool Table::serializeColumnHeaderTo(SerializeOutput &serialize_io) {

    /* NOTE:
       VoltDBEngine uses a binary template to create tables of single integers.
       It's called m_templateSingleLongTable and if you are seeing a serialization
       bug in tables of single integers, make sure that's correct.
    */

    // skip header position
    std::size_t start;

    try {
        // use a cache
        if (m_columnHeaderData) {
            assert(m_columnHeaderSize != -1);
            serialize_io.writeBytes(m_columnHeaderData, m_columnHeaderSize);
            return true;
        }
        assert(m_columnHeaderSize == -1);

        start = serialize_io.position();

        // skip header position
        serialize_io.writeInt(-1);

        // column counts as a short
        serialize_io.writeShort(static_cast<int16_t>(m_columnCount));

        // write an array of column types as bytes
        for (int i = 0; i < m_columnCount; ++i) {
            ValueType type = m_schema->columnType(i);
            serialize_io.writeByte(static_cast<int8_t>(type));
        }

        // write the array of column names as voltdb strings
        // NOTE: strings are ASCII only in metadata (UTF-8 in table storage)
        for (int i = 0; i < m_columnCount; ++i) {
            // column name: write (offset, length) for column definition, and string to string table
            const string& name = columnName(i);
            // column names can't be null, so length must be >= 0
            int32_t length = static_cast<int32_t>(name.size());
            assert(length >= 0);

            // this is standard string serialization for voltdb
            serialize_io.writeInt(length);
            serialize_io.writeBytes(name.data(), length);
        }


        // write the header size which is a non-inclusive int
        size_t position = serialize_io.position();
        m_columnHeaderSize = static_cast<int32_t>(position - start);
        int32_t nonInclusiveHeaderSize = static_cast<int32_t>(m_columnHeaderSize - sizeof(int32_t));
        serialize_io.writeIntAt(start, nonInclusiveHeaderSize);
    }
    catch(...) {
        throwFatalException("Failed while serializing table header.");
        return false;
    }

    // cache the results
    m_columnHeaderData = new char[m_columnHeaderSize];
    memcpy(m_columnHeaderData, static_cast<const char*>(serialize_io.data()) + start, m_columnHeaderSize);

    return true;

}

bool Table::serializeTo(SerializeOutput &serialize_io) {
    // The table is serialized as:
    // [(int) total size]
    // [(int) header size] [num columns] [column types] [column names]
    // [(int) num tuples] [tuple data]

    /* NOTE:
       VoltDBEngine uses a binary template to create tables of single integers.
       It's called m_templateSingleLongTable and if you are seeing a serialization
       bug in tables of single integers, make sure that's correct.
    */

    try {
        // a placeholder for the total table size
        std::size_t pos = serialize_io.position();
        serialize_io.writeInt(-1);

        if (!serializeColumnHeaderTo(serialize_io))
            return false;

        // active tuple counts
        serialize_io.writeInt(static_cast<int32_t>(m_tupleCount));
        int64_t written_count = 0;
        TableIterator titer(this);
        TableTuple tuple(m_schema);
        while (titer.next(tuple)) {
            tuple.serializeTo(serialize_io);
            ++written_count;
        }
        assert(written_count == m_tupleCount);

        // length prefix is non-inclusive
        int32_t sz = static_cast<int32_t>(serialize_io.position() - pos - sizeof(int32_t));
        assert(sz > 0);
        serialize_io.writeIntAt(pos, sz);
    }
    catch(...) {
        throw SQLException(SQLException::volt_output_buffer_overflow,
            "Output from SQL stmt overflowed output/network buffer of 10mb. "
            "Try a \"limit\" clause or a stronger predicate.");
    }

    return true;
}

/**
 * Serialized the table, but only includes the tuples specified (columns data and all).
 * Used by the exception stuff Ariel put in.
 */
bool Table::serializeTupleTo(SerializeOutput &serialize_io, voltdb::TableTuple *tuples, int numTuples) {
    //assert(m_schema->equals(tuples[0].getSchema()));

    try {
        std::size_t pos = serialize_io.position();
        serialize_io.writeInt(-1);

        assert(!tuples[0].isNullTuple());

        if (!serializeColumnHeaderTo(serialize_io))
            return false;

        serialize_io.writeInt(static_cast<int32_t>(numTuples));
        for (int ii = 0; ii < numTuples; ii++) {
            tuples[ii].serializeTo(serialize_io);
        }

        serialize_io.writeIntAt(pos, static_cast<int32_t>(serialize_io.position() - pos - sizeof(int32_t)));
    }
    catch(...) {
        throwFatalException("Failed while serializing table with specific tuples.");
        return false;
    }

    return true;
}

bool Table::deserializeFrom(SerializeInput &serialize_io, Pool *stringPool) {

    try {
        /*int32_t fullSize =*/ serialize_io.readInt();
        //VOLT_DEBUG("FULL SIZE IS %d BYTES", fullSize);

        /*int16_t headerSize =*/ serialize_io.readInt();
        //VOLT_DEBUG("HEADER IS %d BYTES", headerSize);

        //column
        int columnCount = serialize_io.readShort(); // column num
        //VOLT_DEBUG("TABLE HAS %d COLUMNS", columnCount);

        // read the column lengths, types and nullness (only types are serialized)
        std::vector<voltdb::ValueType> columnTypes;
        std::vector<int32_t> columnLengths;
        std::vector<bool> allowNullColumn(columnCount, true);
        for (int i = 0; i < columnCount; ++i) {
            columnTypes.push_back(static_cast<voltdb::ValueType>(serialize_io.readEnumInSingleByte()));
            //VOLT_DEBUG("TYPE OF COLUMN %d IS %d", i, columnTypes[i]);
            if (columnTypes[i] == voltdb::VALUE_TYPE_VARCHAR) {
                columnLengths.push_back(UNINLINEABLE_OBJECT_LENGTH);
            } else {
                columnLengths.push_back(static_cast<uint16_t>(voltdb::NValue::getTupleStorageSize(columnTypes[i])));
            }
        }
        TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnLengths, allowNullColumn, false);

        // read the column names
        string* columnNames = new string[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            columnNames[i] = serialize_io.readTextString();
        }

        // this will set the columns and also make the table empty
        initializeWithColumns(schema, columnNames, true);
        // clean up
        delete[] columnNames;

        int tupleCount = serialize_io.readInt();
        assert(tupleCount >= 0);

        //data
        for (int i = 0; i < tupleCount; ++i) {
            m_tempTuple.deserializeFrom(serialize_io, stringPool);
            insertTuple(m_tempTuple);
        }
    }
    catch(...) {
        throwFatalException("Failed while deserializing table.");
        return false;
    }

    return true;
}

bool Table::equals(const voltdb::Table *other) const {
    if (!(columnCount() == other->columnCount())) return false;
    if (!(indexCount() == other->indexCount())) return false;
    if (!(activeTupleCount() == other->activeTupleCount())) return false;
    if (!(databaseId() == other->databaseId())) return false;
    if (!(tableId() == other->tableId())) return false;
    if (!(name() == other->name())) return false;
    if (!(tableType() == other->tableType())) return false;

    std::vector<voltdb::TableIndex*> indexes = allIndexes();
    std::vector<voltdb::TableIndex*> otherIndexes = other->allIndexes();
    if (!(indexes.size() == indexes.size())) return false;
    for (std::size_t ii = 0; ii < indexes.size(); ii++) {
        if (!(indexes[ii]->equals(otherIndexes[ii]))) return false;
    }

    const voltdb::TupleSchema *otherSchema = other->schema();
    if ((!m_schema->equals(otherSchema))) return false;

    voltdb::TableIterator firstTI(this);
    voltdb::TableIterator secondTI(other);
    voltdb::TableTuple firstTuple(m_schema);
    voltdb::TableTuple secondTuple(otherSchema);
    while(firstTI.next(firstTuple)) {
        if (!(secondTI.next(secondTuple))) return false;
        if (!(firstTuple.equals(secondTuple))) return false;
    }
    return true;
}

voltdb::TableStats* Table::getTableStats() {
    return NULL;
}

std::vector<std::string> Table::getColumnNames() {
    std::vector<std::string> columnNames;
    for (int ii = 0; ii < m_columnCount; ii++) {
        columnNames.push_back(m_columnNames[ii]);
    }
    return columnNames;
}
}
