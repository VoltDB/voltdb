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

#ifndef HSTORETABLE_H
#define HSTORETABLE_H

#include <string>
#include <vector>
#ifdef MEMCHECK_NOFREELIST
#include <set>
#endif
#include "common/ids.h"
#include "common/types.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"

namespace voltdb {

class TableIndex;
class TableColumn;
class TableTuple;
class TableFactory;
class TableIterator;
class CopyOnWriteIterator;
class CopyOnWriteContext;
class UndoLog;
class ReadWriteSet;
class SerializeInput;
class SerializeOutput;
class TableStats;
class StatsSource;
class StreamBlock;
class Topend;

const size_t COLUMN_DESCRIPTOR_SIZE = 1 + 4 + 4; // type, name offset, name length

// use no more than 100mb for temp tables per fragment
const int MAX_TEMP_TABLE_MEMORY = 1024 * 1024 * 100;

/**
 * Represents a table which might or might not be a temporary table.
 * Both TempTable and PersistentTable derive from this class.
 *
 *   free_tuples: a linked list of free (unused) tuples. The data contains
 *     all tuples, including deleted tuples. deleted tuples in this linked
 *     list are reused on next insertion.
 *   temp_tuple: when a transaction is inserting a new tuple, this tuple
 *     object is used as a reusable value-holder for the new tuple.
 *     In this way, we don't have to allocate a temporary tuple each time.
 *
 * Allocated/Active/Deleted tuples:
 *   Tuples in data are called allocated tuples.
 *   Tuples in free_tuples are called deleted tuples.
 *   Tuples in data but not in free_tuples are called active tuples.
 *   Following methods return the number of tuples in each state.
 *         int allocatedTupleCount() const;
 *         int activeTupleCount() const;
 *         int getNumOfTuplesDeleted() const;
 *
 * Table objects including derived classes are only instantiated via a
 * factory class (TableFactory).
 */
class Table {
    friend class TableFactory;
    friend class TableIterator;
    friend class CopyOnWriteIterator;
    friend class CopyOnWriteContext;
    friend class ExecutionEngine;
    friend class TableStats;
    friend class StatsSource;

  private:
    // no default constructor, no copy
    Table();
    Table(Table const&);

public:
    virtual ~Table();

    // ------------------------------------------------------------------
    // ACCESS METHODS
    // ------------------------------------------------------------------
    /** please use TableIterator(table), not this function as this function can't be inlined.*/
    TableIterator tableIterator();

    // ------------------------------------------------------------------
    // OPERATIONS
    // ------------------------------------------------------------------
    virtual void deleteAllTuples(bool freeAllocatedStrings) = 0;
    virtual bool insertTuple(TableTuple &source) = 0;
    virtual bool updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes) = 0;
    virtual bool deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings) = 0;

    // ------------------------------------------------------------------
    // TUPLES
    // ------------------------------------------------------------------
    int64_t allocatedTupleCount() const { return m_allocatedTuples; }
    int64_t activeTupleCount() const { return m_tupleCount; }
#ifdef MEMCHECK_NOFREELIST
    int64_t deletedTupleCount() const { return m_deletedTupleCount; }
#else
    int64_t deletedTupleCount() const { return m_holeFreeTuples.size(); }
#endif
    TableTuple& tempTuple();

    // ------------------------------------------------------------------
    // COLUMNS
    // ------------------------------------------------------------------
    inline const TupleSchema* schema() const { return m_schema; };
    inline const std::string& columnName(int index) const { return m_columnNames[index]; }
    inline int columnCount() const { return m_columnCount; };
    int columnIndex(const std::string &name) const;
    std::vector<std::string> getColumnNames();
    // ------------------------------------------------------------------
    // INDEXES
    // ------------------------------------------------------------------
    virtual int indexCount() const                      { return 0; }
    virtual int uniqueIndexCount() const                { return 0; }
    virtual std::vector<TableIndex*> allIndexes() const { return std::vector<TableIndex*>(); }
    virtual TableIndex *index(std::string name)         { return NULL; }
    virtual TableIndex *primaryKeyIndex()               { return NULL; }
    virtual const TableIndex *primaryKeyIndex() const   { return NULL; }

    // ------------------------------------------------------------------
    // UTILITY
    // ------------------------------------------------------------------
    CatalogId databaseId() const;
    virtual CatalogId tableId() const { assert(false); return -1; }
    const std::string& name() const;

    virtual std::string tableType() const = 0;
    virtual std::string debug();

    // ------------------------------------------------------------------
    // SERIALIZATION
    // ------------------------------------------------------------------
    int getApproximateSizeToSerialize() const;
    bool serializeTo(SerializeOutput &serialize_out);
    bool serializeColumnHeaderTo(SerializeOutput &serialize_io);

    /*
     * Serialize a single tuple as a table so it can be sent to Java.
     */
    bool serializeTupleTo(SerializeOutput &serialize_out, TableTuple *tuples, int numTuples);

    /**
     * Loads only tuple data, not schema, from the serialized table.
     * Used for initial data loading and receiving dependencies.
     * @param allowELT if false, elt enabled is overriden for this load.
     */
    void loadTuplesFrom(bool allowELT,
                                SerializeInput &serialize_in,
                                Pool *stringPool = NULL);
    //------------
    // EL-RELATED
    //------------
    /**
     * Get the next block of committed but unreleased ELT bytes
     */
    virtual StreamBlock* getCommittedEltBytes()
    {
        // default implementation is to return NULL, which
        // indicates an error)
        return NULL;
    }

    /**
     * Release any committed ELT bytes up to the provided stream offset
     */
    virtual bool releaseEltBytes(int64_t releaseOffset)
    {
        // default implementation returns false, which
        // indicates an error
        return false;
    }

    /**
     * Flush tuple stream wrappers. A negative time instructs an
     * immediate flush.
     */
    virtual void flushOldTuples(int64_t timeInMillis) {
    }

protected:
    /*
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * to do additional processing for views and ELT
     */
    virtual void processLoadedTuple(bool allowELT, TableTuple &tuple) {};

    /*
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * to do add tuples to indexes
     */
    virtual void populateIndexes(int tupleCount) {};
public:

    virtual bool equals(const voltdb::Table *other) const;
    virtual voltdb::TableStats* getTableStats();

protected:
    Table(int tableAllocationTargetSize);
    void resetTable();

    void nextFreeTuple(TableTuple *tuple);
    char * dataPtrForTuple(const int index) const;
    char * dataPtrForTupleForced(const int index);
    void allocateNextBlock();

    /**
     * Normally this will return the tuple storage to the free list.
     * In the memcheck build it will return the storage to the heap.
     */
    void deleteTupleStorage(TableTuple &tuple);

    void initializeWithColumns(TupleSchema *schema, const std::string* columnNames, bool ownsTupleSchema);
    virtual void onSetColumns() {};

    // TUPLES
    TableTuple m_tempTuple;
    /** not temptuple. these are for internal use. */
    TableTuple m_tmpTarget1, m_tmpTarget2;
    TupleSchema* m_schema;
    uint32_t m_tupleCount;
    uint32_t m_usedTuples;
    uint32_t m_allocatedTuples;
    uint32_t m_columnCount;
    uint32_t m_tuplesPerBlock;
    uint32_t m_tupleLength;
    // pointers to chunks of data
    std::vector<char*> m_data;

    char *m_columnHeaderData;
    int32_t m_columnHeaderSize;

#ifdef MEMCHECK_NOFREELIST
    int64_t m_deletedTupleCount;
    //Store pointers to all allocated tuples so they can be freed on destruction
    std::set<void*> m_allocatedTuplePointers;
    std::set<void*> m_deletedTuplePointers;
#else
    /**
     * queue of pointers to <b>once used and then deleted</b> tuples.
     * Tuples after used_tuples index are also free, this queue
     * is used to find "hole" tuples which were once used (before used_tuples index)
     * and also deleted.
     * NOTE THAT THESE ARE NOT THE ONLY FREE TUPLES.
    */
    std::vector<char*> m_holeFreeTuples;
#endif

    // schema
    std::string* m_columnNames; // array of string names

    // GENERAL INFORMATION
    CatalogId m_databaseId;
    std::string m_name;

    /* If this table owns the TupleSchema it is responsible for deleting it in the destructor */
    bool m_ownsTupleSchema;

    const int m_tableAllocationTargetSize;

    // ptr to global integer tracking temp table memory allocated per frag
    // should be null for persistent tables
    int* m_tempTableMemoryInBytes;
};

/**
 * Returns a ptr to the tuple data requested. No checks are done that the index
 * is valid.
 */
inline char* Table::dataPtrForTuple(const int index) const {
    size_t blockIndex = index / m_tuplesPerBlock;
    assert (blockIndex < m_data.size());
    char *block = m_data[blockIndex];
    char *retval = block + ((index % m_tuplesPerBlock) * m_tupleLength);

    //VOLT_DEBUG("getDataPtrForTuple = %d,%d", offset / tableAllocationUnitSize, offset % tableAllocationUnitSize);
    return retval;
}

/**
 * Returns a ptr to the tuple data requested, and allocates whatever memory is
 * is needed if the table isn't big enough yet.
 */
inline char* Table::dataPtrForTupleForced(const int index) {
    while (static_cast<size_t>(index / m_tuplesPerBlock) >= m_data.size()) {
        allocateNextBlock();
    }
    return dataPtrForTuple(index);
}

inline TableTuple& Table::tempTuple() {
    assert (m_tempTuple.m_data);
    return m_tempTuple;
}

inline const std::string& Table::name()     const { return m_name; }

inline void Table::allocateNextBlock() {
#ifdef MEMCHECK
    int bytes = m_schema->tupleLength() + TUPLE_HEADER_SIZE;
#else
    int bytes = m_tableAllocationTargetSize;
#endif
    char *memory = (char*)(new char[bytes]);
    m_data.push_back(memory);
#ifdef MEMCHECK_NOFREELIST
    assert(m_allocatedTuplePointers.insert(memory).second);
    m_deletedTuplePointers.erase(memory);
#endif
    m_allocatedTuples += m_tuplesPerBlock;
    if (m_tempTableMemoryInBytes) {
        (*m_tempTableMemoryInBytes) += bytes;
        if ((*m_tempTableMemoryInBytes) > MAX_TEMP_TABLE_MEMORY) {
            throw SQLException(SQLException::volt_temp_table_memory_overflow,
                               "More than 100MB of temp table memory used while"
                               " executing SQL. Aborting.");
        }
    }
}

#ifdef MEMCHECK_NOFREELIST
inline void Table::deleteTupleStorage(TableTuple &tuple) {
    m_tupleCount--;
    m_deletedTupleCount++;
    assert(m_deletedTuplePointers.find(tuple.address()) == m_deletedTuplePointers.end());
    assert(m_allocatedTuplePointers.find(tuple.address()) != m_allocatedTuplePointers.end());
    /**
     * Delete the tuple so valgrind can catch future invalid access
     * and NULL out the reference in m_data so TableIterator can skip it.
     */
    delete []tuple.address();
    for (std::vector<char*>::iterator iter = m_data.begin(); iter != m_data.end(); ++iter) {
        if (*iter == tuple.address()) {
                *iter = NULL;
                break;
        }
    }
    assert(1 == m_allocatedTuplePointers.erase(tuple.address()));
    assert(m_deletedTuplePointers.insert(tuple.address()).second);
}
#else
inline void Table::deleteTupleStorage(TableTuple &tuple) {
    tuple.setDeletedTrue(); // does NOT free strings

    // add to the free list
    m_tupleCount--;
    m_holeFreeTuples.push_back(tuple.address());
}
#endif


inline voltdb::CatalogId Table::databaseId() const { return m_databaseId; }

}
#endif
