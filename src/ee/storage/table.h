/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
#ifndef BTREE_DEBUG
#define BTREE_DEBUG
#endif
#include <string>
#include <vector>
#include <set>
#include <list>
#include "common/ids.h"
#include "common/types.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "storage/TupleBlock.h"
#include "stx/btree_set.h"

class CopyOnWriteTest_CopyOnWriteIterator;

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
class TupleBlock;
class PersistentTableUndoDeleteAction;

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
    friend class TupleBlock;
    friend class ::CopyOnWriteTest_CopyOnWriteIterator;
    friend class PersistentTableUndoDeleteAction;
  private:
    // no default constructor, no copy
    Table();
    Table(Table const&);

public:
    virtual ~Table();

    /*
     * Table lifespan can be managed bya reference count. The
     * reference is trivial to maintain since it is only accessed by
     * the execution engine thread. Snapshot, Export and the
     * corresponding CatalogDelegate may be reference count
     * holders. The table is deleted when the refcount falls to
     * zero. This allows longer running processes to complete
     * gracefully after a table has been removed from the catalog.
     */
    void incrementRefcount() {
        m_refcount += 1;
    }

    void decrementRefcount() {
        m_refcount -= 1;
        if (m_refcount == 0) {
            delete this;
        }
    }

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
    // TUPLES AND MEMORY USAGE
    // ------------------------------------------------------------------
    int64_t allocatedTupleCount() const { return m_data.size() * m_tuplesPerBlock; }
    int64_t activeTupleCount() const { return m_tupleCount; }
    TableTuple& tempTuple();

    int64_t allocatedTupleMemory() const
    {
        return m_data.size() * m_tableAllocationSize;
    }

    int64_t occupiedTupleMemory() const
    {
        return m_tupleCount * m_tempTuple.tupleLength();
    }

    // Only counts persistent table usage, currently
    int64_t nonInlinedMemorySize() const { return m_nonInlinedMemorySize; }

    // ------------------------------------------------------------------
    // COLUMNS
    // ------------------------------------------------------------------
    inline const TupleSchema* schema() const { return m_schema; };
    inline const std::string& columnName(int index) const { return m_columnNames[index]; }
    inline int columnCount() const { return m_columnCount; };
    int columnIndex(const std::string &name) const;
    const std::string *columnNames() { return m_columnNames; }
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
     * Loads only tuple data and assumes there is no schema present.
     * Used for recovery where the schema is not sent.
     * @param allowExport if false, export enabled is overriden for this load.
     */
    void loadTuplesFromNoHeader(bool allowExport,
                                SerializeInput &serialize_in,
                                Pool *stringPool = NULL);

    /**
     * Loads only tuple data, not schema, from the serialized table.
     * Used for initial data loading and receiving dependencies.
     * @param allowExport if false, export enabled is overriden for this load.
     */
    void loadTuplesFrom(bool allowExport,
                        SerializeInput &serialize_in,
                        Pool *stringPool = NULL);
    //------------
    // EL-RELATED
    //------------
    /**
     * Get the next block of committed but unreleased Export bytes
     */
    virtual StreamBlock* getCommittedExportBytes()
    {
        // default implementation is to return NULL, which
        // indicates an error)
        return NULL;
    }

    /**
     * Set the current offset in bytes of the export stream for this Table
     * since startup (used for rejoin/recovery).
     */
    virtual void setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed) {
        // this should be overidden by any table involved in an export
        assert(false);
    }

    /**
     * Release any committed Export bytes up to the provided stream offset
     */
    virtual bool releaseExportBytes(int64_t releaseOffset)
    {
        // default implementation returns false, which
        // indicates an error
        return false;
    }

    /**
     * Reset the Export poll marker
     */
    virtual void resetPollMarker() {
        // default, do nothing.
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
     * to do additional processing for views and Export
     */
    virtual void processLoadedTuple(bool allowExport, TableTuple &tuple) {};

    TBPtr findBlock(char *tuple) {
        TBMapI i =
                        m_data.lower_bound(tuple);
        if (i == m_data.end() && m_data.empty()) {
            throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
        }
        if (i == m_data.end()) {
            i--;
            if (i.key() + m_tableAllocationSize < tuple) {
                throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
            }
        } else {
            if (i.key() != tuple) {
                i--;
                if (i.key() + m_tableAllocationSize < tuple) {
                    throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
                }
            }
        }
        return i.data();
    }

public:

    virtual bool equals(voltdb::Table *other);
    virtual voltdb::TableStats* getTableStats();

protected:
    Table(int tableAllocationTargetSize);
    void resetTable();

    void nextFreeTuple(TableTuple *tuple);
    TBPtr allocateNextBlock();

    /**
     * Normally this will return the tuple storage to the free list.
     * In the memcheck build it will return the storage to the heap.
     */
    void deleteTupleStorage(TableTuple &tuple);

    void initializeWithColumns(TupleSchema *schema, const std::string* columnNames, bool ownsTupleSchema);
    virtual void onSetColumns() {};

    // TUPLES AND MEMORY USAGE
    TableTuple m_tempTuple;
    boost::scoped_array<char> m_tempTupleMemory;
    /** not temptuple. these are for internal use. */
    TableTuple m_tmpTarget1, m_tmpTarget2;
    TupleSchema* m_schema;
    uint32_t m_tupleCount;
    uint32_t m_columnCount;
    uint32_t m_tuplesPerBlock;
    uint32_t m_tupleLength;
    int64_t m_nonInlinedMemorySize;
    // pointers to chunks of data
    TBMap m_data;

    char *m_columnHeaderData;
    int32_t m_columnHeaderSize;

    /**
     * Set of blocks with non-empty free lists or available tuples
     * that have never been allocated
    **/
    stx::btree_set<TBPtr > m_blocksWithSpace;

    // schema
    std::string* m_columnNames; // array of string names

    // GENERAL INFORMATION
    CatalogId m_databaseId;
    std::string m_name;

    /* If this table owns the TupleSchema it is responsible for deleting it in the destructor */
    bool m_ownsTupleSchema;

    const int m_tableAllocationTargetSize;
    int m_tableAllocationSize;

    // ptr to global integer tracking temp table memory allocated per frag
    // should be null for persistent tables
    int* m_tempTableMemoryInBytes;
  private:
    int32_t m_refcount;
};

inline TableTuple& Table::tempTuple() {
    assert (m_tempTuple.m_data);
    return m_tempTuple;
}

inline const std::string& Table::name()     const { return m_name; }

inline TBPtr Table::allocateNextBlock() {
    TBPtr block(new TupleBlock(this));
    m_data.insert( block->address(), block);
    if (m_tempTableMemoryInBytes) {
        (*m_tempTableMemoryInBytes) += m_tableAllocationSize;
        if ((*m_tempTableMemoryInBytes) > MAX_TEMP_TABLE_MEMORY) {
            throw SQLException(SQLException::volt_temp_table_memory_overflow,
                               "More than 100MB of temp table memory used while"
                               " executing SQL. Aborting.");
        }
    }
    return block;
}

inline void Table::deleteTupleStorage(TableTuple &tuple) {
    tuple.setActiveFalse(); // does NOT free strings

    // add to the free list
    m_tupleCount--;

    TBPtr block = findBlock(tuple.address());
    block->freeTuple(tuple.address());
    if (block->isEmpty()) {
        m_data.erase(block->address());
        m_blocksWithSpace.erase(block);
    } else {
        m_blocksWithSpace.insert(block);
    }
}


inline voltdb::CatalogId Table::databaseId() const { return m_databaseId; }

}
#endif
