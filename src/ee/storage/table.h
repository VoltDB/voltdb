/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
#include <cassert>

#include "common/ids.h"
#include "common/types.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "storage/TupleBlock.h"
#include "stx/btree_set.h"
#include "common/ThreadLocalPool.h"

class CopyOnWriteTest_CopyOnWriteIterator;
class CompactionTest_BasicCompaction;
class CompactionTest_CompactionWithCopyOnWrite;

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

/**
 * Represents a table which might or might not be a temporary table.
 * All tables, TempTable, PersistentTable and StreamedTable are derived
 * from this class.
 *
 * Table objects including derived classes are only instantiated via a
 * factory class (TableFactory).
 */
class Table {
    friend class TableFactory;
    friend class TableIterator;
    friend class CopyOnWriteContext;
    friend class ExecutionEngine;
    friend class TableStats;
    friend class StatsSource;
    friend class TupleBlock;
    friend class PersistentTableUndoDeleteAction;

  private:
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
    virtual TableIterator& iterator() = 0;
    virtual TableIterator *makeIterator() = 0;

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
    virtual size_t allocatedBlockCount() const = 0;

    TableTuple& tempTuple() {
        assert (m_tempTuple.m_data);
        return m_tempTuple;
    }

    int64_t allocatedTupleCount() const {
        return allocatedBlockCount() * m_tuplesPerBlock;
    }

    /**
     * Includes tuples that are pending any kind of delete.
     * Used by iterators to determine how many tupels to expect while scanning
     */
    virtual int64_t activeTupleCount() const {
        return m_tupleCount;
    }

    /*
     * Count of tuples that actively contain user data
     */
    int64_t usedTupleCount() const {
        return m_usedTupleCount;
    }

    virtual int64_t allocatedTupleMemory() const {
        return allocatedBlockCount() * m_tableAllocationSize;
    }

    int64_t occupiedTupleMemory() const {
        return m_tupleCount * m_tempTuple.tupleLength();
    }

    // Only counts persistent table usage, currently
    int64_t nonInlinedMemorySize() const {
        return m_nonInlinedMemorySize;
    }

    // ------------------------------------------------------------------
    // COLUMNS
    // ------------------------------------------------------------------
    int columnIndex(const std::string &name) const;
    std::vector<std::string> getColumnNames();

    inline const TupleSchema* schema() const {
        return m_schema;
    }

    inline const std::string& columnName(int index) const {
        return m_columnNames[index];
    }

    inline int columnCount() const {
        return m_columnCount;
    }

    const std::string *columnNames() {
        return m_columnNames;
    }

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
    const std::string& name() const {
        return m_name;
    }

    CatalogId databaseId() const {
        return m_databaseId;
    }

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
     */
    void loadTuplesFromNoHeader(SerializeInput &serialize_in,
                                Pool *stringPool = NULL);

    /**
     * Loads only tuple data, not schema, from the serialized table.
     * Used for initial data loading and receiving dependencies.
     */
    void loadTuplesFrom(SerializeInput &serialize_in,
                        Pool *stringPool = NULL);


    // ------------------------------------------------------------------
    // EXPORT
    // ------------------------------------------------------------------

    /**
     * Set the current offset in bytes of the export stream for this Table
     * since startup (used for rejoin/recovery).
     */
    virtual void setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed) {
        // this should be overidden by any table involved in an export
        assert(false);
    }

    /**
     * Get the current offset in bytes of the export stream for this Table
     * since startup (used for rejoin/recovery).
     */
    virtual void getExportStreamPositions(int64_t &seqNo, size_t &streamBytesUsed) {
        // this should be overidden by any table involved in an export
        assert(false);
    }

    /**
     * Release any committed Export bytes up to the provided stream offset
     */
    virtual bool releaseExportBytes(int64_t releaseOffset) {
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
    /**
     * Inform the tuple stream wrapper of the table's signature and the timestamp
     * of the current export generation
     */
    virtual void setSignatureAndGeneration(std::string signature, int64_t generation) {
    }

    virtual bool isExport() {
        return false;
    }

protected:
    /*
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * to do additional processing for views and Export
     */
    virtual void processLoadedTuple(TableTuple &tuple) {
    };

    virtual void swapTuples(TableTuple sourceTuple, TableTuple destinationTuple) {
        throwFatalException("Unsupported operation");
    }

public:

    virtual bool equals(voltdb::Table *other);
    virtual voltdb::TableStats* getTableStats();

protected:
    // virtual block management functions
    virtual void nextFreeTuple(TableTuple *tuple) = 0;

    Table(int tableAllocationTargetSize);
    void resetTable();

    bool compactionPredicate() {
        assert(m_tuplesPinnedByUndo == 0);
        return allocatedTupleCount() - activeTupleCount() > (m_tuplesPerBlock * 3) && loadFactor() < .95;
    }

    void initializeWithColumns(TupleSchema *schema, const std::string* columnNames, bool ownsTupleSchema);

    // per table-type initialization
    virtual void onSetColumns() {
    };

    double loadFactor() {
        return static_cast<double>(activeTupleCount()) /
            static_cast<double>(allocatedTupleCount());
    }


    // ------------------------------------------------------------------
    // DATA
    // ------------------------------------------------------------------

  protected:
    TableTuple m_tempTuple;
    boost::scoped_array<char> m_tempTupleMemory;

    // not temptuple. these are for internal use.
    TableTuple m_tmpTarget1, m_tmpTarget2;
    TupleSchema* m_schema;

    // schema as array of string names
    std::string* m_columnNames;
    char *m_columnHeaderData;
    int32_t m_columnHeaderSize;

    uint32_t m_tupleCount;
    uint32_t m_usedTupleCount;
    uint32_t m_tuplesPinnedByUndo;
    uint32_t m_columnCount;
    uint32_t m_tuplesPerBlock;
    uint32_t m_tupleLength;
    int64_t m_nonInlinedMemorySize;

    // identity information
    CatalogId m_databaseId;
    std::string m_name;

    // If this table owns the TupleSchema it is responsible for deleting it in the destructor
    bool m_ownsTupleSchema;

    const int m_tableAllocationTargetSize;
    int m_tableAllocationSize;

  private:
    int32_t m_refcount;
    ThreadLocalPool m_tlPool;
};

}
#endif
