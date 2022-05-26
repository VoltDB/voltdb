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

#ifndef HSTORETABLE_H
#define HSTORETABLE_H

#include "common/ids.h"
#include "common/LargeTempTableBlockId.hpp"
#include "common/types.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "common/TheHashinator.h"
#include "storage/TupleBlock.h"
#include "storage/ExportTupleStream.h"
#include "common/ThreadLocalPool.h"
#include "common/HiddenColumnFilter.h"

#include <vector>
#include <string>
#include <common/debuglog.h>

namespace voltdb {
class TableIterator;
class TableStats;

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
    char* m_columnHeaderData = nullptr;
    int32_t m_columnHeaderSize = -1;
    int32_t m_refcount = 0;
    ThreadLocalPool m_tlPool;
    int m_compactionThreshold = 95;

    Table();
    Table(Table const&);

  public:
    virtual ~Table();

    /*
     * Table lifespan can be managed by a reference count. The
     * reference is trivial to maintain since it is only accessed by
     * the execution engine thread. Snapshot, Export and the
     * corresponding CatalogDelegate may be reference count
     * holders. The table is deleted when the refcount falls to
     * zero. This allows longer running processes to complete
     * gracefully after a table has been removed from the catalog.
     */
    void incrementRefcount() { m_refcount += 1; }

    void decrementRefcount() {
        m_refcount -= 1;
        if (m_refcount == 0) {
            delete this;
        }
    }

    // ------------------------------------------------------------------
    // ACCESS METHODS
    // ------------------------------------------------------------------
    virtual TableIterator iterator() = 0;
    virtual TableIterator iteratorDeletingAsWeGo() = 0;

    // ------------------------------------------------------------------
    // OPERATIONS
    // ------------------------------------------------------------------
    virtual void deleteAllTuples() = 0;
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    // -- Most callers should be using TempTable::insertTempTuple, anyway.
    virtual bool insertTuple(TableTuple& tuple) = 0;

    // ------------------------------------------------------------------
    // TUPLES AND MEMORY USAGE
    // ------------------------------------------------------------------
    virtual size_t allocatedBlockCount() const = 0;

    TableTuple& tempTuple() {
        vassert(m_tempTuple.m_data);
        m_tempTuple.resetHeader();
        m_tempTuple.setActiveTrue();
        // Temp tuples are typically re-used so their data can change frequently.
        // Mark inlined, variable-length data as volatile.
        m_tempTuple.setInlinedDataIsVolatileTrue();
        return m_tempTuple;
    }

    int64_t allocatedTupleCount() const {
        return allocatedBlockCount() * m_tuplesPerBlock;
    }

    /**
     * Includes tuples that are pending any kind of delete.
     * Used by iterators to determine how many tuples to expect while scanning
     */
    virtual int64_t activeTupleCount() const { return m_tupleCount; }

    virtual int64_t allocatedTupleMemory() const {
        return allocatedBlockCount() * m_tableAllocationSize;
    }

    // Only counts persistent table usage, currently
    int64_t nonInlinedMemorySize() const { return m_nonInlinedMemorySize; }

    virtual int tupleLimit() const { return INT_MIN; }

    // ------------------------------------------------------------------
    // COLUMNS
    // ------------------------------------------------------------------
    int columnIndex(std::string const& name) const;

    std::vector<std::string> const& getColumnNames() const { return m_columnNames; }

    TupleSchema const* schema() const { return m_schema; }

    std::string const& columnName(int index) const { return m_columnNames[index]; }

    int columnCount() const { return m_columnCount; }

    // ------------------------------------------------------------------
    // UTILITY
    // ------------------------------------------------------------------
    std::string const& name() const { return m_name; }

    CatalogId databaseId() const { return m_databaseId; }

    virtual std::string tableType() const = 0;

    // Return a string containing info about this table
    std::string debug() const {
        return debug("");
    }

    // Return a string containing info about this table
    // (each line prefixed by the given string)
    virtual std::string debug(const std::string &spacer) const;

    // ------------------------------------------------------------------
    // SERIALIZATION
    // ------------------------------------------------------------------
    size_t getColumnHeaderSizeToSerialize();

    size_t getAccurateSizeToSerialize();

    void serializeTo(SerializeOutput& serialOutput);

    void serializeToWithoutTotalSize(SerializeOutput& serialOutput);

    void serializeColumnHeaderTo(SerializeOutput& serialOutput, HiddenColumnFilter::Type hiddenColumnFilter);

    void serializeColumnHeaderTo(SerializeOutput& serialOutput);

    /*
     * Serialize a single tuple as a table so it can be sent to Java.
     */
    void serializeTupleTo(SerializeOutput& serialOutput, TableTuple* tuples, int numTuples);

    /**
     * Loads only tuple data and assumes there is no schema present.
     * Used for recovery where the schema is not sent.
     */
    void loadTuplesFromNoHeader(SerializeInputBE& serialInput,
                                Pool* stringPool = NULL);

    /**
     * Loads only tuple data, not schema, from the serialized table.
     * Used for initial data loading and receiving dependencies.
     */
    void loadTuplesFrom(SerializeInputBE& serialInput,
                        Pool* stringPool = NULL);


    // ------------------------------------------------------------------
    // EXPORT
    // ------------------------------------------------------------------

    /**
     * Set the current offset in bytes of the export stream for this Table
     * since startup (used for rejoin/recovery).
     */
    virtual void setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed, int64_t generationIdCreated) {
        // this should be overidden by any table involved in an export
        vassert(false);
    }

    /**
     * Get the current offset in bytes of the export stream for this Table
     * since startup (used for rejoin/recovery).
     */
    virtual void getExportStreamPositions(int64_t& seqNo, size_t& streamBytesUsed, int64_t &genId) {
        // this should be overidden by any table involved in an export
        vassert(false);
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
     * These metrics are needed by some iterators.
     */
    uint32_t getTupleLength() const {
        return m_tupleLength;
    }
    int getTableAllocationSize() const {
        return m_tableAllocationSize;
    }
    uint32_t getTuplesPerBlock() const {
        return m_tuplesPerBlock;
    }

    virtual int64_t validatePartitioning(TheHashinator* hashinator, int32_t partitionId) {
        throwFatalException("Validate partitioning unsupported on this table type");
        return 0;
    }

    // Used by delete-as-you-go iterators.  Returns an iterator to the block id of the next block.
    virtual std::vector<LargeTempTableBlockId>::iterator releaseBlock(std::vector<LargeTempTableBlockId>::iterator it) {
        throw SerializableEEException("May only use releaseBlock with instances of LargeTempTable.");
    }

    virtual void freeLastScannedBlock(std::vector<TBPtr>::iterator nextBlockIterator) {
        throw SerializableEEException("May only use freeLastScannedBlock with instances of TempTable.");
    }

    bool equals(voltdb::Table* other);
    virtual voltdb::TableStats* getTableStats() = 0;

    // Return tuple blocks addresses
    virtual std::vector<uint64_t> getBlockAddresses() const = 0;

    virtual void swapTuples(TableTuple& sourceTupleWithNewValues, TableTuple& destinationTuple) {
        throwFatalException("Unsupported operation");
    }
protected:
    /*
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * to do additional processing for views and Export
     */
    virtual void processLoadedTuple(TableTuple& tuple,
                                    ReferenceSerializeOutput* uniqueViolationOutput,
                                    int32_t& serializedTupleCount,
                                    size_t& tupleCountPosition,
                                    bool shouldDRStreamRow = false) { }

    // virtual block management functions
    virtual void nextFreeTuple(TableTuple* tuple) = 0;

    Table(int tableAllocationTargetSize);
    void resetTable();

    bool compactionPredicate() {
        //Unfortunate work around for the fact that multiple undo quantums cause this to happen
        //Ideally there would be one per transaction and we could hard fail or
        //the undo log would only trigger compaction once per transaction
        if (m_tuplesPinnedByUndo != 0) {
            return false;
        }

        size_t unusedTupleCount = allocatedTupleCount() - activeTupleCount();
        size_t blockThreshold = m_tuplesPerBlock * 3;
        size_t percentBasedThreshold = (allocatedTupleCount() * (100 - m_compactionThreshold)) / 100;
        size_t actualThreshold = std::max(blockThreshold, percentBasedThreshold);
        return unusedTupleCount > actualThreshold;
    }

    virtual void initializeWithColumns(TupleSchema* schema, std::vector<std::string> const& columnNames,
          bool ownsTupleSchema, int32_t compactionThreshold = 95);
    bool checkNulls(TableTuple& tuple) const;

    void serializeColumnHeaderTo(SerializeOutput& serialOutput, HiddenColumnFilter *hiddenColumnFilter);

    // ------------------------------------------------------------------
    // DATA
    // ------------------------------------------------------------------
    TableTuple m_tempTuple{};
    boost::scoped_array<char> m_tempTupleMemory;

    TupleSchema* m_schema = nullptr;

    // CONSTRAINTS
    std::vector<bool> m_allowNulls;

    // schema as array of string names
    std::vector<std::string> m_columnNames{};

    uint32_t m_tupleCount = 0;
    uint32_t m_tuplesPinnedByUndo = 0;
    uint32_t m_columnCount = 0;
    uint32_t m_tuplesPerBlock = 0;
    uint32_t m_tupleLength;
    int64_t m_nonInlinedMemorySize = 0;

    // identity information
    CatalogId m_databaseId = -1;
    std::string m_name{};

    // If this table owns the TupleSchema it is responsible for deleting it in the destructor
    bool m_ownsTupleSchema = true;

    int const m_tableAllocationTargetSize;
    // This is one block size allocated for this table, equals = m_tuplesPerBlock * m_tupleLength
    int m_tableAllocationSize;
};

}
#endif
