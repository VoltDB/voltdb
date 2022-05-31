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

#ifndef HSTORETEMPTABLE_H
#define HSTORETEMPTABLE_H

#include "common/tabletuple.h"
#include "common/ThreadLocalPool.h"
#include "storage/AbstractTempTable.hpp"
#include "storage/tableiterator.h"
#include "storage/TempTableLimits.h"
#include "storage/TupleBlock.h"

namespace voltdb {

class TableColumn;
class TableFactory;
class TableStats;

/**
 * Represents a Temporary Table to store temporary result (final
 * result or intermediate result).  Temporary Table has no indexes,
 * constraints and reverting.  So, appending tuples to temporary table
 * is much faster than PersistentTable.  deleteTuple is not supported
 * in TempTable to make it faster, use deleteAllTuples instead.  As
 * there is no deleteTuple, there is no freelist; TempTable does a
 * efficient thing for iterating and deleteAllTuples.
 */
class TempTable : public AbstractTempTable {
    friend class TableFactory;
    friend class TableIterator;

  private:
    // no copies, no assignment
    TempTable(TempTable const&);
    TempTable operator=(TempTable const&);

  public:
    TableIterator iterator() {
        return TableIterator(this, m_data.begin(), false);
    }

    TableIterator iteratorDeletingAsWeGo() {
        return TableIterator(this, m_data.begin(), true);
    }

    virtual ~TempTable();

    // ------------------------------------------------------------------
    // GENERIC TABLE OPERATIONS
    // ------------------------------------------------------------------
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    // -- Most callers should be using TempTable::insertTempTuple, anyway.
    virtual bool insertTuple(TableTuple &tuple);

    void deleteAllTempTupleDeepCopies();

    virtual void deleteAllTuples();

    /**
     * Uses the pool to do a deep copy of the tuple including allocations
     * for all uninlined columns. Used by CopyOnWriteContext to back up tuples
     * before they are dirtied
     */
    void insertTempTupleDeepCopy(const TableTuple &source, Pool *pool);

    /**
     * Does a shallow copy that copies the pointer to uninlined columns.
     */
    virtual void insertTempTuple(TableTuple &source);

    virtual void finishInserts() {}

    bool isTempTableEmpty() { return m_tupleCount == 0; }

    virtual int64_t tempTableTupleCount() const { return m_tupleCount; }

    // ------------------------------------------------------------------
    // INDEXES
    // ------------------------------------------------------------------
    int getNumOfIndexes() const             { return (0); }
    int getNumOfUniqueIndexes() const       { return (0); }

    // ------------------------------------------------------------------
    // UTILITIY
    // ------------------------------------------------------------------
    virtual std::string tableType() const;
    virtual voltdb::TableStats* getTableStats();
    virtual const TempTableLimits* getTempTableLimits() const {
        return m_limits;
    }

    /**
     * Swap the contents of this table with another TempTable
     */
    virtual void swapContents(AbstractTempTable* otherTable) {
        TempTable* otherTempTable = dynamic_cast<TempTable*>(otherTable);
        vassert (otherTempTable);
        AbstractTempTable::swapContents(otherTable);
        m_data.swap(otherTempTable->m_data);
    }

  protected:

    TempTable();

    size_t allocatedBlockCount() const {
        return m_data.size();
    }

    TBPtr allocateNextBlock();
    void nextFreeTuple(TableTuple *tuple);

    void freeLastScannedBlock(std::vector<TBPtr>::iterator nextBlockIterator);
    std::vector<TBPtr>::iterator getDataEndBlockIterator();

    virtual void onSetColumns() {
        m_data.clear();
    };

    std::vector<uint64_t> getBlockAddresses() const;

  private:
    // pointers to chunks of data. Specific to table impl. Don't leak this type.
    std::vector<TBPtr> m_data;

    // ptr to global integer tracking temp table memory allocated per frag
    TempTableLimits* m_limits;
};

inline void TempTable::insertTempTupleDeepCopy(const TableTuple &source, Pool *pool) {

    // First get the next free tuple by
    // grabbing a tuple at the end of our chunk of memory
    TableTuple target(m_schema);
    TempTable::nextFreeTuple(&target);

    //
    // Then copy the source into the target. Pass false for heapAllocateStrings.
    // Don't allocate space for the strings on the heap because the strings are being copied from the source
    // are owned by a PersistentTable or part of the EE string pool.
    //
    target.copyForPersistentInsert(source, pool); // tuple in freelist must be already cleared
    target.setActiveTrue();
    target.setInlinedDataIsVolatileFalse();
    target.setNonInlinedDataIsVolatileFalse();
}

inline void TempTable::insertTempTuple(TableTuple &source) {
    //
    // First get the next free tuple
    // This will either give us one from the free slot list, or
    // grab a tuple at the end of our chunk of memory
    //
    TableTuple target(m_schema);
    TempTable::nextFreeTuple(&target);

    //
    // Then copy the source into the target.
    // Any non-inlined variable-length data will have been allocated
    // in the temp string pool, where it can remain until fragment
    // execution is complete.
    //
    target.copy(source); // tuple in freelist must be already cleared
    target.setActiveTrue();
    target.setPendingDeleteFalse();
    target.setPendingDeleteOnUndoReleaseFalse();
    target.setInlinedDataIsVolatileFalse();
    target.setNonInlinedDataIsVolatileFalse();
}

inline void TempTable::deleteAllTuples() {
    if (m_tupleCount == 0) {
        return;
    }

    m_tupleCount = 0;
    int remaining = m_data.size();
    for (; remaining > 1; --remaining) {
        TBPtr blockPtr = m_data.back();
        m_data.pop_back();
        // These temp table blocks may have been cleaned up
        // and set null already by the delete as we go feature.
        if (m_limits && blockPtr) {
            m_limits->reduceAllocated(m_tableAllocationSize);
        }
    }

    // cheap clear of the preserved first block
    if (remaining) {
        m_data[0]->reset();
    }
}

inline TBPtr TempTable::allocateNextBlock() {
    TBPtr block(new TupleBlock(this, TBBucketPtr()));
    m_data.push_back(block);

    if (m_limits) {
        m_limits->increaseAllocated(m_tableAllocationSize);
    }

    return block;
}

inline void TempTable::nextFreeTuple(TableTuple *tuple) {

    if (m_data.empty()) {
        allocateNextBlock();
    }

    TBPtr block = m_data.back();
    if (!block->hasFreeTuples()) {
        block = allocateNextBlock();
    }

    std::pair<char*, int> pair = block->nextFreeTuple();
    if (pair.first == NULL) {
        block = allocateNextBlock();
        pair = block->nextFreeTuple();
    }
    tuple->moveAndInitialize(pair.first);
    ++m_tupleCount;
    return;
}

inline void TempTable::freeLastScannedBlock(std::vector<TBPtr>::iterator nextBlockIterator) {
    if (m_data.begin() != nextBlockIterator) {
        nextBlockIterator--;
        // somehow we preserve the first block
        if (m_data.begin() != nextBlockIterator) {
            *nextBlockIterator = NULL;
            if (m_limits) {
                m_limits->reduceAllocated(m_tableAllocationSize);
            }
        }
    }
}

}

#endif
