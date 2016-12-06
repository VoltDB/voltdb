/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef HSTORETEMPTABLE_H
#define HSTORETEMPTABLE_H

#include "table.h"
#include "common/tabletuple.h"
#include "common/ThreadLocalPool.h"
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
class TempTable : public Table {
    friend class TableFactory;
    friend class TableIterator;

  private:
    // no copies, no assignment
    TempTable(TempTable const&);
    TempTable operator=(TempTable const&);

    // default iterator
    TableIterator m_iter;

  public:
    // Return the table iterator by reference
    TableIterator& iterator() {
        m_iter.reset(m_data.begin());
        return m_iter;
    }

    TableIterator* makeIterator() {
        return new TableIterator(this, m_data.begin());
    }

    TableIterator& iteratorDeletingAsWeGo() {
        m_iter.reset(m_data.begin());
        m_iter.setTempTableDeleteAsGo(true);
        return m_iter;
    }

    virtual ~TempTable();

    // ------------------------------------------------------------------
    // GENERIC TABLE OPERATIONS
    // ------------------------------------------------------------------
    virtual void deleteAllTuples(bool freeAllocatedStrings, bool=true);
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    // -- Most callers should be using TempTable::insertTempTuple, anyway.
    virtual bool insertTuple(TableTuple &tuple);

    void deleteAllTempTupleDeepCopies();

    void deleteAllTempTuples();

    /**
     * Uses the pool to do a deep copy of the tuple including allocations
     * for all uninlined columns. Used by CopyOnWriteContext to back up tuples
     * before they are dirtied
     */
    void insertTempTupleDeepCopy(const TableTuple &source, Pool *pool);

    /**
     * Does a shallow copy that copies the pointer to uninlined columns.
     */
    void insertTempTuple(TableTuple &source);
    // Deprecating this ugly name, and bogus return value. For now it's a wrapper.
    bool isTempTableEmpty() { return m_tupleCount == 0; }

    int64_t tempTableTupleCount() const { return m_tupleCount; }

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
    const TempTableLimits* getTempTableLimits() const {
        return m_limits;
    }

  protected:
    // can not use this constructor to coerce a cast
    explicit TempTable();

    size_t allocatedBlockCount() const {
        return m_data.size();
    }

    TBPtr allocateNextBlock();
    void nextFreeTuple(TableTuple *tuple);

    void freeLastScanedBlock(std::vector<TBPtr>::iterator nextBlockIterator);
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
    // Then copy the source into the target. Pass false for heapAllocateStrings.
    // Don't allocate space for the strings on the heap because the strings are being copied from the source
    // are owned by a PersistentTable or part of the EE string pool.
    //
    target.copy(source); // tuple in freelist must be already cleared
    target.setActiveTrue();
    target.setPendingDeleteFalse();
    target.setPendingDeleteOnUndoReleaseFalse();
}

inline void TempTable::deleteAllTempTuples() {
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
    tuple->move(pair.first);
    ++m_tupleCount;
    return;
}

inline void TempTable::freeLastScanedBlock(std::vector<TBPtr>::iterator nextBlockIterator) {
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
