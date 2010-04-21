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

#ifndef HSTORETEMPTABLE_H
#define HSTORETEMPTABLE_H

#include "table.h"
#include "common/tabletuple.h"

namespace voltdb {

class TableColumn;
class TableFactory;
class TableStats;

/**
 * Represents a Temporary Table to store temporary result (final
 * result or intermediate result).  Temporary Table has no indexes,
 * constraints and reverting.  So, appending tuples to temporary table
 * is much faster than PersistentTable.  deleteTuple is not suported
 * in TempTable to make it faster, use deleteAllTuples instead.  As
 * there is no deleteTuple, there is no freelist; TempTable does a
 * efficient thing for iterating and deleteAllTuples.
 */
class TempTable : public Table {
    friend class TableFactory;

  private:
    // no copies, no assignment
    TempTable(TempTable const&);
    TempTable operator=(TempTable const&);

  public:
        virtual ~TempTable();

        virtual void cleanupManagedBuffers(Topend *cxt) {
        }

        // ------------------------------------------------------------------
        // OPERATIONS
        // ------------------------------------------------------------------
        void deleteAllTuples(bool freeAllocatedStrings);
        bool insertTuple(TableTuple &source);
        bool updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes);
        bool deleteTuple(TableTuple &tuple, bool); // deleting tuple from temp table is not supported. use deleteAllTuples instead
        void deleteAllTuplesNonVirtual(bool freeAllocatedStrings);

        /**
         * Uses the pool to do a deep copy of the tuple including allocations
         * for all uninlined columns. Used by CopyOnWriteContext to back up tuples
         * before they are dirtied
         */
        void insertTupleNonVirtualWithDeepCopy(TableTuple &source, Pool *pool);

        /**
         * Does a shallow copy that copies the pointer to uninlined columns.
         */
        void insertTupleNonVirtual(TableTuple &source);
        void updateTupleNonVirtual(TableTuple &source, TableTuple &target);

        // ------------------------------------------------------------------
        // INDEXES
        // ------------------------------------------------------------------
        int getNumOfIndexes() const             { return (0); }
        int getNumOfUniqueIndexes() const       { return (0); }

        // ------------------------------------------------------------------
        // UTILITIY
        // ------------------------------------------------------------------
        std::string tableType() const;
        void getNextFreeTupleInlined(TableTuple *tuple);
        voltdb::TableStats* getTableStats();

    protected:
        // can not use this constructor to coerce a cast
        explicit TempTable();
};

inline void TempTable::insertTupleNonVirtualWithDeepCopy(TableTuple &source, Pool *pool) {
    //
    // First get the next free tuple
    // This will either give us one from the free slot list, or
    // grab a tuple at the end of our chunk of memory
    //
    getNextFreeTupleInlined(&m_tmpTarget1);
    ++m_tupleCount;
    //
    // Then copy the source into the target. Pass false for heapAllocateStrings.
    // Don't allocate space for the strings on the heap because the strings are being copied from the source
    // are owned by a PersistentTable or part of the EE string pool.
    //
    m_tmpTarget1.copyForPersistentInsert(source, pool); // tuple in freelist must be already cleared
    m_tmpTarget1.setDeletedFalse();
}

inline void TempTable::insertTupleNonVirtual(TableTuple &source) {
    //
    // First get the next free tuple
    // This will either give us one from the free slot list, or
    // grab a tuple at the end of our chunk of memory
    //
    getNextFreeTupleInlined(&m_tmpTarget1);
    ++m_tupleCount;
    //
    // Then copy the source into the target. Pass false for heapAllocateStrings.
    // Don't allocate space for the strings on the heap because the strings are being copied from the source
    // are owned by a PersistentTable or part of the EE string pool.
    //
    m_tmpTarget1.copy(source); // tuple in freelist must be already cleared
    m_tmpTarget1.setDeletedFalse();
}

inline void TempTable::updateTupleNonVirtual(TableTuple &source, TableTuple &target) {
    // Copy the source tuple into the target
    target.copy(source);
}

inline void TempTable::deleteAllTuplesNonVirtual(bool freeAllocatedStrings) {
    // as there must be no hole.
    assert (m_tupleCount == m_usedTuples);

    if (m_tupleCount == 0) {
        return;
    }

    // Mark tuples as deleted and free strings. No indexes to update.
    // Don't call deleteTuple() here.
    const uint16_t uninlinedStringColumnCount = m_schema->getUninlinedObjectColumnCount();
    if (freeAllocatedStrings && uninlinedStringColumnCount > 0) {
        for (int64_t i = m_tupleCount - 1; i >= 0; i--) {
          m_tmpTarget1.move(dataPtrForTuple((int)i));
          m_tmpTarget1.freeObjectColumns();
        }
    }

    m_tupleCount = 0;
    m_usedTuples = 0;

    // make temp tables free memory allocated during fragment execution
    // reset back to base size
    while (m_data.size() > 1) {
#if defined(MEMCHECK_NOFREELIST)
        //Chunks and individual tuples storage are the same in the memcheck build so
        //when doing memcheck call delete tuple storage to delete the chunk in order
        //to correctly update the metadata kept in the memcheck build
        char* chunk = m_data.back();
        m_data.pop_back();
        m_allocatedTuples -= m_tuplesPerBlock;
        // if keeping track of fragment temp memory, decrease it here
        if (m_tempTableMemoryInBytes)
            (*m_tempTableMemoryInBytes) -= (m_tableAllocationTargetSize * (m_schema->tupleLength() + TUPLE_HEADER_SIZE));
        assert(chunk != NULL);
        TableTuple tuple(this->schema());
        tuple.move(chunk);
        deleteTupleStorage(tuple);
        // deleteTupleStorage mutates m_tupleCount. Fix it explictly.
        m_tupleCount = 0;
#elif defined(MEMCHECK)
        char* chunk = m_data.back();
        m_data.pop_back();
        m_allocatedTuples -= m_tuplesPerBlock;
        if (m_tempTableMemoryInBytes) {
            (*m_tempTableMemoryInBytes) -= (m_schema->tupleLength() + TUPLE_HEADER_SIZE);
        }
        assert(chunk != NULL);
        delete[] chunk;
#else
        char* chunk = m_data.back();
        m_data.pop_back();
        m_allocatedTuples -= m_tuplesPerBlock;
        // if keeping track of fragment temp memory, decrease it here
        if (m_tempTableMemoryInBytes)
            (*m_tempTableMemoryInBytes) -= m_tableAllocationTargetSize;
        assert(chunk != NULL);
        delete[] chunk;
#endif
    }

    assert(m_allocatedTuples == m_tuplesPerBlock);
}
inline void TempTable::getNextFreeTupleInlined(TableTuple *tuple) {
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
}

}

#endif
