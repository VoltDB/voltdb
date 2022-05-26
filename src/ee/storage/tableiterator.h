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

#pragma once

#include "common/LargeTempTableBlockCache.h"
#include "common/LargeTempTableBlockId.hpp"
#include "common/debuglog.h"
#include "common/tabletuple.h"

#include "storage/TupleIterator.h"
#include "storage/table.h"


namespace voltdb {

class TempTable;
class PersistentTable;

/**
 * Iterator for table which neglects deleted tuples.
 * TableIterator is a small and copiable object.
 * You can copy it, not passing a pointer of it.
 *
 * This class should be a virtual interface or should
 * be templated on the underlying table data iterator.
 * Either change requires some updating of the iterators
 * that are persvasively stack allocated...
 *
 */
class TableIterator : public TupleIterator {

    friend class TempTable;
    friend class PersistentTable;
    friend class LargeTempTable;

public:

    /**
     * Updates the given tuple so that it points to the next tuple in the table.
     * @param out the tuple will point to the retrieved tuple if this method returns true.
     * @return true if succeeded. false if no more active tuple is there.
    */
    bool next(TableTuple &out);

    /**
     * Updates the given tuple so that it points to the next
     * off-th tuple, relative to the current position, in the table.
     * The semantics is similar to read(2) of <unistd.h>, minus error code.
     * @param out the tuple will advance at most off positions
     * @param off offset to advance
     * @return actual advanced position
     */
    size_t advance(TableTuple& out, size_t const off);

    bool hasNext() const;

    uint32_t getFoundTuples() const {
        return m_foundTuples;
    }

    void setTempTableDeleteAsGo(bool flag) {
        switch (m_iteratorType) {
        case TEMP:
        case LARGE_TEMP:
            m_state.m_tempTableDeleteAsGo = flag;
            break;
        default:
            // For persistent tables, this has no effect
            break;
        }
    }

    /**
     * Sets this iterator to its pre-scan state
     * for its table.
     *
     * For iterators on large temp tables, it will unpin the block
     * that was being scanned.
     */
    void reset() {
        if (m_state.m_tempTableDeleteAsGo) {
            *this = m_table->iteratorDeletingAsWeGo();
        }
        else {
            *this = m_table->iterator();
        }
    }

    bool operator ==(const TableIterator &other) const {
        return (m_table == other.m_table
                && m_foundTuples == other.m_foundTuples
                && m_activeTuples == other.m_activeTuples);
    }

    bool operator !=(const TableIterator &other) const {
        return ! ((*this) == other);
    }

    // Copy constructor
    TableIterator(const TableIterator& that);

    // Copy assignment
    TableIterator& operator=(const TableIterator& that);

    virtual ~TableIterator();

protected:
    /** Constructor for persistent tables */
    TableIterator(Table *, TBMapI);

    /** Constructor for temp tables */
    TableIterator(Table *, std::vector<TBPtr>::iterator, bool deleteAsGo);

    /** Constructor for large temp tables */
    TableIterator(Table *, std::vector<LargeTempTableBlockId>::iterator, bool deleteAsGo);

    /** moves iterator to beginning of table.
        (Called only for persistent tables) */
    void reset(TBMapI);

    /** moves iterator to beginning of table.
        (Called only for temp tables) */
    void reset(std::vector<TBPtr>::iterator);

    /** moves iterator to beginning of table.
        (Called only for large temp tables) */
    void reset(std::vector<LargeTempTableBlockId>::iterator);

    bool continuationPredicate();

    /** Next methods for each table type.  (In a perfect world these
        would be virtual methods in subclasses.)
     */
    bool persistentNext(TableTuple &out);
    bool tempNext(TableTuple &out);
    bool largeTempNext(TableTuple &out);

    /**
     * Unpin the currently scanned block
     */
    void finishLargeTempTableScan();

    TBMapI getBlockIterator() const {
        vassert(m_iteratorType == PERSISTENT);
        return m_state.m_persBlockIterator;
    }

    void setBlockIterator(const TBMapI& it) {
        vassert(m_iteratorType == PERSISTENT);
        m_state.m_persBlockIterator = it;
    }

    /**
     * Do not use this.  It's only need for JumpingTableIterator,
     * which is used in unit tests.
     */
    void setFoundTuples(uint32_t found) {
        m_foundTuples = found;
    }

    uint32_t getTuplesPerBlock() {
        return m_table->getTuplesPerBlock();
    }

private:

    /** The type of table we're iterating over */
    enum IteratorType {
        PERSISTENT,
        TEMP,
        LARGE_TEMP
    };

    /**
     * A struct for members that are used only for some types of
     * iterators.
     *
     * The iterator members of this struct will be exclusive based on
     * the type of table: an iterator for a persistent table will
     * never use m_tempBlockIterator or m_largeTempBlockIterator.
     * This struct could actually be a union, but without C++11
     * support the iterators that use non-trivial constructors are
     * problematic.
     */
    struct TypeSpecificState {

        /** Construct an invalid iterator that needs to be
            intitialized. */
        TypeSpecificState()
        : m_persBlockIterator()
        , m_tempBlockIterator()
        , m_largeTempBlockIterator()
        , m_tempTableDeleteAsGo(false)
        {
        }

        /** Construct an iterator for a persistent table */
        TypeSpecificState(TBMapI it)
        : m_persBlockIterator(it)
        , m_tempBlockIterator()
        , m_largeTempBlockIterator()
        , m_tempTableDeleteAsGo(false)
        {
        }

        /** Construct an iterator for a temp table */
        TypeSpecificState(std::vector<TBPtr>::iterator it, bool deleteAsGo)
        : m_persBlockIterator()
        , m_tempBlockIterator(it)
        , m_largeTempBlockIterator()
        , m_tempTableDeleteAsGo(deleteAsGo)
        {
        }

        /** Construct an iterator for a large temp table */
        TypeSpecificState(std::vector<LargeTempTableBlockId>::iterator it, bool deleteAsGo)
        : m_persBlockIterator()
        , m_tempBlockIterator()
        , m_largeTempBlockIterator(it)
        , m_tempTableDeleteAsGo(deleteAsGo)
        {
        }

        ~TypeSpecificState()
        {
        }

        /** Table block iterator for persistent tables */
        TBMapI m_persBlockIterator;

        /** Table block iterator for normal temp tables */
        std::vector<TBPtr>::iterator m_tempBlockIterator;

        /** Table block iterator for large temp tables */
        std::vector<LargeTempTableBlockId>::iterator m_largeTempBlockIterator;

        /** "delete as you go" flag for normal and large temp tables
         * (Not used for persistent tables)
         */
        bool m_tempTableDeleteAsGo;
    };

    // State that is common to all kinds of iterators:

    /** The table we're iterating over */
    Table *m_table;

    /** The length of each tuple.  This is stored in the table, but is
        cached here for speed.  */
    uint32_t m_tupleLength;

    /** The number of tuples in the table */
    uint32_t m_activeTuples;

    /** The number of tuples returned so far by this iterator, since
        it was constructed or reset.  Scan is complete when
        m_foundTuples == m_activeTuples. */
    uint32_t m_foundTuples;

    /** Pointer to our current position in the current block. */
    char *m_dataPtr;

    /** Pointer to the first tuple after the last valid tuple in the
        current block.  Scan of current block is complete when
        m_dataPtr == m_dataEndPtr. */
    char *m_dataEndPtr;

    /** The type of iterator based on the kind of table that we're scanning. */
    IteratorType m_iteratorType;

    /** State that is specific to the type of table we're iterating
        over: */
    TypeSpecificState m_state;
};

// Construct iterator for persistent tables
inline TableIterator::TableIterator(Table *parent, TBMapI start)
    : m_table(parent)
    , m_tupleLength(parent->getTupleLength())
    , m_activeTuples((int) m_table->activeTupleCount())
    , m_foundTuples(0)
    , m_dataPtr(NULL)
    , m_dataEndPtr(NULL)
    , m_iteratorType(PERSISTENT)
    , m_state(start)
{
}

// Construct iterator for temp tables
inline TableIterator::TableIterator(Table *parent, std::vector<TBPtr>::iterator start, bool deleteAsGo)
    : m_table(parent)
    , m_tupleLength(parent->getTupleLength())
    , m_activeTuples((int) m_table->activeTupleCount())
    , m_foundTuples(0)
    , m_dataPtr(NULL)
    , m_dataEndPtr(NULL)
    , m_iteratorType(TEMP)
    , m_state(start, deleteAsGo)
{
}

//  Construct an iterator for large temp tables
inline TableIterator::TableIterator(Table *parent, std::vector<LargeTempTableBlockId>::iterator start, bool deleteAsGo)
    : m_table(parent)
    , m_tupleLength(parent->getTupleLength())
    , m_activeTuples((int) m_table->activeTupleCount())
    , m_foundTuples(0)
    , m_dataPtr(NULL)
    , m_dataEndPtr(NULL)
    , m_iteratorType(LARGE_TEMP)
    , m_state(start, deleteAsGo)
{
}

// Construct an iterator from another iterator
inline TableIterator::TableIterator(const TableIterator &that)
    : m_table(that.m_table)
    , m_tupleLength(that.m_tupleLength)
    , m_activeTuples(that.m_activeTuples)
    , m_foundTuples(that.m_foundTuples)
    , m_dataPtr(that.m_dataPtr)
    , m_dataEndPtr(that.m_dataEndPtr)
    , m_iteratorType(that.m_iteratorType)
    , m_state(that.m_state)
{
    // This assertion could fail if we are copying an invalid iterator
    // (table changed after iterator was created)
    vassert(that.m_table->activeTupleCount() == that.m_activeTuples);
}

inline TableIterator& TableIterator::operator=(const TableIterator& that) {
    // This assertion could fail if we are copying an invalid iterator
    // (table changed after iterator was created)
    vassert(that.m_table->activeTupleCount() == that.m_activeTuples);

    if (*this != that) {
        if (m_iteratorType == LARGE_TEMP) {
            finishLargeTempTableScan();
        }

        m_table = that.m_table;
        m_tupleLength = that.m_tupleLength;
        m_activeTuples = that.m_activeTuples;
        m_foundTuples = that.m_foundTuples;
        m_dataPtr = that.m_dataPtr;
        m_dataEndPtr = that.m_dataEndPtr;
        m_iteratorType = that.m_iteratorType;
        m_state = that.m_state;
    }

    return *this;
}

inline void TableIterator::reset(TBMapI start) {
    vassert(m_iteratorType == PERSISTENT);

    m_tupleLength = m_table->getTupleLength();
    m_activeTuples = (int) m_table->activeTupleCount();
    m_foundTuples = 0;
    m_dataPtr = NULL;
    m_dataEndPtr = NULL;
    m_state.m_persBlockIterator = start;
}

inline void TableIterator::reset(std::vector<TBPtr>::iterator start) {
    vassert(m_iteratorType == TEMP);

    m_tupleLength = m_table->getTupleLength();
    m_activeTuples = (int) m_table->activeTupleCount();
    m_foundTuples = 0;
    m_dataPtr = NULL;
    m_dataEndPtr = NULL;
    m_state.m_tempBlockIterator = start;
    m_state.m_tempTableDeleteAsGo = false;
}

 inline void TableIterator::reset(std::vector<LargeTempTableBlockId>::iterator start) {
    vassert(m_iteratorType == LARGE_TEMP);

    // Unpin the block of the previous scan before resetting.
    finishLargeTempTableScan();

    m_tupleLength = m_table->getTupleLength();
    m_activeTuples = (int) m_table->activeTupleCount();
    m_foundTuples = 0;
    m_dataPtr = NULL;
    m_dataEndPtr = NULL;
    m_state.m_largeTempBlockIterator = start;
    m_state.m_tempTableDeleteAsGo = false;
}

inline bool TableIterator::hasNext() const {
    return m_foundTuples < m_activeTuples;
}

// This function should be replaced by specific iteration functions
// when the caller knows the table type.
inline bool TableIterator::next(TableTuple &out) {
    switch (m_iteratorType) {
    case TEMP:
        return tempNext(out);
    case PERSISTENT:
        return persistentNext(out);
    case LARGE_TEMP:
    default:
        vassert(m_iteratorType == LARGE_TEMP);
        return largeTempNext(out);
    }
}

inline size_t TableIterator::advance(TableTuple& out, size_t const off) {
   size_t advanced = 0;
   while(advanced++ < off && next(out));
   return advanced;
}

inline bool TableIterator::persistentNext(TableTuple &out) {
    while (m_foundTuples < m_activeTuples) {

        if (m_dataPtr != NULL) {
            m_dataPtr += m_tupleLength;
        }

        if (m_dataPtr == NULL || m_dataPtr >= m_dataEndPtr) {
            // We are either before first tuple (m_dataPtr is null)
            // or at the end of a block.
            m_dataPtr = m_state.m_persBlockIterator.key();

            uint32_t unusedTupleBoundary = m_state.m_persBlockIterator.data()->unusedTupleBoundary();
            m_dataEndPtr = m_dataPtr + (unusedTupleBoundary * m_tupleLength);

            m_state.m_persBlockIterator++;
        }

        vassert(out.columnCount() == m_table->columnCount());
        out.move(m_dataPtr);

        const bool active = out.isActive();
        const bool pendingDelete = out.isPendingDelete();
        const bool isPendingDeleteOnUndoRelease = out.isPendingDeleteOnUndoRelease();

        // Return this tuple only when this tuple is not marked as deleted.
        if (active) {
            ++m_foundTuples;
            if (!(pendingDelete || isPendingDeleteOnUndoRelease)) {
                return true;
            }
        }
    } // end while found tuples is less than active tuples

    return false;
}

inline bool TableIterator::tempNext(TableTuple &out) {
    if (m_foundTuples < m_activeTuples) {

        if (m_dataPtr != NULL) {
            m_dataPtr += m_tupleLength;
        }

        if (m_dataPtr == NULL || m_dataPtr >= m_dataEndPtr) {

            // delete the last block of tuples in this temp table when they will never be used
            if (m_state.m_tempTableDeleteAsGo) {
                m_table->freeLastScannedBlock(m_state.m_tempBlockIterator);
            }

            m_dataPtr = (*m_state.m_tempBlockIterator)->address();

            uint32_t unusedTupleBoundary = (*m_state.m_tempBlockIterator)->unusedTupleBoundary();
            m_dataEndPtr = m_dataPtr + (unusedTupleBoundary * m_tupleLength);

            ++m_state.m_tempBlockIterator;
        }

        vassert(out.columnCount() == m_table->columnCount());
        out.move(m_dataPtr);

        ++m_foundTuples;
        return true;
    }

    return false;
}

inline bool TableIterator::largeTempNext(TableTuple &out) {
    if (m_foundTuples < m_activeTuples) {

        if (m_dataPtr != NULL) {
            m_dataPtr += m_tupleLength;
        }

        if (m_dataPtr == NULL || m_dataPtr >= m_dataEndPtr) {
            LargeTempTableBlockCache& lttCache = ExecutorContext::getExecutorContext()->lttBlockCache();
            auto& blockIdIterator = m_state.m_largeTempBlockIterator;

            if (m_dataPtr != NULL) {
                lttCache.unpinBlock(*blockIdIterator);

                if (m_state.m_tempTableDeleteAsGo) {
                    blockIdIterator = m_table->releaseBlock(blockIdIterator);
                }
                else {
                    ++blockIdIterator;
                }
            }

            LargeTempTableBlock* block = lttCache.fetchBlock(*blockIdIterator);
            m_dataPtr = block->tupleStorage();

            uint32_t unusedTupleBoundary = block->unusedTupleBoundary();
            m_dataEndPtr = m_dataPtr + (unusedTupleBoundary * m_tupleLength);
        }

        out.move(m_dataPtr);

        ++m_foundTuples;

        return true;
    } // end if there are still more tuples

    // Unpin (and release, if delete-as-you-go) the last block
    finishLargeTempTableScan();
    return false;
}

inline void TableIterator::finishLargeTempTableScan() {
    if (m_foundTuples == 0) {
        return;
    }

    LargeTempTableBlockCache& lttCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    auto& blockIdIterator = m_state.m_largeTempBlockIterator;

    if (lttCache.blockIsPinned(*blockIdIterator)) {
        lttCache.unpinBlock(*blockIdIterator);
    }

    if (m_foundTuples == m_activeTuples
        && m_state.m_tempTableDeleteAsGo) {

        blockIdIterator = m_table->releaseBlock(blockIdIterator);
        m_activeTuples = 0;
        m_foundTuples = 0;
        m_dataPtr = NULL;
        m_dataEndPtr = NULL;
    }
}

inline TableIterator::~TableIterator() {
    if (m_iteratorType == LARGE_TEMP) {
        finishLargeTempTableScan();
    }
}

}
