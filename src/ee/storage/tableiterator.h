/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

#ifndef HSTORETABLEITERATOR_H
#define HSTORETABLEITERATOR_H

#include <cassert>

#include "boost/shared_ptr.hpp"

#include "common/LargeTempTableBlockCache.h"
#include "common/tabletuple.h"
#include "table.h"
#include "storage/TupleIterator.h"


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

    bool hasNext();
    uint32_t getLocation() const;

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

    bool operator ==(const TableIterator &other) const {
        return m_table == other.m_table && m_location == other.m_location;
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
    TableIterator(Table *, std::vector<TBPtr>::iterator);

    /** Constructor for large temp tables */
    TableIterator(Table *, std::vector<int64_t>::iterator);

    /** moves iterator to beginning of table.
        (Called only for persistent tables) */
    void reset(TBMapI);

    /** moves iterator to beginning of table.
        (Called only for temp tables) */
    void reset(std::vector<TBPtr>::iterator);

    /** moves iterator to beginning of table.
        (Called only for large temp tables) */
    void reset(std::vector<int64_t>::iterator);

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
        assert (m_iteratorType == PERSISTENT);
        return m_state.m_persBlockIterator;
    }

    void setBlockIterator(const TBMapI& it) {
        assert (m_iteratorType == PERSISTENT);
        m_state.m_persBlockIterator = it;
    }

    uint32_t getBlockOffset() const {
        return m_blockOffset;
    }

    uint32_t getFoundTuples() const {
        return m_foundTuples;
    }

    void setFoundTuples(uint32_t found) {
        m_foundTuples = found;
    }

    void setLocation(uint32_t loc) {
        m_location = loc;
    }

    uint32_t getTuplesPerBlock() {
        return m_tuplesPerBlock;
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
        TypeSpecificState(std::vector<int64_t>::iterator it, bool deleteAsGo)
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
        std::vector<int64_t>::iterator m_largeTempBlockIterator;

        /** "delete as you go" flag for normal and large temp tables
         * (Not used for persistent tables)
         */
        bool m_tempTableDeleteAsGo;
    };

    // State that is common to all kinds of iterators:
    Table *m_table;
    uint32_t m_activeTuples;
    uint32_t m_tupleLength;
    uint32_t m_tuplesPerBlock;
    TBPtr m_currentBlock;
    uint32_t m_foundTuples;
    uint32_t m_blockOffset;
    char *m_dataPtr;
    uint32_t m_location;
    IteratorType m_iteratorType;

    // State that is specific to the type of table we're iterating
    // over:
    TypeSpecificState m_state;
};

// Construct iterator for temp tables
inline TableIterator::TableIterator(Table *parent, std::vector<TBPtr>::iterator start)
    : m_table(parent),
      m_activeTuples((int) m_table->m_tupleCount),
      m_tupleLength(parent->m_tupleLength),
      m_tuplesPerBlock(parent->m_tuplesPerBlock),
      m_currentBlock(NULL),
      m_foundTuples(0),
      m_blockOffset(0),
      m_dataPtr(NULL),
      m_location(0),
      m_iteratorType(TEMP),
      m_state(start, false)
{
}

// Construct iterator for persistent tables
inline TableIterator::TableIterator(Table *parent, TBMapI start)
    : m_table(parent),
      m_activeTuples((int) m_table->m_tupleCount),
      m_tupleLength(parent->m_tupleLength),
      m_tuplesPerBlock(parent->m_tuplesPerBlock),
      m_currentBlock(NULL),
      m_foundTuples(0),
      m_blockOffset(0),
      m_dataPtr(NULL),
      m_location(0),
      m_iteratorType(PERSISTENT),
      m_state(start)
{
}

//  Construct an iterator for large temp tables
inline TableIterator::TableIterator(Table *parent,
                                    std::vector<int64_t>::iterator start)
    : m_table(parent),
      m_activeTuples((int) m_table->m_tupleCount),
      m_tupleLength(parent->m_tupleLength),
      m_tuplesPerBlock(parent->m_tuplesPerBlock),
      m_currentBlock(NULL),
      m_foundTuples(0),
      m_blockOffset(0),
      m_dataPtr(NULL),
      m_location(0),
      m_iteratorType(LARGE_TEMP),
      m_state(start, false)
{
}

inline TableIterator::TableIterator(const TableIterator &that)
    : m_table(that.m_table)
    , m_activeTuples(that.m_activeTuples)
    , m_tupleLength(that.m_tupleLength)
    , m_tuplesPerBlock(that.m_tuplesPerBlock)
    , m_currentBlock(that.m_currentBlock)
    , m_foundTuples(that.m_foundTuples)
    , m_blockOffset(that.m_blockOffset)
    , m_dataPtr(that.m_dataPtr)
    , m_location(that.m_location)
    , m_iteratorType(that.m_iteratorType)
    , m_state(that.m_state)
{
}

inline TableIterator& TableIterator::operator=(const TableIterator& that) {

    if (*this != that) {
        m_table = that.m_table;
        m_activeTuples = that.m_activeTuples;
        m_tupleLength = that.m_tupleLength;
        m_tuplesPerBlock = that.m_tuplesPerBlock;
        m_currentBlock = that.m_currentBlock;
        m_foundTuples = that.m_foundTuples;
        m_blockOffset = that.m_blockOffset;
        m_dataPtr = that.m_dataPtr;
        m_location = that.m_location;
        m_iteratorType = that.m_iteratorType;
        m_state = that.m_state;

        if (m_iteratorType == LARGE_TEMP) {
            finishLargeTempTableScan();
        }
    }

    return *this;
}

inline void TableIterator::reset(std::vector<TBPtr>::iterator start) {
    assert(m_iteratorType == TEMP);
    m_dataPtr= NULL;
    m_location = 0;
    m_blockOffset = 0;
    m_activeTuples = (int) m_table->m_tupleCount;
    m_foundTuples = 0;
    m_tupleLength = m_table->m_tupleLength;
    m_tuplesPerBlock = m_table->m_tuplesPerBlock;
    m_currentBlock = NULL;
    m_state.m_tempBlockIterator = start;
    m_state.m_tempTableDeleteAsGo = false;
}

inline void TableIterator::reset(std::vector<int64_t>::iterator start) {
    assert(m_iteratorType == LARGE_TEMP);

    // Unpin the block of the previous scan before resetting.
    finishLargeTempTableScan();

    m_dataPtr= NULL;
    m_location = 0;
    m_blockOffset = 0;
    m_activeTuples = (int) m_table->m_tupleCount;
    m_foundTuples = 0;
    m_tupleLength = m_table->m_tupleLength;
    m_tuplesPerBlock = m_table->m_tuplesPerBlock;
    m_currentBlock = NULL;
    m_state.m_largeTempBlockIterator = start;
    m_state.m_tempTableDeleteAsGo = false;
}

inline void TableIterator::reset(TBMapI start) {
    assert(m_iteratorType == PERSISTENT);
    m_dataPtr= NULL;
    m_location = 0;
    m_blockOffset = 0;
    m_activeTuples = (int) m_table->m_tupleCount;
    m_foundTuples = 0;
    m_tupleLength = m_table->m_tupleLength;
    m_tuplesPerBlock = m_table->m_tuplesPerBlock;
    m_currentBlock = NULL;
    m_state.m_persBlockIterator = start;
}

inline bool TableIterator::hasNext() {
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
        assert(m_iteratorType == LARGE_TEMP);
        return largeTempNext(out);
    }
}

inline bool TableIterator::persistentNext(TableTuple &out) {
    while (m_foundTuples < m_activeTuples) {
        if (m_currentBlock == NULL ||
            m_blockOffset >= m_currentBlock->unusedTupleBoundary()) {
//            assert(m_blockIterator != m_table->m_data.end());
//            if (m_blockIterator == m_table->m_data.end()) {
//                throwFatalException("Could not find the expected number of tuples during a table scan");
//            }
            m_dataPtr = m_state.m_persBlockIterator.key();
            m_currentBlock = m_state.m_persBlockIterator.data();
            m_blockOffset = 0;
            m_state.m_persBlockIterator++;
        } else {
            m_dataPtr += m_tupleLength;
        }
        assert (out.sizeInValues() == m_table->columnCount());
        out.move(m_dataPtr);
        assert(m_dataPtr < m_currentBlock.get()->address() + m_table->m_tableAllocationTargetSize);
        assert(m_dataPtr < m_currentBlock.get()->address() + (m_table->m_tupleLength * m_table->m_tuplesPerBlock));
        //assert(m_foundTuples == m_location);
        ++m_location;
        ++m_blockOffset;

        //assert(out.isActive());

        const bool active = out.isActive();
        const bool pendingDelete = out.isPendingDelete();
        const bool isPendingDeleteOnUndoRelease = out.isPendingDeleteOnUndoRelease();
        // Return this tuple only when this tuple is not marked as deleted.
        if (active) {
            ++m_foundTuples;
            if (!(pendingDelete || isPendingDeleteOnUndoRelease)) {
                //assert(m_foundTuples == m_location);
                return true;
            }
        }
    }
    return false;
}

inline bool TableIterator::tempNext(TableTuple &out) {
    if (m_foundTuples < m_activeTuples) {
        if (m_currentBlock == NULL ||
            m_blockOffset >= m_currentBlock->unusedTupleBoundary())
        {
            // delete the last block of tuples in this temp table when they will never be used
            if (m_state.m_tempTableDeleteAsGo) {
                m_table->freeLastScannedBlock(m_state.m_tempBlockIterator);
            }

            m_currentBlock = *(m_state.m_tempBlockIterator);
            m_dataPtr = m_currentBlock->address();
            m_blockOffset = 0;
            ++m_state.m_tempBlockIterator;
        } else {
            m_dataPtr += m_tupleLength;
        }
        assert (out.sizeInValues() == m_table->columnCount());
        out.move(m_dataPtr);
        assert(m_dataPtr < m_currentBlock.get()->address() + m_table->m_tableAllocationTargetSize);
        assert(m_dataPtr < m_currentBlock.get()->address() + (m_table->m_tupleLength * m_table->m_tuplesPerBlock));


        //assert(m_foundTuples == m_location);

        ++m_location;
        ++m_blockOffset;

        //assert(out.isActive());
        ++m_foundTuples;
        //assert(m_foundTuples == m_location);
        return true;
    }

    return false;
}

inline bool TableIterator::largeTempNext(TableTuple &out) {
    if (m_foundTuples < m_activeTuples) {

        if (m_currentBlock == NULL ||
            m_blockOffset >= m_currentBlock->unusedTupleBoundary()) {

            LargeTempTableBlockCache* lttCache = ExecutorContext::getExecutorContext()->lttBlockCache();
            auto& blockIdIterator = m_state.m_largeTempBlockIterator;

            if (m_currentBlock != NULL) {
                lttCache->unpinBlock(*blockIdIterator);
                ++blockIdIterator;
            }

            m_currentBlock = lttCache->fetchBlock(*blockIdIterator)->getTupleBlockPointer();
            m_dataPtr = m_currentBlock->address();
            m_blockOffset = 0;
        }
        else {
            m_dataPtr += m_tupleLength;
        }

        out.move(m_dataPtr);

        ++m_foundTuples;
        ++m_blockOffset;

        return true;
    } // end if there are still more tuples

    // Unpin the last block
    finishLargeTempTableScan();
    return false;
}

inline void TableIterator::finishLargeTempTableScan() {
    if (m_foundTuples == 0) {
        return;
    }

    auto& blockIdIterator = m_state.m_largeTempBlockIterator;

    LargeTempTableBlockCache* lttCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    if (lttCache->blockIsPinned(*blockIdIterator)) {
        lttCache->unpinBlock(*blockIdIterator);
    }
}

inline uint32_t TableIterator::getLocation() const {
    return m_location;
}

inline TableIterator::~TableIterator() {
    // Note: some executors delete all tuples from input tables before
    // iterators go out of scope, so cannot rely on any iterators
    // being valid here.
}

}

#endif
