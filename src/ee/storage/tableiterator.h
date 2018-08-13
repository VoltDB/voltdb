/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#pragma once

#include <cassert>

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
        assert (m_iteratorType == PERSISTENT);
        return m_state.m_persBlockIterator;
    }

    void setBlockIterator(const TBMapI& it) {
        assert (m_iteratorType == PERSISTENT);
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
    uint32_t m_foundTuples = 0;

    /** Pointer to our current position in the current block. */
    char *m_dataPtr = nullptr;

    /** Pointer to the first tuple after the last valid tuple in the
        current block.  Scan of current block is complete when
        m_dataPtr == m_dataEndPtr. */
    char *m_dataEndPtr = nullptr;

    /** The type of iterator based on the kind of table that we're scanning. */
    IteratorType m_iteratorType;

    /** State that is specific to the type of table we're iterating
        over: */
    TypeSpecificState m_state;
};
}

