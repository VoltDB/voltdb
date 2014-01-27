/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#ifndef COMPACTINGTREEUNIQUEINDEX_H_
#define COMPACTINGTREEUNIQUEINDEX_H_

#include <iostream>
#include <cassert>

#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"
#include "structures/CompactingMap.h"

namespace voltdb {

/**
 * Index implemented as a Binary Tree Unique Map.
 * @see TableIndex
 */
template<typename KeyType, bool hasRank>
class CompactingTreeUniqueIndex : public TableIndex
{
    typedef typename KeyType::KeyComparator KeyComparator;
    typedef CompactingMap<KeyType, const void*, KeyComparator, hasRank> MapType;
    typedef typename MapType::iterator MapIterator;

    ~CompactingTreeUniqueIndex() {};

    bool addEntry(const TableTuple *tuple)
    {
        ++m_inserts;
        return m_entries.insert(setKeyFromTuple(tuple), tuple->address());
    }

    bool deleteEntry(const TableTuple *tuple)
    {
        ++m_deletes;
        return m_entries.erase(setKeyFromTuple(tuple));
    }

    /**
     * Update in place an index entry with a new tuple address
     */
    bool replaceEntryNoKeyChange(const TableTuple &destinationTuple, const TableTuple &originalTuple)
    {
        assert(originalTuple.address() != destinationTuple.address());

        // full delete and insert for certain key types
        if (KeyType::keyDependsOnTupleAddress()) {
            if ( ! CompactingTreeUniqueIndex::deleteEntry(&originalTuple)) {
                return false;
            }
            return CompactingTreeUniqueIndex::addEntry(&destinationTuple);
        }

        MapIterator mapiter = findTuple(originalTuple);
        if (mapiter.isEnd()) {
            return false;
        }
        mapiter.setValue(destinationTuple.address());
        m_updates++;
        return true;
    }

    bool keyUsesNonInlinedMemory() { return KeyType::keyUsesNonInlinedMemory(); }

    bool checkForIndexChange(const TableTuple* lhs, const TableTuple* rhs)
    {
        return  0 != m_cmp(setKeyFromTuple(lhs), setKeyFromTuple(rhs));
    }

    bool exists(const TableTuple *persistentTuple)
    {
        ++m_lookups;
        return ! findTuple(*persistentTuple).isEnd();
    }

    bool moveToKey(const TableTuple *searchKey)
    {
        ++m_lookups;
        m_forward = true;
        m_keyIter = findKey(searchKey);
        if (m_keyIter.isEnd()) {
            m_match.move(NULL);
            return false;
        }
        m_match.move(const_cast<void*>(m_keyIter.value()));
        return true;
    }

    void moveToKeyOrGreater(const TableTuple *searchKey)
    {
        ++m_lookups;
        m_forward = true;
        m_keyIter = m_entries.lowerBound(KeyType(searchKey));
    }

    bool moveToGreaterThanKey(const TableTuple *searchKey)
    {
        ++m_lookups;
        m_forward = true;
        m_keyIter = m_entries.upperBound(KeyType(searchKey));
        return m_keyIter.isEnd();
    }

    void moveToLessThanKey(const TableTuple *searchKey)
    {
        // do moveToKeyOrGreater()
        ++m_lookups;
        m_keyIter = m_entries.lowerBound(KeyType(searchKey));
        // find prev entry
        if (m_keyIter.isEnd()) {
            moveToEnd(false);
        } else {
            m_forward = false;
            m_keyIter.movePrev();
        }
    }

    // only be called after moveToGreaterThanKey() for LTE case
    void moveToBeforePriorEntry()
    {
        assert(m_forward);
        m_forward = false;
        if (m_keyIter.isEnd()) {
            m_keyIter = m_entries.rbegin();
        } else {
            // go back 2 entries
            // entries: [..., A, B, C, ...], currently m_keyIter = C (not NULL if reach here)
            // B is the entry we just evaluated and didn't pass initial_expression test (can not be NULL)
            // so A is the correct starting point (can be NULL)
            m_keyIter.movePrev();
        }
        m_keyIter.movePrev();
    }

    void moveToEnd(bool begin)
    {
        ++m_lookups;
        m_forward = begin;
        if (begin)
            m_keyIter = m_entries.begin();
        else
            m_keyIter = m_entries.rbegin();
    }

    TableTuple nextValue()
    {
        TableTuple retval(getTupleSchema());

        if (! m_keyIter.isEnd()) {
            retval.move(const_cast<void*>(m_keyIter.value()));
            if (m_forward) {
                m_keyIter.moveNext();
            } else {
                m_keyIter.movePrev();
            }
        }

        return retval;
    }

    TableTuple nextValueAtKey()
    {
        TableTuple retval = m_match;
        m_match.move(NULL);
        return retval;
    }

    bool advanceToNextKey()
    {
        if (m_forward) {
            m_keyIter.moveNext();
        } else {
            m_keyIter.movePrev();
        }
        if (m_keyIter.isEnd())
        {
            m_match.move(NULL);
            return false;
        }
        m_match.move(const_cast<void*>(m_keyIter.value()));
        return true;
    }

    TableTuple uniqueMatchingTuple(const TableTuple &searchTuple)
    {
        ++m_lookups;
        TableTuple retval(getTupleSchema());
        const MapIterator keyIter = findTuple(searchTuple);
        if ( ! keyIter.isEnd()) {
            retval.move(const_cast<void*>(keyIter.value()));
        }
        return retval;
    }

    bool hasKey(const TableTuple *searchKey)
    {
        return ! findKey(searchKey).isEnd();
    }

    /**
     * @See comments in parent class TableIndex
     */
    int64_t getCounterGET(const TableTuple* searchKey, bool isUpper) {
        if (!hasRank) {
            return -1;
        }
        CompactingTreeUniqueIndex::moveToKeyOrGreater(searchKey);
        if (m_keyIter.isEnd()) {
            return m_entries.size() + 1;
        }
        return m_entries.rankAsc(m_keyIter.key());
    }

    /**
     * See comments in parent class TableIndex
     */
    int64_t getCounterLET(const TableTuple* searchKey, bool isUpper) {
        if (!hasRank) {
           return -1;
        }
        ++m_lookups;
        const KeyType tmpKey(searchKey);
        MapIterator mapIter = m_entries.lowerBound(tmpKey);
        if (mapIter.isEnd()) {
            return m_entries.size();
        }
        int cmp = m_cmp(tmpKey, mapIter.key());
        if (cmp != 0) {
            mapIter.movePrev();
            if (mapIter.isEnd()) {
                // we can not find a previous key
                return 0;
            }
        }
        return m_entries.rankAsc(mapIter.key());
    }

    size_t getSize() const { return m_entries.size(); }

    int64_t getMemoryEstimate() const
    {
        return m_entries.bytesAllocated();
    }

    std::string debug() const
    {
        std::ostringstream buffer;
        buffer << TableIndex::debug() << std::endl;
        MapIterator iter = m_entries.begin();
        while (!iter.isEnd()) {
            TableTuple retval(getTupleSchema());
            retval.move(const_cast<void*>(iter.value()));
            buffer << retval.debugNoHeader() << std::endl;
            iter.moveNext();
        }
        std::string ret(buffer.str());
        return (ret);
    }

    std::string getTypeName() const { return "CompactingTreeUniqueIndex"; };

    virtual TableIndex *cloneEmptyNonCountingTreeIndex() const
    {
        return new CompactingTreeUniqueIndex<KeyType, false >(TupleSchema::createTupleSchema(getKeySchema()), m_scheme);
    }


    MapIterator findKey(const TableTuple *searchKey) {
        return m_entries.find(KeyType(searchKey));
    }

    MapIterator findTuple(const TableTuple &originalTuple) {
        return m_entries.find(setKeyFromTuple(&originalTuple));
    }

    const KeyType setKeyFromTuple(const TableTuple *tuple)
    {
        KeyType result(tuple, m_scheme.columnIndices, m_scheme.indexedExpressions, m_keySchema);
        return result;
    }

    MapType m_entries;

    // iteration stuff
    bool m_forward;
    typename MapType::iterator m_keyIter;
    TableTuple m_match;

    // comparison stuff
    KeyComparator m_cmp;

public:
    CompactingTreeUniqueIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme) :
        TableIndex(keySchema, scheme),
        m_entries(true, KeyComparator(keySchema)),
        m_forward(true),
        m_match(getTupleSchema()),
        m_cmp(keySchema)
    {}
};

}

#endif // COMPACTINGTREEUNIQUEINDEX_H_
