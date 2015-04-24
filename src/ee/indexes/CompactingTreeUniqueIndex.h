/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include "structures/btree.h"

namespace voltdb {

/**
 * Index implemented as a Binary Tree Unique Map.
 * @see TableIndex
 */
template<typename KeyValuePair, bool hasRank>
class CompactingTreeUniqueIndex : public TableIndex
{
    typedef typename KeyValuePair::first_type KeyType;
    typedef typename KeyValuePair::second_type ValueType;
    typedef BtreeTagComparator<typename KeyType::KeyComparator> KeyComparator;
    typedef typename std::allocator<std::pair<const KeyType, ValueType> > Allocator;
    typedef btree::btree<btree::btree_map_params<KeyType, ValueType, KeyComparator, Allocator, 256> > MapType;
    typedef typename MapType::iterator MapIterator;
    typedef typename MapType::const_iterator MapConstIterator;

    ~CompactingTreeUniqueIndex() {};

    static MapConstIterator& castToIter(IndexCursor& cursor) {
        return *reinterpret_cast<MapConstIterator*> (cursor.m_keyIter);
    }

    bool isEnd(MapConstIterator &mapIter) const {
        return mapIter == m_entries.end();
    }

    void movePrev(MapConstIterator &mapIter) const {
        if (mapIter != m_entries.begin()) {
            mapIter--;
        } else {
            mapIter = m_entries.end();
        }
    }

    void moveNext(MapConstIterator &mapIter) const {
        if (mapIter != m_entries.end()) {
            mapIter++;
        }
    }

    bool addEntryDo(const TableTuple *tuple)
    {
        ++m_inserts;
        return m_entries.insert_unique(std::pair<KeyType, ValueType>(setKeyFromTuple(tuple), tuple->address())).second;
    }

    bool deleteEntryDo(const TableTuple *tuple)
    {
        ++m_deletes;
        return m_entries.erase_unique(setKeyFromTuple(tuple)) == 1;
    }

    /**
     * Update in place an index entry with a new tuple address
     */
    bool replaceEntryNoKeyChangeDo(const TableTuple &destinationTuple, const TableTuple &originalTuple)
    {
        assert(originalTuple.address() != destinationTuple.address());

        // full delete and insert for certain key types
        if (KeyType::keyDependsOnTupleAddress()) {
            if ( ! CompactingTreeUniqueIndex::deleteEntry(&originalTuple)) {
                return false;
            }
            return CompactingTreeUniqueIndex::addEntry(&destinationTuple);
        }

        MapIterator mapIter = findTuple(originalTuple);
        if (mapIter == m_entries.end()) {
            return false;
        }

        mapIter->second = destinationTuple.address();
        m_updates++;
        return true;
    }

    bool keyUsesNonInlinedMemory() const { return KeyType::keyUsesNonInlinedMemory(); }

    bool checkForIndexChangeDo(const TableTuple* lhs, const TableTuple* rhs) const
    {
        return  0 != m_cmp(setKeyFromTuple(lhs), setKeyFromTuple(rhs));
    }

    bool existsDo(const TableTuple *persistentTuple) const
    {
        return findTuple(*persistentTuple) != m_entries.end();
    }

    bool moveToKey(const TableTuple *searchKey, IndexCursor& cursor) const
    {
        cursor.m_forward = true;
        MapConstIterator &mapIter = castToIter(cursor);
        mapIter = findKey(searchKey);

        if (isEnd(mapIter)) {
            cursor.m_match.move(NULL);
            return false;
        }
        cursor.m_match.move(const_cast<void*>(mapIter->second));
        return true;
    }

    void moveToKeyOrGreater(const TableTuple *searchKey, IndexCursor& cursor) const
    {
        cursor.m_forward = true;
        MapConstIterator &mapIter = castToIter(cursor);

        mapIter = m_entries.lower_bound(KeyType(searchKey));
    }

    bool moveToGreaterThanKey(const TableTuple *searchKey, IndexCursor& cursor) const
    {
        cursor.m_forward = true;
        MapConstIterator &mapIter = castToIter(cursor);
        mapIter = m_entries.upper_bound(KeyType(searchKey));

        return isEnd(mapIter);
    }

    void moveToLessThanKey(const TableTuple *searchKey, IndexCursor& cursor) const
    {
        // do moveToKeyOrGreater()
        MapConstIterator &mapIter = castToIter(cursor);
        mapIter = m_entries.lower_bound(KeyType(searchKey));

        // find prev entry
        if (isEnd(mapIter)) {
            moveToEnd(false, cursor);
        } else {
            cursor.m_forward = false;
            movePrev(mapIter);
        }
    }

    // only be called after moveToGreaterThanKey() for LTE case
    void moveToBeforePriorEntry(IndexCursor& cursor) const
    {
        assert(cursor.m_forward);
        cursor.m_forward = false;
        MapConstIterator &mapIter = castToIter(cursor);

        if (isEnd(mapIter)) {
            //mapIter = m_entries.rbegin();
            mapIter = m_entries.end();
            movePrev(mapIter);
        } else {
            // go back 2 entries
            // entries: [..., A, B, C, ...], currently mapIter = C (not NULL if reach here)
            // B is the entry we just evaluated and didn't pass initial_expression test (can not be NULL)
            // so A is the correct starting point (can be NULL)
            movePrev(mapIter);
        }
        movePrev(mapIter);
    }

    void moveToEnd(bool begin, IndexCursor& cursor) const
    {
        cursor.m_forward = begin;
        MapConstIterator &mapIter = castToIter(cursor);

        if (begin) {
            mapIter = m_entries.begin();
        } else {
            //mapIter = m_entries.rbegin();
            mapIter = m_entries.end();
            movePrev(mapIter);
        }
    }

    TableTuple nextValue(IndexCursor& cursor) const
    {
        TableTuple retval(getTupleSchema());

        MapConstIterator &mapIter = castToIter(cursor);

        if (! isEnd(mapIter)) {
            retval.move(const_cast<void*>(mapIter->second));
            if (cursor.m_forward) {
                moveNext(mapIter);
            } else {
                movePrev(mapIter);
            }
        }

        return retval;
    }

    TableTuple nextValueAtKey(IndexCursor& cursor) const
    {
        TableTuple retval = cursor.m_match;
        cursor.m_match.move(NULL);
        return retval;
    }

    bool advanceToNextKey(IndexCursor& cursor) const
    {
        MapConstIterator &mapIter = castToIter(cursor);

        if (cursor.m_forward) {
            moveNext(mapIter);
        } else {
            movePrev(mapIter);
        }

        if (isEnd(mapIter))
        {
            cursor.m_match.move(NULL);
            return false;
        }

        cursor.m_match.move(const_cast<void*>(mapIter->second));
        return true;
    }

    TableTuple uniqueMatchingTuple(const TableTuple &searchTuple) const
    {
        TableTuple retval(getTupleSchema());
        const MapConstIterator keyIter = findTuple(searchTuple);
        if ( keyIter != m_entries.end()) {
            retval.move(const_cast<void*>(keyIter->second));
        }
        return retval;
    }

    bool hasKey(const TableTuple *searchKey) const
    {
        return findKey(searchKey) != m_entries.end();
    }

    /**
     * @See comments in parent class TableIndex
     */
    int64_t getCounterGET(const TableTuple* searchKey, bool isUpper, IndexCursor& cursor) const {
        return 0;
//        if (!hasRank) {
//            return -1;
//        }
//        CompactingTreeUniqueIndex::moveToKeyOrGreater(searchKey, cursor);
//        MapIterator &mapIter = castToIter(cursor);
//
//        if (isEnd(mapIter)) {
//            return m_entries.size() + 1;
//        }
//        return m_entries.rankAsc(mapIter.key());
    }

    /**
     * See comments in parent class TableIndex
     */
    int64_t getCounterLET(const TableTuple* searchKey, bool isUpper, IndexCursor& cursor) const {
        return 0;
//        if (!hasRank) {
//           return -1;
//        }
//        const KeyType tmpKey(searchKey);
//        MapIterator mapIter = m_entries.lower_bound(tmpKey);
//        if (isEnd(mapIter)) {
//            return m_entries.size();
//        }
//        int cmp = m_cmp(tmpKey, mapIter.key());
//        if (cmp != 0) {
//            mapIter--;
//            if (isEnd(mapIter)) {
//                // we can not find a previous key
//                return 0;
//            }
//        }
//        return m_entries.rankAsc(mapIter.key());
    }

    size_t getSize() const { return m_entries.size(); }

    int64_t getMemoryEstimate() const
    {
        return m_entries.bytes_used();
    }

    std::string debug() const
    {
        std::ostringstream buffer;
        buffer << TableIndex::debug() << std::endl;
        MapConstIterator iter = m_entries.begin();
        while (iter != m_entries.end()) {
            TableTuple retval(getTupleSchema());
            retval.move(const_cast<void*>(iter->second));
            buffer << retval.debugNoHeader() << std::endl;
            iter++;
        }
        std::string ret(buffer.str());
        return (ret);
    }

    std::string getTypeName() const { return "CompactingTreeUniqueIndex"; };

    virtual TableIndex *cloneEmptyNonCountingTreeIndex() const
    {
        return new CompactingTreeUniqueIndex<KeyValuePair, false >(TupleSchema::createTupleSchema(getKeySchema()), m_scheme);
    }

    MapConstIterator findKey(const TableTuple *searchKey) const {
        return m_entries.find_unique(KeyType(searchKey));
    }

    MapConstIterator findTuple(const TableTuple &originalTuple) const {
        return m_entries.find_unique(setKeyFromTuple(&originalTuple));
    }

    MapIterator findTuple(const TableTuple &originalTuple) {
        return m_entries.find_unique(setKeyFromTuple(&originalTuple));
    }

    const KeyType setKeyFromTuple(const TableTuple *tuple) const
    {
        KeyType result(tuple, m_scheme.columnIndices, m_scheme.indexedExpressions, m_keySchema);
        return result;
    }

    // comparison stuff
    KeyComparator m_cmp;
    Allocator m_alloc;
    MapType m_entries;

public:
    CompactingTreeUniqueIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme) :
        TableIndex(keySchema, scheme),
        m_cmp(keySchema),
        m_alloc(Allocator()),
        m_entries(m_cmp, m_alloc)
    {}
};

}

#endif // COMPACTINGTREEUNIQUEINDEX_H_
