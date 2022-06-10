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

#ifndef COMPACTINGTREEMULTIMAPINDEX_H_
#define COMPACTINGTREEMULTIMAPINDEX_H_

#include <iostream>
#include <common/debuglog.h>
#include "indexes/tableindex.h"
#include "common/tabletuple.h"
#include "structures/CompactingMap.h"

namespace voltdb {

/**
 * Index implemented as a Binary Tree Multimap.
 * @see TableIndex
 */
template<typename KeyValuePair, bool hasRank>
class CompactingTreeMultiMapIndex : public TableIndex
{
    typedef typename KeyValuePair::first_type KeyType;
    typedef typename KeyType::KeyComparator KeyComparator;
    typedef CompactingMap<KeyValuePair, KeyComparator, hasRank> MapType;
    typedef typename MapType::iterator MapIterator;
    typedef std::pair<MapIterator, MapIterator> MapRange;


    ~CompactingTreeMultiMapIndex() {
#ifdef VOLT_POOL_CHECKING
        m_entries.shutdown(m_shutdown);
#endif
    }

    static MapIterator& castToIter(IndexCursor& cursor) {
        return *reinterpret_cast<MapIterator*> (cursor.m_keyIter);
    }

    static MapIterator& castToEndIter(IndexCursor& cursor) {
        return *reinterpret_cast<MapIterator*> (cursor.m_keyEndIter);
    }

    void addEntryDo(const TableTuple *tuple, TableTuple *conflictTuple) {
        ++m_inserts;
        m_entries.insert(setKeyFromTuple(tuple), tuple->address());
    }

    bool deleteEntryDo(const TableTuple *tuple) {
        ++m_deletes;
        MapIterator iter = findTuple(*tuple);
        if (iter.isEnd()) {
            return false;
        }
        return m_entries.erase(iter);
    }

    /**
     * Update in place an index entry with a new tuple address
     * (e.g., due to table compaction)
     */
    bool replaceEntryNoKeyChangeDo(const TableTuple &destinationTuple, const TableTuple &originalTuple)
    {
        vassert(originalTuple.address() != destinationTuple.address());
        // The KeyType will always depend on tuple address, excpet for CompactingTreeMultiIndexTest.
        if ( ! CompactingTreeMultiMapIndex::deleteEntry(&originalTuple)) {
            return false;
        }
        CompactingTreeMultiMapIndex::addEntry(&destinationTuple, NULL);
        return true;
    }

    bool keyUsesNonInlinedMemory() const { return KeyType::keyUsesNonInlinedMemory(); }

    bool checkForIndexChangeDo(const TableTuple *lhs, const TableTuple *rhs) const
    {
        return 0 != m_cmp(setKeyFromTuple(lhs), setKeyFromTuple(rhs));
    }

    bool existsDo(const TableTuple *persistentTuple) const
    {
        return ! findTuple(*persistentTuple).isEnd();
    }

    bool moveToKey(const TableTuple *searchKey, IndexCursor& cursor) const
    {
        cursor.m_forward = true;
        MapRange iter_pair = m_entries.equalRange(KeyType(searchKey));

        MapIterator &mapIter = castToIter(cursor);
        MapIterator &mapEndIter = castToEndIter(cursor);

        mapIter = iter_pair.first;
        mapEndIter = iter_pair.second;

        if (mapIter.equals(mapEndIter)) {
            cursor.m_match.move(NULL);
            return false;
        }
        cursor.m_match.move(const_cast<void*>(mapIter.value()));

        return true;
    }

    bool moveToKeyByTuple(const TableTuple *persistentTuple, IndexCursor &cursor) const
    {
        cursor.m_forward = true;
        MapRange iter_pair = m_entries.equalRange(setKeyFromTuple(persistentTuple));

        MapIterator &mapIter = castToIter(cursor);
        MapIterator &mapEndIter = castToEndIter(cursor);

        mapIter = iter_pair.first;
        mapEndIter = iter_pair.second;

        if (mapIter.equals(mapEndIter)) {
            cursor.m_match.move(NULL);
            return false;
        }
        cursor.m_match.move(const_cast<void*>(mapIter.value()));

        return true;
    }

    void moveToKeyOrGreater(const TableTuple *searchKey, IndexCursor& cursor) const
    {
        cursor.m_forward = true;
        MapIterator &mapIter = castToIter(cursor);
        mapIter = m_entries.lowerBound(KeyType(searchKey));
    }

    bool moveToGreaterThanKey(const TableTuple *searchKey, IndexCursor& cursor) const
    {
        cursor.m_forward = true;
        MapIterator &mapIter = castToIter(cursor);
        mapIter = m_entries.upperBound(KeyType(searchKey));

        return mapIter.isEnd();
    }

    void moveToLessThanKey(const TableTuple *searchKey, IndexCursor& cursor) const
    {
        // do moveToKeyOrGreater()
        MapIterator &mapIter = castToIter(cursor);
        mapIter = m_entries.lowerBound(KeyType(searchKey));
        // find prev entry
        if (mapIter.isEnd()) {
            moveToEnd(false, cursor);
        } else {
            cursor.m_forward = false;
            mapIter.movePrev();
        }
    }

    void moveToKeyOrLess(TableTuple *searchKey, IndexCursor& cursor) {
        // do moveToGreaterThanKey(), null values in the search key will be treated as maximum

        // IntsKey will pack the key data into uint64, so we can not tell if it is
        // a NULL value then (a TINYINT NULL is a valid value in INT).
        // In that case, we will change all numeric null key values into maximum.
        for (int i = 0; i < searchKey->getSchema()->totalColumnCount(); i++) {
            if (searchKey->getNValue(i).isNull()) {
                const ValueType valueType = searchKey->getSchema()->columnType(i);
                switch (valueType) {
                    case ValueType::tBIGINT:
                        searchKey->setNValue(i, ValueFactory::getBigIntValue(INT64_MAX));
                        break;
                    case ValueType::tINTEGER:
                        searchKey->setNValue(i, ValueFactory::getIntegerValue(INT32_MAX));
                        break;
                    case ValueType::tSMALLINT:
                        searchKey->setNValue(i, ValueFactory::getSmallIntValue(INT16_MAX));
                        break;
                    case ValueType::tTINYINT:
                        searchKey->setNValue(i, ValueFactory::getTinyIntValue(INT8_MAX));
                        break;
                    default: // other null types will be handled in GenericComparator
                        break;
                }
            }
        }
        MapIterator &mapIter = castToIter(cursor);
        mapIter = m_entries.upperBoundNullAsMax(KeyType(searchKey));
        // find prev entry
        if (mapIter.isEnd()) {
            moveToEnd(false, cursor);
        } else {
            cursor.m_forward = false;
            mapIter.movePrev();
        }
    }

    // only be called after moveToGreaterThanKey() for LTE case
    void moveToBeforePriorEntry(IndexCursor& cursor) const
    {
        vassert(cursor.m_forward);
        cursor.m_forward = false;
        MapIterator &mapIter = castToIter(cursor);

        if (mapIter.isEnd()) {
            mapIter = m_entries.rbegin();
        } else {
            // go back 2 entries
            // entries: [..., A, B, C, ...], currently m_keyIter = C (not NULL if reach here)
            // B is the entry we just evaluated and didn't pass initial_expression test (can not be NULL)
            // so A is the correct starting point (can be NULL)
            mapIter.movePrev();
        }
        mapIter.movePrev();
    }

    void moveToPriorEntry(IndexCursor& cursor) const
    {
        vassert(cursor.m_forward);
        cursor.m_forward = false;
        MapIterator &mapIter = castToIter(cursor);

        if (mapIter.isEnd()) {
            mapIter = m_entries.rbegin();
        } else {
            mapIter.movePrev();
        }
    }

    void moveToEnd(bool begin, IndexCursor& cursor) const
    {
        cursor.m_forward = begin;
        MapIterator &mapIter = castToIter(cursor);

        if (begin)
            mapIter = m_entries.begin();
        else
            mapIter = m_entries.rbegin();
    }

    TableTuple nextValue(IndexCursor& cursor) const
    {
        TableTuple retval(getTupleSchema());
        MapIterator &mapIter = castToIter(cursor);

        if (! mapIter.isEnd()) {
            retval.move(const_cast<void*>(mapIter.value()));
            if (cursor.m_forward) {
                mapIter.moveNext();
            } else {
                mapIter.movePrev();
            }
        }

        return retval;
    }

    TableTuple nextValueAtKey(IndexCursor& cursor) const
    {
        if (cursor.m_match.isNullTuple()) {
            return cursor.m_match;
        }
        TableTuple retval = cursor.m_match;
        MapIterator &mapIter = castToIter(cursor);
        MapIterator &mapEndIter = castToEndIter(cursor);

        mapIter.moveNext();
        if (mapIter.equals(mapEndIter)) {
            cursor.m_match.move(NULL);
        } else {
            cursor.m_match.move(const_cast<void*>(mapIter.value()));
        }
        return retval;
    }

    bool advanceToNextKey(IndexCursor& cursor) const
    {
        MapIterator &mapEndIter = castToEndIter(cursor);
        if (mapEndIter.isEnd()) {
            return false;
        }
        MapIterator &mapIter = castToIter(cursor);

        cursor.m_forward = true;
        MapRange iter_pair = m_entries.equalRange(mapEndIter.key());
        mapEndIter = iter_pair.second;
        mapIter = iter_pair.first;

        if (mapIter.isEnd()) {
            cursor.m_match.move(NULL);
            return false;
        }
        cursor.m_match.move(const_cast<void*>(mapIter.value()));
        return true;
    }

    bool hasKey(const TableTuple *searchKey) const
    {
        return ! findKey(searchKey).isEnd();
    }

    /**
     * @See comments in parent class TableIndex
     */
    int64_t getCounterGET(const TableTuple* searchKey, bool isUpper, IndexCursor& cursor) const {
        if (!hasRank) {
            return -1;
        }
        CompactingTreeMultiMapIndex::moveToKeyOrGreater(searchKey, cursor);
        MapIterator &mapIter = castToIter(cursor);

        if (mapIter.isEnd()) {
            return m_entries.size() + 1;
        }
        if (isUpper) {
            return m_entries.rankUpper(mapIter.key());
        } else {
            return m_entries.rankLower(mapIter.key());
        }
    }

    /**
     * @See comments in parent class TableIndex
     */
    int64_t getCounterLET(const TableTuple* searchKey, bool isUpper, IndexCursor& cursor) const {
        if (!hasRank) {
           return -1;
        }
        KeyType tmpKey(searchKey);
        MapIterator mapIter = m_entries.lowerBound(tmpKey);
        if (mapIter.isEnd()) {
            return m_entries.size();
        }
        setPointerValue(tmpKey, MAXPOINTER);
        int cmp = m_cmp(tmpKey, mapIter.key());
        if (cmp < 0) {
            mapIter.movePrev();
            if (mapIter.isEnd()) {
                // we can not find a previous key
                return 0;
            }
        }
        if (isUpper) {
            return m_entries.rankUpper(mapIter.key());
        } else {
            return m_entries.rankLower(mapIter.key());
        }
    }

    bool moveToRankTuple(int64_t denseRank, bool forward, IndexCursor& cursor) const {
        cursor.m_forward = forward;
        MapIterator &mapConstIter = castToIter(cursor);
        mapConstIter = m_entries.findRank(denseRank);

        if (mapConstIter.isEnd()) {
            cursor.m_match.move(NULL);
            return false;
        }
        cursor.m_match.move(const_cast<void*>(mapConstIter.value()));
        return true;
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

    std::string getTypeName() const { return "CompactingTreeMultiMapIndex"; };

    MapIterator findKey(const TableTuple *searchKey) const {
        KeyType tempKey(searchKey);
        MapIterator rv = m_entries.lowerBound(tempKey);
        KeyType rvKey = rv.key();
        setPointerValue(tempKey, MAXPOINTER);
        if (m_cmp(rvKey, tempKey) <= 0) {
            return rv;
        }
        return MapIterator();
    }

    MapIterator findTuple(const TableTuple &originalTuple) const {
        // TODO: couldn't remove this, because of CompactingTreeMultiIndexTest in eecheck
        // code will force to use non-pointer-key.
        if (KeyType::keyDependsOnTupleAddress()) {
            return m_entries.find(setKeyFromTuple(&originalTuple));
        }

        for (MapRange iter_pair = m_entries.equalRange(setKeyFromTuple(&originalTuple));
             ! iter_pair.first.equals(iter_pair.second);
             iter_pair.first.moveNext()) {
            if (iter_pair.first.value() == originalTuple.address()) {
                return iter_pair.first;
            }
        }
        return MapIterator();
    }

    const KeyType setKeyFromTuple(const TableTuple *tuple) const
    {
        KeyType result(tuple, m_scheme.columnIndices, m_scheme.indexedExpressions, m_keySchema);
        return result;
    }

    MapType m_entries;

    // comparison stuff
    KeyComparator m_cmp;

public:
    CompactingTreeMultiMapIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme) :
        TableIndex(keySchema, scheme),
        m_entries(false, KeyComparator(keySchema)),
        m_cmp(keySchema)
    {}
};

}

#endif // COMPACTINGTREEMULTIMAPINDEX_H_
