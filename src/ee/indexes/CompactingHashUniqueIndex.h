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

#ifndef COMPACTINGHASHUNIQUEINDEX_H_
#define COMPACTINGHASHUNIQUEINDEX_H_

#include <iostream>
#include <common/debuglog.h>

#include "indexes/tableindex.h"
#include "structures/CompactingHashTable.h"

namespace voltdb {

/**
 * Index implemented as a Hash Table Unique Map.
 * @see TableIndex
 */
template<typename KeyType>
class CompactingHashUniqueIndex : public TableIndex
{
    typedef typename KeyType::KeyEqualityChecker KeyEqualityChecker;
    typedef typename KeyType::KeyHasher KeyHasher;
    typedef CompactingHashTable<KeyType, const void*, KeyHasher, KeyEqualityChecker> MapType;
    typedef typename MapType::iterator MapIterator;

    ~CompactingHashUniqueIndex() {};

    static MapIterator& castToIter(IndexCursor& cursor) {
        return *reinterpret_cast<MapIterator*> (cursor.m_keyIter);
    }

    void addEntryDo(const TableTuple *tuple, TableTuple *conflictTuple) {
        ++m_inserts;
        const void* const* conflictEntry = m_entries.insert(setKeyFromTuple(tuple), tuple->address());
        if (conflictEntry != NULL && conflictTuple != NULL) {
            conflictTuple->move(const_cast<void*>(*conflictEntry));
        }
    }

    bool deleteEntryDo(const TableTuple *tuple) {
        ++m_deletes;
        return m_entries.erase(setKeyFromTuple(tuple));
    }

    /**
     * Update in place an index entry with a new tuple address
     */
    bool replaceEntryNoKeyChangeDo(const TableTuple &destinationTuple, const TableTuple &originalTuple)
    {
        vassert(originalTuple.address() != destinationTuple.address());

        // full delete and insert for certain key types
        if (KeyType::keyDependsOnTupleAddress()) {
            if ( ! CompactingHashUniqueIndex::deleteEntry(&originalTuple)) {
                return false;
            }
            TableTuple conflict(destinationTuple.getSchema());
            CompactingHashUniqueIndex::addEntry(&destinationTuple, &conflict);
            return conflict.isNullTuple();
        }

        MapIterator mapiter = findTuple(originalTuple);
        if (mapiter.isEnd()) {
            return false;
        }
        mapiter.setValue(destinationTuple.address());
        m_updates++;
        return true;
    }

    bool keyUsesNonInlinedMemory() const { return KeyType::keyUsesNonInlinedMemory(); }

    bool checkForIndexChangeDo(const TableTuple *lhs, const TableTuple *rhs) const {
        return !(m_eq(setKeyFromTuple(lhs), setKeyFromTuple(rhs)));
    }

    bool existsDo(const TableTuple *persistentTuple) const
    {
        return ! findTuple(*persistentTuple).isEnd();
    }

    bool moveToKey(const TableTuple *searchKey, IndexCursor& cursor) const {
        MapIterator &mapIter = castToIter(cursor);
        mapIter = findKey(searchKey);

        if (mapIter.isEnd()) {
            cursor.m_match.move(NULL);
            return false;
        }
        cursor.m_match.move(const_cast<void*>(mapIter.value()));

        return true;
    }

    bool moveToKeyByTuple(const TableTuple *persistentTuple, IndexCursor &cursor) const
    {
        MapIterator &mapIter = castToIter(cursor);
        mapIter = findTuple(*persistentTuple);

        if (mapIter.isEnd()) {
            cursor.m_match.move(NULL);
            return false;
        }
        cursor.m_match.move(const_cast<void*>(mapIter.value()));

        return true;
    }

    TableTuple nextValueAtKey(IndexCursor& cursor) const {
        TableTuple retval = cursor.m_match;
        cursor.m_match.move(NULL);
        return retval;
    }

    TableTuple uniqueMatchingTuple(const TableTuple &searchTuple) const
    {
        TableTuple retval(getTupleSchema());
        const MapIterator keyIter = findTuple(searchTuple);
        if ( ! keyIter.isEnd()) {
            retval.move(const_cast<void*>(keyIter.value()));
        }
        return retval;
    }

    bool hasKey(const TableTuple *searchKey) const {
        return ! findKey(searchKey).isEnd();
    }

    size_t getSize() const { return m_entries.size(); }

    int64_t getMemoryEstimate() const
    {
        return m_entries.bytesAllocated();
    }

    std::string getTypeName() const { return "CompactingHashUniqueIndex"; };

    TableIndex *cloneEmptyNonCountingTreeIndex() const
    {
        return new CompactingTreeUniqueIndex<NormalKeyValuePair<KeyType, void const *>, false >(TupleSchema::createTupleSchema(getKeySchema()), m_scheme);
    }

    // Non-virtual (so "really-private") helper methods.
    MapIterator findKey(const TableTuple *searchKey) const
    {
        return m_entries.find(KeyType(searchKey));
    }

    MapIterator findTuple(const TableTuple &originalTuple) const
    {
        return m_entries.find(setKeyFromTuple(&originalTuple));
    }

    const KeyType setKeyFromTuple(const TableTuple *tuple) const
    {
        KeyType result(tuple, m_scheme.columnIndices, m_scheme.indexedExpressions, m_keySchema);
        return result;
    }

    MapType m_entries;

    // comparison stuff
   KeyEqualityChecker m_eq;

public:
    CompactingHashUniqueIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme) :
        TableIndex(keySchema, scheme),
        m_entries(true, KeyHasher(keySchema), KeyEqualityChecker(keySchema)),
        m_eq(keySchema)
    {}
};

}

#endif // COMPACTINGHASHUNIQUEINDEX_H_
