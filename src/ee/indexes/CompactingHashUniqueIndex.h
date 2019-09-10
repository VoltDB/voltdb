/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
class CompactingHashUniqueIndex : public TableIndex {
    using KeyEqualityChecker = typename KeyType::KeyEqualityChecker;
    using KeyHasher = typename KeyType::KeyHasher;
    using MapType = CompactingHashTable<KeyType, const void*, KeyHasher, KeyEqualityChecker>;
    using MapIterator = typename MapType::iterator;

    MapType m_entries;
    // comparison stuff
    KeyEqualityChecker m_eq;

    ~CompactingHashUniqueIndex() {};

    static MapIterator& castToIter(IndexCursor& cursor) {
        return *reinterpret_cast<MapIterator*>(cursor.m_keyIter);
    }

    void moveToKeyOrGreater(const TableTuple *searchKey, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToKeyOrGreater which has no implementation");
    }

    bool moveToGreaterThanKey(const TableTuple *searchKey, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToGreaterThanKey which has no implementation");
    }

    void moveToLessThanKey(const TableTuple *searchKey, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToLessThanKey which has no implementation");
    }

    void moveToKeyOrLess(TableTuple *searchKey, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToKeyOrLess which has no implementation");
    }

    bool moveToCoveringCell(const TableTuple* searchKey, IndexCursor &cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToCoveringCell which has no implementation");
    }

    void moveToBeforePriorEntry(IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToBeforePriorEntry which has no implementation");
    }

    void moveToPriorEntry(IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToPriorEntry which has no implementation");
    }

    void moveToEnd(bool begin, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToEnd which has no implementation");
    }

    TableTuple nextValue(IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method nextValue which has no implementation");
    }

    bool advanceToNextKey(IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method advanceToNextKey which has no implementation");
    }

    int64_t getCounterGET(const TableTuple *searchKey, bool isUpper, IndexCursor& cursor) const override {
        throwFatalException("Invoked non-countable TableIndex virtual method getCounterGET which has no implementation");
    }

    int64_t getCounterLET(const TableTuple *searchKey, bool isUpper, IndexCursor& cursor) const override {
        throwFatalException("Invoked non-countable TableIndex virtual method getCounterLET which has no implementation");
    }

    bool moveToRankTuple(int64_t denseRank, bool forward, IndexCursor& cursor) const override {
        throwFatalException("Invoked non-countable TableIndex virtual method moveToRankTuple which has no implementation");
    }

    void addEntryDo(const TableTuple* tuple, TableTuple* conflictTuple) override {
        ++m_inserts;
        const void* const* conflictEntry = m_entries.insert(setKeyFromTuple(tuple), tuple->address());
        if (conflictEntry != NULL && conflictTuple != NULL) {
            conflictTuple->move(const_cast<void*>(*conflictEntry));
        }
    }

    bool deleteEntryDo(const TableTuple* tuple) override {
        ++m_deletes;
        return m_entries.erase(setKeyFromTuple(tuple));
    }

    /**
     * Update in place an index entry with a new tuple address
     */
    bool replaceEntryNoKeyChangeDo(const TableTuple& destinationTuple, const TableTuple& originalTuple) override {
        vassert(originalTuple.address() != destinationTuple.address());

        // full delete and insert for certain key types
        if (KeyType::keyDependsOnTupleAddress()) {
            if (! CompactingHashUniqueIndex::deleteEntry(&originalTuple)) {
                return false;
            } else {
                TableTuple conflict(destinationTuple.getSchema());
                CompactingHashUniqueIndex::addEntry(&destinationTuple, &conflict);
                return conflict.isNullTuple();
            }
        }

        MapIterator mapiter = findTuple(originalTuple);
        if (mapiter.isEnd()) {
            return false;
        } else {
            mapiter.setValue(destinationTuple.address());
            m_updates++;
            return true;
        }
    }

    bool keyUsesNonInlinedMemory() const override {
        return KeyType::keyUsesNonInlinedMemory();
    }

    bool checkForIndexChangeDo(const TableTuple* lhs, const TableTuple* rhs) const override {
        return ! m_eq(setKeyFromTuple(lhs), setKeyFromTuple(rhs));
    }

    bool existsDo(const TableTuple *persistentTuple) const override {
        return ! findTuple(*persistentTuple).isEnd();
    }

    bool moveToKey(const TableTuple* searchKey, IndexCursor &cursor) const override {
        MapIterator &mapIter = castToIter(cursor);
        mapIter = findKey(searchKey);

        if (mapIter.isEnd()) {
            cursor.m_match.move(nullptr);
            return false;
        } else {
            cursor.m_match.move(const_cast<void*>(mapIter.value()));
            return true;
        }
    }

    bool moveToKeyByTuple(const TableTuple* persistentTuple, IndexCursor& cursor) const override {
        MapIterator &mapIter = castToIter(cursor);
        mapIter = findTuple(*persistentTuple);

        if (mapIter.isEnd()) {
            cursor.m_match.move(NULL);
            return false;
        } else {
            cursor.m_match.move(const_cast<void*>(mapIter.value()));
            return true;
        }
    }

    TableTuple nextValueAtKey(IndexCursor& cursor) const override {
        TableTuple retval = cursor.m_match;
        cursor.m_match.move(NULL);
        return retval;
    }

    TableTuple uniqueMatchingTuple(const TableTuple& searchTuple) const override {
        TableTuple retval(getTupleSchema());
        const MapIterator keyIter = findTuple(searchTuple);
        if ( ! keyIter.isEnd()) {
            retval.move(const_cast<void*>(keyIter.value()));
        }
        return retval;
    }

    bool hasKey(const TableTuple* searchKey) const override {
        return ! findKey(searchKey).isEnd();
    }

    size_t getSize() const override {
        return m_entries.size();
    }

    int64_t getMemoryEstimate() const override {
        return m_entries.bytesAllocated();
    }

    std::string getTypeName() const override {
        return "CompactingHashUniqueIndex";
    }

    TableIndex* cloneEmptyNonCountingTreeIndex() const override {
        return new CompactingTreeUniqueIndex<NormalKeyValuePair<KeyType, void const *>, false>(
                TupleSchema::createTupleSchema(getKeySchema()), m_scheme);
    }

    // Non-virtual (so "really-private") helper methods.
    MapIterator findKey(const TableTuple *searchKey) const {
        return m_entries.find(KeyType(searchKey));
    }

    MapIterator findTuple(const TableTuple &originalTuple) const {
        return m_entries.find(setKeyFromTuple(&originalTuple));
    }

    const KeyType setKeyFromTuple(const TableTuple *tuple) const {
        return KeyType(tuple, m_scheme.columnIndices, m_scheme.indexedExpressions, m_keySchema);
    }
public:
    CompactingHashUniqueIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme) :
        TableIndex(keySchema, scheme), m_entries(true, KeyHasher(keySchema), KeyEqualityChecker(keySchema)),
        m_eq(keySchema) {}
};

}

