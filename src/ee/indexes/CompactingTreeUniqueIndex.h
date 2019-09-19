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

#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"
#include "structures/CompactingMap.h"

namespace voltdb {

/**
 * Index implemented as a Binary Tree Unique Map.
 * @see TableIndex
 */
template<typename KeyValuePair, bool hasRank>
class CompactingTreeUniqueIndex : public TableIndex {
    using KeyType = typename KeyValuePair::first_type;
    using KeyComparator = typename KeyType::KeyComparator;
    using MapType = CompactingMap<KeyValuePair, KeyComparator, hasRank>;
    using MapIterator = typename MapType::iterator;
    MapType m_entries;
    // comparison stuff
    KeyComparator m_cmp;

    ~CompactingTreeUniqueIndex() {}

    static MapIterator& castToIter(IndexCursor& cursor) {
        return *reinterpret_cast<MapIterator*> (cursor.m_keyIter);
    }

    bool moveToCoveringCell(const TableTuple* searchKey, IndexCursor &cursor) const override {
        notImplemented("moveToCoveringCell");
        return {};
    }

    void addEntryDo(const TableTuple *tuple, TableTuple *conflictTuple) override {
        ++m_inserts;
        auto const* conflictEntry = m_entries.insert(setKeyFromTuple(tuple), tuple->address());
        if (conflictEntry != nullptr && conflictTuple != nullptr) {
            conflictTuple->move(const_cast<void*>(*conflictEntry));
        }
    }

    bool deleteEntryDo(const TableTuple *tuple) override {
        ++m_deletes;
        return m_entries.erase(setKeyFromTuple(tuple));
    }

    /**
     * Update in place an index entry with a new tuple address
     */
    bool replaceEntryNoKeyChangeDo(const TableTuple &destinationTuple, const TableTuple &originalTuple) override {
        vassert(originalTuple.address() != destinationTuple.address());

        // full delete and insert for certain key types
        if (KeyType::keyDependsOnTupleAddress()) {
            if (! deleteEntry(&originalTuple)) {
                return false;
            } else {
                TableTuple conflict(destinationTuple.getSchema());
                addEntry(&destinationTuple, &conflict);
                return conflict.isNullTuple();
            }
        }

        auto mapIter = findTuple(originalTuple);
        if (mapIter.isEnd()) {
            return false;
        } else {
            mapIter.setValue(destinationTuple.address());
            m_updates++;
            return true;
        }
    }

    bool keyUsesNonInlinedMemory() const override {
        return KeyType::keyUsesNonInlinedMemory();
    }

    bool checkForIndexChangeDo(const TableTuple* lhs, const TableTuple* rhs) const override {
        return  0 != m_cmp(setKeyFromTuple(lhs), setKeyFromTuple(rhs));
    }

    bool existsDo(const TableTuple *persistentTuple) const override {
        return ! findTuple(*persistentTuple).isEnd();
    }

    bool moveToKey(const TableTuple *searchKey, IndexCursor& cursor) const override {
        cursor.m_forward = true;
        auto &mapIter = castToIter(cursor);
        mapIter = findKey(searchKey);

        if (mapIter.isEnd()) {
            cursor.m_match.move(nullptr);
            return false;
        } else {
            cursor.m_match.move(const_cast<void*>(mapIter.value()));
            return true;
        }
    }

    bool moveToKeyByTuple(const TableTuple *persistentTuple, IndexCursor &cursor) const override {
        cursor.m_forward = true;
        auto &mapIter = castToIter(cursor);
        mapIter = findTuple(*persistentTuple);

        if (mapIter.isEnd()) {
            cursor.m_match.move(nullptr);
            return false;
        } else {
            cursor.m_match.move(const_cast<void*>(mapIter.value()));
            return true;
        }
    }

    void moveToKeyOrGreater(const TableTuple *searchKey, IndexCursor& cursor) const override {
        cursor.m_forward = true;
        auto &mapIter = castToIter(cursor);
        mapIter = m_entries.lowerBound(KeyType(searchKey));
    }

    bool moveToGreaterThanKey(const TableTuple *searchKey, IndexCursor& cursor) const override {
        cursor.m_forward = true;
        auto &mapIter = castToIter(cursor);
        mapIter = m_entries.upperBound(KeyType(searchKey));
        return mapIter.isEnd();
    }

    void moveToLessThanKey(const TableTuple *searchKey, IndexCursor& cursor) const override {
        // do moveToKeyOrGreater()
        auto &mapIter = castToIter(cursor);
        mapIter = m_entries.lowerBound(KeyType(searchKey));

        // find prev entry
        if (mapIter.isEnd()) {
            moveToEnd(false, cursor);
        } else {
            cursor.m_forward = false;
            mapIter.movePrev();
        }
    }

    void moveToKeyOrLess(TableTuple *searchKey, IndexCursor &cursor) const override {
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
        auto &mapIter = castToIter(cursor);
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
    void moveToBeforePriorEntry(IndexCursor& cursor) const override {
        vassert(cursor.m_forward);
        cursor.m_forward = false;
        auto &mapIter = castToIter(cursor);

        if (mapIter.isEnd()) {
            mapIter = m_entries.rbegin();
        } else {
            // go back 2 entries
            // entries: [..., A, B, C, ...], currently mapIter = C (not NULL if reach here)
            // B is the entry we just evaluated and didn't pass initial_expression test (can not be NULL)
            // so A is the correct starting point (can be NULL)
            mapIter.movePrev();
        }
        mapIter.movePrev();
    }

    void moveToPriorEntry(IndexCursor& cursor) const override {
        vassert(cursor.m_forward);
        cursor.m_forward = false;
        auto &mapIter = castToIter(cursor);

        if (mapIter.isEnd()) {
            mapIter = m_entries.rbegin();
        } else {
            mapIter.movePrev();
        }
    }

    void moveToEnd(bool begin, IndexCursor& cursor) const override {
        cursor.m_forward = begin;
        auto &mapIter = castToIter(cursor);

        if (begin) {
            mapIter = m_entries.begin();
        } else {
            mapIter = m_entries.rbegin();
        }
    }

    TableTuple nextValue(IndexCursor& cursor) const override {
        TableTuple retval(getTupleSchema());
        auto &mapIter = castToIter(cursor);
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

    TableTuple nextValueAtKey(IndexCursor& cursor) const override {
        TableTuple retval = cursor.m_match;
        cursor.m_match.move(nullptr);
        return retval;
    }

    bool advanceToNextKey(IndexCursor& cursor) const override {
        auto &mapIter = castToIter(cursor);

        if (cursor.m_forward) {
            mapIter.moveNext();
        } else {
            mapIter.movePrev();
        }
        if (mapIter.isEnd()) {
            cursor.m_match.move(nullptr);
            return false;
        } else {
            cursor.m_match.move(const_cast<void*>(mapIter.value()));
            return true;
        }
    }

    TableTuple uniqueMatchingTuple(const TableTuple &searchTuple) const override {
        TableTuple retval(getTupleSchema());
        const auto keyIter = findTuple(searchTuple);
        if (! keyIter.isEnd()) {
            retval.move(const_cast<void*>(keyIter.value()));
        }
        return retval;
    }

    bool hasKey(const TableTuple *searchKey) const override {
        return ! findKey(searchKey).isEnd();
    }

    /**
     * @See comments in parent class TableIndex
     */
    int64_t getCounterGET(const TableTuple* searchKey, bool isUpper, IndexCursor& cursor) const override {
        if (!hasRank) {
            return -1;
        }
        CompactingTreeUniqueIndex::moveToKeyOrGreater(searchKey, cursor);
        auto &mapIter = castToIter(cursor);

        if (mapIter.isEnd()) {
            return m_entries.size() + 1;
        } else {
            return m_entries.rankLower(mapIter.key());
        }
    }

    /**
     * See comments in parent class TableIndex
     */
    int64_t getCounterLET(const TableTuple* searchKey, bool isUpper, IndexCursor& cursor) const override {
        if (!hasRank) {
           return -1;
        }
        const KeyType tmpKey(searchKey);
        auto mapIter = m_entries.lowerBound(tmpKey);
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
        return m_entries.rankLower(mapIter.key());
    }

    bool moveToRankTuple(int64_t denseRank, bool forward, IndexCursor& cursor) const override {
        cursor.m_forward = forward;
        auto &mapConstIter = castToIter(cursor);
        mapConstIter = m_entries.findRank(denseRank);
        if (mapConstIter.isEnd()) {
            cursor.m_match.move(nullptr);
            return false;
        } else {
            cursor.m_match.move(const_cast<void*>(mapConstIter.value()));
            return true;
        }
    }

    size_t getSize() const override {
        return m_entries.size();
    }

    int64_t getMemoryEstimate() const override {
        return m_entries.bytesAllocated();
    }

    std::string debug() const override {
        std::ostringstream buffer;
        buffer << TableIndex::debug() << std::endl;
        auto iter = m_entries.begin();
        while (!iter.isEnd()) {
            TableTuple retval(getTupleSchema());
            retval.move(const_cast<void*>(iter.value()));
            buffer << retval.debugNoHeader() << std::endl;
            iter.moveNext();
        }
        return buffer.str();
    }

    char const* getTypeName() const override {
        return "CompactingTreeUniqueIndex";
    };

    TableIndex *cloneEmptyNonCountingTreeIndex() const override {
        return new CompactingTreeUniqueIndex<KeyValuePair, false>(
                TupleSchema::createTupleSchema(getKeySchema()), m_scheme);
    }


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
    CompactingTreeUniqueIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme) :
        TableIndex(keySchema, scheme), m_entries(true, KeyComparator(keySchema)), m_cmp(keySchema) {}
};

}

