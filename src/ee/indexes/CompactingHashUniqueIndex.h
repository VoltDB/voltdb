/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

#ifndef COMPACTINGHASHUNIQUEINDEX_H_
#define COMPACTINGHASHUNIQUEINDEX_H_

#include <iostream>
#include <cassert>

#include "indexes/tableindex.h"
#include "structures/CompactingHashTable.h"

namespace voltdb {

/**
 * Index implemented as a Hash Table Unique Map.
 * @see TableIndex
 */
template<typename KeyType, class KeyHasher, class KeyEqualityChecker>
class CompactingHashUniqueIndex : public TableIndex {
    friend class TableIndexFactory;

    typedef CompactingHashTable<KeyType, const void*, KeyHasher, KeyEqualityChecker> MapType;

public:

    ~CompactingHashUniqueIndex() {};

    bool addEntry(const TableTuple *tuple) {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return addEntryPrivate(tuple, m_tmp1);
    }

    bool deleteEntry(const TableTuple *tuple) {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return deleteEntryPrivate(m_tmp1);
    }

    bool replaceEntry(const TableTuple *oldTupleValue, const TableTuple* newTupleValue) {
        // this can probably be optimized
        m_tmp1.setFromTuple(oldTupleValue, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(newTupleValue, column_indices_, m_keySchema);

        if (m_eq(m_tmp1, m_tmp2)) return true; // no update is needed for this index

        bool deleted = deleteEntryPrivate(m_tmp1);
        bool inserted = addEntryPrivate(newTupleValue, m_tmp2);
        --m_deletes;
        --m_inserts;
        ++m_updates;
        return (deleted && inserted);
    }

    /**
     * Update in place an index entry with a new tuple address
     */
    bool replaceEntryNoKeyChange(const TableTuple *oldTupleValue,
                              const TableTuple *newTupleValue) {
        assert(oldTupleValue->address() != newTupleValue->address());
        m_tmp1.setFromTuple(oldTupleValue, column_indices_, m_keySchema);
        typename MapType::iterator mapiter = m_entries.find(m_tmp1);
        assert(!mapiter.isEnd());
        if (mapiter.isEnd()) {
            return false;
        }
        mapiter.setValue(newTupleValue->address());
        m_updates++;
        return true;
    }

    bool checkForIndexChange(const TableTuple *lhs, const TableTuple *rhs) {
        m_tmp1.setFromTuple(lhs, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(rhs, column_indices_, m_keySchema);
        return !(m_eq(m_tmp1, m_tmp2));
    }
    bool exists(const TableTuple* values) {
        ++m_lookups;
        m_tmp1.setFromTuple(values, column_indices_, m_keySchema);
        return (m_entries.find(m_tmp1).isEnd() == false);
    }
    bool moveToKey(const TableTuple *searchKey) {
        ++m_lookups;
        m_tmp1.setFromKey(searchKey);
        m_keyIter = m_entries.find(m_tmp1);
        if (m_keyIter.isEnd()) {
            m_match.move(NULL);
            return false;
        }
        m_match.move(const_cast<void*>(m_keyIter.value()));
        return m_match.address() != NULL;
    }
    bool moveToTuple(const TableTuple *searchTuple) {
        ++m_lookups;
        m_tmp1.setFromTuple(searchTuple, column_indices_, m_keySchema);
        m_keyIter = m_entries.find(m_tmp1);
        if (m_keyIter.isEnd()) {
            m_match.move(NULL);
            return false;
        }
        m_match.move(const_cast<void*>(m_keyIter.value()));
        return m_match.address() != NULL;
    }
    TableTuple nextValueAtKey() {
        TableTuple retval = m_match;
        m_match.move(NULL);
        return retval;
    }

    size_t getSize() const { return m_entries.size(); }

    int64_t getMemoryEstimate() const
    {
        return m_entries.bytesAllocated();
    }

    std::string getTypeName() const { return "CompactingHashUniqueIndex"; };

protected:
    CompactingHashUniqueIndex(const TableIndexScheme &scheme) :
        TableIndex(scheme),
        m_entries(true, KeyHasher(m_keySchema), KeyEqualityChecker(m_keySchema)),
        m_eq(m_keySchema)
    {
        m_match = TableTuple(m_tupleSchema);
    }

    inline bool addEntryPrivate(const TableTuple *tuple, const KeyType &key) {
        ++m_inserts;
        bool success = m_entries.insert(key, tuple->address());
        return success;
    }

    inline bool deleteEntryPrivate(const KeyType &key) {
        ++m_deletes;
        return m_entries.erase(key);
    }

    MapType m_entries;
    KeyType m_tmp1;
    KeyType m_tmp2;

    // iteration stuff
    typename MapType::iterator m_keyIter;
    TableTuple m_match;

    // comparison stuff
    typename MapType::KeyEqChecker m_eq;
};

}

#endif // COMPACTINGHASHUNIQUEINDEX_H_
