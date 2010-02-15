/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

#ifndef BINARYTREEMULTIMAPINDEX_H_
#define BINARYTREEMULTIMAPINDEX_H_

#include <map>
#include <iostream>
#include "indexes/tableindex.h"
#include "common/tabletuple.h"

namespace voltdb {

/**
 * Index implemented as a Binary Tree Multimap.
 * @see TableIndex
 */
template<typename KeyType, class KeyComparator, class KeyEqualityChecker>
class BinaryTreeMultiMapIndex : public TableIndex
{

    friend class TableIndexFactory;

    typedef std::multimap<KeyType, const void*, KeyComparator> MapType;
    typedef typename MapType::const_iterator MMCIter;
    typedef typename MapType::iterator MMIter;
    typedef typename MapType::const_reverse_iterator MMCRIter;
    typedef typename MapType::reverse_iterator MMRIter;

public:

    ~BinaryTreeMultiMapIndex() {};

    bool addEntry(const TableTuple *tuple)
    {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return addEntryPrivate(tuple, m_tmp1);
    }

    bool deleteEntry(const TableTuple *tuple)
    {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return deleteEntryPrivate(tuple, m_tmp1);
    }

    bool replaceEntry(const TableTuple *oldTupleValue,
                      const TableTuple* newTupleValue)
    {
        // this can probably be optimized
        m_tmp1.setFromTuple(oldTupleValue, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(newTupleValue, column_indices_, m_keySchema);
        if (m_eq(m_tmp1, m_tmp2))
        {
            // no update is needed for this index
            return true;
        }

        bool deleted = deleteEntryPrivate(newTupleValue, m_tmp1);
        bool inserted = addEntryPrivate(newTupleValue, m_tmp2);
        --m_deletes;
        --m_inserts;
        ++m_updates;
        return (deleted && inserted);
    }

    bool checkForIndexChange(const TableTuple *lhs, const TableTuple *rhs)
    {
        m_tmp1.setFromTuple(lhs, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(rhs, column_indices_, m_keySchema);
        return !(m_eq(m_tmp1, m_tmp2));
    }

    bool exists(const TableTuple* values)
    {
        ++m_lookups;
        m_tmp1.setFromTuple(values, column_indices_, m_keySchema);
        //m_keyIter = m_entries.lower_bound(m_tmp1);
        return (m_entries.find(m_tmp1) != m_entries.end());
    }

    bool moveToKey(const TableTuple *searchKey)
    {
        m_tmp1.setFromKey(searchKey);
        return moveToKey(m_tmp1);
    }

    bool moveToTuple(const TableTuple *searchTuple)
    {
        m_tmp1.setFromTuple(searchTuple, column_indices_, m_keySchema);
        return moveToKey(m_tmp1);
    }

    void moveToKeyOrGreater(const TableTuple *searchKey)
    {
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_seqIter = m_entries.lower_bound(m_tmp1);
    }

    void moveToGreaterThanKey(const TableTuple *searchKey)
    {
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_seqIter = m_entries.upper_bound(m_tmp1);
    }

    void moveToEnd(bool begin)
    {
        ++m_lookups;
        m_begin = begin;
        if (begin)
            m_seqIter = m_entries.begin();
        else
            m_seqRIter = m_entries.rbegin();
    }

    TableTuple nextValue()
    {
        TableTuple retval(m_tupleSchema);

        if (m_begin) {
            if (m_seqIter == m_entries.end())
                return TableTuple();
            retval.move(const_cast<void*>(m_seqIter->second));
            ++m_seqIter;
        } else {
            if (m_seqRIter == (typename MapType::const_reverse_iterator) m_entries.rend())
                return TableTuple();
            retval.move(const_cast<void*>(m_seqRIter->second));
            ++m_seqRIter;
        }

        return retval;
    }

    TableTuple nextValueAtKey()
    {
        if (m_match.isNullTuple()) return m_match;
        TableTuple retval = m_match;
        ++(m_keyIter.first);
        if (m_keyIter.first == m_keyIter.second)
            m_match.move(NULL);
        else
            m_match.move(const_cast<void*>(m_keyIter.first->second));
        return retval;
    }

    bool advanceToNextKey()
    {
        if (m_keyIter.second == m_entries.end())
            return false;
        return moveToKey(m_keyIter.second->first);
    }

    size_t getSize() const { return m_entries.size(); }
    std::string getTypeName() const { return "BinaryTreeMultiMapIndex"; };

protected:
    BinaryTreeMultiMapIndex(const TableIndexScheme &scheme) :
        TableIndex(scheme),
        m_entries(KeyComparator(m_keySchema)),
        m_begin(true),
        m_eq(m_keySchema)
    {
        m_match = TableTuple(m_tupleSchema);
    }

    inline bool addEntryPrivate(const TableTuple *tuple, const KeyType &key)
    {
        ++m_inserts;
        m_entries.insert(std::pair<KeyType, const void*>(key, tuple->address()));
        return true;
    }

    inline bool deleteEntryPrivate(const TableTuple *tuple, const KeyType &key)
    {
        ++m_deletes;
        std::pair<MMIter,MMIter> key_iter;
        for (key_iter = m_entries.equal_range(key);
             key_iter.first != key_iter.second;
             ++(key_iter.first))
        {
            if (key_iter.first->second == tuple->address())
            {
                m_entries.erase(key_iter.first);
                //deleted
                return true;
            }
        }
        //key exists, but tuple not exists
        return false;
    }

    bool moveToKey(const KeyType &key)
    {
        ++m_lookups;
        m_begin = true;
        m_keyIter = m_entries.equal_range(key);
        if (m_keyIter.first == m_keyIter.second)
        {
            m_match.move(NULL);
            return false;
        }
        m_match.move(const_cast<void*>(m_keyIter.first->second));
        return !m_match.isNullTuple();
    }

    MapType m_entries;
    KeyType m_tmp1;
    KeyType m_tmp2;

    // iteration stuff
    bool m_begin;
    typename std::pair<MMCIter, MMCIter> m_keyIter;
    MMCIter m_seqIter;
    MMCRIter m_seqRIter;
    TableTuple m_match;

    // comparison stuff
    KeyEqualityChecker m_eq;
};

}

#endif // BINARYTREEMULTIMAPINDEX_H_
