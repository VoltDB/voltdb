/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef COMPACTINGSET_H_
#define COMPACTINGSET_H_

#include "CompactingMap.h"

namespace voltdb {

/**
 * A class that implements a tree-based set on top of CompactingMap,
 * which uses our homegrown pools and allocators to avoid fragmenting
 * memory or bottlenecking on calls to malloc/free.
 */
template<typename Key, typename Compare >
class CompactingSet {
private:
    // To be used as the value type for the map
    struct EmptyStruct {};
    typedef struct EmptyStruct Data;

    typedef CompactingMap<NormalKeyValuePair<Key, Data>, Compare, false> tree_type;

 public:
    typedef typename tree_type::iterator iterator;

public:
    inline CompactingSet() : m_map(true, Compare()) {}

    inline bool exists(const Key &key) const { return !m_map.find(key).isEnd(); }
    inline iterator find(const Key &key) const { return m_map.find(key); }

    // Returns true if an element was inserted, false if the value is already in the set.
    inline bool insert(const Key &key) { return (m_map.insert(key, Data()) == NULL); }

    // Returns true if an element was erased.
    bool erase(const Key &key) { return m_map.erase(key); }

    inline size_t size() const { return m_map.size(); }
    inline bool empty() const { return m_map.size() == 0; }

    inline iterator begin() { return m_map.begin(); }

    inline iterator lowerBound(const Key& key) { return m_map.lowerBound(key); }
    inline iterator upperBound(const Key& key) { return m_map.upperBound(key); }

private:
    // unimplemented copy ctor and assignment operator
    CompactingSet(const CompactingSet<Key, Compare> &);
    CompactingSet<Key, Compare>& operator=(const CompactingSet<Key, Compare>&);

    tree_type m_map;
};

/**
 * A simple comparator that works for any kind of pointer.
 */
struct PointerComparator {
    int operator()(const void* v1, const void* v2) const {
        // C++ does not like it if you try to subtract void pointers,
        // because the size of void is ambiguous
        return static_cast<const char*>(v1) - static_cast<const char*>(v2);
    }
};

} // namespace voltdb

#endif // COMPACTINGSET_H_
