/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
template<typename Key, typename Compare = comp<Key> >
class CompactingSet {
protected:
    // To be used as the value type for the map
    struct EmptyStruct {};
    typedef struct EmptyStruct Data;

    typedef CompactingMap<NormalKeyValuePair<Key, Data>, Compare, false, Key> tree_type;

    tree_type m_map;

public:
    typedef typename tree_type::iterator iterator;
    typedef typename tree_type::const_iterator const_iterator;

    // The following are required by the STL API.
    typedef Compare key_compare;
    typedef typename tree_type::value_type value_type;
    typedef typename tree_type::value_compare value_compare;

public:
    inline CompactingSet(Compare comper = Compare()) : m_map(true, comper) {}
    inline CompactingSet(const CompactingSet<Key, Compare> &other) : m_map(other.m_map) {}

    inline bool exists(const Key &key) const { return !m_map.find(key).isEnd(); }
    inline bool insert(const Key &key) { return (m_map.insert(key, Data()) == NULL); }
    bool erase(const Key &key) { return m_map.erase(key); }

    inline bool empty() const { return m_map.empty(); }
    inline size_t size() const { return m_map.size(); }
    inline void clear() { m_map.clear(); }

    inline iterator begin() { return m_map.begin(); }
    inline const_iterator begin() const { return m_map.begin(); }
    inline iterator end() { return m_map.end(); }
    inline const_iterator end() const { return m_map.end(); }
    inline iterator lower_bound(const Key &key) { return m_map.lowerBound(key); }
    inline const_iterator lower_bound(const Key &key) const { return m_map.lowerBound(key); }
    inline iterator upper_bound(const Key &key) { return m_map.upperBound(key); }
    inline const_iterator upper_bound(const Key &key) const { return m_map.upperBound(key); }

    // Required by the STL API.
    inline key_compare key_comp() const { return m_map.key_comp(); }
    inline value_compare value_comp() const { return m_map.value_comp(); }

    inline bool operator==(const CompactingSet<Key, Compare> &other) const
    {
        return m_map == other.m_map;
    }

    inline bool operator!=(const CompactingSet<Key, Compare> &other) const
    {
        return m_map != other.m_map;
    }
};

} // namespace voltdb

#endif // COMPACTINGSET_H_
