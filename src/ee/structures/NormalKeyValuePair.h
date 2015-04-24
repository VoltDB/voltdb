/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include <cstdlib>

namespace voltdb {

template <typename T> inline void setPointerValue(T& t, const void * v) {}

// the template for KeyTypes don't contain a pointer to the tuple
template <typename Key, typename Data = const void*>
class NormalKeyValuePair : public std::pair<Key, Data> {
public:
    NormalKeyValuePair() {}
    NormalKeyValuePair(const Key &key, const Data &value) : std::pair<Key, Data>(key, value) {}

    const Key& getKey() const { return std::pair<Key, Data>::first; }
    const Data& getValue() const { return std::pair<Key, Data>::second; }
    void setKey(const Key &key) { std::pair<Key, Data>::first = key; }
    void setValue(const Data &value) { std::pair<Key, Data>::second = value; }

    // This function does nothing, and is only to offer the same API as PointerKeyValuePair.
    const void *setPointerValue(const void *value) { return NULL; }
};

}

