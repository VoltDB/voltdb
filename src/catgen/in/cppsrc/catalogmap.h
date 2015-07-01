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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

#ifndef CATALOG_CATALOG_MAP_H_
#define CATALOG_CATALOG_MAP_H_

#include <boost/algorithm/string.hpp> // for boost::to_upper(std::string)

#include <map>
#include <string>

namespace catalog {

const char MAP_SEPARATOR = '#';

class Catalog;
class CatalogType;

/**
 * A safe interface to a generic map of CatalogType instances. It is safe
 * because it is read-only. (Exception: maps can be cleared.)
 *
 * @param <T> The subclass of CatalogType that this map will contain.
 */
template <class T>
class CatalogMap {
    friend class Catalog;
    friend class CatalogType;

protected:
    std::map<std::string, T*> m_items;

    Catalog *m_catalog;
    std::string m_path;
    CatalogType *m_parent;

// temporarily public
public:
    CatalogMap(Catalog *globalCatalog, CatalogType *parent, const std::string &path);
    T * add(const std::string &name);
    bool remove(const std::string &name);

public:
    /**
     * Get an item from the map by name
     * @param name The name of the requested CatalogType instance in the map
     * @return The item found in the map, or null if not found
     */
    T * get(const std::string &name) const;

    /**
     * How many items are in the map?
     * @return The number of items in the map
     */
    int32_t size() const;

    typedef typename std::map<std::string, T*>::const_iterator field_map_iter;

    /**
     * Get an iterator for the items in the map
     * @return The iterator for the items in the map
     */
    field_map_iter begin() const;

    /**
     * Get the end iterator for the items in the map
     * @return The end iterator for the items in the map
     */
    field_map_iter end() const;

    /**
     * Clear the map. Does no destruction.
     */
    void clear();
};

template <class T>
CatalogMap<T>::CatalogMap(Catalog *globalCatalog, CatalogType *parent, const std::string &path) {
    m_catalog = globalCatalog;
    m_parent = parent;
    m_path = path;
}

template <class T>
T * CatalogMap<T>::add(const std::string &name) {
    std::string childPath = m_path + MAP_SEPARATOR + name;
    T *retval = new T(m_catalog, m_parent, childPath, name);
    std::string mapKey = name;
    boost::to_upper(mapKey);
    m_items[mapKey] = retval;

    // assign all the children of this map a relative index
    int index = 1;
    typename std::map<std::string, T*>::const_iterator iter;
    for (iter = m_items.begin(); iter != m_items.end(); iter++)
        iter->second->m_relativeIndex = index++;

    return retval;
}

template <class T>
bool CatalogMap<T>::remove(const std::string &name) {
    std::string mapKey = name;
    boost::to_upper(mapKey);
    typename std::map<std::string, T*>::iterator iter = m_items.find(mapKey);
    if (iter == m_items.end()) {
        return false;
    }
    m_items.erase(iter);

    // assign all the children of this map a relative index
    int index = 1;
    typename std::map<std::string, T*>::const_iterator iter2;
    for (iter2 = m_items.begin(); iter2 != m_items.end(); iter2++) {
        iter2->second->m_relativeIndex = index++;
    }

    return true;
}

template <class T>
T * CatalogMap<T>::get(const std::string &name) const {
    std::string mapKey = name;
    boost::to_upper(mapKey);
    const typename std::map<std::string, T*>::const_iterator found = m_items.find(mapKey);
    return (found == m_items.end()) ? NULL : found->second;
}

template <class T>
int32_t CatalogMap<T>::size() const {
    return static_cast<int32_t>(m_items.size());
}

template <class T>
typename std::map<std::string, T*>::const_iterator CatalogMap<T>::begin() const {
    return m_items.begin();
}

template <class T>
typename std::map<std::string, T*>::const_iterator CatalogMap<T>::end() const {
    return m_items.end();
}

// this is totally not const
template <class T>
void CatalogMap<T>::clear() {
    m_items.clear();
}

} // namespace catalog

// Workaround for type inference when applying BOOST_FOREACH to const CatalogMaps.
// @See http://www.boost.org/doc/libs/1_35_0/doc/html/foreach/extensibility.html
#define ENABLE_BOOST_FOREACH_ON_CONST_MAP(CatalogClass)                                  \
namespace boost {                                                                        \
    template<> struct range_const_iterator< catalog::CatalogMap<catalog::CatalogClass> > \
    { typedef catalog::CatalogMap<catalog::CatalogClass>::field_map_iter type; };        \
}

#endif // CATALOG_CATALOG_MAP_H_
