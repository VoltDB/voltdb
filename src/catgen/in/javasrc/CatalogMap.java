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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * A safe interface to a generic map of CatalogType instances. It is safe
 * because it is mostly read-only. All operations that modify the map are
 * either non-public, or they are convenience methods which generate
 * catalog commands and execute them on the root Catalog instance. By
 * generating commands, transactional safety is easier to assure.
 *
 * @param <T> The subclass of CatalogType that this map will contain.
 */
public final class CatalogMap<T extends CatalogType> implements Iterable<T> {

    TreeMap<String, T> m_items = null;
    Class<T> m_cls;
    Catalog m_catalog;
    CatalogType m_parent;
    String m_name;
    String m_cachedPath = null;
    boolean m_hasComputedOrder = false;
    int m_depth;

    CatalogMap(Catalog catalog, CatalogType parent, String name, Class<T> cls, int depth) {
        m_catalog = catalog;
        m_parent = parent;
        m_name = name;
        m_cls = cls;
        m_depth = depth;
        if (depth <= 3) {
            m_cachedPath = getPath();
        }
    }

    public String getPath() {
        if (m_cachedPath != null) {
            return m_cachedPath;
        }
        // If parent is the catalog root, don't add an extra slash to the existing one.
        if (m_parent == m_catalog) {
            return "/" + m_name;
        } else {
            return m_parent.getCatalogPath() + "/" + m_name;
        }
    }

    /**
     * Get an item from the map by name
     * @param name The name of the requested CatalogType instance in the map
     * @return The item found in the map, or null if not found
     */
    public T get(String name) {
        if (m_items == null) {
            return null;
        }
        return m_items.get(name.toUpperCase());
    }

    @SuppressWarnings("unused")
    private Object _get(String name) {
        return get(name);
    }

    public T getExact(String name) {
        if (m_items == null) {
            return null;
        }
        return m_items.get(name);
    }

    /**
     * Get an item from the map by name, ignoring case
     * @param name The name of the requested CatalogType instance in the map
     * @return The item found in the map, or null if not found
     */
    public T getIgnoreCase(String name) {
        if (m_items == null) {
            return null;
        }
        return m_items.get(name.toUpperCase());
    }

    /**
     * How many items are in the map?
     * @return The number of items in the map
     */
    public int size() {
        if (m_items == null) {
            return 0;
        }
        return m_items.size();
    }

    /**
     * Is the map empty?
     * @return A boolean indicating whether the map is empty
     */
    public boolean isEmpty() {
        if (m_items == null) {
            return true;
        }
        return (m_items.size() == 0);
    }

    /**
     * Get an iterator for the items in the map
     * @return The iterator for the items in the map
     */
    @Override
    public Iterator<T> iterator() {
        if (m_items == null) {
            m_items = new TreeMap<String, T>();
        }
        return m_items.values().iterator();
    }

    /**
     * Create a new instance of a CatalogType as a child of this map with a
     * given name. Note: this just makes a catalog command and calls
     * catalog.execute(..).
     * @param name The name of the new instance to create, the thing to add
     * @return The newly created CatalogType instance
     */
    public T add(String name) {
        try {
            if (m_items == null) {
                m_items = new TreeMap<String, T>();
            }

            String mapKey = name.toUpperCase();
            if (m_items.containsKey(mapKey)) {
                throw new CatalogException("Catalog item '" + mapKey + "' already exists for " + m_parent);
            }

            T x = m_cls.newInstance();
            x.setBaseValues(this, name);
            x.initChildMaps();

            m_items.put(mapKey, x);

            if (m_hasComputedOrder) {
                recomputeRelativeIndexes();
            }

            return x;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("unused")
    private void _add(String name) {
        add(name);
    }

    /**
     * Remove a {@link CatalogType} object from this collection.
     * @param name The name of the object to remove.
     */
    public void delete(String name) {
        try {
            String mapKey = name.toUpperCase();
            if ((m_items == null) || (m_items.containsKey(mapKey) == false)) {
                throw new CatalogException("Catalog item '" + mapKey + "' doesn't exists in " + m_parent);
            }

            m_items.remove(mapKey);

            // assign a relative index to every child item
            int index = 1;
            for (Entry<String, T> e : m_items.entrySet()) {
                e.getValue().m_relativeIndex = index++;
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Clear the contents of the map
     */
    public void clear() {
        if (m_items == null) {
            return;
        }
        m_items.clear();
    }

    void accept(CatalogVisitor visitor) {
        for (T item : this) {
            item.accept(visitor);
        }
    }

    @SuppressWarnings("unchecked")
    void copyFrom(CatalogMap<? extends CatalogType> catalogMap) {
        CatalogMap<T> castedMap = (CatalogMap<T>) catalogMap;
        m_hasComputedOrder = castedMap.m_hasComputedOrder;
        if (castedMap.m_items == null) {
            return;
        }
        if (m_items == null) {
            m_items = new TreeMap<String, T>();
        }
        for (Entry<String, T> e : castedMap.m_items.entrySet()) {
            m_items.put(e.getKey(), (T) e.getValue().deepCopy(m_catalog, this));
        }
    }

    @Override
    public boolean equals(Object obj) {
        // returning false if null isn't the convention, oh well
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }

        // Do the identity check
        if (obj == this) {
            return true;
        }

        @SuppressWarnings("unchecked")
        CatalogMap<T> other = (CatalogMap<T>) obj;

        if (other.size() != size()) {
            return false;
        }

        if (m_items == null) {
            return (other.m_items == null) || (other.m_items.size() == 0);
        }

        for (Entry<String, T> e : m_items.entrySet()) {
            assert(e.getValue() != null);
            T type = other.get(e.getKey());
            if (type == null) {
                return false;
            }
            if (type.equals(e.getValue()) == false) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        if (m_items == null) {
            return 0;
        }

        // based on implementation of equals
        int result = size();

        for (Entry<String, T> e : m_items.entrySet()) {
            String key = e.getKey();
            if (key != null) {
                result += key.hashCode();
            }
            T value = e.getValue();
            if (value != null) {
                result += value.hashCode();
            }
        }
        return result;
    }

    void recomputeRelativeIndexes() {
        if (m_items == null) {
            return;
        }

        // assign a relative index to every child item
        int index = 1;
        for (Entry<String, T> e : m_items.entrySet()) {
            e.getValue().m_relativeIndex = index++;
        }
        m_hasComputedOrder = true;
    }

}
