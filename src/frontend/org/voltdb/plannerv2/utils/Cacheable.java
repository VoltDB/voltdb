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

package org.voltdb.plannerv2.utils;

import com.google_voltpatches.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A generic LRU cache. Client must ensure that the key type K must properly override hashCode() method.
 */
public abstract class Cacheable<K, V> {
    private final Map<Integer, V> m_map;
    protected Cacheable(int cap) {
        m_map = new LinkedHashMap<>(cap);
    }
    public synchronized V get(K key) {
        final int hashedKey = hashCode(key);
        if (m_map.containsKey(hashedKey)) {
            return m_map.get(hashedKey);
        } else {
            final V value = calculate(key);
            Preconditions.checkNotNull(value, "Cached value cannot be null");
            m_map.put(hashedKey, value);
            return value;
        }
    }

    /**
     * K-V map
     * @param key key of the cache entry
     * @return value of the cache entry
     */
    abstract protected V calculate(K key);

    /**
     * Hashes key object
     * @param key key object
     * @return hash code of the key object, that must ensure cache map functionality.
     */
    abstract protected int hashCode(K key);
}
