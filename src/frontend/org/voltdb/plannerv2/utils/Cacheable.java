/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
 * A generic LRU cache
 */
public abstract class Cacheable<K, V> extends LinkedHashMap<K, V> {
    private final Map<K, V> m_map;
    protected Cacheable(int cap) {
        m_map = new LinkedHashMap<>(cap);
    }
    public synchronized V cache_get(K key) {
        if (m_map.containsKey(key)) {
            return m_map.get(key);
        } else {
            final V value = calculate(key);
            Preconditions.checkNotNull(value, "Cached value cannot be null");
            m_map.put(key, value);
            return value;
        }
    }
    abstract protected V calculate(K key);
}
