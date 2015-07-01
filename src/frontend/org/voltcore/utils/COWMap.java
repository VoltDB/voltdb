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
package org.voltcore.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ForwardingMap;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableMap.Builder;

/**
 * Key set, value set, and entry set are all immutable as are their iterators.
 * Otherwise behaves as you would expect.
 */
public class COWMap<K, V>  extends ForwardingMap<K, V> implements ConcurrentMap<K, V> {
    private final AtomicReference<ImmutableMap<K, V>> m_map;

    public COWMap() {
        m_map = new AtomicReference<ImmutableMap<K, V>>(new Builder<K, V>().build());
    }

    public COWMap(Map<K, V> map) {
        if (map == null) {
            throw new IllegalArgumentException("Wrapped map cannot be null");
        }
        m_map = new AtomicReference<ImmutableMap<K, V>>(new Builder<K, V>().putAll(map).build());
    }

    @Override
    public V put(K key, V value) {
        while (true) {
            ImmutableMap<K, V> original = m_map.get();
            Builder<K, V> builder = new Builder<K, V>();
            V oldValue = null;
            boolean replaced = false;
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    oldValue = entry.getValue();
                    builder.put(key, value);
                    replaced = true;
                } else {
                    builder.put(entry);
                }
            }
            if (!replaced) {
                builder.put(key, value);
            }
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                return oldValue;
            }
        }
    }

    @Override
    public V remove(Object key) {
        Preconditions.checkNotNull(key);
        while (true) {
            ImmutableMap<K, V> original = m_map.get();
            Builder<K, V> builder = new Builder<K, V>();
            V oldValue = null;
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    oldValue = entry.getValue();
                } else {
                    builder.put(entry);
                }
            }
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original,copy)) {
                return oldValue;
            }
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        while (true) {
            ImmutableMap<K, V> original = m_map.get();
            Builder<K, V> builder = new Builder<K, V>();
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (!m.containsKey(entry.getKey())) {
                    builder.put(entry);
                }
            }
            builder.putAll(m);
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                return;
            }
        }
    }

    @Override
    public void clear() {
        m_map.set(new Builder<K, V>().build());
    }

    @Override
    protected Map<K, V> delegate() {
        return m_map.get();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V existingValue;
        while ((existingValue = get(key)) == null) {
            ImmutableMap<K, V> original = m_map.get();
            if ((existingValue = original.get(key)) != null) break;

            Builder<K, V> builder = new Builder<K, V>();
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    throw new RuntimeException("Shouldn't happen already checked");
                } else {
                    builder.put(entry);
                }
            }
            builder.put(key, value);
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                break;
            }
        }
        return existingValue;
    }

    @Override
    public V get(Object key) {
        Preconditions.checkNotNull(key);
        return delegate().get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        Preconditions.checkNotNull(key);
        return delegate().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        Preconditions.checkNotNull(value);
        return delegate().containsValue(value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        Preconditions.checkNotNull(key);
        if (value == null) return false;
        while (true) {
            ImmutableMap<K, V> original = m_map.get();
            V existingValue = original.get(key);
            if (existingValue == null) break;
            if (!existingValue.equals(value)) break;

            Builder<K, V> builder = new Builder<K, V>();
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    continue;
                } else {
                    builder.put(entry);
                }
            }
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(oldValue);
        Preconditions.checkNotNull(newValue);
        while (true) {
            ImmutableMap<K, V> original = m_map.get();
            V existingValue = original.get(key);
            if (existingValue == null) break;
            if (!existingValue.equals(oldValue)) break;

            Builder<K, V> builder = new Builder<K, V>();
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    continue;
                } else {
                    builder.put(entry);
                }
            }
            builder.put(key, newValue);
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V replace(K key, V value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        while (true) {
            ImmutableMap<K, V> original = m_map.get();
            V existingValue = original.get(key);
            if (existingValue == null) break;

            Builder<K, V> builder = new Builder<K, V>();
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    continue;
                } else {
                    builder.put(entry);
                }
            }
            builder.put(key, value);
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                return existingValue;
            }
        }
        return null;
    }
}
