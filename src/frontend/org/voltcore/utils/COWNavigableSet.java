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

import java.util.Collection;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicReference;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ForwardingNavigableSet;
import com.google_voltpatches.common.collect.ImmutableSortedSet;


public class COWNavigableSet<E extends Comparable<E>> extends ForwardingNavigableSet<E> {

    private final AtomicReference<ImmutableSortedSet<E>> m_set;

    public COWNavigableSet() {
        m_set = new AtomicReference<ImmutableSortedSet<E>>(ImmutableSortedSet.<E>of());
    }

    public COWNavigableSet(Collection<E> c) {
        Preconditions.checkNotNull(c);
        ImmutableSortedSet.Builder<E> builder = ImmutableSortedSet.naturalOrder();
        for (E e : c) {
            builder.add(e);
        }
        m_set = new AtomicReference<ImmutableSortedSet<E>>(builder.build());
    }

    @Override
    protected NavigableSet<E> delegate() {
        return m_set.get();
    }

    @Override
    public E pollFirst() {
        while (true) {
            ImmutableSortedSet<E> snapshot = m_set.get();
            E first = null;
            if (snapshot.size() > 0) {
               first = snapshot.first();
            } else {
                return null;
            }
            ImmutableSortedSet.Builder<E> builder = ImmutableSortedSet.naturalOrder();
            builder.addAll(snapshot.tailSet(first, false));
            if (m_set.compareAndSet(snapshot, builder.build())) {
                return first;
            }
        }
    }

    @Override
    public E pollLast() {
        while (true) {
            ImmutableSortedSet<E> snapshot = m_set.get();
            E last = null;
            if (snapshot.size() > 0) {
                last = snapshot.last();
            } else {
                return null;
            }
            ImmutableSortedSet.Builder<E> builder = ImmutableSortedSet.naturalOrder();
            builder.addAll(snapshot.headSet(last, false));
            if (m_set.compareAndSet(snapshot, builder.build())) {
                return last;
            }
        }
    }

    @Override
    public boolean add(E e) {
        while (true) {
            ImmutableSortedSet<E> snapshot = m_set.get();
            if (snapshot.contains(e)) return false;

            ImmutableSortedSet.Builder<E> builder = ImmutableSortedSet.naturalOrder();
            builder.addAll(snapshot);
            builder.add(e);
            if (m_set.compareAndSet(snapshot, builder.build())) {
                return true;
            }
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        while (true) {
            ImmutableSortedSet<E> snapshot = m_set.get();
            ImmutableSortedSet.Builder<E> builder = ImmutableSortedSet.naturalOrder();

            boolean hadValues = false;
            for (E e : c) {
                if (!snapshot.contains(e)) {
                    builder.add(e);
                    hadValues = true;
                }
            }
            if (hadValues) {
                builder.addAll(snapshot);
                if (m_set.compareAndSet(snapshot, builder.build())) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public void clear() {
        m_set.set(ImmutableSortedSet.<E>of());
    }

    @Override
    public boolean remove(Object o) {
        while (true) {
            ImmutableSortedSet<E> snapshot = m_set.get();
            if (!snapshot.contains(o)) return false;

            ImmutableSortedSet.Builder<E> builder = ImmutableSortedSet.naturalOrder();
            for (E e : snapshot) {
                if (e.equals(o)) continue;
                builder.add(e);
            }
            if (m_set.compareAndSet(snapshot, builder.build())) {
                return true;
            }
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        while (true) {
            ImmutableSortedSet<E> snapshot = m_set.get();
            ImmutableSortedSet.Builder<E> builder = ImmutableSortedSet.naturalOrder();

            boolean hadValues = false;
            for (E e : snapshot) {
                if (c.contains(e)) {
                    hadValues = true;
                    continue;
                }
                builder.add(e);
            }

            if (hadValues) {
                if (m_set.compareAndSet(snapshot, builder.build())) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        while (true) {
            ImmutableSortedSet<E> snapshot = m_set.get();
            ImmutableSortedSet.Builder<E> builder = ImmutableSortedSet.naturalOrder();

            boolean removedValues = false;
            for (E e : snapshot) {
                if (c.contains(e)) {
                    builder.add(e);
                    continue;
                }
                removedValues = true;
            }

            if (removedValues) {
                if (m_set.compareAndSet(snapshot, builder.build())) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }
}
