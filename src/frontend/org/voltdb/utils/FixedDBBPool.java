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

package org.voltdb.utils;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.EELibraryLoader;
import org.voltdb.VoltDB;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class FixedDBBPool {
    // Key is the size of the buffers for the corresponding permits
    protected final NonBlockingHashMap<Integer, Semaphore> m_permits =
        new NonBlockingHashMap<Integer, Semaphore>();

    public FixedDBBPool()
    {
        if (!VoltDB.getLoadLibVOLTDB()) {
            throw new RuntimeException("Unable to load native library to allocate direct byte buffers");
        }

        EELibraryLoader.loadExecutionEngineLibrary(true);
    }

    public void allocate(int bufLenInBytes, int capacity)
    {
        m_permits.putIfAbsent(bufLenInBytes, new Semaphore(capacity));
    }

    public BlockingQueue<DBBPool.BBContainer> getQueue(final int bufLenInBytes)
    {
        return new BlockingQueue<BBContainer>() {

            @Override
            public boolean add(BBContainer bbContainer) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean offer(BBContainer bbContainer) {
                throw new UnsupportedOperationException();
            }

            @Override
            public BBContainer remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public BBContainer poll() {
                throw new UnsupportedOperationException();
            }

            @Override
            public BBContainer element() {
                throw new UnsupportedOperationException();
            }

            @Override
            public BBContainer peek() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void put(BBContainer bbContainer) throws InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean offer(BBContainer bbContainer, long timeout, TimeUnit unit) throws InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public BBContainer take() throws InterruptedException {
                final Semaphore permits = m_permits.get(bufLenInBytes);
                permits.acquire();
                final BBContainer origin = DBBPool.allocateDirectAndPool(bufLenInBytes);
                return new BBContainer(origin.b()) {
                    @Override
                    public void discard() {
                        checkDoubleFree();
                        permits.release();
                        origin.discard();
                    }
                };
            }

            @Override
            public BBContainer poll(long timeout, TimeUnit unit) throws InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int remainingCapacity() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean addAll(Collection<? extends BBContainer> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int size() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isEmpty() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean contains(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<BBContainer> iterator() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int drainTo(Collection<? super BBContainer> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int drainTo(Collection<? super BBContainer> c, int maxElements) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Discard all allocated buffers in the pool. Must call this after using the pool to free the
     * memory.
     *
     * This method is idempotent.
     */
    public void clear()
    {
        m_permits.clear();
    }
}
