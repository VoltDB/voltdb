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

package org.voltdb.utils;

import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.rejoin.RejoinTaskBuffer;

import com.google_voltpatches.common.base.Preconditions;

public enum AllocationStrategy {
    LR(32, RejoinTaskBuffer.DEFAULT_BUFFER_SIZE);

    private final AllocatorThreadLocal m_allocator;

    AllocationStrategy(int poolSize, int defaultBufferSize) {
        m_allocator = new AllocatorThreadLocal(poolSize, defaultBufferSize);
    }

    public BBContainer allocate(int allocationSize) {
        return m_allocator.get().allocate(allocationSize);
    }

    public void track(int size) {
        m_allocator.get().track(size);
    }

    public static class AllocatorThreadLocal extends ThreadLocal<Allocator> {
        private final int m_poolSize;
        private final int m_defaultBufferSize;

        public AllocatorThreadLocal(int poolSize, int defaultBufferSize) {
            m_poolSize = poolSize;
            m_defaultBufferSize = defaultBufferSize;
        }

        @Override
        protected Allocator initialValue() {
            return new Allocator(m_poolSize, m_defaultBufferSize);
        }
    }

    /**
     * A {@link BBContainer} allocator that depending on the requested size, it
     * uses a limited pool of globally pooled direct buffer containers, or allocate
     * it from native memory heap
     *
     */
    public static class Allocator {
        private final SizeTracker m_tracker;
        private final int m_defaultBufferSize;

        /**
         * allowed number of pooled direct buffer containers
         */
        private final AtomicInteger m_allowances;

        public Allocator(int allowances, int defaultBufferSize) {
            Preconditions.checkArgument(allowances > 0, "allowances must be greater than zero");
            m_tracker = new SizeTracker(defaultBufferSize);
            m_defaultBufferSize = defaultBufferSize;
            m_allowances = new AtomicInteger(allowances);
        }
        /**
         * if the requested allocation size is bellow default size, it attempts to allocate
         * from pooled direct buffer container if allowances permit it. Otherwise it uses
         * the last ten allocations to determine the allocation size, and allocate it directly
         * from memory
         * @param allocationSize requested allocation size
         * @return a {@link BBContainer}
         */
        public BBContainer allocate(int allocationSize) {
            BBContainer container;
            Preconditions.checkArgument(allocationSize >= 0, "allocationSize must be greater than or equal to zero");
            if (allocationSize <= m_defaultBufferSize) {
                if (m_allowances.getAndDecrement() > 0) {
                    container = DBBPool.allocateDirectAndPool(m_defaultBufferSize);
                    container = new TrackedBBContainer(container);
                } else {
                    m_allowances.incrementAndGet();
                    container =  DBBPool.allocateUnsafeByteBuffer(m_defaultBufferSize);
                }
            } else {
                int lastTenMax = m_tracker.max();
                if (allocationSize < lastTenMax) {
                    allocationSize = lastTenMax;
                }
                container =  DBBPool.allocateUnsafeByteBuffer((int)(allocationSize*1.1));
            }
            return container;
        }
        /**
         * records a used allocation size
         * @param size
         */
        public void track(int size) {
            m_tracker.track(size);
        }
        /**
         * Delegate {@link BBContainer} class that is used to account for allowanced
         */
        private class TrackedBBContainer extends BBContainer {
            private final BBContainer m_delegate;
            TrackedBBContainer(BBContainer container) {
                super(container.b());
                m_delegate = container;
            }
            @Override
            public void discard() {
                checkDoubleFree();
                m_delegate.discard();
                Allocator.this.m_allowances.incrementAndGet();
            }
        }
    }

    /**
     * used to track the last ten byte buffer allocations
     */
    public static class SizeTracker {
        final private int sizes[] = new int[10];
        private long at = 0L;

        public SizeTracker(int defaultBufferSize) {
            for (int i=0; i < sizes.length; ++i) {
                sizes[i] = defaultBufferSize;
            }
        }

        public void track(int size) {
            int idx = (int)(at++ % sizes.length);
            sizes[idx] = size;
        }

        public int max() {
            int max = sizes[0];
            for (int i = 1; i < sizes.length; ++i) {
                if (sizes[i] > max) max = sizes[i];
            }
            return max;
        }
    }
}
