/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltcore.network;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;

import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;

public enum CipherExecutor {

    SERVER(getWishedThreadCount()),
    CLIENT(2);

    public final static int PAGE_SHIFT = 14; // 16384 (max TLS fragment)
    public final static int PAGE_SIZE = 1 << PAGE_SHIFT;

    volatile ListeningExecutorService m_es = null;
    AtomicBoolean m_active = new AtomicBoolean(false);
    final int m_threadCount;

    private CipherExecutor(int nthreads) {
        m_threadCount = nthreads;
        m_es = null;
    }

    private static final int getWishedThreadCount() {
        Runtime rt = null;
        try {
            rt = Runtime.getRuntime();
        } catch (Throwable t) {
            rt = null;
        }
        int coreCount = rt != null ? rt.availableProcessors() : 2;
        return Math.max(2, coreCount/2);
    }

    public ListeningExecutorService getES() {
        if (m_es == null) {
            synchronized (this) {
                if (!m_active.get()) {
                    throw new IllegalStateException(name() + " cipher is not active");
                }
                if (m_es == null) {
                    ThreadFactory thrdfct = CoreUtils.getThreadFactory(
                            name () + " SSL cipher service", CoreUtils.MEDIUM_STACK_SIZE);
                    m_es = MoreExecutors.listeningDecorator(
                            Executors.newFixedThreadPool(
                                    m_threadCount,
                                    thrdfct));
                }
            }
        }
        return m_es;
    }

    public void startup() {
        if (m_active.compareAndSet(false, true)) {
            getES();
        }
    }

    public boolean isActive() {
        return m_active.get();
    }

    public void shutdown() {
        if (m_active.compareAndSet(true, false)) {
            synchronized (this) {
                m_es.shutdown();
                try {
                    m_es.awaitTermination(365, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(
                            "Interruped while waiting for " + name() + " cipher service shutdown",e);
                }
                m_es = null;
            }
        }
    }

    public io.netty_voltpatches.buffer.PooledByteBufAllocator allocator() {
        if (!m_active.get()) {
            throw new IllegalStateException(name() + " cipher is not active");
        }
        switch (this) {
        case CLIENT:
            return ClientPoolHolder.INSTANCE;
        case SERVER:
            return ServerPoolHolder.INSTANCE;
        default:
            return /* impossible */ null;
        }
    }

    public final static int framesFor(int size) {
        int pages = (size >> PAGE_SHIFT);
        int modulo = size & (PAGE_SIZE - 1);
        return modulo > 0 ? pages+1 : pages;
    }

    private static class ClientPoolHolder {
        static final io.netty_voltpatches.buffer.PooledByteBufAllocator INSTANCE =
                new io.netty_voltpatches.buffer.PooledByteBufAllocator(
                        true,
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultNumHeapArena(),
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultNumDirectArena(),
                        PAGE_SIZE, /* page size */
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultMaxOrder(),
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultTinyCacheSize(),
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultSmallCacheSize(),
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultNormalCacheSize());
    }

    private static class ServerPoolHolder {
        static final io.netty_voltpatches.buffer.PooledByteBufAllocator INSTANCE =
                new io.netty_voltpatches.buffer.PooledByteBufAllocator(
                        true,
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultNumHeapArena(),
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultNumDirectArena(),
                        PAGE_SIZE, /* page size */
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultMaxOrder(),
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultTinyCacheSize(),
                        io.netty_voltpatches.buffer.PooledByteBufAllocator.defaultSmallCacheSize(),
                        512);
    }

    public static CipherExecutor valueOf(SSLEngine engn) {
        return engn.getUseClientMode() ? CLIENT : SERVER;
    }
}
