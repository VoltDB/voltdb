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

package org.voltcore.network;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;

import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public enum CipherExecutor {

    SERVER(getWishedThreadCount()),
    CLIENT(2);

    public final static int FRAME_SHIFT = 14; // 16384 (max TLS fragment)
    public final static int FRAME_SIZE = 1 << FRAME_SHIFT;

    volatile ListeningExecutorService m_es;
    AtomicBoolean m_active = new AtomicBoolean(false);
    final int m_threadCount;

    private CipherExecutor(int nthreads) {
        m_threadCount = nthreads;
        m_es = CoreUtils.LISTENINGSAMETHREADEXECUTOR;
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

    /**
     * Guarantee execution of the given {@link Runnable} whether or not its
     * executor service is active. When it is not the {@link Runnable} is
     * executed in situ on the same thread that invokes this method
     *
     * @param r a {@link Runnable} task
     * @return a {@link ListenableFuture} for the given task
     */
    final public ListenableFuture<?> submit(Runnable r) {
        try {
            return m_es.submit(r);
        } catch (RejectedExecutionException e) {
            return CoreUtils.LISTENINGSAMETHREADEXECUTOR.submit(r);
        }
    }

    /**
     * Guarantee execution of the given {@link Callable&lt;T&gt;} whether or not its
     * executor service is active. When it is not the {@link Callable&lt;T&gt;} is
     * executed in situ on the same thread that invokes this method
     *
     * @param r a {@link Callable&lt;T&gt;} task
     * @return a {@link ListenableFuture&lt;T&gt;} for the given task
     */
    final public <T> ListenableFuture<T> submit(Callable<T> c) {
        try {
            return m_es.submit(c);
        } catch (RejectedExecutionException e) {
            return CoreUtils.LISTENINGSAMETHREADEXECUTOR.submit(c);
        }
    }

    public void startup() {
        if (m_active.compareAndSet(false, true)) {
            synchronized(this) {
                ThreadFactory thrdfct = CoreUtils.getThreadFactory(
                        name () + " SSL cipher service", CoreUtils.MEDIUM_STACK_SIZE);
                m_es = MoreExecutors.listeningDecorator(
                        Executors.newFixedThreadPool(m_threadCount, thrdfct));
            }
        }
    }

    public void shutdown() {
        if (m_active.compareAndSet(true, false)) {
            synchronized(this) {
                ListeningExecutorService es = m_es;
                if (es != CoreUtils.LISTENINGSAMETHREADEXECUTOR) {
                    m_es = CoreUtils.LISTENINGSAMETHREADEXECUTOR;
                    es.shutdown();
                    try {
                        es.awaitTermination(365, TimeUnit.DAYS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(
                                "Interrupted while waiting for " + name() + " cipher service shutdown",e);
                    }
                }
            }
        }
    }

    /*
     * To check for allocator leaks start your JVM with the following property set
     * -Dio.netty.leakDetectionLevel=PARANOID
     */
    public PooledByteBufAllocator allocator() {
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
        int pages = (size >> FRAME_SHIFT);
        int modulo = size & (FRAME_SIZE - 1);
        return modulo > 0 ? pages+1 : pages;
    }

    // Initialization on demand holder (JSR-133)
    private static class ClientPoolHolder {
        static final PooledByteBufAllocator INSTANCE =
                new PooledByteBufAllocator(
                        true,
                        PooledByteBufAllocator.defaultNumHeapArena(),
                        PooledByteBufAllocator.defaultNumDirectArena(),
                        FRAME_SIZE, /* page size */
                        PooledByteBufAllocator.defaultMaxOrder(),
                        PooledByteBufAllocator.defaultTinyCacheSize(),
                        PooledByteBufAllocator.defaultSmallCacheSize(),
                        PooledByteBufAllocator.defaultNormalCacheSize(),
                        PooledByteBufAllocator.defaultUseCacheForAllThreads());
    }

    // Initialization on demand holder (JSR-133)
    private static class ServerPoolHolder {
        static final PooledByteBufAllocator INSTANCE =
                new PooledByteBufAllocator(
                        true,
                        PooledByteBufAllocator.defaultNumHeapArena(),
                        PooledByteBufAllocator.defaultNumDirectArena(),
                        FRAME_SIZE, /* page size */
                        PooledByteBufAllocator.defaultMaxOrder(),
                        PooledByteBufAllocator.defaultTinyCacheSize(),
                        PooledByteBufAllocator.defaultSmallCacheSize(),
                        512,
                        PooledByteBufAllocator.defaultUseCacheForAllThreads());
    }

    public static CipherExecutor valueOf(SSLEngine engn) {
        return engn.getUseClientMode() ? CLIENT : SERVER;
    }

    private final static BigInteger LSB_MASK = new BigInteger(new byte[] {
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255 });

    /*
     * for debugging purposes
     */
    public static final UUID digest(ByteBuf buf, int offset) {
        if (offset < 0) {
            return null;
        }

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("failed to get instantiate MD5 digester", e);
        }
        md.reset();
        ByteBuf bb = buf.slice();
        if (buf.readableBytes() <= offset) {
            return null;
        }
        bb.readerIndex(bb.readerIndex() + offset);
        while (bb.isReadable()) {
            md.update(bb.readByte());
        }
        BigInteger bi = new BigInteger(1, md.digest());
        return new UUID(bi.shiftRight(64).longValue(), bi.and(LSB_MASK).longValue());
    }

    /*
     * for debugging purposes
     */
    public static final UUID digest(ByteBuffer buf, int offset) {
        if (offset < 0) {
            return null;
        }
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("failed to instantiate MD5 digester", e);
        }
        md.reset();
        ByteBuffer bb = null;
        if (!buf.hasRemaining() && buf.limit() > 0) {
            bb = ((ByteBuffer)buf.duplicate().flip());
        } else {
            bb = buf.slice();
        }
        if (bb.remaining() <= offset) {
            return null;
        }
        bb.position(bb.position() + offset);
        while (bb.hasRemaining()) {
            md.update(bb.get());
        }
        BigInteger bi = new BigInteger(1, md.digest());
        return new UUID(bi.shiftRight(64).longValue(), bi.and(LSB_MASK).longValue());
    }
}
