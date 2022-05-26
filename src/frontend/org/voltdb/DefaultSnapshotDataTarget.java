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

package org.voltdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.voltcore.logging.Level;
import org.voltcore.utils.Bits;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.PosixAdvise;

import com.google_voltpatches.common.util.concurrent.Callables;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.ListeningScheduledExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;


public class DefaultSnapshotDataTarget extends NativeSnapshotDataTarget {
    private final File m_file;
    private final FileChannel m_channel;
    private final FileOutputStream m_fos;
    private final RateLimitedLogger m_syncServiceLogger =  new RateLimitedLogger(TimeUnit.MINUTES.toNanos(1), SNAP_LOG, Level.ERROR);

    /*
     * If a write fails then this snapshot is hosed.
     * Set the flag so all writes return immediately. The system still
     * needs to scan all the tables to clear the dirty bits
     * so the process continues as if the writes are succeeding.
     * A more efficient failure mode would do the scan but not the
     * extra serialization work.
     */
    private volatile boolean m_writeFailed = false;
    private volatile IOException m_writeException = null;

    private volatile long m_bytesWritten = 0;

    /**
     * Ideally this number should be equal or more than
     * 2MB * (# of persistent tables + # of materialized views),
     * 2MB is the maximum snapshot buffer size.
     * If this number is set too low, database will sync frequently, results in longer snapshot generation time.
     */
    private static final int s_maxPermit = Integer.getInteger("SNAPSHOT_MEGABYTES_ALLOWED_BEFORE_SYNC", 256);
    private static final Semaphore s_bytesAllowedBeforeSync = new Semaphore((1024 * 1024) * s_maxPermit);
    private final AtomicInteger m_bytesWrittenSinceLastSync = new AtomicInteger(0);

    private final ScheduledFuture<?> m_syncTask;

    private final AtomicInteger m_outstandingWriteTasks = new AtomicInteger(0);
    private final ReentrantLock m_outstandingWriteTasksLock = new ReentrantLock();
    private final Condition m_noMoreOutstandingWriteTasksCondition =
            m_outstandingWriteTasksLock.newCondition();

    private static final ListeningExecutorService m_es = CoreUtils.getListeningSingleThreadExecutor("Snapshot write service ");
    static final ListeningScheduledExecutorService m_syncService = MoreExecutors.listeningDecorator(
            Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("Snapshot sync service")));

    public DefaultSnapshotDataTarget(
            final File file,
            final int hostId,
            final String clusterName,
            final String databaseName,
            final String tableName,
            final int numPartitions,
            final boolean isReplicated,
            final List<Integer> partitionIds,
            final byte[] schemaBytes,
            final long txnId,
            final long timestamp,
            int version[],
            boolean isTruncationSnapshot,
            UnaryOperator<FileChannel> channelOperator
            ) throws IOException {
        super(isReplicated);

        m_file = file;
        m_fos = new FileOutputStream(file);
        m_channel = channelOperator.apply(m_fos.getChannel());

        BBContainer container = serializeHeader(DBBPool::allocateDirect, hostId, clusterName, databaseName, tableName,
                numPartitions, isReplicated, partitionIds, schemaBytes, txnId, timestamp, version);

        /*
         * Be completely sure the write succeeded. If it didn't
         * the disk is probably full or the path is bunk etc.
         */
        ListenableFuture<?> writeFuture = write(Callables.returning(container), false);
        try {
            writeFuture.get();
        } catch (InterruptedException e) {
            m_fos.close();
            throw new java.io.InterruptedIOException();
        } catch (ExecutionException e) {
            m_fos.close();
            throw m_writeException;
        }

        ScheduledFuture<?> syncTask = null;
        syncTask = m_syncService.scheduleAtFixedRate(new Runnable() {
            private long fadvisedBytes = 0;
            private long syncedBytes = 0;
            @Override
            public void run() {
                /*
                 * Only sync for at least 4 megabyte of data, enough to amortize the cost of seeking
                 * on ye olden platters. Since we are appending to a file it's actually 2 seeks.
                 *
                 * Sync for at least single page size (4K, thus more frequently) if bytes allowed to
                 * write is running low.
                 */
                while (m_bytesWrittenSinceLastSync.get() > (1024 * 1024 * 4) ||
                        (m_bytesWrittenSinceLastSync.get() > Bits.pageSize() &&
                                s_bytesAllowedBeforeSync.availablePermits() < SnapshotSiteProcessor.m_snapshotBufferLength)) {
                    long positionAtSync = 0;
                    try {
                        positionAtSync = m_channel.position();
                        final long syncStart = syncedBytes;
                        syncedBytes = Bits.sync_file_range(SNAP_LOG, m_fos.getFD(), m_channel, syncStart, positionAtSync);
                    } catch (IOException e) {
                        if (!(e instanceof java.nio.channels.AsynchronousCloseException )) {
                            m_syncServiceLogger.log(System.nanoTime(), Level.ERROR, e,
                                    "Error syncing snapshot " + m_file +
                                    ". This message is rate limited to once every one minute.");
                        } else {
                            SNAP_LOG.info("Asynchronous close syncing snasphot data, presumably graceful", e);
                        }
                    } catch (Throwable t) {
                        m_syncServiceLogger.log(System.nanoTime(), Level.ERROR, t,
                                "Unexpected error while fsyncing snapshot data to file " + m_file +
                                ". This message is rate limited to once every one minute.");
                    } finally {
                        final int bytesSinceLastSync = m_bytesWrittenSinceLastSync.getAndSet(0);
                        s_bytesAllowedBeforeSync.release(bytesSinceLastSync);
                    }

                    /*
                     * Don't pollute the page cache with snapshot data, use fadvise
                     * to periodically request the kernel drop pages we have written
                     */
                    try {
                        if (positionAtSync - fadvisedBytes > SNAPSHOT_FADVISE_BYTES) {
                            //Get aligned start and end position
                            final long fadviseStart = fadvisedBytes;
                            //-1 because we don't want to drop the last page because
                            //we might modify it while appending
                            fadvisedBytes = ((positionAtSync / Bits.pageSize()) - 1) * Bits.pageSize();
                            final long retval = PosixAdvise.fadvise(
                                    m_fos.getFD(),
                                    fadviseStart,
                                    fadvisedBytes - fadviseStart,
                                    PosixAdvise.POSIX_FADV_DONTNEED );
                            if (retval != 0) {
                                SNAP_LOG.error("Error fadvising snapshot data: " + retval);
                                SNAP_LOG.error(
                                        "Params offset " + fadviseStart +
                                        " length " + (fadvisedBytes - fadviseStart));
                            }
                        }
                    } catch (Throwable t) {
                        m_syncServiceLogger.log(System.nanoTime(), Level.ERROR, t,
                                "Error fadvising snapshot data." +
                                " This message is rate limited to once every one minute.");
                    }
                }
            }
        }, SNAPSHOT_SYNC_FREQUENCY, SNAPSHOT_SYNC_FREQUENCY, TimeUnit.MILLISECONDS);
        m_syncTask = syncTask;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try {
            m_outstandingWriteTasksLock.lock();
            try {
                while (m_outstandingWriteTasks.get() > 0) {
                    m_noMoreOutstandingWriteTasksCondition.await();
                }
            } finally {
                m_outstandingWriteTasksLock.unlock();
            }
            m_syncTask.cancel(false);
            ListenableFuture<?> task = m_syncService.submit(new Runnable() {
                @Override
                public void run() {
                    // Empty task to wait on 'cancel' above, since m_syncTask.get()
                    // will immediately throw a CancellationException
                }
            });
            try {
                task.get();
            } catch (ExecutionException e) {
                SNAP_LOG.error("Error waiting on snapshot sync task cancellation", e);
            }
            m_channel.force(false);

            m_channel.position(8);
            if (!m_writeFailed && m_reportedSerializationFailure == null) {
                ByteBuffer completed = ByteBuffer.allocate(1);
                completed.put((byte) 1).flip();
                m_channel.write(completed);
            }

            m_channel.force(false);
        } finally {
            s_bytesAllowedBeforeSync.release(m_bytesWrittenSinceLastSync.getAndSet(0));
            m_channel.close();
        }

        postClose();
    }

    /*
     * Prepend length is basically synonymous with writing actual tuple data and not
     * the header.
     */
    private ListenableFuture<?> write(final Callable<BBContainer> tupleDataC, final boolean prependLength) {
        /*
         * Unwrap the data to be written. For the traditional
         * snapshot data target this should be a noop.
         */
        BBContainer tupleDataTemp;
        try {
            tupleDataTemp = tupleDataC.call();
            /*
             * Can be null if the dedupe filter nulled out the buffer
             */
            if (tupleDataTemp == null) {
                return Futures.immediateFuture(null);
            }
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
        final BBContainer tupleDataCont = tupleDataTemp;


        if (m_writeFailed) {
            tupleDataCont.discard();
            return null;
        }

        ByteBuffer tupleData = tupleDataCont.b();

        m_outstandingWriteTasks.incrementAndGet();

        Future<BBContainer> compressionTask = null;
        if (prependLength) {
            BBContainer cont =
                    DBBPool.allocateDirectAndPool(SnapshotSiteProcessor.m_snapshotBufferCompressedLen);
            //Skip 4-bytes so the partition ID is not compressed
            //That way if we detect a corruption we know what partition is bad
            tupleData.position(tupleData.position() + 4);
            /*
             * Leave 12 bytes, it's going to be a 4-byte length prefix, a 4-byte partition id,
             * and a 4-byte CRC32C of just the header bytes, in addition to the compressed payload CRC
             * that is 16 bytes, but 4 of those are done by CompressionService
             */
            cont.b().position(12);
            compressionTask = CompressionService.compressAndCRC32cBufferAsync(tupleData, cont);
        }
        final Future<BBContainer> compressionTaskFinal = compressionTask;

        ListenableFuture<?> writeTask = m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                int permitAcquired = 0;
                try {
                    final ByteBuffer tupleData = tupleDataCont.b();
                    int totalWritten = 0;
                    if (prependLength) {
                        BBContainer payloadContainer = compressionTaskFinal.get();
                        try {
                            final ByteBuffer payloadBuffer = payloadContainer.b();
                            payloadBuffer.position(0);

                            ByteBuffer lengthPrefix = ByteBuffer.allocate(12);
                            permitAcquired = payloadBuffer.remaining();
                            s_bytesAllowedBeforeSync.acquire(permitAcquired);
                            //Length prefix does not include 4 header items, just compressd payload
                            //that follows
                            lengthPrefix.putInt(payloadBuffer.remaining() - 16);//length prefix
                            lengthPrefix.putInt(tupleData.getInt(0)); // partitionId

                            /*
                             * Checksum the header and put it in the payload buffer
                             */
                            PureJavaCrc32C crc = new PureJavaCrc32C();
                            crc.update(lengthPrefix.array(), 0, 8);
                            lengthPrefix.putInt((int)crc.getValue());
                            lengthPrefix.flip();
                            payloadBuffer.put(lengthPrefix);
                            payloadBuffer.position(0);

                            enforceSnapshotRateLimit(payloadBuffer.remaining());

                            /*
                             * Write payload to file
                             */
                            while (payloadBuffer.hasRemaining()) {
                                totalWritten += m_channel.write(payloadBuffer);
                            }
                        } finally {
                            payloadContainer.discard();
                        }
                    } else {
                        permitAcquired = tupleData.remaining();
                        s_bytesAllowedBeforeSync.acquire(permitAcquired);
                        while (tupleData.hasRemaining()) {
                            totalWritten += m_channel.write(tupleData);
                        }
                    }
                    m_bytesWritten += totalWritten;
                    m_bytesWrittenSinceLastSync.addAndGet(totalWritten);
                } catch (IOException e) {
                    if (permitAcquired > 0) {
                        s_bytesAllowedBeforeSync.release(permitAcquired);
                    }
                    m_writeException = e;
                    SNAP_LOG.error("Error while attempting to write snapshot data to file " + m_file, e);
                    m_writeFailed = true;
                    throw e;
                } finally {
                    try {
                        tupleDataCont.discard();
                    } finally {
                        m_outstandingWriteTasksLock.lock();
                        try {
                            if (m_outstandingWriteTasks.decrementAndGet() == 0) {
                                m_noMoreOutstandingWriteTasksCondition.signalAll();
                            }
                        } finally {
                            m_outstandingWriteTasksLock.unlock();
                        }
                    }
                }
                return null;
            }
        });
        return writeTask;
    }

    @Override
    public ListenableFuture<?> write(final Callable<BBContainer> tupleData, int tableId) {
        return write(tupleData, true);
    }

    @Override
    public long getBytesWritten() {
        return m_bytesWritten;
    }

    @Override
    public Exception getLastWriteException() {
        return m_writeException;
    }

    @Override
    public String toString() {
        return m_file.toString();
    }
}
