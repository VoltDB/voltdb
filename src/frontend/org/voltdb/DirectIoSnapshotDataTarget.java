/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import static org.voltcore.utils.Bits.roundupToPage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import org.voltcore.utils.Bits;
import org.voltcore.utils.DBBPool;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.DirectIoFileChannel;
import org.voltdb.utils.VoltSnapshotFile;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

/**
 * {@link SnapshotDataTarget} implementation which writes a native snapshot to a file using direct IO.
 * <p>
 * Direct IO is when the user application sends pages directly to a device to be written avoiding the OS page cache. The
 * advantage of this approach is that writes happen immediately and avoid the OS IO scheduler.
 * <p>
 * The biggest disadvantages of direct IO with a write only work load is that data needs to be written in multiples of
 * page size and the memory buffer being used for the write also needs to be page aligned, so normally allocated memory
 * cannot be used.
 */
class DirectIoSnapshotDataTarget extends NativeSnapshotDataTarget {
    private static final int s_pageSize = Bits.pageSize();
    /** Used by {@link CompressTask} to perform compression of table blocks and {@link #flushCurrentWrite()} */
    private static final ExecutorService s_compressionExecutor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, DirectIoSnapshotDataTarget.class.getSimpleName() + "-compressor"));

    /** Used by {@link #writeOutstandingBuffers() } to write buffers to the file target */
    private static final ScheduledExecutorService s_writeExecutor = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, DirectIoSnapshotDataTarget.class.getSimpleName() + "-writer"));

    static final int s_forceWriteOutstandingCount = Integer.getInteger("SNAPSHOT_FORCE_WRITE_BUFFER_COUNT", 4);

    private final MemoryPool m_pool;
    private final BlockingQueue<OutstandingWrite> m_outstandingWrites = new LinkedBlockingQueue<>();
    private final Path m_path;
    private final FileChannel m_channel;
    // Keep a cache of the first page around to write the complete bit. This is necessary because there is no page cache
    private final DBBPool.BBContainer m_firstPage;
    // Buffer to reuse as the target of compression
    private DBBPool.BBContainer m_compressedBuffer;
    // Current outstanding write that is being populated by CompressTask
    private OutstandingWrite m_currentWrite;
    private volatile Exception m_writeException;

    // Only can be modified by CompressTask
    int m_writesQueuedSinceListSubmit = 0;

    private volatile long m_bytesWritten;

    /**
     * Utility method for testing if directIO is supported in {@code directory}
     *
     * @param directory being tested
     * @return {@code true} if {@code directory} can have direct IO files written inside of it
     */
    public static boolean directIoSupported(String directory) {
        return DirectIoFileChannel.supported(new VoltSnapshotFile(directory).toPath());
    }

    static DBBPool.BBContainer allocateContainer(int minSize) {
        return DBBPool.allocateAlignedUnsafeByteBuffer(s_pageSize, roundupToPage(minSize));
    }

    /**
     * Create a new factory for instances of this class
     *
     * @param directory     where snapshot files will be written
     * @param hostId        of this host
     * @param clusterName   for this cluster
     * @param databaseName  for this database
     * @param numPartitions in the cluster
     * @param partitions    list of partitions on this host
     * @param txnId         of snapshot
     * @param timestamp     of snapshot
     * @return {@link Factory} to create {@link DirectIoSnapshotDataTarget} instances
     */
    public static Factory factory(String directory, int hostId, final String clusterName, final String databaseName,
            int numPartitions, List<Integer> partitions, long txnId, long timestamp, int[] version, boolean isTruncationSnapshot,
            UnaryOperator<FileChannel> channelOperator) {
        DirectIoSnapshotDataTarget.MemoryPool pool = new DirectIoSnapshotDataTarget.MemoryPool();
        return (fileName, tableName, isReplicated, schema) -> {
            Path p = isTruncationSnapshot ? new File(directory, fileName).toPath() : new VoltSnapshotFile(directory, fileName).toPath();
            return new DirectIoSnapshotDataTarget(pool,
                    new VoltSnapshotFile(directory, fileName).toPath(), hostId, clusterName, databaseName, tableName, numPartitions,
                    isReplicated, partitions, schema, txnId, timestamp, version, isTruncationSnapshot, channelOperator);
        };
    }

    private DirectIoSnapshotDataTarget(MemoryPool pool, Path path, int hostId, String clusterName, String databaseName,
            String tableName, int numPartitions, boolean isReplicated, List<Integer> partitionIds, byte[] schemaBytes,
            long txnId, long timestamp, int[] version, boolean isTruncationSnapshot, UnaryOperator<FileChannel> channelOperator)
            throws IOException {
        super(isReplicated);
        m_pool = pool.reference();
        m_path = path;
        m_channel = channelOperator.apply(DirectIoFileChannel.open(path));

        m_firstPage = serializeHeader(DirectIoSnapshotDataTarget::allocateContainer, hostId, clusterName, databaseName,
                tableName, numPartitions, isReplicated, partitionIds, schemaBytes, txnId, timestamp, version);

        ByteBuffer firstBuffer = m_firstPage.b();

        // Setup the position and limit to cover the unused portion of the buffer
        firstBuffer.position(firstBuffer.limit());
        firstBuffer.limit(firstBuffer.capacity());

        // Don't discard since m_firstPage is kept around until close
        m_currentWrite = new OutstandingWrite(m_firstPage, false);
    }

    @Override
    public ListenableFuture<?> write(Callable<DBBPool.BBContainer> tupleData, int tableId) {
        try {
            DBBPool.BBContainer container = tupleData.call();
            if (m_writeException != null) {
                container.discard();
                return Futures.immediateFailedFuture(m_writeException);
            }
            CompressTask task = new CompressTask(container);
            s_compressionExecutor.execute(task);
            return task.m_future;
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try {
            // Make sure that anything submitted to the compressor has been processed
            int subtractFromFileSize = s_compressionExecutor.submit(this::flushCurrentWrite).get();

            // Make sure any remaining writes have been performed
            s_writeExecutor.submit(this::writeOutstandingBuffers).get();

            if (m_writeException == null && m_reportedSerializationFailure == null) {
                // The last write will round up to page size so the final size of the file needs to be reduced
                long actualSize = m_channel.size() - subtractFromFileSize;

                // Update first page with complete bit and write it out
                ByteBuffer firstPage = m_firstPage.b();
                firstPage.rewind();
                firstPage.put(s_offsetComplete, (byte) 1);
                firstPage.limit(s_pageSize);
                m_channel.write(firstPage, 0);

                m_channel.truncate(actualSize);
            }

            // One last flush before close
            m_channel.force(true);
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), IOException.class);
            throw new RuntimeException(e);
        } finally {
            if (m_compressedBuffer != null) {
                m_compressedBuffer.discard();
            }
            m_firstPage.discard();
            m_pool.dereference();

            m_channel.close();
        }
        postClose();
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
        return getClass().getCanonicalName() + ' ' + m_path;
    }

    private void writeOutstandingBuffers() {
        int outstanding = m_outstandingWrites.size();
        if (outstanding == 0) {
            return;
        }

        // Allocate outstanding plus a little more so we hopefully do not have to resize the array
        List<OutstandingWrite> writes = new ArrayList<>(outstanding + 5);
        try {
            if (m_writeException != null) {
                writes.forEach(ow -> ow.error(m_writeException));
                return;
            }

            m_outstandingWrites.drainTo(writes);

            ByteBuffer[] buffers = new ByteBuffer[writes.size()];
            long toWrite = 0;
            for (int i = 0; i < buffers.length; ++i) {
                OutstandingWrite ow = writes.get(i);
                ByteBuffer buffer = ow.m_container.b();
                int remaining = buffer.remaining();

                toWrite += remaining;
                buffers[i] = buffer;
            }

            long written = 0;
            do {
                written += m_channel.write(buffers, 0, buffers.length);
            } while (written < toWrite);

            assert written == toWrite : "Wrote more data than expected: " + written + " vs " + toWrite;

            m_bytesWritten += toWrite;
        } catch (IOException e) {
            m_writeException = e;
            writes.forEach(ow -> ow.error(e));
        } catch (Throwable e) {
            writes.forEach(ow -> ow.error(e));
        } finally {
            writes.forEach(OutstandingWrite::discard);
        }
    }

    /**
     * If {@link #m_currentWrite} is not null round the buffer up to page size and enqueue the write
     *
     * @return the number of bytes beyond the end of data which was write to stay page aligned
     */
    private int flushCurrentWrite() {
        int roundedUpSize = 0;
        OutstandingWrite currentWrite = m_currentWrite;
        m_currentWrite = null;
        if (currentWrite != null) {
            ByteBuffer current = currentWrite.m_container.b();
            current.flip();
            int originalLimit = current.limit();
            int roundedLimit = roundupToPage(originalLimit);
            roundedUpSize = roundedLimit - originalLimit;
            current.limit(roundedLimit);
            m_outstandingWrites.add(currentWrite);
        }

        return roundedUpSize;
    }

    /**
     * Utility class for compressing a table block and adding the header to the compressed into the current active page
     * aligned memory
     */
    private final class CompressTask implements Runnable {
        // Uncompressed data has a partitionID prefix which doesn't get compressed
        private static final int s_uncompressedHeaderSize = Integer.BYTES;
        private static final int s_compressedHeaderSize = s_uncompressedHeaderSize + Integer.BYTES * 2;

        final DBBPool.BBContainer m_container;
        final SettableFuture<?> m_future = SettableFuture.create();

        CompressTask(DBBPool.BBContainer container) {
            m_container = container;
        }

        @Override
        public void run() {
            try {
                if (m_writeException != null) {
                    m_future.setException(m_writeException);
                    return;
                }

                DBBPool.BBContainer compressedContainer = m_compressedBuffer;
                if (compressedContainer == null) {
                    m_compressedBuffer = compressedContainer = DBBPool
                            .allocateDirectAndPool(SnapshotSiteProcessor.m_snapshotBufferCompressedLen);
                }

                // Set up buffers to start reading and writing after the respective headers
                ByteBuffer compressed = compressedContainer.b();
                compressed.position(s_compressedHeaderSize);

                ByteBuffer uncompressed = m_container.b();
                int uncompressedOriginalPosition = uncompressed.position();
                uncompressed.position(uncompressedOriginalPosition + s_uncompressedHeaderSize);

                int compressedSize = CompressionService.compressAndCRC32cBuffer(uncompressed, compressedContainer);

                // Fill in the header for compressed block
                compressed.rewind();
                compressed.putInt(compressedSize);
                // Copy the partitionID from the uncompressed buffer
                compressed.putInt(uncompressed.getInt(uncompressedOriginalPosition));

                ByteBuffer toCrc = compressed.asReadOnlyBuffer();
                toCrc.flip();
                int crc = DBBPool.getCRC32C(compressedContainer.address(), 0, Integer.BYTES * 2);
                compressed.putInt(crc);

                // Rewind to fill in the current write buffer
                compressed.rewind();

                // Keep copying the data from compressed buffer to page aligned buffer until it is all copied
                OutstandingWrite currentWrite = m_currentWrite;
                do {
                    if (currentWrite == null) {
                        m_currentWrite = currentWrite = new OutstandingWrite(m_pool.get(), true);
                    }
                    ByteBuffer writeBuffer = currentWrite.m_container.b();

                    int originalLimit = compressed.limit();

                    if (writeBuffer.remaining() < compressed.remaining()) {
                        // Not enough space in current buffer so reduce the limit in the compressed buffer
                        compressed.limit(compressed.position() + writeBuffer.remaining());
                    }

                    // Move bytes from compressed buffer to page aligned write buffer
                    writeBuffer.put(compressed);

                    compressed.limit(originalLimit);

                    // Read all of the snapshot data push the future into the outstanding write
                    if (!compressed.hasRemaining()) {
                        currentWrite.addFuture(m_future);
                    }

                    // Filled up the page so send it off to the write thread
                    if (!writeBuffer.hasRemaining()) {
                        writeBuffer.flip();

                        // Performing the rate limiting here because this is the easiest place to do it
                        enforceSnapshotRateLimit(writeBuffer.remaining());

                        m_outstandingWrites.add(currentWrite);
                        m_currentWrite = currentWrite = null;

                        // If there are enough outstanding writes force a write
                        if (++m_writesQueuedSinceListSubmit >= s_forceWriteOutstandingCount) {
                            s_writeExecutor.execute(DirectIoSnapshotDataTarget.this::writeOutstandingBuffers);
                            m_writesQueuedSinceListSubmit = 0;
                        }
                    }
                } while (compressed.hasRemaining());
            } catch (IOException e) {
                m_writeException = e;
                m_future.setException(e);
            } catch (Throwable t) {
                m_future.setException(t);
            } finally {
                m_container.discard();
            }
        }
    }

    /**
     * Simple struct to for an outstanding block to write and all of the associated futures
     */
    private final static class OutstandingWrite {
        final boolean m_discard;
        final DBBPool.BBContainer m_container;
        final List<SettableFuture<?>> m_futures = new ArrayList<>();

        OutstandingWrite(DBBPool.BBContainer container, boolean discard) {
            m_discard = discard;
            m_container = container;
        }

        void addFuture(SettableFuture<?> future) {
            m_futures.add(future);
        }

        void error(Throwable t) {
            for (SettableFuture<?> future : m_futures) {
                future.setException(t);
            }
        }

        void discard() {
            for (SettableFuture<?> future : m_futures) {
                future.set(null);
            }
            if (m_discard) {
                m_container.discard();
            }
        }
    }

    /**
     * Simple memory pool to be shared by the different targets during a snapshot. All buffers will be discarded once
     * the reference count hits 0
     */
    private static final class MemoryPool {
        private final AtomicInteger m_refCounts = new AtomicInteger();
        private final Queue<DBBPool.BBContainer> m_containers = new LinkedTransferQueue<>();

        MemoryPool reference() {
            m_refCounts.getAndIncrement();
            return this;
        }

        void dereference() {
            if (m_refCounts.decrementAndGet() == 0) {
                DBBPool.BBContainer cont;
                while ((cont = m_containers.poll()) != null) {
                    cont.discard();
                }
            }
        }

        DBBPool.BBContainer get() {
            DBBPool.BBContainer cont = m_containers.poll();
            if (cont == null) {
                cont = allocateContainer(SnapshotSiteProcessor.m_snapshotBufferLength);
            }

            return new DBBPool.DBBDelegateContainer(cont) {
                @Override
                public void discard() {
                    if (m_refCounts.get() == 0) {
                        super.discard();
                    } else {
                        checkDoubleFree();
                        m_delegate.b().clear();
                        m_containers.add(m_delegate);
                    }
                };
            };
        }
    }
}
