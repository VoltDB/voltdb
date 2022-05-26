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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Bits;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class SimpleFileSnapshotDataTarget implements SnapshotDataTarget {
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    private final File m_tempFile;
    private final File m_file;
    private final FileChannel m_fc;
    private final RandomAccessFile m_ras;
    private long m_bytesWritten = 0;
    private Runnable m_onCloseTask;
    private Runnable m_inProgress;
    private boolean m_needsFinalClose;

    /*
     * Remember to sync regularly. SimpleFileSnapshotDataTarget
     * takes a simpler approach to bounding the number of bytes synced
     * and keeping the disk working on a regular basis.
     *
     * The two roles are split, one thread syncs periodically to keep the disk busy
     * and the main thread writing will only stop on syncing if it has written
     * 256 megabytes past what the sync thread is doing
     *
     * This removes the messy coordination you see in DefaultSnapshotDataTarget
     * where the two threads use a semaphore to keep track of permits
     */
    private static final int m_bytesAllowedBeforeSync = (1024 * 1024) * 256;
    private AtomicInteger m_bytesSinceLastSync = new AtomicInteger(0);

    private final ScheduledFuture<?> m_syncTask;

    /*
     * If a write fails then this snapshot is hosed.
     * Set the flag so all writes return immediately. The system still
     * needs to scan all the tables to clear the dirty bits
     * so the process continues as if the writes are succeeding.
     * A more efficient failure mode would do the scan but not the
     * extra serialization work.
     */
    private volatile boolean m_writeFailed = false;
    private volatile Exception m_writeException = null;
    private volatile IOException m_reportedSerializationFailure = null;

    public SimpleFileSnapshotDataTarget(
            File file, boolean needsFinalClose) throws IOException {
        m_file = file;
        m_tempFile = new File(m_file.getParentFile(), m_file.getName() + ".incomplete");
        m_ras = new RandomAccessFile(m_tempFile, "rw");
        m_fc = m_ras.getChannel();
        m_needsFinalClose = needsFinalClose;

        m_es = CoreUtils.getListeningSingleThreadExecutor("Snapshot write thread for " + m_file);
        ScheduledFuture<?> syncTask = null;
        syncTask = DefaultSnapshotDataTarget.m_syncService.scheduleAtFixedRate(new Runnable() {
            private long syncedBytes = 0;
            @Override
            public void run() {
                //Only sync for at least 4 megabyte of data, enough to amortize the cost of seeking
                //on ye olden platters. Since we are appending to a file it's actually 2 seeks.
                while (m_bytesSinceLastSync.get() > 1024 * 1024 * 4) {
                    try {
                        final long syncStart = syncedBytes;
                        syncedBytes =  Bits.sync_file_range(SNAP_LOG, m_ras.getFD(), m_fc, syncStart, m_fc.position());
                    } catch (IOException e) {
                        if (!(e instanceof java.nio.channels.AsynchronousCloseException )) {
                            SNAP_LOG.error("Error syncing snapshot", e);
                        } else {
                            SNAP_LOG.debug("Asynchronous close syncing snapshot data, presumably graceful", e);
                        }
                    }
                    //Blind setting to 0 means we could technically write more than
                    //256 megabytes at a time but 512 is the worst case and that is fine
                    m_bytesSinceLastSync.set(0);
                }
            }
        }, DefaultSnapshotDataTarget.SNAPSHOT_SYNC_FREQUENCY, DefaultSnapshotDataTarget.SNAPSHOT_SYNC_FREQUENCY, TimeUnit.MILLISECONDS);
        m_syncTask = syncTask;
    }

    private final ListeningExecutorService m_es;

    @Override
    public int getHeaderSize() {
        return 0;
    }

    @Override
    public ListenableFuture<?> write(final Callable<BBContainer> tupleData, int tableId) {
        final ListenableFuture<BBContainer> computedData = VoltDB.instance().getComputationService().submit(tupleData);

        return m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    final BBContainer data = computedData.get();
                    /*
                     * If a filter nulled out the buffer do nothing.
                     */
                    if (data == null) return null;
                    if (m_writeFailed) {
                        data.discard();
                        return null;
                    }
                    try {
                        int totalWritten = 0;
                        final ByteBuffer dataBuf = data.b();

                        //Do not enforce rate limiter if nothing to be written
                        if (dataBuf.hasRemaining()) {
                            DefaultSnapshotDataTarget.enforceSnapshotRateLimit(dataBuf.remaining());
                        }

                        while (dataBuf.hasRemaining()) {
                            int written = m_fc.write(dataBuf);
                            if (written > 0) {
                                m_bytesWritten += written;
                                totalWritten += written;
                            }
                        }
                        if (m_bytesSinceLastSync.addAndGet(totalWritten) > m_bytesAllowedBeforeSync) {
                            m_fc.force(false);
                            m_bytesSinceLastSync.set(0);
                        }
                    } finally {
                        data.discard();
                    }
                } catch (InterruptedException | ExecutionException | IOException e) {
                    m_writeException = e;
                    m_writeFailed = true;
                    throw e;
                }
                return null;
            }
        });
    }

    @Override
    public void reportSerializationFailure(IOException ex) {
        m_reportedSerializationFailure = ex;
    }

    @Override
    public Exception getSerializationException() {
        return m_reportedSerializationFailure;
    }

    @Override
    public boolean needsFinalClose()
    {
        return m_needsFinalClose;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try {
            m_es.shutdown();
            m_es.awaitTermination(356, TimeUnit.DAYS);
            m_syncTask.cancel(false);
            m_fc.force(false);
            m_fc.close();
            m_tempFile.renameTo(m_file);
        } finally {
            m_onCloseTask.run();
        }
        if (m_reportedSerializationFailure != null) {
            // There was an error reported by the EE during serialization
            throw m_reportedSerializationFailure;
        }
   }

    @Override
    public long getBytesWritten() {
        return m_bytesWritten;
    }

    @Override
    public void setOnCloseHandler(Runnable onClose) {
        m_onCloseTask = onClose;
    }

    @Override
    public Exception getLastWriteException() {
        return m_writeException;
    }

    @Override
    public SnapshotFormat getFormat() {
        return SnapshotFormat.CSV;
    }

    @Override
    public String toString() {
        return m_file.toString();
    }

    /**
     * Get the row count if any, of the content wrapped in the given {@link BBContainer}
     * @param tupleData
     * @return the numbers of tuple data rows contained within a container
     */
    @Override
    public int getInContainerRowCount(BBContainer tupleData) {
        return SnapshotDataTarget.ROW_COUNT_UNSUPPORTED;
    }

    public void setInProgressHandler(Runnable inProgress) {
        m_inProgress = inProgress;
    }

    public void trackProgress() {
        m_inProgress.run();
    }
}
