/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class SimpleFileSnapshotDataTarget implements SnapshotDataTarget {
    private final File m_tempFile;
    private final File m_file;
    private final FileChannel m_fc;
    private long m_bytesWritten = 0;
    private Runnable m_onCloseTask;
    private boolean m_needsFinalClose;

    /*
     * If a write fails then this snapshot is hosed.
     * Set the flag so all writes return immediately. The system still
     * needs to scan all the tables to clear the dirty bits
     * so the process continues as if the writes are succeeding.
     * A more efficient failure mode would do the scan but not the
     * extra serialization work.
     */
    private volatile boolean m_writeFailed = false;
    private volatile Throwable m_writeException = null;

    public SimpleFileSnapshotDataTarget(
            File file, boolean needsFinalClose) throws IOException {
        m_file = file;
        m_tempFile = new File(m_file.getParentFile(), m_file.getName() + ".incomplete");
        RandomAccessFile ras = new RandomAccessFile(m_tempFile, "rw");
        m_fc = ras.getChannel();
        m_needsFinalClose = needsFinalClose;

        m_es = CoreUtils.getSingleThreadExecutor("Snapshot write thread for " + m_file);
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
                        while (data.b.hasRemaining()) {
                            int written = m_fc.write(data.b);
                            if (written > 0) {
                                m_bytesWritten += written;
                            }
                        }
                    } finally {
                        data.discard();
                    }
                } catch (Throwable t) {
                    m_writeException = t;
                    m_writeFailed = true;
                    throw Throwables.propagate(t);
                }
                return null;
            }
        });
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
            m_fc.force(false);
            m_fc.close();
            m_tempFile.renameTo(m_file);
        } finally {
            m_onCloseTask.run();
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
    public Throwable getLastWriteException() {
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
}
