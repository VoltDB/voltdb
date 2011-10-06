/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.export;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.logging.VoltLogger;
import org.voltdb.network.Connection;
import org.voltdb.network.InputHandler;
import org.voltdb.network.QueueMonitor;

/**
 * Glue an export block queue to the network and push its contents
 * to the connected socket. No communication is required, or read,
 * from the client. This is a one way street.
 */
public class ExportStreamHandler implements InputHandler {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    /** the network read/write streams */
    private Connection m_cxn = null;

    /** name of the associated export string (generationid-partitionid-signature). */
    private final String m_streamName;

    /** the export data */
    private final StreamBlockQueue m_sbq;

    /** Current (approximate) write queue bytes */
    private final AtomicInteger counter = new AtomicInteger(0);

    /** Try to keep at least this many bytes in the NIO write stream */
    private final int kLowWaterMark = 1024 * 1024 * 2;

    /** Max bytes (approximately) allowed in the NIO write stream */
    private final int kHighWaterMark = 1024 * 1024 * 20;

    /** Controls the transfer of bytes from m_sbq to m_cxn */
    class Writer implements Runnable {
        @Override
        public void run() {
            StreamBlock sb = null;
            int goalBytes = kHighWaterMark - counter.get();
            int queuedBytes = 0;
            while ((goalBytes > queuedBytes) && (sb = m_sbq.poll()) != null) {
                if (exportLog.isTraceEnabled()) {
                    exportLog.trace("Advertisement: " + m_streamName +
                        " enqueuing: " + sb.block().b.remaining() +
                        " bytes to stream from uso: " + sb.uso() +
                        " container: " + sb.block() +
                        " goal bytes: " + goalBytes +
                        " queued bytes: " + queuedBytes);
                }
                queuedBytes += sb.block().b.remaining();
                m_cxn.writeStream().enqueue(sb.block());
            }
        }
    }

    /** The writer instance */
    private final ExportStreamHandler.Writer m_writer;

    /** Schedules m_writer when the queue falls below kLowWaterMark bytes */
    class WriteMonitor implements QueueMonitor {
        @Override
        public boolean queue(int bytes) {
            int queued = counter.addAndGet(bytes);
            if (queued < kLowWaterMark) {
                m_cxn.scheduleRunnable(ExportStreamHandler.this.m_writer);
            }
            // never request artificial back-pressure
            return false;
        }
    }

    /** Create an input handler for a given StreamBlockQueue */
    public ExportStreamHandler(String streamname, StreamBlockQueue sbq) {
        m_streamName = streamname;
        m_sbq = sbq;
        m_writer = this.new Writer();
    }

    @Override
    public String toString() {
        return "ExportClientStream(" + m_streamName + ")";
    }

    /**
     * The EE fills export with 2MB allocations
     */
    @Override
    public int getExpectedOutgoingMessageSize() {
        return 2 * 1024 * 1024;
    }

    /** Create and return an instance of WriteMonitor */
    @Override
    public QueueMonitor writestreamMonitor() {
        return this.new WriteMonitor();
    }

    /**
     * Never reads. We take the word "export" to heart here.
     */
    @Override
    public int getMaxRead() {
        return 0;
    }

    @Override
    public ByteBuffer retrieveNextMessage(Connection c) throws IOException {
        assert(false);
        return null;
    }

    @Override
    public void handleMessage(ByteBuffer message, Connection c) {
        assert(false);
    }

    /** VoltNetwork passes in the Connection here. */
    @Override
    public void starting(Connection c) {
        assert(m_cxn == null);
        m_cxn = c;
    }

    /** Prime StreamBlockQueue flushing pump */
    @Override
    public void started(Connection c) {
        m_cxn.scheduleRunnable(m_writer);
    }

    @Override
    public void stopping(Connection c) {
    }

    @Override
    public void stopped(Connection c) {
    }

    @Override
    public Runnable onBackPressure() {
        return null;
    }

    @Override
    public Runnable offBackPressure() {
        return null;
    }

    @Override
    public long connectionId() {
        if (m_cxn != null) {
            return m_cxn.connectionId();
        }
        return 0;
    }




}
