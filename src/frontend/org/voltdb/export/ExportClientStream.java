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
public class ExportClientStream implements InputHandler {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    /** the network read/write streams */
    private Connection m_cxn = null;

    /** name of the associated export string (generationid-partitionid-signature). */
    private final String m_streamName;

    /** the export data */
    private final StreamBlockQueue m_sbq;

    /** Runnable that flushes sbq to the write stream */
    private final ExportClientStream.Writer m_writer;

    class Writer implements Runnable {
        @Override
        public void run() {
            StreamBlock sb = m_sbq.poll();
            if (sb != null) {
                exportLog.info("Advertisement: " + m_streamName +
                    " enqueuing: " + sb.block().b.remaining() +
                    " bytes to stream from uso: " + sb.uso() +
                    " container: " + sb.block());
                m_cxn.writeStream().enqueue(sb.block());
            }
        }
    }

    /** Write monitor that pushes a writer runnable when stream is empty */
    class WriteMonitor implements QueueMonitor {
        private AtomicInteger counter = new AtomicInteger(0);
        @Override
        public boolean queue(int bytes) {
            int queued = counter.addAndGet(bytes);
            if (queued == 0) {
                // network drained the connection. push more data.
                m_cxn.scheduleRunnable(ExportClientStream.this.m_writer);
            }
            // never request artificial back-pressure
            return false;
        }
    }

    /** Create an input handler for a given StreamBlockQueue */
    public ExportClientStream(String streamname, StreamBlockQueue sbq) {
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
    public QueueMonitor writestreamMonitor() {
        return this.new WriteMonitor();
    }

    @Override
    public long connectionId() {
        if (m_cxn != null) {
            return m_cxn.connectionId();
        }
        return 0;
    }




}
