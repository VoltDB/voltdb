/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.rejoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotTableTask;
import org.voltdb.VoltDB;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A stream snapshot target for sending snapshot data directly to a rejoining
 * partition.
 */
public class StreamSnapshotDataTarget extends StreamSnapshotBase
implements SnapshotDataTarget {
    private static final VoltLogger rejoinLog = new VoltLogger("JOIN");

    // schemas for all the tables on this partition
    private final Map<Integer, byte[]> m_schemas;
    // Mailbox used to transfer snapshot data
    private Mailbox m_mb;
    // HSId of the destination mailbox
    private final long m_destHSId;
    // tracks all the acks for in-flight buffers
    private final StreamSnapshotAckTracker m_ackTracker;
    // input and output threads
    private final StreamSnapshotAckReceiver m_in;
    private final Thread m_inThread;
    private final StreamSnapshotSender m_out;
    private final Thread m_outThread;
    // Skip all subsequent writes if one fails
    private boolean m_writeFailed = false;

    private int m_blockIndex = 0;
    private Runnable m_onCloseHandler = null;

    public StreamSnapshotDataTarget(long hsId, Map<Integer, byte[]> schemas)
    throws IOException {
        super();
        m_schemas = schemas;
        m_destHSId = hsId;
        m_mb = VoltDB.instance().getHostMessenger().createMailbox();

        m_ackTracker = new StreamSnapshotAckTracker(m_numBuffers);
        m_in = new StreamSnapshotAckReceiver(m_mb, m_ackTracker);
        m_inThread = new Thread(m_in);
        m_inThread.setDaemon(true);
        m_out = new StreamSnapshotSender(m_mb, m_destHSId);
        m_outThread = new Thread(m_out);

        m_inThread.start();
        m_outThread.start();
    }

    /**
     * Terminates the IO threads.
     */
    private void closeIO() {
        rejoinLog.debug("Closing stream snapshot target");

        // Send EOF
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) StreamSnapshotMessageType.END.ordinal());
        buf.flip();
        BBContainer container = DBBPool.wrapBB(buf);
        m_out.offer(container);

        // This chunk will terminate the sender
        m_out.offer(new BBContainer(null, 0) {
            @Override
            public void discard() {}
        });

        while (!m_writeFailed && m_ackTracker.hasOutstanding()) {
            Thread.yield();
        }
        m_in.close();
    }

    @Override
    public int getHeaderSize() {
        return contentOffset;
    }

    @Override
    public ListenableFuture<?> write(Callable<BBContainer> tupleData,
                                     SnapshotTableTask context) {
        assert(context != null);

        BBContainer chunk = null;
        try {
            chunk = tupleData.call();
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }

        if (m_writeFailed) {
            if (chunk != null) {
                chunk.discard();
            }
            return null;
        }
        if (!m_outThread.isAlive()) {
            if (chunk != null) {
                chunk.discard();
            }

            m_writeFailed = true;
            IOException e = new IOException("Trying to write snapshot data " +
                    "after the stream is closed");
            return Futures.immediateFailedFuture(e);
        }

        if (chunk != null) {
            // Have we seen this table before, if not, send schema
            if (m_schemas.containsKey(context.getTableId())) {
                // remove the schema once sent
                byte[] schema = m_schemas.remove(context.getTableId());
                rejoinLog.debug("Sending schema for table " + context.getTableId());
                sendSchema(schema);
            }

            chunk.b.put((byte) StreamSnapshotMessageType.DATA.ordinal());
            chunk.b.putInt(m_blockIndex++); // put chunk index
            chunk.b.putInt(context.getTableId()); // put table ID
            chunk.b.position(0);

            m_ackTracker.waitForAcks(m_blockIndex - 1, 1);
            m_out.offer(chunk);
        }

        return null;
    }

    @Override
    public synchronized void close() throws IOException, InterruptedException {
        /*
         * could be called multiple times, because all tables share one stream
         * target
         */
        if (m_mb != null) {
            closeIO();
            /*
             * only join the out thread, once the socket is closed, the in
             * thread will terminate
             */
            m_outThread.join();
            VoltDB.instance().getHostMessenger().removeMailbox(m_mb.getHSId());
            m_mb = null;
        }

        if (m_onCloseHandler != null) {
            m_onCloseHandler.run();
        }
    }

    @Override
    public long getBytesWritten() {
        return m_out.getBytesSent();
    }

    @Override
    public void setOnCloseHandler(Runnable onClose) {
        m_onCloseHandler = onClose;
    }

    @Override
    public Throwable getLastWriteException() {
        Throwable lastException = m_out.getLastException();
        if (lastException == null) {
            lastException = m_in.getLastException();
        }
        return lastException;
    }

    @Override
    public SnapshotFormat getFormat() {
        return SnapshotFormat.STREAM;
    }

    private void sendSchema(byte[] schema) {
        ByteBuffer buf = ByteBuffer.allocate(schema.length + 1); // 1 byte for the type
        buf.put((byte) StreamSnapshotMessageType.SCHEMA.ordinal());
        buf.put(schema);
        buf.flip();
        BBContainer container = DBBPool.wrapBB(buf);
        m_out.offer(container);
    }
}
