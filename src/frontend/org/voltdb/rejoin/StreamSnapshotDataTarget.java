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
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotTableTask;
import org.voltdb.VoltDB;
import org.voltdb.utils.CompressionService;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

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
    // input and output threads
    private final AckReceiver m_in;
    // Skip all subsequent writes if one fails
    private boolean m_writeFailed = false;

    private final Object m_compressionLock = new Object();
    final ByteBuffer m_compressionBuffer =
            ByteBuffer.allocateDirect(
                    CompressionService.maxCompressedLength(1024 * 1024 * 2 + (1024 * 256)));

    private final AtomicLong m_bytesSent = new AtomicLong();

    protected AtomicInteger m_outstandingWorkCount = new AtomicInteger(0);
    protected Map<Integer, SendWork> m_outstandingWork =
            Collections.synchronizedMap(new TreeMap<Integer, SendWork>());

    private int m_blockIndex = 0;
    private Runnable m_onCloseHandler = null;

    boolean m_closed = false;
    Exception m_lastAckReceiverException = null;
    Exception m_lastSenderException = null;

    private static final ListeningExecutorService m_es = VoltDB.instance().getComputationService();

    public StreamSnapshotDataTarget(long hsId, Map<Integer, byte[]> schemas)
    throws IOException {
        super();
        m_schemas = schemas;
        m_destHSId = hsId;
        m_mb = VoltDB.instance().getHostMessenger().createMailbox();

        m_in = new AckReceiver();
        m_in.setDaemon(true);
        m_in.start();
    }

    class SendWork implements Callable<Boolean> {
        BBContainer m_message;
        BBContainer m_schema;
        int m_blockIndex;
        AtomicReference<ListenableFuture<Boolean>> m_future = new AtomicReference<ListenableFuture<Boolean>>(null);

        SendWork (int blockIndex, BBContainer schema, BBContainer message) {
            m_blockIndex = blockIndex;
            m_schema = schema;
            m_message = message;
        }

        @Override
        protected void finalize() {
            discard(); // idempotent
        }

        void setFuture(ListenableFuture<Boolean> future) {
            m_future.set(future);
        }

        ListenableFuture<Boolean> getFuture() {
            return m_future.get();
        }

        public synchronized void discard() {
            rejoinLog.info("Discarding buffer at index " + String.valueOf(m_blockIndex));

            // try to cancel the work if it's not done yet
            ListenableFuture<Boolean> future = m_future.get();
            if (future != null) {
                if (m_future.compareAndSet(future, null)) {
                    future.cancel(true);
                }
                // should be null no matter what the result of the
                // compare and set above
                assert(m_future.get() == null);
            }

            // discard the buffers and null them out
            if (m_message != null) {
                m_message.discard();
                m_message = null;
            }
            if (m_schema != null) {
                m_schema.discard();
                m_schema = null;
            }
        }

        protected boolean send(BBContainer message) {
            try {
                if (message.b.isDirect()) {
                    byte[] data = null;
                    int compressedSize = 0;

                    synchronized (m_compressionLock) {
                        m_compressionBuffer.clear();
                        compressedSize = CompressionService.compressBuffer(message.b, m_compressionBuffer);
                        m_compressionBuffer.limit(compressedSize);
                        m_compressionBuffer.position(0);

                        data = new byte[compressedSize];
                        m_compressionBuffer.get(data);
                    }

                    RejoinDataMessage msg = new RejoinDataMessage(data);
                    m_mb.send(m_destHSId, msg);
                    m_bytesSent.addAndGet(compressedSize);

                    rejoinLog.info("Sending direct buffer");
                } else {
                    byte compressedBytes[] =
                            CompressionService.compressBytes(
                                    message.b.array(), message.b.position(),
                                    message.b.remaining());

                    RejoinDataMessage msg = new RejoinDataMessage(compressedBytes);
                    m_mb.send(m_destHSId, msg);
                    m_bytesSent.addAndGet(compressedBytes.length);

                    rejoinLog.info("Sending heap buffer");
                }
            } catch (IOException e) {
                rejoinLog.error("Error writing rejoin snapshot block", e);
                return false;
            }
            return true;
        }

        @Override
        public synchronized Boolean call() throws Exception {
            // this work has already been discarded
            if (m_message == null) {
                return true;
            }

            if (m_schema != null) {
                if (!send(m_schema)) {
                    return false;
                }
            }
            return send(m_message);
        }
    }

    class AckReceiver extends Thread {
        @Override
        public void run() {
            rejoinLog.info("Starting ack receiver thread");

            try {
                while (!m_closed) {
                    rejoinLog.info("Blocking on receiving mailbox");
                    VoltMessage msg = m_mb.recvBlocking();
                    assert(msg instanceof RejoinDataAckMessage);
                    RejoinDataAckMessage ackMsg = (RejoinDataAckMessage) msg;
                    final int blockIndex = ackMsg.getBlockIndex();

                    rejoinLog.info("Recieved block ack for index " + String.valueOf(blockIndex));

                    m_outstandingWorkCount.decrementAndGet();
                    SendWork work = m_outstandingWork.remove(blockIndex);
                    assert(work != null);

                    work.discard();
                }
            } catch (Exception e) {

                //if (m_closed) {
                //    return;
                //}
                m_lastAckReceiverException = e;
                rejoinLog.error("Error reading a message from a recovery stream", e);
            }
        }
    }

    @Override
    public int getHeaderSize() {
        return contentOffset;
    }

    @Override
    public ListenableFuture<?> write(Callable<BBContainer> tupleData,
                                     SnapshotTableTask context) {
        assert(context != null);

        rejoinLog.info("Starting write");

        try {
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
            if (m_closed) {
                if (chunk != null) {
                    chunk.discard();
                }

                m_writeFailed = true;
                IOException e = new IOException("Trying to write snapshot data " +
                        "after the stream is closed");
                return Futures.immediateFailedFuture(e);
            }

            if (chunk != null) {
                BBContainer schemaContainer = null;

                // Have we seen this table before, if not, send schema
                if (m_schemas.containsKey(context.getTableId())) {
                    // remove the schema once sent
                    byte[] schema = m_schemas.remove(context.getTableId());
                    rejoinLog.debug("Sending schema for table " + context.getTableId());

                    ByteBuffer buf = ByteBuffer.allocate(schema.length + 1); // 1 byte for the type
                    buf.put((byte) StreamSnapshotMessageType.SCHEMA.ordinal());
                    buf.put(schema);
                    buf.flip();
                    schemaContainer = DBBPool.wrapBB(buf);

                    rejoinLog.info("Writing schema as part of this write");
                }

                chunk.b.put((byte) StreamSnapshotMessageType.DATA.ordinal());
                chunk.b.putInt(m_blockIndex); // put chunk index
                chunk.b.putInt(context.getTableId()); // put table ID
                chunk.b.position(0);

                SendWork sendWork = new SendWork(m_blockIndex, schemaContainer, chunk);
                m_outstandingWork.put(m_blockIndex, sendWork);
                m_outstandingWorkCount.incrementAndGet();
                m_es.submit(sendWork);

                rejoinLog.info("Submitted write with index " + String.valueOf(m_blockIndex));

                m_blockIndex++;
            }

            return null;
        }
        finally {
            rejoinLog.info("Finished call to write");
        }
    }

    @Override
    public synchronized void close() throws IOException, InterruptedException {
        /*
         * could be called multiple times, because all tables share one stream
         * target
         */
        if (!m_closed) {
            rejoinLog.debug("Closing stream snapshot target");

            // block until all acks have arrived
            while (!m_writeFailed && m_outstandingWorkCount.get() > 0) {
                Thread.yield();
            }

            // Send EOF
            ByteBuffer buf = ByteBuffer.allocate(1);
            buf.put((byte) StreamSnapshotMessageType.END.ordinal());
            buf.flip();
            byte compressedBytes[] =
                    CompressionService.compressBytes(
                            buf.array(), buf.position(),
                            buf.remaining());
            RejoinDataMessage msg = new RejoinDataMessage(compressedBytes);
            m_mb.send(m_destHSId, msg);
            m_bytesSent.addAndGet(compressedBytes.length);

            // release the mailbox and close the socket
            VoltDB.instance().getHostMessenger().removeMailbox(m_mb.getHSId());
            m_mb = null;

            m_closed = true;

            rejoinLog.debug("Closed stream snapshot target");
        }

        if (m_onCloseHandler != null) {
            m_onCloseHandler.run();
        }
    }

    @Override
    public long getBytesWritten() {
        return m_bytesSent.get();
    }

    @Override
    public void setOnCloseHandler(Runnable onClose) {
        m_onCloseHandler = onClose;
    }

    @Override
    public Throwable getLastWriteException() {
        if (m_lastSenderException != null) {
            return m_lastSenderException;
        }
        return m_lastAckReceiverException;
    }

    @Override
    public SnapshotFormat getFormat() {
        return SnapshotFormat.STREAM;
    }
}
