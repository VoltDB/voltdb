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

package org.voltdb.rejoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotTableTask;
import org.voltdb.VoltDB;
import org.voltdb.utils.CompressionService;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A stream snapshot target for sending snapshot data directly to a rejoining
 * partition.
 */
public class StreamSnapshotDataTarget extends StreamSnapshotBase
implements SnapshotDataTarget {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    // triggers specific test code for TestMidRejoinDeath
    private static boolean m_rejoinDeathTestMode = System.getProperties().containsKey("rejoindeathtest");

    private static AtomicLong m_totalSnapshotTargetCount = new AtomicLong(0);
    private final long m_snapshotProcessorId;

    // shortened when in test mode
    final static long WRITE_TIMEOUT_MS = m_rejoinDeathTestMode ? 10000 : 60000;
    final static long WATCHDOG_PERIOS_S = 5;

    // schemas for all the tables on this partition
    private final Map<Integer, byte[]> m_schemas = new HashMap<Integer, byte[]>();
    // Mailbox used to transfer snapshot data
    private Mailbox m_mb;
    // HSId of the destination mailbox
    private final long m_destHSId;
    // input and output threads
    private final AckReceiver m_in;
    private final SnapshotSender m_out;

    // list of send work in order
    private final LinkedBlockingQueue<SendWork> m_sendQueue = new LinkedBlockingQueue<SendWork>();

    // Skip all subsequent writes if one fails
    private final AtomicBoolean m_writeFailed = new AtomicBoolean(false);

    // a scratch buffer for compression work
    private final ByteBuffer m_compressionBuffer =
            ByteBuffer.allocateDirect(
                    CompressionService.maxCompressedLength(1024 * 1024 * 2 + (1024 * 256)));

    private final AtomicLong m_bytesSent = new AtomicLong();

    // number of sent, but un-acked buffers
    private final AtomicInteger m_outstandingWorkCount = new AtomicInteger(0);
    // map of sent, but un-acked buffers, packaged up a bit
    private final Map<Integer, SendWork> m_outstandingWork = (new TreeMap<Integer, SendWork>());

    private int m_blockIndex = 0;
    private final AtomicReference<Runnable> m_onCloseHandler = new AtomicReference<Runnable>(null);

    private final AtomicBoolean m_closed = new AtomicBoolean(false);
    private Exception m_lastAckReceiverException = null;
    private Exception m_lastSenderException = null;

    public StreamSnapshotDataTarget(long HSId, Map<Integer, byte[]> schemas)
    {
        super();
        m_schemas.putAll(schemas);
        m_destHSId = HSId;
        m_mb = VoltDB.instance().getHostMessenger().createMailbox();

        m_snapshotProcessorId = m_totalSnapshotTargetCount.getAndIncrement();
        rejoinLog.info(String.format("Initializing snapshot stream processor " +
                "for source site id: %s, and with processorid: %d",
                CoreUtils.hsIdToString(HSId), m_snapshotProcessorId));

        m_in = new AckReceiver();
        m_in.setDaemon(true);
        m_in.start();

        m_out = new SnapshotSender();
        m_out.setDaemon(true);
        m_out.start();

        // start a periodic task to look for timed out connections
        VoltDB.instance().scheduleWork(new Watchdog(m_bytesSent.get()), WATCHDOG_PERIOS_S, -1, TimeUnit.SECONDS);
    }

    /**
     * Packages up a pending write into a piece of work that can be tracked
     * and can be scheduled.
     */
    class SendWork implements Callable<Boolean> {
        BBContainer m_message;
        BBContainer m_schema;
        int m_blockIndex;
        long m_ts;
        AtomicReference<ListenableFuture<Boolean>> m_future = new AtomicReference<ListenableFuture<Boolean>>(null);

        SendWork (int blockIndex, BBContainer schema, BBContainer message) {
            m_blockIndex = blockIndex;
            m_schema = schema;
            m_message = message;
            m_ts = System.currentTimeMillis();
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

        /**
         * Idempotent method to cancel any pending work and release any
         * BBContainters held.
         */
        public synchronized void discard() {
            rejoinLog.trace("Discarding buffer at index " + String.valueOf(m_blockIndex));

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

        /**
         * Compress the data in the BBContainer provided, then package it up in
         * a RejoinDataMessage instance, and finally hand it off to the messaging
         * subsystem.
         */
        protected boolean send(BBContainer message) {
            try {
                if (message.b.isDirect()) {
                    byte[] data = null;
                    int compressedSize = 0;

                    // sync access to the one scratch buffer for compression
                    synchronized (m_compressionBuffer) {
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

                    rejoinLog.trace("Sending direct buffer");
                } else {
                    byte compressedBytes[] =
                            CompressionService.compressBytes(
                                    message.b.array(), message.b.position(),
                                    message.b.remaining());

                    RejoinDataMessage msg = new RejoinDataMessage(compressedBytes);
                    m_mb.send(m_destHSId, msg);
                    m_bytesSent.addAndGet(compressedBytes.length);

                    rejoinLog.trace("Sending heap buffer");
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

    /**
     * Task run every so often to look for writes that haven't been acked
     * in WRITE_TIMEOUT_MS time.
     */
    class Watchdog implements Runnable {

        final long m_bytesWrittenSinceConstruction;

        Watchdog(long bytesWritten) {
            m_bytesWrittenSinceConstruction = bytesWritten;
        }

        @Override
        public synchronized void run() {
            if (m_closed.get()) {
                return;
            }

            long bytesWritten = m_bytesSent.get();
            rejoinLog.info(String.format("While sending rejoin data to site %s, %d bytes have been sent in the past %s seconds.",
                    CoreUtils.hsIdToString(m_destHSId), bytesWritten - m_bytesWrittenSinceConstruction, WATCHDOG_PERIOS_S));

            long now = System.currentTimeMillis();
            for (Entry<Integer, SendWork> e : m_outstandingWork.entrySet()) {
                SendWork work = e.getValue();
                if ((now - work.m_ts) > WRITE_TIMEOUT_MS) {
                    rejoinLog.error(String.format(
                            "A snapshot write task failed after a timeout (currently %d seconds outstanding).",
                            (now - work.m_ts) / 1000));
                    m_writeFailed.set(true);
                    break;
                }
            }
            if (m_writeFailed.get()) {
                clearOutstanding(); // idempotent
            }

            // schedule to run again
            VoltDB.instance().scheduleWork(new Watchdog(bytesWritten), WATCHDOG_PERIOS_S, -1, TimeUnit.SECONDS);
        }
    }

    /**
     * Idempotent, synchronized method to perform all cleanup of outstanding
     * work so buffers aren't leaked.
     */
    synchronized void clearOutstanding() {
        if (m_outstandingWork.isEmpty() && (m_outstandingWorkCount.get() == 0)) {
            return;
        }

        rejoinLog.trace("Clearing outstanding work.");

        for (Entry<Integer, SendWork> e : m_outstandingWork.entrySet()) {
            e.getValue().discard();
        }
        m_outstandingWork.clear();
        m_outstandingWorkCount.set(0);
    }

    /**
     * Thread that blocks on the receipt of Acks.
     */
    class AckReceiver extends Thread {
        @Override
        public void run() {
            rejoinLog.trace("Starting ack receiver thread");

            try {
                while (!m_closed.get()) {
                    rejoinLog.trace("Blocking on receiving mailbox");
                    VoltMessage msg = m_mb.recvBlocking(5000);
                    if (msg == null) continue;
                    assert(msg instanceof RejoinDataAckMessage);
                    RejoinDataAckMessage ackMsg = (RejoinDataAckMessage) msg;
                    final int blockIndex = ackMsg.getBlockIndex();

                    // TestMidRejoinDeath ignores acks to trigger the watchdog
                    if (m_rejoinDeathTestMode && (m_snapshotProcessorId == 1)) {
                        continue;
                    }

                    receiveAck(blockIndex);
                }
            }
            catch (Exception e) {
                if (m_closed.get()) {
                    return;
                }
                m_lastAckReceiverException = e;
                rejoinLog.error("Error reading a message from a recovery stream", e);
            }
            finally {
                rejoinLog.trace("Ack receiver thread exiting");
            }
        }
    }

    /**
     * Synchronized method to handle the arrival of an Ack.
     * @param blockIndex The index of the block that is being acked.
     */
    public synchronized void receiveAck(int blockIndex) {
        rejoinLog.trace("Received block ack for index " + String.valueOf(blockIndex));

        m_outstandingWorkCount.decrementAndGet();
        SendWork work = m_outstandingWork.remove(blockIndex);

        // releases the BBContainers and cleans up
        work.discard();
    }

    /**
     * Thread that runs send work (sending snapshot blocks)
     */
    class SnapshotSender extends Thread {
        @Override
        public void run() {
            rejoinLog.trace("Starting stream sender thread");

            try {
                while (!m_closed.get()) {
                    rejoinLog.trace("Blocking on sending work queue");
                    SendWork work = m_sendQueue.poll(5, TimeUnit.SECONDS);
                    if (work != null) {
                        work.call();
                    }
                }
            }
            catch (Exception e) {
                if (m_closed.get()) {
                    return;
                }
                m_lastSenderException = e;
                rejoinLog.error("Error sending a recovery stream message", e);
            }
            finally {
                rejoinLog.trace("Stream sender thread exiting");
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

        rejoinLog.trace("Starting write");

        try {
            BBContainer chunk = null;
            try {
                chunk = tupleData.call();
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }

            // cleanup and exit immediately if in failure mode
            // or on null imput
            if (m_writeFailed.get() || (chunk == null)) {
                if (chunk != null) {
                    chunk.discard();
                }
                return null;
            }

            // cleanup and exit immediately if in failure mode
            // but here, throw an exception because this isn't supposed to happen
            if (m_closed.get()) {
                if (chunk != null) {
                    chunk.discard();
                }

                m_writeFailed.set(true);
                IOException e = new IOException("Trying to write snapshot data " +
                        "after the stream is closed");
                return Futures.immediateFailedFuture(e);
            }

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

                rejoinLog.trace("Writing schema as part of this write");
            }

            chunk.b.put((byte) StreamSnapshotMessageType.DATA.ordinal());
            chunk.b.putInt(m_blockIndex); // put chunk index
            chunk.b.putInt(context.getTableId()); // put table ID
            chunk.b.position(0);

            send(m_blockIndex++, schemaContainer, chunk);

            rejoinLog.trace("Submitted write with index " + String.valueOf(m_blockIndex));

            return null;
        }
        finally {
            rejoinLog.trace("Finished call to write");
        }
    }

    /**
     * Send data to the rejoining node, tracking what was sent for ack tracking.
     * Synchronized to protect access to m_outstandingWork and to keep
     * m_outstandingWorkCount in sync with m_outstandingWork.
     *
     * @param blockIndex Index useful for ack tracking and debugging
     * @param schemaContainer Optional schema for table (can be null)
     * @param chunk Snapshot data to send.
     */
    synchronized void send(int blockIndex, BBContainer schemaContainer, BBContainer chunk) {
        SendWork sendWork = new SendWork(blockIndex, schemaContainer, chunk);
        m_outstandingWork.put(blockIndex, sendWork);
        m_outstandingWorkCount.incrementAndGet();
        m_sendQueue.add(sendWork);
    }

    @Override
    public boolean needsFinalClose()
    {
        // Streamed snapshot targets always need to be closed by the last site
        return true;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        /*
         * could be called multiple times, because all tables share one stream
         * target
         */
        if (!m_closed.get()) {
            rejoinLog.trace("Closing stream snapshot target");

            // block until all acks have arrived
            while (!m_writeFailed.get() && (m_outstandingWorkCount.get() > 0)) {
                Thread.yield();
            }

            // if here because a write failed, cleanup outstanding work
            clearOutstanding();

            // Send EOF
            ByteBuffer buf = ByteBuffer.allocate(1);
            if (m_writeFailed.get()) {
                // signify failure, at least on this end
                buf.put((byte) StreamSnapshotMessageType.FAILURE.ordinal());
            }
            else {
                // success - join the cluster
                buf.put((byte) StreamSnapshotMessageType.END.ordinal());
            }

            buf.flip();
            byte compressedBytes[] =
                    CompressionService.compressBytes(
                            buf.array(), buf.position(),
                            buf.remaining());
            RejoinDataMessage msg = new RejoinDataMessage(compressedBytes);
            m_mb.send(m_destHSId, msg);
            m_bytesSent.addAndGet(compressedBytes.length);

            // locked so m_closed is true when the ack thread dies
            synchronized(this) {
                // release the mailbox and close the socket,
                // this should stop the ack receiver thread
                VoltDB.instance().getHostMessenger().removeMailbox(m_mb.getHSId());
                m_mb = null;
                m_closed.set(true);

                assert(m_outstandingWork.size() == 0);
            }

            rejoinLog.trace("Closed stream snapshot target");
        }

        Runnable closeHandle = m_onCloseHandler.get();
        if (closeHandle != null) {
            closeHandle.run();
        }
    }

    @Override
    public long getBytesWritten() {
        return m_bytesSent.get();
    }

    @Override
    public void setOnCloseHandler(Runnable onClose) {
        m_onCloseHandler.set(onClose);
    }

    @Override
    public synchronized Throwable getLastWriteException() {
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
