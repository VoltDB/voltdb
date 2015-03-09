/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Collections;
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
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.VoltDB;
import org.voltdb.utils.CompressionService;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

/**
 * A stream snapshot target for sending snapshot data directly to a rejoining
 * partition.
 */
public class StreamSnapshotDataTarget extends StreamSnapshotBase
implements SnapshotDataTarget, StreamSnapshotAckReceiver.AckCallback {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    // triggers specific test code for TestMidRejoinDeath
    static boolean m_rejoinDeathTestMode = System.getProperties().containsKey("rejoindeathtest");

    private static AtomicLong m_totalSnapshotTargetCount = new AtomicLong(0);
    final long m_targetId;

    // shortened when in test mode
    public final static long DEFAULT_WRITE_TIMEOUT_MS = m_rejoinDeathTestMode ? 10000 : Long.getLong("REJOIN_WRITE_TIMEOUT_MS", 60000);
    final static long WATCHDOG_PERIOS_S = 5;

    // schemas for all the tables on this partition
    private final Map<Integer, byte[]> m_schemas = new HashMap<Integer, byte[]>();
    // HSId of the destination mailbox
    private final long m_destHSId;
    // input and output threads
    private final SnapshotSender m_sender;
    private final StreamSnapshotAckReceiver m_ackReceiver;

    // Skip all subsequent writes if one fails
    private final AtomicReference<IOException> m_writeFailed = new AtomicReference<IOException>();
    // true if the failure is already reported to the SnapshotSiteProcessor, prevent throwing
    // the same exception multiple times.
    private boolean m_failureReported = false;

    // number of sent, but un-acked buffers
    final AtomicInteger m_outstandingWorkCount = new AtomicInteger(0);
    // map of sent, but un-acked buffers, packaged up a bit
    private final TreeMap<Integer, SendWork> m_outstandingWork = new TreeMap<Integer, SendWork>();

    int m_blockIndex = 0;
    private final AtomicReference<Runnable> m_onCloseHandler = new AtomicReference<Runnable>(null);

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    public StreamSnapshotDataTarget(long HSId, byte[] hashinatorConfig, Map<Integer, byte[]> schemas,
                                    SnapshotSender sender, StreamSnapshotAckReceiver ackReceiver)
    {
        this(HSId, hashinatorConfig, schemas, DEFAULT_WRITE_TIMEOUT_MS, sender, ackReceiver);
    }

    public StreamSnapshotDataTarget(long HSId, byte[] hashinatorConfig, Map<Integer, byte[]> schemas,
                                    long writeTimeout, SnapshotSender sender, StreamSnapshotAckReceiver ackReceiver)
    {
        super();
        m_targetId = m_totalSnapshotTargetCount.getAndIncrement();
        m_schemas.putAll(schemas);
        m_destHSId = HSId;
        m_sender = sender;
        m_sender.registerDataTarget(m_targetId);
        m_ackReceiver = ackReceiver;
        m_ackReceiver.setCallback(m_targetId, this);

        rejoinLog.debug(String.format("Initializing snapshot stream processor " +
                "for source site id: %s, and with processorid: %d",
                CoreUtils.hsIdToString(HSId), m_targetId));

        // start a periodic task to look for timed out connections
        VoltDB.instance().scheduleWork(new Watchdog(0, writeTimeout), WATCHDOG_PERIOS_S, -1, TimeUnit.SECONDS);

        if (hashinatorConfig != null) {
            // Send the hashinator config as  the first block
            send(StreamSnapshotMessageType.HASHINATOR, -1, hashinatorConfig);
        }
    }

    /**
     * Packages up a pending write into a piece of work that can be tracked
     * and can be scheduled.
     */
    public static class SendWork {
        BBContainer m_message;
        final long m_targetId;
        final long m_destHSId;
        final long m_ts;

        final boolean m_isEmpty;

        // A listenable future used to notify a listener when this buffer is discarded
        final SettableFuture<Boolean> m_future;

        /**
         * Creates an empty send work to terminate the sender thread
         */
        SendWork() {
            m_isEmpty = true;
            m_targetId = -1;
            m_destHSId = -1;
            m_ts = -1;
            m_future = null;
        }

        SendWork (long targetId, long destHSId,
                  BBContainer message,
                  SettableFuture<Boolean> future) {
            m_isEmpty = false;
            m_targetId = targetId;
            m_destHSId = destHSId;
            m_message = message;
            m_ts = System.currentTimeMillis();
            m_future = future;
        }

        /**
         * Idempotent method to cancel any pending work and release any
         * BBContainters held.
         */
        public synchronized void discard() {
            // discard the buffers and null them out
            if (m_message != null) {
                m_message.discard();
                m_message = null;
            }
        }

        /**
         * Compress the data in the BBContainer provided, then package it up in
         * a RejoinDataMessage instance, and finally hand it off to the messaging
         * subsystem.
         */
        protected int send(Mailbox mb, MessageFactory msgFactory, BBContainer message) throws IOException {
            final ByteBuffer messageBuffer = message.b();
            if (messageBuffer.isDirect()) {
                byte[] data = CompressionService.compressBuffer(messageBuffer);
                mb.send(m_destHSId, msgFactory.makeDataMessage(m_targetId, data));

                if (rejoinLog.isTraceEnabled()) {
                    rejoinLog.trace("Sending direct buffer");
                }

                return data.length;
            } else {
                byte compressedBytes[] =
                    CompressionService.compressBytes(
                            messageBuffer.array(), messageBuffer.position(),
                            messageBuffer.remaining());

                mb.send(m_destHSId, msgFactory.makeDataMessage(m_targetId, compressedBytes));

                if (rejoinLog.isTraceEnabled()) {
                    rejoinLog.trace("Sending heap buffer");
                }

                return compressedBytes.length;
            }
        }

        public synchronized int doWork(Mailbox mb, MessageFactory msgFactory) throws Exception {
            // this work has already been discarded
            if (m_message == null) {
                return 0;
            }

            try {
                return send(mb, msgFactory, m_message);
            } finally {
                // Buffers are only discarded after they are acked. Discarding them here would cause the sender to
                // generate too much work for the receiver.
                m_future.set(true);
            }
        }
    }

    public static class StreamSnapshotTimeoutException extends IOException {
        public StreamSnapshotTimeoutException(String message) {
            super(message);
        }
    }

    /**
     * Task run every so often to look for writes that haven't been acked
     * in writeTimeout time.
     */
    class Watchdog implements Runnable {

        final long m_bytesWrittenSinceConstruction;
        final long m_writeTimeout;

        Watchdog(long bytesWritten, long writeTimout) {
            m_bytesWrittenSinceConstruction = bytesWritten;
            m_writeTimeout = writeTimout;
        }

        @Override
        public void run() {
            if (m_closed.get()) {
                return;
            }

            long bytesWritten = 0;
            try {
                bytesWritten = m_sender.m_bytesSent.get(m_targetId).get();
                rejoinLog.info(String.format("While sending rejoin data to site %s, %d bytes have been sent in the past %s seconds.",
                        CoreUtils.hsIdToString(m_destHSId), bytesWritten - m_bytesWrittenSinceConstruction, WATCHDOG_PERIOS_S));

                checkTimeout(m_writeTimeout);
                if (m_writeFailed.get() != null) {
                    clearOutstanding(); // idempotent
                }
            } catch (Throwable t) {
                rejoinLog.error("Stream snapshot watchdog thread threw an exception", t);
            } finally {
                // schedule to run again
                VoltDB.instance().scheduleWork(new Watchdog(bytesWritten, m_writeTimeout), WATCHDOG_PERIOS_S, -1, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Called by the watchdog from the periodic work thread to check if the
     * oldest unacked block is older than the timeout interval.
     */
    private synchronized void checkTimeout(final long timeoutMs) {
        final Entry<Integer, SendWork> oldest = m_outstandingWork.firstEntry();
        if (oldest != null) {
            final long now = System.currentTimeMillis();
            SendWork work = oldest.getValue();
            if ((now - work.m_ts) > timeoutMs) {
                StreamSnapshotTimeoutException exception =
                        new StreamSnapshotTimeoutException(String.format(
                                "A snapshot write task failed after a timeout (currently %d seconds outstanding). " +
                                        "Node rejoin may need to be retried",
                                (now - work.m_ts) / 1000));
                rejoinLog.error(exception.getMessage());
                m_writeFailed.compareAndSet(null, exception);
            }
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
     * Synchronized method to handle the arrival of an Ack.
     * @param blockIndex The index of the block that is being acked.
     */
    @Override
    public synchronized void receiveAck(int blockIndex) {
        rejoinLog.trace("Received block ack for index " + String.valueOf(blockIndex));

        m_outstandingWorkCount.decrementAndGet();
        SendWork work = m_outstandingWork.remove(blockIndex);

        // releases the BBContainers and cleans up
        work.discard();
    }

    /**
     * Thread that runs send work (sending snapshot blocks). One per node
     */
    public static class SnapshotSender implements Runnable {
        private final Mailbox m_mb;
        private final MessageFactory m_msgFactory;
        private final LinkedBlockingQueue<SendWork> m_workQueue;
        private final AtomicInteger m_expectedEOFs;

        final Map<Long, AtomicLong> m_bytesSent;
        final Map<Long, AtomicLong> m_worksSent;
        volatile Exception m_lastException = null;

        public SnapshotSender(Mailbox mb)
        {
            this(mb, new DefaultMessageFactory());
        }

        public SnapshotSender(Mailbox mb, MessageFactory msgFactory)
        {
            Preconditions.checkArgument(mb != null);
            m_mb = mb;
            m_msgFactory = msgFactory;
            m_workQueue = new LinkedBlockingQueue<SendWork>();
            m_expectedEOFs = new AtomicInteger();
            m_bytesSent = Collections.synchronizedMap(new HashMap<Long, AtomicLong>());
            m_worksSent = Collections.synchronizedMap(new HashMap<Long, AtomicLong>());
        }

        public void registerDataTarget(long targetId)
        {
            m_expectedEOFs.incrementAndGet();
            m_bytesSent.put(targetId, new AtomicLong());
            m_worksSent.put(targetId, new AtomicLong());
        }

        public void offer(SendWork work)
        {
            m_workQueue.offer(work);
        }

        @Override
        public void run() {
            rejoinLog.trace("Starting stream sender thread");

            while (true) {
                SendWork work;

                try {
                    rejoinLog.trace("Blocking on sending work queue");
                    work = m_workQueue.poll(10, TimeUnit.MINUTES);

                    if (work == null) {
                        rejoinLog.warn("No stream snapshot send work was produced in the past 10 minutes");
                        break;
                    } else if (work.m_isEmpty) {
                        // Empty work indicates the end of the queue.
                        // The sender is shared by multiple data targets, each of them will
                        // send an end-of-queue work, must wait until all end-of-queue works
                        // are received before terminating the thread.
                        if (m_expectedEOFs.decrementAndGet() == 0) {
                            break;
                        } else {
                            continue;
                        }
                    }

                    m_bytesSent.get(work.m_targetId).addAndGet(work.doWork(m_mb, m_msgFactory));
                    m_worksSent.get(work.m_targetId).incrementAndGet();
                }
                catch (Exception e) {
                    m_lastException = e;
                    rejoinLog.error("Error sending a recovery stream message", e);
                }
            }
            CompressionService.releaseThreadLocal();
            rejoinLog.trace("Stream sender thread exiting");
        }
    }

    @Override
    public int getHeaderSize() {
        return contentOffset;
    }

    @Override
    public ListenableFuture<?> write(Callable<BBContainer> tupleData, int tableId) {
        rejoinLog.trace("Starting write");

        try {
            BBContainer chunkC;
            ByteBuffer chunk;
            try {
                chunkC = tupleData.call();
                chunk = chunkC.b();
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }

            // cleanup and exit immediately if in failure mode
            // or on null imput
            if (m_writeFailed.get() != null || (chunkC == null)) {
                if (chunkC != null) {
                    chunkC.discard();
                }

                if (m_failureReported) {
                    return null;
                } else {
                    m_failureReported = true;
                    return Futures.immediateFailedFuture(m_writeFailed.get());
                }
            }

            // cleanup and exit immediately if in failure mode
            // but here, throw an exception because this isn't supposed to happen
            if (m_closed.get()) {
                chunkC.discard();

                IOException e = new IOException("Trying to write snapshot data " +
                        "after the stream is closed");
                m_writeFailed.set(e);
                return Futures.immediateFailedFuture(e);
            }

            // Have we seen this table before, if not, send schema
            if (m_schemas.containsKey(tableId)) {
                // remove the schema once sent
                byte[] schema = m_schemas.remove(tableId);
                rejoinLog.debug("Sending schema for table " + tableId);

                rejoinLog.trace("Writing schema as part of this write");
                send(StreamSnapshotMessageType.SCHEMA, tableId, schema);
            }

            chunk.put((byte) StreamSnapshotMessageType.DATA.ordinal());
            chunk.putInt(m_blockIndex); // put chunk index
            chunk.putInt(tableId); // put table ID

            chunk.position(0);

            return send(m_blockIndex++, chunkC);
        } finally {
            rejoinLog.trace("Finished call to write");
        }
    }

    private ListenableFuture<Boolean> send(StreamSnapshotMessageType type, int tableId, byte[] content)
    {
        // 1 byte for the type, 4 bytes for the block index, 4 bytes for table Id
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 4 + content.length);
        buf.put((byte) type.ordinal());
        buf.putInt(m_blockIndex);
        buf.putInt(tableId);
        buf.put(content);
        buf.flip();

        return send(m_blockIndex++, DBBPool.wrapBB(buf));
    }

    /**
     * Send data to the rejoining node, tracking what was sent for ack tracking.
     * Synchronized to protect access to m_outstandingWork and to keep
     * m_outstandingWorkCount in sync with m_outstandingWork.
     *
     * @param blockIndex Index useful for ack tracking and debugging
     * @param schemaContainer Optional schema for table (can be null)
     * @param chunk Snapshot data to send.
     * @return return a listenable future for the caller to wait until the buffer is sent
     */
    synchronized ListenableFuture<Boolean> send(int blockIndex, BBContainer chunk) {
        SettableFuture<Boolean> sendFuture = SettableFuture.create();
        SendWork sendWork = new SendWork(m_targetId, m_destHSId, chunk, sendFuture);
        m_outstandingWork.put(blockIndex, sendWork);
        m_outstandingWorkCount.incrementAndGet();
        m_sender.offer(sendWork);
        return sendFuture;
    }

    @Override
    public boolean needsFinalClose()
    {
        // Streamed snapshot targets always need to be closed by the last site
        return true;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        boolean hadFailureBeforeClose = m_writeFailed.get() != null;

        /*
         * could be called multiple times, because all tables share one stream
         * target
         */
        if (!m_closed.get()) {
            rejoinLog.trace("Closing stream snapshot target");

            // block until all acks have arrived
            waitForOutstandingWork();

            // Send the EOS message after clearing outstanding work so that if there's a failure,
            // we'll send the correct EOS to the receiving end
            sendEOS();

            // Terminate the sender thread after the last block
            m_sender.offer(new SendWork());

            // locked so m_closed is true when the ack thread dies
            synchronized(this) {
                m_closed.set(true);

                assert(m_outstandingWork.size() == 0);
            }

            rejoinLog.trace("Closed stream snapshot target");
        }

        Runnable closeHandle = m_onCloseHandler.get();
        if (closeHandle != null) {
            closeHandle.run();
        }

        // If there was an error during close(), throw it so that the snapshot
        // can be marked as failed.
        if (!hadFailureBeforeClose && m_writeFailed.get() != null) {
            throw m_writeFailed.get();
        }
    }

    private void sendEOS()
    {
        // Send EOF
        ByteBuffer buf = ByteBuffer.allocate(1 + 4); // 1 byte type, 4 bytes index
        if (m_writeFailed.get() != null) {
            // signify failure, at least on this end
            buf.put((byte) StreamSnapshotMessageType.FAILURE.ordinal());
        }
        else {
            // success - join the cluster
            buf.put((byte) StreamSnapshotMessageType.END.ordinal());
        }
        buf.putInt(m_blockIndex);
        buf.flip();
        send(m_blockIndex++, DBBPool.wrapBB(buf));

        // Wait for the ack of the EOS message
        waitForOutstandingWork();
    }

    private void waitForOutstandingWork()
    {
        while (m_writeFailed.get() == null && (m_outstandingWorkCount.get() > 0)) {
            Thread.yield();
        }

        // if here because a write failed, cleanup outstanding work
        clearOutstanding();
    }

    @Override
    public long getBytesWritten() {
        return m_sender.m_bytesSent.get(m_targetId).get();
    }

    public long getWorksWritten()
    {
        return m_sender.m_worksSent.get(m_targetId).get();
    }

    @Override
    public void setOnCloseHandler(Runnable onClose) {
        m_onCloseHandler.set(onClose);
    }

    @Override
    public synchronized Throwable getLastWriteException() {
        Exception exception = m_sender.m_lastException;
        if (exception != null) {
            return exception;
        }
        exception = m_ackReceiver.m_lastException;
        if (exception != null) {
            return exception;
        }
        return m_writeFailed.get();
    }

    @Override
    public SnapshotFormat getFormat() {
        return SnapshotFormat.STREAM;
    }

    /**
     * Get the row count if any, of the content wrapped in the given {@link BBContainer}
     * @param tupleData
     * @return the numbers of tuple data rows contained within a container
     */
    @Override
    public int getInContainerRowCount(BBContainer tupleData) {
        // according to TableOutputStream.cpp:TupleOutputStream::endRows() the row count is
        // at offset 4 (second integer)
        ByteBuffer bb = tupleData.b().duplicate();
        bb.position(getHeaderSize());
        bb.getInt(); // skip first four (partition id)

        return bb.getInt();
    }
}
