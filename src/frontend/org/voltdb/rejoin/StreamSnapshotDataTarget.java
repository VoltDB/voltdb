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

package org.voltdb.rejoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.VoltDB;
import org.voltdb.utils.CompressionService;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.primitives.Longs;
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
    final static long WATCHDOG_PERIOD_S = 5;

    // Number of bytes in the fixed header of a table data Block Type(1) + BlockIndex(4) + TableId(4) + partition id(4) + row count(4)
    final static int ROW_COUNT_OFFSET = contentOffset + 4;
    final static int DATA_HEADER_BYTES = contentOffset + 4 + 4;

    // schemas for all the tables on this partition
    private final Map<Integer, Pair<Boolean, byte[]>> m_schemas;
    // HSId of the source site mailbox
    private final long m_srcHSId;
    // HSId of the destination site mailbox
    private final long m_destHSId;
    private final Set<Long> m_otherDestHostHSIds;
    private final boolean m_replicatedTableTarget;
    // input and output threads
    private final SnapshotSender m_sender;
    private final StreamSnapshotAckReceiver m_ackReceiver;

    // Skip all subsequent writes if one fails
    private final AtomicReference<Exception> m_writeFailed = new AtomicReference<>();
    // true if the failure is already reported to the SnapshotSiteProcessor, prevent throwing
    // the same exception multiple times.
    private boolean m_failureReported = false;

    private volatile IOException m_reportedSerializationFailure = null;

    // number of sent, but un-acked buffers
    final AtomicInteger m_outstandingWorkCount = new AtomicInteger(0);
    // map of sent, but un-acked buffers, packaged up a bit
    private final TreeMap<Integer, SendWork> m_outstandingWork = new TreeMap<Integer, SendWork>();

    int m_blockIndex = 0;
    private final AtomicReference<Runnable> m_onCloseHandler = new AtomicReference<Runnable>(null);
    private Runnable m_progressHandler = null;

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    public StreamSnapshotDataTarget(long srcHSId, long destHSId, boolean lowestDestSite, Set<Long> allDestHostHSIds,
            byte[] hashinatorConfig, List<SnapshotTableInfo> tables, SnapshotSender sender,
            StreamSnapshotAckReceiver ackReceiver)
    {
        this(srcHSId, destHSId, lowestDestSite, allDestHostHSIds, hashinatorConfig, tables, DEFAULT_WRITE_TIMEOUT_MS, sender,
                ackReceiver);
    }

    public StreamSnapshotDataTarget(long srcHSId, long destHSId, boolean lowestDestSite, Set<Long> allDestHostHSIds,
            byte[] hashinatorConfig, List<SnapshotTableInfo> tables, long writeTimeout, SnapshotSender sender,
            StreamSnapshotAckReceiver ackReceiver)
    {
        super();
        // A unit test should never set a static test variable because it will bleed into other tests
        assert(VoltDB.instanceOnServerThread() ? !m_rejoinDeathTestMode : true);
        m_targetId = m_totalSnapshotTargetCount.getAndIncrement();
        m_schemas = tables.stream().collect(
                Collectors.toMap(SnapshotTableInfo::getTableId, t -> Pair.of(t.isReplicated(), t.getSchema())));
        m_srcHSId = srcHSId;
        m_destHSId = destHSId;
        m_replicatedTableTarget = lowestDestSite;
        m_otherDestHostHSIds = new HashSet<>(allDestHostHSIds);
        m_otherDestHostHSIds.remove(m_destHSId);
        m_sender = sender;
        m_sender.registerDataTarget(m_targetId);
        m_ackReceiver = ackReceiver;
        m_ackReceiver.setCallback(m_targetId, this, m_replicatedTableTarget ? allDestHostHSIds.size() : 1);

        rejoinLog.debug(String.format("Initializing snapshot stream processor " +
                "for src site id : %s, dest site id: %s, and with processorid: %d%s" ,
                CoreUtils.hsIdToString(m_srcHSId), CoreUtils.hsIdToString(m_destHSId),
                m_targetId, (lowestDestSite?" [Lowest Site]":"")));

        // start a periodic task to look for timed out connections
        VoltDB.instance().scheduleWork(new Watchdog(0, writeTimeout, System.currentTimeMillis()), WATCHDOG_PERIOD_S, -1, TimeUnit.SECONDS);

        if (hashinatorConfig != null) {
            // Send the hashinator config as  the first block
            send(StreamSnapshotMessageType.HASHINATOR, -1, hashinatorConfig, false);
        }
    }

    public boolean isReplicatedTableTarget() {
        return m_replicatedTableTarget;
    }

    /**
     * Packages up a pending write into a piece of work that can be tracked
     * and can be scheduled.
     */
    public static class SendWork {
        BBContainer m_message;
        final StreamSnapshotMessageType m_type;
        final long m_targetId;
        final long m_destHSId;
        final Set<Long> m_otherDestHSIds;
        AtomicInteger m_ackCounter;
        final long m_ts;

        final boolean m_isEmpty;

        // A listenable future used to notify a listener when this buffer is discarded
        final SettableFuture<Boolean> m_future;

        /**
         * Creates an empty send work to terminate the sender thread
         */
        SendWork() {
            m_type = StreamSnapshotMessageType.DATA;
            m_isEmpty = true;
            m_targetId = -1;
            m_destHSId = -1;
            m_otherDestHSIds = null;
            m_ts = -1;
            m_future = null;
        }

        SendWork (StreamSnapshotMessageType type, long targetId, long destHSId,
                  Set<Long> otherDestIds, BBContainer message,
                  SettableFuture<Boolean> future) {
            m_isEmpty = false;
            m_type = type;
            m_targetId = targetId;
            m_destHSId = destHSId;
            m_otherDestHSIds = otherDestIds;
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
                return data.length;
            } else {
                byte compressedBytes[] =
                    CompressionService.compressBytes(
                            messageBuffer.array(), messageBuffer.position(),
                            messageBuffer.remaining());

                mb.send(m_destHSId, msgFactory.makeDataMessage(m_targetId, compressedBytes));
                return compressedBytes.length;
            }
        }

        private void sendReplicatedDataToNonLowestSites(Mailbox mb, MessageFactory msgFactory, ByteBuffer message, int len) throws IOException {
            byte[] compressedBytes;
            if (message.isDirect()) {
                compressedBytes = CompressionService.compressBuffer(message);
            }
            else {
                compressedBytes =
                    CompressionService.compressBytes(message.array(), 0, len);
            }
            mb.send(Longs.toArray(m_otherDestHSIds), msgFactory.makeDataMessage(m_targetId, compressedBytes));
        }

        public synchronized int doWork(Mailbox mb, MessageFactory msgFactory) throws Exception {
            // this work has already been discarded
            if (m_message == null) {
                m_ackCounter = new AtomicInteger(1);
                return 0;
            }

            try {
                int sentBytes;
                if (m_otherDestHSIds != null) {
                    m_ackCounter = new AtomicInteger(m_otherDestHSIds.size()+1);
                    sentBytes = send(mb, msgFactory, m_message);
                    if (m_type == StreamSnapshotMessageType.DATA) {
                        // Copy the header from the real buffer and add a dummy table that the other non-lowest site can parse
                        ByteBuffer dummyBuffer = ByteBuffer.allocate(DATA_HEADER_BYTES);
                        m_message.b().get(dummyBuffer.array(), 0, ROW_COUNT_OFFSET);
                        m_message.b().position(0);
                        dummyBuffer.position(ROW_COUNT_OFFSET);
                        dummyBuffer.putInt(0);  // Row Count
                        dummyBuffer.position(0);
                        sendReplicatedDataToNonLowestSites(mb, msgFactory, dummyBuffer, DATA_HEADER_BYTES);
                    }
                    else if (m_type == StreamSnapshotMessageType.END) {
                        // Special case for sending END messages to Non-Leader sites from the site that sent the replicated
                        // Tables. We do this because replicated tables can race with partitioned tables so the sending 2
                        // ENDs (one from the Replicated Table data target and one from the Partitioned tables data target)
                        // means that the sink can be deallocated.
                        sendReplicatedDataToNonLowestSites(mb, msgFactory, m_message.b(), m_message.b().limit());
                    }
                    else {
                        // Special case for sending schema for replicated table to all sites of host
                        sendReplicatedDataToNonLowestSites(mb, msgFactory, m_message.b(), m_message.b().remaining());
                    }
                }
                else {
                    m_ackCounter = new AtomicInteger(1);
                    sentBytes = send(mb, msgFactory, m_message);
                }
                if (rejoinLog.isTraceEnabled()) {
                    rejoinLog.trace("Sent " + m_type.name() + " from " + m_targetId +
                            " expected ackCounter " + m_ackCounter +
                            " otherDestHSIds " + m_otherDestHSIds);
                }
                return sentBytes;
            } finally {
                // Buffers are only discarded after they are acked. Discarding them here would cause the sender to
                // generate too much work for the receiver.
                m_future.set(true);
            }
        }

        public boolean receiveAck() {
            return m_ackCounter.decrementAndGet() == 0;
        }
    }

    public static class StreamSnapshotTimeoutException extends IOException {
        private static final long serialVersionUID = 1L;

        public StreamSnapshotTimeoutException(String message) {
            super(message);
        }
    }

    public static class SnapshotSerializationException extends IOException {
        private static final long serialVersionUID = 1L;

        public SnapshotSerializationException(String message) {
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

        // Last time data written to destination
        final long m_lastDataWrite;
        Watchdog(long bytesWritten, long writeTimout, long lastDataWrite) {
            m_bytesWrittenSinceConstruction = bytesWritten;
            m_writeTimeout = writeTimout;
            m_lastDataWrite = lastDataWrite;
        }

        @Override
        public void run() {
            if (m_closed.get()) {
                return;
            }
            boolean watchAgain = true;
            long bytesWritten = 0;
            long bytesSentSinceLastCheck = 0;
            try {
                final int destHostId = CoreUtils.getHostIdFromHSId(m_destHSId);
                bytesWritten = m_sender.m_bytesSent.get(m_targetId).get();
                bytesSentSinceLastCheck = bytesWritten - m_bytesWrittenSinceConstruction;
                rejoinLog.info(String.format("While sending rejoin data from site %s to site %s, %d bytes have been sent in the past %s seconds.",
                        CoreUtils.hsIdToString(m_srcHSId), CoreUtils.hsIdToString(m_destHSId),
                        bytesSentSinceLastCheck, WATCHDOG_PERIOD_S));

                checkTimeout(m_writeTimeout);
                if (m_writeFailed.get() != null) {
                    clearOutstanding();
                    watchAgain = false;
                } else if (bytesSentSinceLastCheck == 0) {
                    watchAgain = VoltDB.instance().getHostMessenger().getLiveHostIds().contains(destHostId);
                    if (!watchAgain) {
                        if(m_writeFailed.get() == null) {
                            setWriteFailed(new StreamSnapshotTimeoutException(
                              "A snapshot write task failed after rejoining node is down."));
                        }
                        clearOutstanding();
                    }
                }
            } catch (Throwable t) {
                rejoinLog.error("Stream snapshot watchdog thread threw an exception", t);
            } finally {
                // schedule to run again
                if (watchAgain) {
                    VoltDB.instance().scheduleWork(new Watchdog(bytesWritten, m_writeTimeout, bytesSentSinceLastCheck > 0 ? System.currentTimeMillis() : m_lastDataWrite),
                            WATCHDOG_PERIOD_S, -1, TimeUnit.SECONDS);
                } else {
                    rejoinLog.info(String.format("Stop watching stream snapshot from site %s to site %s",
                            CoreUtils.hsIdToString(m_srcHSId),
                            CoreUtils.hsIdToString(m_destHSId)));
                }
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
                                "A snapshot write task failed on site %s after a timeout (currently %d seconds outstanding). " +
                                        "Node rejoin may need to be retried",
                                CoreUtils.hsIdToString(m_srcHSId),
                                (now - work.m_ts) / 1000));
                rejoinLog.error(exception.getMessage());
                setWriteFailed(exception);
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
        notifyAll();
    }

    /**
     * Synchronized method to handle the arrival of an Ack.
     * @param blockIndex The index of the block that is being acked.
     */
    @Override
    public synchronized void receiveAck(int blockIndex) {
        SendWork work = m_outstandingWork.get(blockIndex);

        // releases the BBContainers and cleans up
        if (work == null || work.m_ackCounter == null) {
            rejoinLog.warn("Received invalid blockIndex ack for targetId " + m_targetId +
                    " for index " + String.valueOf(blockIndex) +
                    ((work == null) ? " already removed the block." : " ack counter haven't been initialized."));
            return;
        }
        if (work.receiveAck()) {
            rejoinLog.trace("Received ack for targetId " + m_targetId +
                    " removes block for index " + String.valueOf(blockIndex));
            if (m_outstandingWorkCount.decrementAndGet() == 0) {
                notifyAll();
            }
            m_outstandingWork.remove(blockIndex);
            work.discard();
        }
        else {
            rejoinLog.trace("Received ack for targetId " + m_targetId +
                    " decrements counter for block index " + String.valueOf(blockIndex));
        }
    }

    @Override
    public synchronized void receiveError(Exception exception) {
        setWriteFailed(exception);
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
        synchronized(this) {
            rejoinLog.trace("Starting write");
            try {
                BBContainer chunkC;
                ByteBuffer chunk;
                try {
                    chunkC = tupleData.call();
                    chunk = chunkC.b();
                } catch (Exception e) {
                    setWriteFailed(e);
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
                    setWriteFailed(e);
                    return Futures.immediateFailedFuture(e);
                }

                // Have we seen this table before, if not, send schema
                Pair<Boolean, byte[]> tableInfo = m_schemas.get(tableId);
                if (tableInfo.getSecond() != null) {
                    // remove the schema once sent
                    byte[] schema = tableInfo.getSecond();
                    m_schemas.put(tableId, Pair.of(tableInfo.getFirst(), null));
                    if (rejoinLog.isDebugEnabled()) {
                        rejoinLog.debug("Sending schema for table " + tableId);
                    }

                    send(StreamSnapshotMessageType.SCHEMA, tableId, schema, tableInfo.getFirst());
                }

                chunk.put((byte) StreamSnapshotMessageType.DATA.ordinal());
                chunk.putInt(m_blockIndex); // put chunk index
                chunk.putInt(tableId); // put table ID

                chunk.position(0);
                return send(StreamSnapshotMessageType.DATA, m_blockIndex++, chunkC, tableInfo.getFirst());
            } finally {
                rejoinLog.trace("Finished call to write");
            }
        }
    }

    synchronized private ListenableFuture<Boolean> send(StreamSnapshotMessageType type,
            int tableId, byte[] content, boolean replicatedTable)
    {
        // 1 byte for the type, 4 bytes for the block index, 4 bytes for table Id
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 4 + content.length);
        buf.put((byte) type.ordinal());
        buf.putInt(m_blockIndex);
        buf.putInt(tableId);
        buf.put(content);
        buf.flip();
        return send(type, m_blockIndex++, DBBPool.wrapBB(buf), replicatedTable);
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
    synchronized ListenableFuture<Boolean> send(StreamSnapshotMessageType type, int blockIndex, BBContainer chunk, boolean replicatedTable) {
        SettableFuture<Boolean> sendFuture = SettableFuture.create();
        if (rejoinLog.isTraceEnabled()) {
            rejoinLog.trace("Sending block " + blockIndex + " of type " + (replicatedTable?"REPLICATED ":"PARTITIONED ") + type.name() +
                    " from targetId " + m_targetId + " site " + CoreUtils.hsIdToString(m_srcHSId) +
                    " to " + CoreUtils.hsIdToString(m_destHSId) +
                    (replicatedTable?", " + CoreUtils.hsIdCollectionToString(m_otherDestHostHSIds):""));
        }
        SendWork sendWork = new SendWork(type, m_targetId, m_destHSId,
                replicatedTable?m_otherDestHostHSIds:null, chunk, sendFuture);
        m_outstandingWork.put(blockIndex, sendWork);
        m_outstandingWorkCount.incrementAndGet();
        m_sender.offer(sendWork);
        return sendFuture;
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
            rejoinLog.trace("Closing stream snapshot target " + m_targetId);

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

            rejoinLog.trace("Closed stream snapshot target " + m_targetId);
        }

        Runnable closeHandle = m_onCloseHandler.get();
        if (closeHandle != null) {
            closeHandle.run();
        }

        if (m_reportedSerializationFailure != null) {
            // There was an error reported by the EE during serialization
            throw m_reportedSerializationFailure;
        }
        // If there was an error during close(), throw it so that the snapshot
        // can be marked as failed.
        Exception e = m_writeFailed.get();
        if (e != null) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException(e);
        }
    }

    private void sendEOS()
    {
        // There should be no race for sending EOS since only last one site close the target.
        // Send EOF
        ByteBuffer buf = ByteBuffer.allocate(1 + 4); // 1 byte type, 4 bytes index
        if (m_writeFailed.get() != null) {
            // signify failure, at least on this end
            buf.put((byte) StreamSnapshotMessageType.FAILURE.ordinal());
        } else {
            // success - join the cluster
            buf.put((byte) StreamSnapshotMessageType.END.ordinal());
        }

        buf.putInt(m_blockIndex);
        buf.flip();
        send(StreamSnapshotMessageType.END, m_blockIndex++, DBBPool.wrapBB(buf), m_replicatedTableTarget);

        // Wait for the ack of the EOS message
        waitForOutstandingWork();
    }

    private synchronized void waitForOutstandingWork()
    {
        boolean interrupted = false;
        while (m_writeFailed.get() == null && (m_outstandingWorkCount.get() > 0) && !m_ackReceiver.isStopped()) {
            try {
                wait();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
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
    public synchronized Exception getLastWriteException() {
        Exception exception = m_sender.m_lastException;
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

    private synchronized void setWriteFailed(Exception exception) {
        m_ackReceiver.forceStop();
        if (m_writeFailed.compareAndSet(null, exception)) {
            notifyAll();
        }
    }

    /**
     * @param tableId ID of table
     * @return serialized schema for {@code tableId} or {@code null} if the schema has already been sent
     */
    protected byte[] getSchema(int tableId) {
        return m_schemas.get(tableId).getSecond();
    }

    public void setInProgressHandler(Runnable handler) {
        m_progressHandler = handler;
    }

    public void trackProgress() {
        m_progressHandler.run();
    }
}
