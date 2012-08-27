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
package org.voltdb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.RecoveryMessage;
import org.voltcore.messaging.RecoveryMessageType;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.jni.ExecutionEngine;
import org.voltcore.logging.VoltLogger;
import org.voltdb.rejoin.RejoinDataAckMessage;
import org.voltdb.rejoin.RejoinDataMessage;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;

/**
 * Encapsulates the state managing the activities related to streaming recovery data
 * to some remote partitions. This class is only used on the partition that is a source of recovery data.
 *
 */
public class RecoverySiteProcessorSource extends RecoverySiteProcessor {


    /*
     * Some offsets for where data is serialized from/to
     * with the length prefix at the front
     */
    final int siteIdOffset = 4;
    final int blockIndexOffset = siteIdOffset + 8;
    final int messageTypeOffset = blockIndexOffset + 4;

    private static final VoltLogger recoveryLog = new VoltLogger("JOIN");

    /**
     * List of tables that need to be streamed
     */
    private final ArrayDeque<RecoveryTable> m_tablesToStream = new ArrayDeque<RecoveryTable>();

    /**
     * The engine that will be the source for the streams
     */
    private final ExecutionEngine m_engine;

    /**
     * Mailbox used to send message containing recovery data
     */
    private final Mailbox m_mailbox;

    /**
     * Mailbox used to transfer snapshot data
     */
    private final Mailbox m_mb;

    /**
     * HSId of the destination mailbox
     */
    private final long m_destHSId;

    /**
     * Generate unique identifiers for each block of table data sent over the wire
     */
    private int m_blockIndex = 0;

    private final AckTracker m_ackTracker = new AckTracker();

    /**
     * Placed in each message as a return address for acks
     */
    private final long m_siteId;

    /** Used to get a txn-specific version of the catalog */
    private final ExecutionSite m_site;

    /**
     * Number of buffers that can be sent before having to wait for acks to previously sent buffers
     * This limits the rate at which buffers are generated to ensure that the remote partitions can keep
     * up.
     */
    private final AtomicInteger m_allowedBuffers = new AtomicInteger(m_numBuffers);

    /**
     * What to do when all recovery data has been sent
     */
    private Runnable m_onCompletion;

    /**
     * Transaction the remote partition stopped at (without executing it) before
     * sending the initiate message
     */
    private long m_destinationStoppedBeforeTxnId = Long.MAX_VALUE;

    private final long m_destinationSiteId;

    /*
     * Only send the response to the initiate request once in doRecoveryWork
     */
    private boolean m_sentInitiateResponse = false;

    /**
     * If blocked on a multi-part txn, send a message to the destination
     * telling it to continue txn execution until it has executed past the multi-part
     * this source is blocked. This boolean ensures that the message is only sent once per txn
     * and is reset every time a new txn is pulled from the priority queue (see ExecutionSite.run())
     */
    private boolean m_sentSkipPastMultipartMsg = false;

    /**
     * Transaction to stop before and do the sync. Once doRecoveryWork is passed a txnId
     * that is >= this txnId it will stop and do the recovery
     */
    private long m_stopBeforeTxnId;

    private final MessageHandler m_messageHandler;

    private long m_bytesSent = 0;

    private final AtomicLong m_timeSpentSerializing = new AtomicLong();

    private volatile boolean m_ioclosed = false;

    private final LinkedBlockingQueue<BBContainer> m_outgoing = new LinkedBlockingQueue<BBContainer>();

    private final Thread m_outThread = new Thread() {
        @Override
        public void run() {
            try {
                final ByteBuffer compressionBuffer =
                        ByteBuffer.allocateDirect(
                                CompressionService.maxCompressedLength(1024 * 1024 * 2 + (1024 * 256)));
                while (true) {
                    BBContainer message = m_outgoing.take();
                    if (message.b == null) {
                        return;
                    }

                    try {
                        if (message.b.isDirect()) {
                            message.b.position(message.b.position() + 4);
                            compressionBuffer.clear();
                            final int compressedSize =
                                    CompressionService.compressBuffer( message.b, compressionBuffer);
                            compressionBuffer.limit(compressedSize);
                            compressionBuffer.position(0);

                            byte[] data = new byte[compressedSize];
                            compressionBuffer.get(data);
                            RejoinDataMessage msg = new RejoinDataMessage(data);
                            m_mb.send(m_destHSId, msg);
                        } else {
                            byte compressedBytes[] =
                                    CompressionService.compressBytes(
                                            message.b.array(), message.b.position() + 4, message.b.remaining() - 4);

                            RejoinDataMessage msg = new RejoinDataMessage(compressedBytes);
                            m_mb.send(m_destHSId, msg);
                        }
                    } catch (IOException e) {
                        recoveryLog.error("Error writing recovery message", e);
                    } finally {
                        message.discard();
                    }

                }
            } catch (InterruptedException e) {
                return;
            }
        }
    };

    private final Thread m_inThread = new Thread() {
        @Override
        public void run() {
            while (true) {
                VoltMessage msg = m_mb.recvBlocking();
                if (msg == null) {
                    break;
                }
                assert(msg instanceof RejoinDataAckMessage);
                RejoinDataAckMessage ackMsg = (RejoinDataAckMessage) msg;
                final int blockIndex = ackMsg.getBlockIndex();
                if (m_ackTracker.ackReceived(blockIndex)) {
                    m_allowedBuffers.incrementAndGet();

                    /*
                     * All recovery messages have been acked by the remote partition.
                     * Recovery is really complete so run the handler
                     */
                    if (m_allowedBuffers.get() == m_numBuffers && m_tablesToStream.isEmpty()) {
                        recoveryLog.info("Processor spent " +
                                (m_timeSpentSerializing.get() / 1000.0) + " seconds serializing");
                        synchronized (RecoverySiteProcessorSource.this) {
                            if (!m_ioclosed) {
                                closeIO();
                            }
                        }
                    } else {
                        RecoveryMessage rm = new RecoveryMessage();
                        rm.m_sourceHSId = m_siteId;
                        //Notify that a new buffer is available
                        m_mailbox.deliver(rm);
                    }
                }
            }
        }
    };

    private void closeIO() {
        m_ioclosed = true;
        m_outgoing.offer(new BBContainer( null,0) {
            @Override
            public void discard() {}
        });
        m_inThread.interrupt();
    }

    /**
     * Keep track of how many times a block has been acked and how many acks are expected
     */
    private static class AckTracker {
        private final HashMap<Integer, Integer> m_acks = new HashMap<Integer, Integer>();

        private synchronized void waitForAcks(int blockIndex, int acksExpected) {
            assert(!m_acks.containsKey(blockIndex));
            m_acks.put(blockIndex, acksExpected);
        }

        private synchronized boolean ackReceived(int blockIndex) {
            assert(m_acks.containsKey(blockIndex));
            int acksRemaining = m_acks.get(blockIndex);
            acksRemaining--;
            if (acksRemaining == 0) {
                recoveryLog.trace("Ack received for block " + blockIndex);
                m_acks.remove(blockIndex);
                return true;
            }
            recoveryLog.trace("Ack received for block " + blockIndex + " with " + acksRemaining + " remaining");
            m_acks.put(blockIndex, acksRemaining);
            return false;
        }

        @SuppressWarnings("unused")
        private synchronized void handleNodeFault(HashSet<Integer> failedNodes, SiteTracker tracker) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Data about a table that is being used as a source for a recovery stream.
     * Includes the id of the table as well as the name for human readability.
     * Also lists the destinations where blocks of recovery data should be sent.
     */
    public static class RecoveryTable {
        final String m_name;

        /**
         * What phase of recovery is this table currently in? e.g.
         * is it stream the base data or streaming updates.
         * This will be SCAN_TUPLES or SCAN_COMPLETE.
         */
        RecoveryMessageType m_phase;

        /**
         * Id of the table that is being used a source of recovery data.
         */
        final int m_tableId;

        /**
         * Destinations that the recovery data is supposed to go to.
         * Will be null if this partition is a recipient.
         */
        final long m_destinationIds[];

        public RecoveryTable(String tableName, int tableId, HashSet<Long> destinations) {
            assert(destinations.size() == 1);
            m_name = tableName;
            m_tableId = tableId;
            m_destinationIds = new long[destinations.size()];
            int ii = 0;
            for (Long destinationId : destinations) {
                m_destinationIds[ii++] = destinationId;
            }
            m_phase = RecoveryMessageType.ScanTuples;
        }
    }

    /**
     * The source site may have to decide to abort recovery if the recovering site is no longer available.
     * Because doRecoveryWork is blocking this method will eventually return back to doRecoveryWork
     */
    @Override
    public void handleSiteFaults(HashSet<Long> failedSites, SiteTracker tracker) {
        long destinationSite = 0;
        for (RecoveryTable table : m_tablesToStream) {
            destinationSite = table.m_destinationIds[0];
        }
        if (failedSites.contains(destinationSite)) {
            recoveryLog.error("Failing recovery of " + CoreUtils.hsIdToString(destinationSite) +
                    " at source site " + CoreUtils.hsIdToString(m_siteId));
            final BBContainer origin = org.voltcore.utils.DBBPool.allocateDirect(m_bufferLength);
            try {
                long bufferAddress = 0;
                if (VoltDB.getLoadLibVOLTDB()) {
                    bufferAddress = org.voltcore.utils.DBBPool.getBufferAddress(origin.b);
                }
                final BBContainer buffer = new BBContainer(origin.b, bufferAddress) {
                    @Override
                    public void discard() {
                    }
                };
                handleFailure(buffer);
            } finally {
                origin.discard();
                closeIO();
            }
        }
    }

    /**
     * Flush the recovery stream for the table that is currently being streamed.
     * Then clear the list of tables to stream.
     */
    private final void handleFailure(BBContainer container) {
        container.b.clear();
        while (!m_tablesToStream.isEmpty()) {
            ByteBuffer buffer = container.b;
            /*
             * Set the position to where the EE should serialize its portion of the recovery message.
             * RecoveryMessage.getHeaderLength() defines the position where the EE portion starts.
             */
            buffer.clear();
            buffer.position(messageTypeOffset);
            RecoveryTable table = m_tablesToStream.peek();

            /*
             * Ask the engine to serialize more data.
             */
            int serialized = m_engine.tableStreamSerializeMore(container, table.m_tableId, TableStreamType.RECOVERY);

            if (serialized <= 0) {
                /*
                 * Unlike COW the EE will actually serialize a message saying that recovery is
                 * complete so it should never return 0 bytes written. It will return 0 bytes
                 * written if a table is asked for more data when the recovery stream is not active
                 * or complete (same thing really).
                 */
                VoltDB.crashLocalVoltDB("Error in recovery stream", false, null);
            } else {
                //Position should be unchanged from when tableStreamSerializeMore was called.
                //Set the limit based on how much data the EE serialized past that position.
                buffer.limit(buffer.position() + serialized);
            }

            RecoveryMessageType type = RecoveryMessageType.values()[buffer.get(messageTypeOffset)];
            if (type == RecoveryMessageType.Complete) {
                m_tablesToStream.clear();
            }
        }
    }

    /**
     * Create a recovery site processor
     * @param tableToSites The key pair contains the name and id of the table and the
     *  value contains the set of destination sites where the recovery data should be sent to.
     *  Currently only one site can be delivered too.
     * @param onCompletion What to do when data recovery is complete.
     */
    public RecoverySiteProcessorSource(
            ExecutionSite site,
            long destinationTxnId,
            long destinationSiteId,
            long destHSId, // HSId of the destination mailbox
            HashMap<Pair<String, Integer>, HashSet<Long>> tableToSites,
            ExecutionEngine engine,
            Mailbox mailbox,
            Mailbox dataMailbox,
            final long siteId,
            Runnable onCompletion,
            MessageHandler messageHandler) {
        super();
        m_site = site;
        m_mailbox = mailbox;
        m_mb = dataMailbox;
        m_destHSId = destHSId;
        m_engine = engine;
        m_siteId = siteId;
        m_destinationSiteId = destinationSiteId;
        m_destinationStoppedBeforeTxnId = destinationTxnId;
        m_messageHandler = messageHandler;
        for (Map.Entry<Pair<String, Integer>, HashSet<Long>> entry : tableToSites.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            m_tablesToStream.add(
                    new RecoveryTable(
                            entry.getKey().getFirst(),
                            entry.getKey().getSecond(),
                            entry.getValue()));
        }
        m_onCompletion = onCompletion;
        RecoveryTable table = m_tablesToStream.peek();
        if (!m_engine.activateTableStream(table.m_tableId, TableStreamType.RECOVERY )) {
            VoltDB.crashLocalVoltDB("Attempted to activate recovery stream for table "
                    + table.m_name + " and failed", false, null);
        }
        m_outThread.start();
        m_inThread.start();
    }

    /**
     * Notify the destination that this site is blocked on a multi-part txn.
     * The destination should continue execution until it has finished the multi-part
     * and then block waiting for the initiate message that will contain the txnid
     * to stop at.
     * @param currentTxnId
     */
    @Override
    public void notifyBlockedOnMultiPartTxn(long currentTxnId) {
        if (!m_sentSkipPastMultipartMsg && !m_sentInitiateResponse) {
            m_sentSkipPastMultipartMsg = true;
            recoveryLog.info(
                    "Sending blocked on multi-part notification from " + CoreUtils.hsIdToString(m_siteId) +
                    " at txnId " + currentTxnId + " to site " + CoreUtils.hsIdToString(m_destinationSiteId));
            ByteBuffer buf = ByteBuffer.allocate(25);
            BBContainer cont = DBBPool.wrapBB(buf);
            buf.putInt(21);//Length prefix
            buf.putLong(m_siteId);
            buf.put(kEXECUTE_PAST_TXN);
            buf.putLong(currentTxnId);
            buf.putInt(0); // placeholder for USO sync string
            buf.flip();
            m_outgoing.offer(cont);
        }
    }

    private static final byte kSTOP_AT_TXN = 0;
    private static final byte kEXECUTE_PAST_TXN = 1;

    /**
     * Stream out recovery messages from tables until all tables have been completely streamed.
     * This mechanism is a little delicate because it is the only place in the system
     * where VoltMessage's are backed by pooled direct byte buffers. Because of this it is necessary
     * that the pooled byte buffer be sent all the way to the NIOWriteStream and then discarded. The
     * current implementation of HostMessenger and ForeignHost does this, but only when delivering to a
     * single site.
     */
    @Override
    public void doRecoveryWork(long nextTxnId) {

        /*
         * Re-enable sending the skip past multi-part message. If the site passes
         * through this loop for a new txn it might be a multi-part that will
         * block if the destination doesn't skip past it re-enable informing
         * the destinatino.
         */
        m_sentSkipPastMultipartMsg = false;

        /*
         * Send an initiate response to the recovering partition with txnid it should stop at
         * before receiving the recovery data. Pick a txn id that is >= the next txn that
         * hasn't been executed yet at this partition. We know the next txnId from nextTxnId.
         * That will be an actual transactionId, or minimum safe txnId based on heartbeats if
         * the priority queue is empty.
         */
        if (!m_sentInitiateResponse) {
            m_stopBeforeTxnId = Math.max(nextTxnId, m_destinationStoppedBeforeTxnId);
            recoveryLog.info(
                    "Sending recovery initiate response from " + CoreUtils.hsIdToString(m_siteId) +
                    " before txnId " + nextTxnId + " to site " + CoreUtils.hsIdToString(m_destinationSiteId) +
                    " choosing to stop before txnId " + m_stopBeforeTxnId);
            m_sentInitiateResponse = true;

            // get the export table info
            StringBuilder sb = new StringBuilder();
            if (m_site != null) {
                for (Table t : m_site.m_context.database.getTables()) {
                    if (!CatalogUtil.isTableExportOnly(m_site.m_context.database, t))
                        continue;
                    // now export tables only
                    String sig = t.getSignature();
                    sb.append(sig);
                    long[] temp = m_engine.getUSOForExportTable(sig);
                    sb.append(",").append(temp[0]).append(",").append(temp[1]).append("\n");
                }
                assert(m_site.ee == m_engine);
                //System.err.printf("SiteID: %d\n", m_site.m_siteId);
                //System.err.println("USO SOURCE: " + sb.toString());
            }

            // get the bytes for the export info
            byte[] exportUSOBytes = null;
            try { exportUSOBytes = sb.toString().getBytes("UTF-8"); }
            catch (UnsupportedEncodingException e) {}

            // write the message
            ByteBuffer buf = ByteBuffer.allocate(25 + exportUSOBytes.length);
            BBContainer cont = DBBPool.wrapBB(buf);
            buf.putInt(25 + exportUSOBytes.length); // length prefix
            buf.putLong(m_siteId);
            buf.put(kSTOP_AT_TXN);
            buf.putLong(m_stopBeforeTxnId);
            buf.putInt(exportUSOBytes.length);
            buf.put(exportUSOBytes);

            buf.flip();
            System.out.println("Offering initiate response");
            m_outgoing.offer(cont);
        }

        /*
         * Need to execute more transactions to approach the txn id
         * that we agreed to stop before. The nextTxnId will be a txnId that is for the
         * next transaction to execute that is greater then the one agreed to stop at (if the agreement
         * was to stop at an actual heartbeat) or it may be greater if nextTxnId is based on heartbeat data
         * and not actual work.
         */
        if (nextTxnId < m_stopBeforeTxnId) {
            return;
        }
        recoveryLog.info(
                "Starting recovery of " + CoreUtils.hsIdToString(m_destinationSiteId) + " work before txnId " + nextTxnId);
        while (true) {
            while (m_allowedBuffers.get() > 0 && !m_tablesToStream.isEmpty() && !m_buffers.isEmpty()) {
                /*
                 * Retrieve a buffer from the pool and decrement the number of buffers we are allowed
                 * to send.
                 */
                m_allowedBuffers.decrementAndGet();
                BBContainer container = m_buffers.poll();
                ByteBuffer buffer = container.b;

                /*
                 * Set the Java portion of the message that contains
                 * the source (siteId) as well as blockIndex (for acks)
                 * Leave room for the length prefix. The end position
                 * is where the EE will start serializing data
                 */
                buffer.clear();
                buffer.position(4);
                buffer.putLong(m_siteId);
                buffer.putInt(m_blockIndex++);

                RecoveryTable table = m_tablesToStream.peek();

                /*
                 * Ask the engine to serialize more data.
                 */
                long startSerializing = System.currentTimeMillis();
                int serialized = m_engine.tableStreamSerializeMore(container, table.m_tableId, TableStreamType.RECOVERY);
                long endSerializing = System.currentTimeMillis();
                m_timeSpentSerializing.addAndGet(endSerializing - startSerializing);

                recoveryLog.trace("Serialized " + serialized + " for table " + table.m_name);
                if (serialized <= 0) {
                    /*
                     * Unlike COW the EE will actually serialize a message saying that recovery is
                     * complete so it should never return 0 bytes written. It will return 0 bytes
                     * written if a table is asked for more data when the recovery stream is not active
                     * or complete (same thing really).
                     */
                    VoltDB.crashLocalVoltDB("Recovery stream error", false, null);
                } else {
                    //Position should be unchanged from when tableStreamSerializeMore was called.
                    //Set the limit based on how much data the EE serialized past that position.
                    buffer.limit(buffer.position() + serialized);
                    buffer.putInt(0, buffer.limit() - 4);
                    buffer.position(0);
                }
                m_bytesSent += buffer.remaining();

                RecoveryMessageType type = RecoveryMessageType.values()[buffer.get(messageTypeOffset)];
                /*
                 * If the EE encoded a recovery message with the type complete it indicates that there is no
                 * more data for this table. Remove it from the list of tables waiting to be recovered.
                 * The EE automatically cleans up its recovery data after it generates this message.
                 * This message still needs to be forwarded to the destinations so they know not
                 * to expect more data for that table.
                 */
                if (type == RecoveryMessageType.Complete) {
                    m_tablesToStream.poll();
                    RecoveryTable nextTable = m_tablesToStream.peek();
                    if (nextTable != null) {
                        if (!m_engine.activateTableStream(nextTable.m_tableId, TableStreamType.RECOVERY )) {
                            VoltDB.crashLocalVoltDB("Attempted to activate recovery stream for table "
                                    + nextTable.m_name + " and failed", false, null);
                        }
                    }
                }

                final int numDestinations = table.m_destinationIds.length;

                //Record that we are expecting this number of acks for this block
                //before more data should be sent. The complete message is also acked
                //and is given a block index.
                m_ackTracker.waitForAcks( buffer.getInt(blockIndexOffset), numDestinations);

                m_outgoing.offer(container);
            }

            if (m_tablesToStream.isEmpty()) {
                if (!m_recoveryComplete) {
                    /*
                     * Make sure all buffers get discarded. Those that have been returned already
                     * are discarded. Those that will be returned will check m_recoveryComplete
                     * and behave appropriately
                     */
                    synchronized (this) {
                        m_recoveryComplete = true;
                        BBContainer buffer = null;
                        while ((buffer = m_buffers.poll()) != null) {
                            m_bufferToOriginMap.remove(buffer).discard();
                        }
                    }
                }
                /*
                 * Extended timeout to 60 seconds. With 2 servers on one node
                 * with a lot of load this timeout was problematic at 5 seconds.
                 */
                final long startWait = System.currentTimeMillis();
                while (m_inThread.isAlive() && System.currentTimeMillis() - startWait < 60000) {
                    /*
                     * Process mailbox messages as part of this loop. This is necessary to ensure the txn
                     * ordering and heartbeat process moves forward at the recovering partition.
                     */
                    checkMailbox(false);
                }
                //Make sure that the last ack is processed and onCompletion is run
                synchronized (this) {
                    if (m_inThread.isAlive()) {
                        recoveryLog.error("Timed out waiting for acks for the last few recovery messages");
                        closeIO();
                    }
                    if (m_onCompletion != null) {
                        m_onCompletion.run();
                        m_onCompletion = null;
                    }
                }
                return;
            }

            checkMailbox(true);
        }
    }

    private void checkMailbox(boolean block) {
        VoltMessage message = null;
        if (block) {
            message = m_mailbox.recvBlocking();
        } else {
            message = m_mailbox.recv();
        }
        if (message != null) {
            if (message instanceof RecoveryMessage) {
                RecoveryMessage rm = (RecoveryMessage)message;
                if (!rm.recoveryMessagesAvailable()) {
                    recoveryLog.error("Received a recovery initiate request from " +
                            CoreUtils.hsIdToString(rm.sourceSite()) +
                            " while a recovery was already in progress. Ignoring it.");
                }
            } else {
                m_messageHandler.handleMessage(message, Long.MIN_VALUE);
            }
        }
    }

    public static RecoverySiteProcessorSource createProcessor(
            ExecutionSite site,
            RecoveryMessage rm,
            Database db,
            SiteTracker tracker,
            ExecutionEngine engine,
            Mailbox mailbox,
            final long siteId,
            Runnable onCompletion,
            MessageHandler messageHandler) {
        final long destinationSiteId = rm.sourceSite();
        /*
         * First make sure the recovering partition didn't go down before this message was received.
         * Return null so no recovery work is done.
         */
        if (!tracker.getAllSites().contains(destinationSiteId)) {
            return null;
        }

        ArrayList<Pair<String, Integer>> tables = new ArrayList<Pair<String, Integer>>();
        Iterator<Table> ti = db.getTables().iterator();
        while (ti.hasNext()) {
            Table t = ti.next();
            if (!CatalogUtil.isTableExportOnly( db, t) && t.getMaterializer() == null) {
                tables.add(Pair.of(t.getTypeName(), t.getRelativeIndex()));
            }
        }

        recoveryLog.info("Found " + tables.size() + " tables to recover");
        HashMap<Pair<String, Integer>, HashSet<Long>> tableToDestinationSite =
            new HashMap<Pair<String, Integer>, HashSet<Long>>();
        for (Pair<String, Integer> table : tables) {
            recoveryLog.info("Initiating recovery for table " + table.getFirst());
            HashSet<Long> destinations = tableToDestinationSite.get(table);
            if (destinations == null) {
                destinations = new HashSet<Long>();
                tableToDestinationSite.put(table, destinations);
            }
            destinations.add(destinationSiteId);
        }


        RecoverySiteProcessorSource source = null;
        final long destinationTxnId = rm.txnId();
        source = new RecoverySiteProcessorSource(
                                                 site,
                                                 destinationTxnId,
                                                 destinationSiteId,
                                                 rm.getHSId(),
                                                 tableToDestinationSite,
                                                 engine,
                                                 mailbox,
                                                 VoltDB.instance().getHostMessenger().createMailbox(),
                                                 siteId,
                                                 onCompletion,
                                                 messageHandler);
        return source;
    }

    public static SocketChannel createRecoveryConnection(List<byte[]> addresses, int port) throws IOException {
        SocketChannel sc = null;
        List<Exception> exceptions = new ArrayList<Exception>();

        for (byte address[] : addresses) {
            try {
                InetAddress inetAddr = InetAddress.getByAddress(address);
                InetSocketAddress inetSockAddr = new InetSocketAddress(inetAddr, port);
                recoveryLog.debug("Attempting to create recovery connection to " + inetSockAddr);
                sc = SocketChannel.open(inetSockAddr);
                break;
            } catch (Exception e) {
                recoveryLog.debug("Failed to create a recovery connection", e);
                exceptions.add(e);
            }
        }

        if (sc == null) {
            for (Exception e : exceptions) {
                recoveryLog.fatal("Connection error", e);
            }
            throw new IOException("Unable to create recovery connection due to previously logged exceptions");
        }

        sc.configureBlocking(true);
        sc.socket().setTcpNoDelay(true);

        return sc;
    }

    @Override
    public long bytesTransferred() {
        return m_bytesSent;
    }
}
