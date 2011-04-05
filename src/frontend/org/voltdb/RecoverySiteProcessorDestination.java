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
package org.voltdb;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.RecoveryMessage;
import org.voltdb.messaging.RecoveryMessageType;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.Pair;

/**
 * Manages recovery of a partition. By sending messages via the mailbox system and interacting with
 * the execution engine directly. Uses the ExecutionSites thread to do work via the doRecoveryWork method
 * that is invoked by ExecutionSite.run().
 */
public class RecoverySiteProcessorDestination extends RecoverySiteProcessor {

    /*
     * Some offsets for where data is serialized to/from
     * without the length prefix at the front
     */
    final int siteIdOffset = 0;
    final int blockIndexOffset = siteIdOffset + 4;
    final int messageTypeOffset = blockIndexOffset + 4;
    final int tableIdOffset = messageTypeOffset + 1;

    private static final VoltLogger recoveryLog = new VoltLogger("RECOVERY");

    /**
     * List of tables that need to be streamed
     */
    private final HashMap<Integer, RecoveryTable>  m_tables = new HashMap<Integer, RecoveryTable>();

    /**
     * The engine that will be the destination for recovery data
     */
    private final ExecutionEngine m_engine;

    /**
     * Mailbox used to send acks and receive recovery messages
     */
    private final Mailbox m_mailbox;

    /**
     * Encoded in acks to show where they came from
     */
    private final int m_siteId;

    /**
     * What to do when data recovery is completed
     */
    private final Runnable m_onCompletion;

    /**
     * Transaction to stop before and do the sync
     */
    private long m_stopBeforeTxnId = Long.MAX_VALUE;

    /**
     * Transaction the source partition has requested that this destination
     * execute past. This allwos the source partition to finish any pending
     * multi-partition txns.
     */
    private Long m_skipPastTxnId = null;

    private final Semaphore m_toggleProfiling = new Semaphore(0);

    private final MessageHandler m_messageHandler;

    private final int m_sourceSiteId;

    private boolean m_sentInitiate = false;

    private long m_bytesReceived = 0;

    private long m_timeSpentHandlingData = 0;

    private SocketChannel m_sc;

    private final LinkedBlockingQueue<BBContainer> m_incoming = new LinkedBlockingQueue<BBContainer>();

    private IODaemon m_iodaemon;

    private final int m_partitionId;

    private class IODaemon {

        private final SocketChannel m_sc;
        private volatile boolean closed = false;
        private volatile Exception m_lastException = null;
        private final Thread m_inThread = new Thread("Recovery in thread for site " + m_siteId) {
            @Override
            public void run() {
                try {
                    while (true) {
                        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
                        while (lengthBuffer.hasRemaining()) {
                            int read = m_sc.read(lengthBuffer);
                            if (read == -1) {
                                throw new EOFException();
                            }
                        }
                        lengthBuffer.flip();

                        BBContainer container = m_buffers.take();
                        boolean success = false;
                        try {
                            ByteBuffer messageBuffer = container.b;
                            messageBuffer.clear();
                            messageBuffer.limit(lengthBuffer.getInt());
                            while(messageBuffer.hasRemaining()) {
                                int read = m_sc.read(messageBuffer);
                                if (read == -1) {
                                    throw new EOFException();
                                }
                            }
                            messageBuffer.flip();
                            recoveryLog.trace("Received message");
                            m_incoming.offer(container);
                            m_mailbox.deliver(new RecoveryMessage());
                            success = true;
                        } finally {
                            if (!success) {
                                container.discard();
                            }
                        }
                    }
                } catch (IOException e) {
                    m_lastException = e;
                    /*
                     * Wait until the last message is delivered
                     * and then wait some more so it can be processed
                     * so that closed can be set and the exception
                     * suppressed.
                     */
                    try {
                        while (!m_incoming.isEmpty()) {
                            Thread.sleep(50);
                        }
                        Thread.sleep(300);
                    } catch (InterruptedException e2) {}
                    if (closed) {
                        return;
                    }
                    recoveryLog.error(
                            "Error reading a message from a recovery stream.", e);
                } catch (InterruptedException e) {
                    return;
                }
            }
        };

        public IODaemon(SocketChannel sc) throws IOException {
            m_sc = sc;
            m_inThread.start();
        }

        public void close() {
            closed = true;
            m_inThread.interrupt();
            try {
                m_sc.close();
            } catch (IOException e) {
            }
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

        final int m_sourceSiteId;

        public RecoveryTable(String tableName, int tableId, int sourceSiteId) {
            m_name = tableName;
            m_tableId = tableId;
            m_sourceSiteId = sourceSiteId;
            m_phase = RecoveryMessageType.ScanTuples;
        }
    }

    static {
        //new VoltLogger("RECOVERY").setLevel(Level.TRACE);
    }

    public void handleRecoveryMessage(ByteBuffer message, long pointer) {
        RecoveryMessageType type = RecoveryMessageType.values()[message.get(messageTypeOffset)];
        assert(type == RecoveryMessageType.ScanTuples ||
                type == RecoveryMessageType.Complete);
        message.getInt(siteIdOffset);
        final int blockIndex = message.getInt(blockIndexOffset);
        final int tableId = message.getInt(tableIdOffset);
        ByteBuffer ackMessage = ByteBuffer.allocate(12);
        ackMessage.putInt( 8);
        ackMessage.putInt( m_siteId);
        ackMessage.putInt( blockIndex);
        ackMessage.flip();
        if (type == RecoveryMessageType.ScanTuples) {
            if (m_toggleProfiling.tryAcquire()) {
                m_engine.toggleProfiler(1);
            }
            m_bytesReceived += message.remaining();
            long startTime = System.currentTimeMillis();
            message.position(messageTypeOffset);
            m_engine.processRecoveryMessage( message, pointer);
            long endTime = System.currentTimeMillis();
            m_timeSpentHandlingData += endTime - startTime;
            recoveryLog.trace("Received tuple data at site " + m_siteId +
                    " for table " + m_tables.get(tableId).m_name);
        } else if (type == RecoveryMessageType.Complete) {
            message.position(messageTypeOffset + 5);

//            long seqNo = message.getLong();
//            long bytesUsed = message.getLong();
//            int signatureLength = message.getInt();
//            assert(signatureLength > 0);
//            byte signatureBytes[] = new byte[signatureLength];
//            String signature;
//            try {
//                signature = new String(signatureBytes, "UTF-8");
//            } catch (UnsupportedEncodingException e) {
//                throw new RuntimeException(e);
//            }
//            assert(seqNo >= -1);
            RecoveryTable table = m_tables.remove(tableId);
            recoveryLog.info("Received completion message at site " + m_siteId +
                    " for table " + table.m_name );//+ " with export info (" + seqNo +
            if (m_tables.isEmpty()) {
                /*
                 * Send ack for the last thing, but if the other side is gone, no worries
                 */
                while(ackMessage.hasRemaining()) {
                    int written = 0;
                    try {
                        written = m_sc.write(ackMessage);
                    } catch (IOException e) {
                        break;
                    }
                    if (written == -1) {
                        break;
                    }
                }
                return;
            }
//                    "," + bytesUsed + ")");
//            if (seqNo >= 0) {
//                m_engine.exportAction( true, bytesUsed, seqNo, m_partitionId, signature);
//            }

        } else {
            recoveryLog.fatal("Received an unexpect message of type " + type);
            VoltDB.crashVoltDB();
        }
        while(ackMessage.hasRemaining()) {
            int written = 0;
            try {
                written = m_sc.write(ackMessage);
            } catch (IOException e) {
                recoveryLog.fatal("Unable to write ack message", e);
                VoltDB.crashVoltDB();
            }
            if (written == -1) {
                recoveryLog.fatal("Unable to write ack message");
                VoltDB.crashVoltDB();
            }
        }
        recoveryLog.trace("Writing ack for block " + blockIndex + " from " + m_siteId);
    }

    /**
     * Send the initiate message if necessary. Block if the sync after txn has been completed
     * and perform recovery work.
     */
    @Override
    public void doRecoveryWork(long txnId) {
        if (!m_sentInitiate) {
            m_sentInitiate = true;
            try {
                sendInitiateMessage(txnId);
            } catch (IOException e) {
                recoveryLog.fatal("Error sending initiate message", e);
                VoltDB.crashVoltDB();
            }
        }

        /**
         * If the source partition provided a txnid this partition must execute past
         * (because it is multi-part) then return control here so it can be executed
         * past. If it has been executed past, go read the next message from the source.
         * This will be another txnid to skip past or the txnid to sync at.
         */
        if (m_skipPastTxnId != null) {
            if (m_skipPastTxnId >= txnId) {
                return;
            } else {
                m_skipPastTxnId = null;
                try {
                    processNextInitiateResponse(txnId);
                } catch (IOException e) {
                    recoveryLog.fatal("Error process a second initiate response", e);
                    VoltDB.crashVoltDB();
                }
            }
        }

        if (txnId < m_stopBeforeTxnId) {
            return;
        }

        recoveryLog.trace(
                "Starting recovery before txnid " + txnId +
                " for site " + m_siteId + " from " + m_sourceSiteId);


        while (true) {
            BBContainer container = null;
            while ((container = m_incoming.poll()) != null) {
                try {
                    handleRecoveryMessage(container.b, container.address);
                } finally {
                    container.discard();
                }
            }

            if (m_tables.isEmpty()) {
                synchronized (this) {
                    m_recoveryComplete = true;
                    BBContainer buffer = null;
                    while ((buffer = m_buffers.poll()) != null) {
                        m_bufferToOriginMap.remove(buffer).discard();
                    }
                }
                recoveryLog.info("Processor spent " + (m_timeSpentHandlingData / 1000.0) + " seconds handling data");
                m_onCompletion.run();
                m_iodaemon.close();
                return;
            }

            if (!checkMailbox(false, txnId)) {
                Thread.yield();
            }
        }
    }

    private boolean checkMailbox(boolean block, long txnId) {
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
                    recoveryLog.error("Received a recovery initiate request from " + rm.sourceSite() +
                            " while a recovery was already in progress. Ignoring it.");
                }
            } else {
                m_messageHandler.handleMessage(message, txnId);
            }
        }
        return message != null;
    }

    /**
     * Create a recovery site processor
     * @param tableToSites The key pair contains the name and id of the table and the value
     * contains the source site for the recovery data
     * @param onCompletion What to do when data recovery is complete.
     */
    public RecoverySiteProcessorDestination(
            HashMap<Pair<String, Integer>, Integer> tableToSourceSite,
            ExecutionEngine engine,
            Mailbox mailbox,
            final int siteId,
            final int partitionId,
            Runnable onCompletion,
            MessageHandler messageHandler) {
        super();
        m_mailbox = mailbox;
        m_engine = engine;
        m_siteId = siteId;
        m_partitionId = partitionId;
        m_messageHandler = messageHandler;

        /*
         * Only support recovering from one partition for now so just grab
         * the first source site and send it the initiate message containing
         * the txnId this site stopped at
         */
        int sourceSiteId = 0;
        for (Map.Entry<Pair<String, Integer>, Integer> entry : tableToSourceSite.entrySet()) {
            m_tables.put(entry.getKey().getSecond(), new RecoveryTable(entry.getKey().getFirst(), entry.getKey().getSecond(), entry.getValue()));
            sourceSiteId = entry.getValue();
        }
        m_sourceSiteId = sourceSiteId;
        m_onCompletion = onCompletion;
        //assert(!m_tables.isEmpty());

    }

    /**
     * Need a separate method outside of constructor so that
     * failures can be delivered to this processor while waiting
     * for a response to the initiate message
     */
    public void sendInitiateMessage(long txnId) throws IOException {
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        InetAddress addr = InetAddress.getByName(VoltDB.instance().getConfig().m_selectedRejoinInterface);
        InetSocketAddress sockAddr = new InetSocketAddress( addr, 0);
        ssc.socket().bind(sockAddr);
        final int port = ssc.socket().getLocalPort();
        final byte address[] = ssc.socket().getInetAddress().getAddress();
        ByteBuffer buf = ByteBuffer.allocate(2048);
        BBContainer container = DBBPool.wrapBB(buf);
        RecoveryMessage recoveryMessage = new RecoveryMessage(container, m_siteId, txnId, address, port);
        recoveryLog.trace(
                "Sending recovery initiate request before txnid " + txnId +
                " from site " + m_siteId + " to " + m_sourceSiteId);
        try {
            m_mailbox.send( m_sourceSiteId, 0, recoveryMessage);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

        final long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 5000) {
            try {
                m_sc = ssc.accept();
                if (m_sc != null) {
                    break;
                }
            } catch (IOException e) {
                ssc.close();
            }
            Thread.yield();
        }
        if (m_sc == null) {
            recoveryLog.fatal("Timed out waiting for connection from source partition");
            VoltDB.crashVoltDB();
        }
        ssc.close();
        m_sc.configureBlocking(true);
        m_sc.socket().setTcpNoDelay(true);

        m_iodaemon = new IODaemon(m_sc);

        processNextInitiateResponse(txnId);

        return;
    }

    /**
     * Get the next initiate response. There might be a few to work the system passed
     * multi-part transactions.
     * @throws IOException
     */
    private void processNextInitiateResponse(long txnId) throws IOException {
        final long startTime = System.currentTimeMillis();
        /*
         * Poll the mailbox so that heartbeat and txns messages are received. It is necessary
         * to participate in that process so the source site can unblock its priority queue if it
         * is blocked on safety or ordering.
         */
        while (m_incoming.peek() == null && System.currentTimeMillis() - startTime < 5000) {
            checkMailbox(false, txnId);
            Thread.yield();
        }
        if (m_incoming.peek() == null) {
            recoveryLog.fatal("Timed out waiting to read recovery initiate ack message");
            VoltDB.crashVoltDB();
        }
        if (m_iodaemon.m_lastException != null) {
            recoveryLog.fatal(
                    "There was an error while reading the recovery initiate ack message", m_iodaemon.m_lastException);
            VoltDB.crashVoltDB();
        }

        BBContainer ackMessageContainer = m_incoming.poll();
        ByteBuffer ackMessage = ackMessageContainer.b;
        final int sourceSite = ackMessage.getInt();
        //True if the txn Id should be stopped before
        //False if the txn id should executed past. After executing past wait for new instructions
        //      in processNextInitiateResponse
        final boolean stopBeforeOrSkipPast = ackMessage.get() == 0 ? true : false;
        if (stopBeforeOrSkipPast) {
            m_stopBeforeTxnId = ackMessage.getLong();
            recoveryLog.info("Recovery initiate ack received at site " + m_siteId + " from site " +
                    sourceSite + " will sync after txnId " + m_stopBeforeTxnId);
        } else {
            m_skipPastTxnId = ackMessage.getLong();
            recoveryLog.info("Recovery initiate ack received at site " + m_siteId + " from site " +
                    sourceSite + " will delay sync until after executing txnId " + m_skipPastTxnId);
        }

        // get the information about ackOffsets and seqNo from the export tables
        // this is a set of newline-delimited strings, one per export-table
        // each a triple of the form: "table-signature,ackOffset,seqNo"
        int byteLen = ackMessage.getInt();
        if (byteLen > 0) {
            byte[] exportUSOBytes = new byte[byteLen];
            ackMessage.get(exportUSOBytes);
            String exportUSOs = new String(exportUSOBytes, "UTF-8");

            // parse the strings and set the values in the engine
            String[] tableUSOs = exportUSOs.trim().split("\n");
            for (String tableUSO : tableUSOs) {
                String[] parts = tableUSO.trim().split(",");
                assert(parts.length == 3);
                long ackOffset = Long.parseLong(parts[1]);
                long seqNo = Long.parseLong(parts[2]);
                m_engine.exportAction(true, ackOffset, seqNo, m_partitionId, parts[0]);
            }
        }

        ackMessageContainer.discard();
    }

    /*
     * On a failure a destination needs to check if its source site was on the list and if it
     * is it should call crash VoltDB.
     */
    @Override
    public void handleSiteFaults(HashSet<Integer> failedSites,
            SiteTracker tracker) {
        for (Map.Entry<Integer, RecoveryTable> entry : m_tables.entrySet()) {
            if (failedSites.contains(entry.getValue().m_sourceSiteId)) {
                recoveryLog.fatal("Node fault during recovery of Site " + m_siteId +
                        " resulted in source Site " + entry.getValue().m_sourceSiteId +
                        " becoming unavailable. Failing recovering node.");
                VoltDB.crashVoltDB();
            }
        }
    }

    public static RecoverySiteProcessorDestination createProcessor(
            Database db,
            SiteTracker tracker,
            ExecutionEngine engine,
            Mailbox mailbox,
            final int siteId,
            Runnable onCompletion,
            MessageHandler messageHandler) {
        ArrayList<Pair<String, Integer>> tables = new ArrayList<Pair<String, Integer>>();
        Iterator<Table> ti = db.getTables().iterator();
        while (ti.hasNext()) {
            Table t = ti.next();
            if (!CatalogUtil.isTableExportOnly( db, t) && t.getMaterializer() == null) {
                tables.add(Pair.of(t.getTypeName(),t.getRelativeIndex()));
            }
        }
        int partitionId = tracker.getPartitionForSite(siteId);
        ArrayList<Integer> sourceSites = new ArrayList<Integer>(tracker.getLiveSitesForPartition(partitionId));
        sourceSites.remove(new Integer(siteId));

        if (sourceSites.isEmpty()) {
            recoveryLog.fatal("Could not find a source site for siteId " + siteId + " partition id " + partitionId);
            VoltDB.crashVoltDB();
        }

        HashMap<Pair<String, Integer>, Integer> tableToSourceSite =
            new HashMap<Pair<String, Integer>, Integer>();
        for (Pair<String, Integer> table : tables) {
            tableToSourceSite.put( table, sourceSites.get(0));
        }

        return new RecoverySiteProcessorDestination(
                tableToSourceSite,
                engine,
                mailbox,
                siteId,
                partitionId,
                onCompletion,
                messageHandler);
    }

    @Override
    public long bytesTransferred() {
        return m_bytesReceived;
    }

    @Override
    public void notifyBlockedOnMultiPartTxn(long currentTxnId) {
        //Don't need to do anything at the destination
    }
}
