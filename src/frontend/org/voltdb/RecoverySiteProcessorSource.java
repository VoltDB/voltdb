/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayDeque;

import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.RecoveryMessage;
import org.voltdb.messaging.RecoveryMessageType;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.messaging.Mailbox;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.Pair;

/**
 * Encapsulates the state managing the activities related to streaming recovery data
 * to some remote partitions. This class is only used on the partition that is a source of recovery data.
 *
 */
public class RecoverySiteProcessorSource implements RecoverySiteProcessor {

    /** Number of buffers to use */
    static final int m_numBuffers = 3;

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger recoveryLog = new VoltLogger("RECOVERY");

    /**
     * Pick a buffer length that is big enough to store at least one of the largest size tuple supported
     * in the system (2 megabytes). Add a fudge factor for metadata.
     */
    public static final int m_bufferLength = (1024 * 1024 * 2) + Short.MAX_VALUE;

    /**
     * Keep track of the origin for each buffer so that they can be freed individually
     * as the each thread with ownership of the buffer discard them post recovery complettion.
     */
    private final HashMap<BBContainer, BBContainer> m_bufferToOriginMap = new HashMap<BBContainer, BBContainer>();
    private final ConcurrentLinkedQueue<BBContainer> m_buffers
                                                                            = new ConcurrentLinkedQueue<BBContainer>();

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
     * Generate unique identifiers for each block of table data sent over the wire
     */
    private int m_blockIndex = 0;

    private final AckTracker m_ackTracker = new AckTracker();

    /**
     * Placed in each message as a return address for acks
     */
    private final int m_siteId;

    /**
     * Number of buffers that can be sent before having to wait for acks to previously sent buffers
     * This limits the rate at which buffers are generated to ensure that the remote partitions can keep
     * up.
     */
    private int m_allowedBuffers = m_numBuffers;

    /**
     * After the last table has been streamed the buffers should be returned to the global pool
     * This is set to true while the RecoverySiteProcessorSource lock is held
     * and any already returned buffers are then returned to the global pool. Buffers
     * that have not been returned also check this value while the lock is held
     * to ensure that they are returned to the global pool as well.
     */
    private boolean m_recoveryComplete = false;

    /**
     * What to do when all recovery data has been sent
     */
    private final Runnable m_onCompletion;

    /**
     * What to do when the initiate message is received from the recovering partition.
     * Returns the txnId this EE will stop at and block on streaming out data
     */
    private final OnRecoveringPartitionInitiate m_onInitiate;

    /**
     * Transaction to stop and send recovery data at
     */
    private long m_txnIdToBlockAfter = Long.MAX_VALUE;

    private final MessageHandler m_messageHandler;

    /**
     * When the recovering partition initiates the streaming of recovery data it will specify
     * the transaction id that it stopped after. This handler will take that information
     * and decide what transaction to stop after and stream the data. It will be some txnid >=
     * what was supplied by the recovering partition, and the recovering partition will then
     * skip to the transaction after the one specified by the return value once recovery is complete
     */
    public interface OnRecoveringPartitionInitiate {
        public long pickTxnToStopAfter(long recoveringPartitionTxnId);
    }

    /**
     * Keep track of how many times a block has been acked and how many acks are expected
     */
    private static class AckTracker {
        private final HashMap<Integer, Integer> m_acks = new HashMap<Integer, Integer>();

        private void waitForAcks(int blockIndex, int acksExpected) {
            assert(!m_acks.containsKey(blockIndex));
            m_acks.put(blockIndex, acksExpected);
        }

        private boolean ackReceived(int blockIndex) {
            assert(m_acks.containsKey(blockIndex));
            int acksRemaining = m_acks.get(blockIndex);
            acksRemaining--;
            if (acksRemaining == 0) {
                m_acks.remove(blockIndex);
                return true;
            }
            m_acks.put(blockIndex, acksRemaining);
            return false;
        }

        @SuppressWarnings("unused")
        private void handleNodeFault(HashSet<Integer> failedNodes, SiteTracker tracker) {
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
        final int m_destinationIds[];

        public RecoveryTable(String tableName, int tableId, HashSet<Integer> destinations) {
            assert(destinations.size() == 1);
            m_name = tableName;
            m_tableId = tableId;
            m_destinationIds = new int[destinations.size()];
            int ii = 0;
            for (Integer destinationId : destinations) {
                m_destinationIds[ii++] = destinationId;
            }
            m_phase = RecoveryMessageType.ScanTuples;
        }
    }

    /**
     * Perform necessary recovery
     * @param failedNodes
     * @param tracker
     */
    @Override
    public void handleNodeFault(HashSet<Integer> failedNodes, SiteTracker tracker) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a recovery site processor
     * @param tableToSites The key pair contains the name and id of the table and the
     *  value contains the set of destination sites where the recovery data should be sent to.
     *  Currently only one site can be delivered too.
     * @param onCompletion What to do when data recovery is complete.
     */
    public RecoverySiteProcessorSource(
            HashMap<Pair<String, Integer>, HashSet<Integer>> tableToSites,
            ExecutionEngine engine,
            Mailbox mailbox,
            final int siteId,
            Runnable onCompletion,
            OnRecoveringPartitionInitiate initiateHandler,
            MessageHandler messageHandler) {
        m_mailbox = mailbox;
        m_engine = engine;
        m_siteId = siteId;
        m_onInitiate = initiateHandler;
        m_messageHandler = messageHandler;
        for (Map.Entry<Pair<String, Integer>, HashSet<Integer>> entry : tableToSites.entrySet()) {
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
        if (m_tablesToStream.isEmpty()) {
            onCompletion.run();
            return;
        }
        RecoveryTable table = m_tablesToStream.peek();
        if (!m_engine.activateTableStream(table.m_tableId, TableStreamType.RECOVERY )) {
            hostLog.error("Attempted to activate recovery stream for table "
                    + table.m_name + " and failed");
            VoltDB.crashVoltDB();
        }
        initializeBufferPool();
    }

    void initializeBufferPool() {
        for (int ii = 0; ii < m_numBuffers; ii++) {
            final BBContainer origin = org.voltdb.utils.DBBPool.allocateDirect(m_bufferLength);
            long bufferAddress = 0;
            if (VoltDB.getLoadLibVOLTDB()) {
                bufferAddress = org.voltdb.utils.DBBPool.getBufferAddress(origin.b);
            }
            final BBContainer buffer = new BBContainer(origin.b, bufferAddress) {
                /**
                 * This method is careful to check if recovery is complete and if it is,
                 * return the buffer to the global pool via its origin rather then returning it to this
                 * pool where it will be leaked.
                 */
                @Override
                public void discard() {
                    synchronized (this) {
                        if (m_recoveryComplete) {
                            m_bufferToOriginMap.remove(this).discard();
                            return;
                        }
                    }
                    m_buffers.offer(this);
                }
            };
            m_bufferToOriginMap.put(buffer, origin);
            m_buffers.offer(buffer);
        }
    }

    /**
     * Process acks that are sent by recovering sites
     */
    @Override
    public void handleRecoveryMessage(RecoveryMessage message) {
        assert(message.type() == RecoveryMessageType.Ack || message.type() == RecoveryMessageType.Initiate);
        if (message.type() == RecoveryMessageType.Ack) {
            if (m_ackTracker.ackReceived(message.blockIndex())) {
                m_allowedBuffers++;
            }
        } else if (message.type() == RecoveryMessageType.Initiate) {
            /*
             * This should supply return the last committed txn id. Need this to know if
             * the processor should immediately start doing recovery work.
             */
            final long lastCommittedTxnId = m_onInitiate.pickTxnToStopAfter(Long.MIN_VALUE);

            /*
             * Provide the txn id from the message and see what txnId was picked. Will be lastCommittedTxnId
             * or the one from the message (if that is greater).
             */
            m_txnIdToBlockAfter = m_onInitiate.pickTxnToStopAfter(message.txnId());

            /*
             * Send a response to the recovering partition with txnid it should resume after
             */
            ByteBuffer buf = ByteBuffer.allocate(2048);
            BBContainer cont = DBBPool.wrapBB(buf);
            RecoveryMessage response = new RecoveryMessage(cont, m_siteId, m_txnIdToBlockAfter);
            try {
                m_mailbox.send( message.sourceSite(), 0, response);
            } catch (MessagingException e) {
                throw new RuntimeException(e);
            }

            /*
             * If the txnid to block after is the last commited txn id, start recovery work
             */
            if (lastCommittedTxnId == m_txnIdToBlockAfter) {
                doRecoveryWork(lastCommittedTxnId);
            }
        } else {
            VoltDB.crashVoltDB();
        }
    }

    /**
     * Stream out recovery messages from tables until all tables have been completely streamed.
     * This mechanism is a little delicate because it is the only place in the system
     * where VoltMessage's are backed by pooled direct byte buffers. Because of this it is necessary
     * that the pooled byte buffer be sent all the way to the NIOWriteStream and then discarded. The
     * current implementation of HostMessenger and ForeignHost does this, but only when delivering to a
     * single site.
     *
     * The currentTxnId is an argument that is used to determine if the txnId that this partition will
     * block on until all recovery data is streamed has been reached.
     */
    @Override
    public void doRecoveryWork(long lastCommittedTxnId) {
        if (lastCommittedTxnId < m_txnIdToBlockAfter) {
            return;
        } else if (lastCommittedTxnId > m_txnIdToBlockAfter) {
            recoveryLog.error("Last committed txnId " + lastCommittedTxnId + " is after the txnid to block after " +
                    m_txnIdToBlockAfter + " recovery has failed");
            return;
        }
        while (true) {
            while (m_allowedBuffers > 0 && !m_tablesToStream.isEmpty() && !m_buffers.isEmpty()) {
                /*
                 * Retrieve a buffer from the pool and decrement the number of buffers we are allowed
                 * to send.
                 */
                m_allowedBuffers--;
                BBContainer container = m_buffers.poll();
                ByteBuffer buffer = container.b;

                /*
                 * Constructor will set the Java portion of the message that contains
                 * the return address (siteId) as well as blockIndex (for acks), and the message id
                 * (so that Java knows what class to use to deserialize the message).
                 */
                RecoveryMessage rm = new RecoveryMessage(container, m_siteId, m_blockIndex++);

                /*
                 * Set the position to where the EE should serialize its portion of the recovery message.
                 * RecoveryMessage.getHeaderLength() defines the position where the EE portion starts.
                 */
                buffer.clear();
                buffer.position(RecoveryMessage.getHeaderLength());
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
                    VoltDB.crashVoltDB();
                } else {
                    //Position should be unchanged from when tableStreamSerializeMore was called.
                    //Set the limit based on how much data the EE serialized past that position.
                    buffer.limit(buffer.position() + serialized);
                }
                assert(rm != null);

                /*
                 * If the EE encoded a recovery message with the type complete it indicates that there is no
                 * more data for this table. Remove it from the list of tables waiting to be recovered.
                 * The EE automatically cleans up its recovery data after it generates this message.
                 * This message still needs to be forwarded to the destinations so they know not
                 * to expect more data for that table.
                 */
                if (rm.type() == RecoveryMessageType.Complete) {
                    m_tablesToStream.poll();
                    RecoveryTable nextTable = m_tablesToStream.peek();
                    if (nextTable != null) {
                        if (!m_engine.activateTableStream(nextTable.m_tableId, TableStreamType.RECOVERY )) {
                            hostLog.error("Attempted to activate recovery stream for table "
                                    + nextTable.m_name + " and failed");
                            VoltDB.crashVoltDB();
                        }
                    }
                }

                final int numDestinations = table.m_destinationIds.length;

                //Record that we are expecting this number of acks for this block
                //before more data should be sent.
                m_ackTracker.waitForAcks( rm.blockIndex(), numDestinations);

                /*
                 * The common case is recovering a single destination. Take the slightly faster no
                 * copy path.
                 */
                if (numDestinations == 1) {
                    try {
                        m_mailbox.send(table.m_destinationIds[0], 0, rm);
                    } catch (MessagingException e) {
                        // Continuing to propagate this horrible exception
                        throw new RuntimeException(e);
                    }
                } else {
                    /*
                     * This path is broken right now because this version of send will not discard
                     * the message. We don't plan on using it anyways so no biggie.
                     */
                    VoltDB.crashVoltDB();
    //                try {
    //                    m_mailbox.send(table.m_destinationIdsArray, 0, rm);
    //                } catch (MessagingException e) {
    //                    // Continuing to propagate this horrible exception
    //                    throw new RuntimeException(e);
    //                }
                }
            }

            if (m_tablesToStream.isEmpty()) {
                /*
                 * See if it is possible to discard all the buffers
                 */
                synchronized (this) {
                    m_recoveryComplete = true;
                    BBContainer buffer = null;
                    while ((buffer = m_buffers.poll()) != null) {
                        m_bufferToOriginMap.remove(buffer).discard();
                    }
                }
                m_onCompletion.run();
                return;
            }

            VoltMessage message = m_mailbox.recv();
            if (message != null) {
                if (message instanceof RecoveryMessage) {
                    handleRecoveryMessage((RecoveryMessage)message);
                } else {
                    m_messageHandler.handleMessage(message);
                }

            }
            Thread.yield();
        }
    }
}
