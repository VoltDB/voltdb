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
import java.util.*;

import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.messaging.RecoveryMessage;
import org.voltdb.messaging.RecoveryMessageType;
import org.voltdb.utils.Pair;
import org.voltdb.catalog.*;
import org.voltdb.utils.CatalogUtil;

/**
 * Manages recovery of a partition. By sending messages via the mailbox system and interacting with
 * the execution engine directly. Uses the ExecutionSites thread to do work via the doRecoveryWork method
 * that is invoked by ExecutionSite.run().
 */
public class RecoverySiteProcessorDestination implements RecoverySiteProcessor {
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
    private long m_stopBeforeTxnId;

    private final MessageHandler m_messageHandler;

    private final int m_sourceSiteId;

    private boolean m_sentInitiate = false;


    /*
     * Recovery messages received before the appropriate txn has been committed
     * are buffered and then played back in doRecoveryWork
     */
    public boolean m_recoveryBegan = false;

    private final ArrayList<RecoveryMessage> m_buffered = new ArrayList<RecoveryMessage>();

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

    /**
     * Process acks that are sent by recovering sites
     */
    @Override
    public void handleRecoveryMessage(RecoveryMessage message) {
        assert(message.type() == RecoveryMessageType.ScanTuples ||
                message.type() == RecoveryMessageType.Complete);
        if (!m_recoveryBegan) {
            m_buffered.add(message);
            return;
        }

        if (message.type() == RecoveryMessageType.ScanTuples) {
            m_engine.processRecoveryMessage(message.getMessageData());
            recoveryLog.info("Received tuple data at site " + m_siteId +
                    " for table " + m_tables.get(message.tableId()).m_name);
        } else if (message.type() == RecoveryMessageType.Complete) {
            RecoveryTable table = m_tables.remove(message.tableId());
            recoveryLog.info("Received completion message at site " + m_siteId +
                    " for table " + table.m_name);
        } else {
            System.out.println(message.type());
            VoltDB.crashVoltDB();
        }
        int sourceSite = message.sourceSite();
        RecoveryMessage ack = new RecoveryMessage( message, m_siteId);
        try {
            m_mailbox.send( sourceSite, 0, ack);
        } catch (MessagingException e) {
            // Continuing to propagate this horrible exception
            throw new RuntimeException(e);
        }
    }

    /**
     * Send the initiate message if necessary. Block if the sync after txn has been completed
     * and perform recovery work.
     */
    @Override
    public void doRecoveryWork(long txnId) {
        if (!m_sentInitiate) {
            m_sentInitiate = true;
            sendInitiateMessage(txnId);
        }

        if (txnId < m_stopBeforeTxnId) {
            return;
        }

        m_recoveryBegan = true;
        for (RecoveryMessage rm : m_buffered) {
            handleRecoveryMessage(rm);
        }
        m_buffered.clear();

        while (true) {
            VoltMessage message = m_mailbox.recv();
            if (message != null) {
                if (message instanceof RecoveryMessage) {
                    handleRecoveryMessage((RecoveryMessage)message);
                } else {
                    m_messageHandler.handleMessage(message);
                }
            } else {
                Thread.yield();
            }

            if (m_tables.isEmpty()) {
                m_onCompletion.run();
                return;
            }
        }
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
            Runnable onCompletion,
            MessageHandler messageHandler) {
        m_mailbox = mailbox;
        m_engine = engine;
        m_siteId = siteId;
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
        assert(!m_tables.isEmpty());

    }

    /**
     * Need a separate method outside of constructor so that
     * failures can be delivered to this processor while waiting
     * for a response to the initiate message
     */
    public void sendInitiateMessage(long txnId) {
        ByteBuffer buf = ByteBuffer.allocate(2048);
        BBContainer container = DBBPool.wrapBB(buf);
        RecoveryMessage recoveryMessage = new RecoveryMessage(container, m_siteId, txnId);
        try {
            m_mailbox.send( m_sourceSiteId, 0, recoveryMessage);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

        while (true) {
            VoltMessage message = m_mailbox.recv();
            if (message != null) {
                if (message instanceof RecoveryMessage) {
                    RecoveryMessage rm = (RecoveryMessage)message;
                    if (rm.type() == RecoveryMessageType.Initiate) {
                        m_stopBeforeTxnId = rm.txnId();
                        recoveryLog.info("Recovery initiate ack received at site " + m_siteId + " from site " +
                                rm.sourceSite() + " will sync after txnId " + rm.txnId());
                        return;
                    } else {
                        VoltDB.crashVoltDB();
                    }
                    return;
                } else {
                    m_messageHandler.handleMessage(message);
                }
                continue;
            } else {
                Thread.yield();
            }
        }
    }

    /*
     * On a failure a destination needs to check if its source site was on the list and if it
     * is it should call crash VoltDB.
     */
    @Override
    public void handleNodeFault(HashSet<Integer> failedNodes,
            SiteTracker tracker) {
        HashSet<Integer> failedSites = new HashSet<Integer>();
        for (Integer failedHost : failedNodes) {
            failedSites.addAll(tracker.getAllSitesForHost(failedHost));
        }

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
        ArrayList<Integer> sourceSites = new ArrayList<Integer>(tracker.getAllSitesForPartition(partitionId));
        sourceSites.remove(new Integer(siteId));

        if (sourceSites.isEmpty()) {
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
                onCompletion,
                messageHandler);
    }
}
