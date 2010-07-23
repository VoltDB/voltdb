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

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.dtxn.SiteTracker;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.RecoveryMessage;
import org.voltdb.messaging.RecoveryMessageType;
import org.voltdb.utils.Pair;

public class RecoverySiteProcessorDestination implements RecoverySiteProcessor {

    /**
     * List of tables that need to be streamed
     */
    private final HashMap<Integer, RecoveryTable>  m_tables = new HashMap<Integer, RecoveryTable>();

    /**
     * The engine that will be the destination for recovery data
     */
    private final ExecutionEngine m_engine;

    /**
     * Mailbox used to send acks
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
        assert(message.type() == RecoveryMessageType.ScanTuples || message.type() == RecoveryMessageType.Complete);

        if (message.type() == RecoveryMessageType.ScanTuples) {
            m_engine.processRecoveryMessage(message.getMessageData());
            int sourceSite = message.sourceSite();
            RecoveryMessage ack = new RecoveryMessage( message, m_siteId);
            try {
                m_mailbox.send( sourceSite, 0, ack);
            } catch (MessagingException e) {
                // Continuing to propagate this horrible exception
                throw new RuntimeException(e);
            }
        } else if (message.type() == RecoveryMessageType.Complete) {
            m_tables.remove(message.tableId());
            if (m_tables.isEmpty()) {
                m_onCompletion.run();
            }
        }
    }

    /**
     * A destination doesn't have periodic recovery work to do.
     */
    @Override
    public void doRecoveryWork() {
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
            Runnable onCompletion) {
        m_mailbox = mailbox;
        m_engine = engine;
        m_siteId = siteId;
        for (Map.Entry<Pair<String, Integer>, Integer> entry : tableToSourceSite.entrySet()) {
            m_tables.put(entry.getKey().getSecond(), new RecoveryTable(entry.getKey().getFirst(), entry.getKey().getSecond(), entry.getValue()));
        }
        m_onCompletion = onCompletion;
    }

    @Override
    public void handleNodeFault(HashSet<Integer> failedNodes,
            SiteTracker tracker) {
        throw new UnsupportedOperationException();
    }
}
