/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.DummyTransactionResponseMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

/**
 * Track responses from each partition. This should be subsumed
 * by proper response tracking for the usual replication case?
 */
public class DuplicateCounter
{
    static final int MISMATCH = 0;
    static final int DONE = 1;
    static final int WAITING = 2;

    protected static final VoltLogger tmLog = new VoltLogger("TM");

    final long m_destinationId;
    Long m_responseHash = null;
    protected VoltMessage m_lastResponse = null;
    protected VoltTable m_lastResultTables[] = null;
    final List<Long> m_expectedHSIds;
    final long m_txnId;
    final VoltMessage m_openMessage;

    DuplicateCounter(
            long destinationHSId,
            long realTxnId,
            List<Long> expectedHSIds,
            VoltMessage openMessage)
    {
        m_destinationId = destinationHSId;
        m_txnId = realTxnId;
        m_expectedHSIds = new ArrayList<Long>(expectedHSIds);
        m_openMessage = openMessage;
    }

    long getTxnId()
    {
        return m_txnId;
    }

    int updateReplicas(List<Long> replicas) {
        m_expectedHSIds.retainAll(replicas);
        if (m_expectedHSIds.size() == 0) {
            return DONE;
        }
        else {
            return WAITING;
        }
    }

    void logRelevantMismatchInformation(long hash, VoltMessage recentMessage) {
        String msg = String.format("HASH MISMATCH COMPARING: %d to %d\n"
                + "REQUEST MESSAGE: %s\n"
                + "PREV RESPONSE MESSAGE: %s\n"
                + "CURR RESPONSE MESSAGE: %s\n",
                hash,
                m_responseHash,
                m_openMessage.toString(),
                m_lastResponse.toString(),
                recentMessage.toString());
        tmLog.error(msg);
    }

    void logWithCollidingDuplicateCounters(DuplicateCounter other) {
        String msg = String.format("DUPLICATE COUNTER COLLISION:\n"
                + "REQUEST MESSAGE 1: %s\n"
                + "REQUEST MESSAGE 2: %s\n",
                m_openMessage.toString(),
                other.m_openMessage.toString());
        tmLog.error(msg);
    }

    StoredProcedureInvocation getInvocation() {
        Iv2InitiateTaskMessage initTask = null;
        if (m_openMessage instanceof Iv2InitiateTaskMessage) {
            initTask = (Iv2InitiateTaskMessage) m_openMessage;
        }
        else if (m_openMessage instanceof FragmentTaskMessage) {
            initTask = ((FragmentTaskMessage) m_openMessage).getInitiateTask();
        }
        if (initTask != null) {
            return initTask.getStoredProcedureInvocation();
        }
        return null;
    }

    String getStoredProcedureName() {
        StoredProcedureInvocation invocation = getInvocation();
        if (invocation != null) {
            return invocation.getProcName();
        }

        // handle other cases
        if (m_openMessage instanceof FragmentTaskMessage) {
            return "MP_DETERMINISM_ERROR";
        }

        return "UNKNOWN_PROCEDURE_NAME";
    }

    protected int checkCommon(long hash, boolean rejoining, VoltTable resultTables[], VoltMessage message)
    {
        if (!rejoining) {
            if (m_responseHash == null) {
                m_responseHash = Long.valueOf(hash);
            }
            else if (!m_responseHash.equals(hash)) {
                tmLog.fatal("Stored procedure " + getStoredProcedureName()
                        + " generated different SQL queries at different partitions."
                        + " Shutting down to preserve data integrity.");
                logRelevantMismatchInformation(hash, message);
                return MISMATCH;
            }
            m_lastResponse = message;
            m_lastResultTables = resultTables;
        }

        /*
         * Set m_lastResponse to a response once at least. It's possible
         * that all responses are dummy responses in the case of elastic
         * join. So only setting m_lastResponse when the message is not
         * a dummy will leave the variable as null.
         */
        if (m_lastResponse == null) {
            m_lastResponse = message;
            m_lastResultTables = resultTables;
        }

        m_expectedHSIds.remove(message.m_sourceHSId);
        if (m_expectedHSIds.size() == 0) {
            return DONE;
        }
        else {
            return WAITING;
        }
    }

    int offer(InitiateResponseMessage message)
    {
        ClientResponseImpl r = message.getClientResponseData();
        // get the hash of sql run
        long hash = 0;
        Integer sqlHash = r.getHash();
        if (sqlHash != null) {
            hash = sqlHash.intValue();
        }
        return checkCommon(hash, message.isRecovering(), r.getResults(), message);
    }

    int offer(FragmentResponseMessage message)
    {
        return checkCommon(0, message.isRecovering(), null, message);
    }

    int offer(CompleteTransactionResponseMessage message)
    {
        return checkCommon(0, message.isRecovering(), null, message);
    }

    int offer(DummyTransactionResponseMessage message)
    {
        return checkCommon(0, false, null, message);
    }

    VoltMessage getLastResponse()
    {
        return m_lastResponse;
    }

    @Override
    public String toString()
    {
        String msg = String.format("DuplicateCounter: txnId: %s, outstanding HSIds: %s\n",
               TxnEgo.txnIdToString(m_txnId),
               CoreUtils.hsIdCollectionToString(m_expectedHSIds));
        return msg;
    }
}
