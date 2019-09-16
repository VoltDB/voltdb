/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.DummyTransactionResponseMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;

/**
 * Track responses from each partition. This should be subsumed
 * by proper response tracking for the usual replication case?
 */
public class DuplicateCounter
{
    private final static String FAIL_MSG = "Stored procedure %s succeeded on one partition but failed on another partition.";
    private final static String MISMATCH_MSG = "Stored procedure %s generated different SQL queries at different partitions.";

    static final int MISMATCH = 0;
    static final int DONE = 1;
    static final int WAITING = 2;
    static final int ABORT = 3;

    protected static final VoltLogger tmLog = new VoltLogger("TM");

    static final int[] ZERO_HASHES = new int[] { 0, 0, 0 };
    final long m_destinationId;
    int[] m_responseHashes = null;
    protected VoltMessage m_lastResponse = null;
    // if any response shows the transaction aborted
    boolean m_txnSucceed = false;
    final List<Long> m_expectedHSIds;
    final long m_txnId;
    final long m_leaderHSID;
    final TransactionInfoBaseMessage m_openMessage;
    Map<Long, ResponseResult> m_responses = Maps.newTreeMap();
    private boolean m_allMatched = true;
    Set<Long> m_replicas = Sets.newHashSet();

    final boolean m_forEverySite;

    static class ResponseResult {
        final int[] hashes;
        final boolean success;
        final VoltMessage message;
        public ResponseResult(int[] respHashes, boolean status, VoltMessage msg) {
            hashes = respHashes;
            success = status;
            message = msg;
        }
    }

    DuplicateCounter(
            long destinationHSId,
            long realTxnId,
            List<Long> expectedHSIds,
            TransactionInfoBaseMessage openMessage,
            long leaderHSID,
            boolean forEverySite) {
        m_destinationId = destinationHSId;
        m_txnId = realTxnId;
        m_expectedHSIds = new ArrayList<Long>(expectedHSIds);
        m_openMessage = openMessage;
        m_leaderHSID = leaderHSID;
        m_forEverySite = forEverySite;
        m_replicas.addAll(expectedHSIds);
    }

    DuplicateCounter(
            long destinationHSId,
            long realTxnId,
            List<Long> expectedHSIds,
            TransactionInfoBaseMessage openMessage,
            long leaderHSID) {
        this(destinationHSId, realTxnId, expectedHSIds, openMessage, leaderHSID, false);
    }

    long getTxnId() {
        return m_txnId;
    }

    int updateReplicas(List<Long> replicas) {
        m_expectedHSIds.retainAll(replicas);
        m_replicas.retainAll(replicas);
        if (m_expectedHSIds.isEmpty()) {
            determineResult();
            return DONE;
        }
        return WAITING;
    }

    void addReplicas(long[] newReplicas) {
        for (long replica : newReplicas) {
            m_expectedHSIds.add(replica);
            m_replicas.add(replica);
        }
    }

    public void updateReplica (Long previousMaster, Long newMaster){
        m_expectedHSIds.remove(previousMaster);
        m_expectedHSIds.add(newMaster);
        m_replicas.remove(previousMaster);
        m_replicas.add(newMaster);
        ResponseResult respResult = m_responses.remove(previousMaster);
        if (respResult != null) {
            m_responses.put(newMaster,respResult);
        }
    }

    void logRelevantMismatchInformation(String reason, int[] hashes, VoltMessage recentMessage, int misMatchPos) {
        if (misMatchPos >= 0) {
            ((InitiateResponseMessage) recentMessage).setMismatchPos(misMatchPos);
            ((InitiateResponseMessage) m_lastResponse).setMismatchPos(misMatchPos);
        }
        String msg = String.format(reason + " COMPARING: %d to %d\n"
                + "REQUEST MESSAGE: %s\n"
                + "PREV RESPONSE MESSAGE: %s\n"
                + "CURR RESPONSE MESSAGE: %s\n",
                hashes[0],
                m_responseHashes[0],
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

    public TransactionInfoBaseMessage getOpenMessage() {
        return m_openMessage;
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

    protected int checkCommon(int[] hashes, boolean rejoining, VoltMessage message, boolean txnSucceed)
    {
        if (!rejoining) {
            m_lastResponse = message;
            // Every partition sys proc InitiateResponseMessage
            if (m_forEverySite) {
                int pos = -1;
                if (m_responseHashes == null) {
                    m_responseHashes = hashes;
                    m_txnSucceed = txnSucceed;
                } else if (m_txnSucceed != txnSucceed) {
                    tmLog.error(String.format(FAIL_MSG, getStoredProcedureName()));
                    logRelevantMismatchInformation("PARTIAL ROLLBACK/ABORT", hashes, message, pos);
                    return ABORT;
                } else if ((pos = DeterminismHash.compareHashes(m_responseHashes, hashes)) >= 0) {
                    tmLog.error(String.format(MISMATCH_MSG, getStoredProcedureName()));
                    logRelevantMismatchInformation("HASH MISMATCH", hashes, message, pos);
                    return MISMATCH;
                }
            } else {
                m_responses.put(message.m_sourceHSId, new ResponseResult(hashes,txnSucceed, message));
            }
        }

        /*
         * Set m_lastResponse to a response once at least. It's possible
         * that all responses are dummy responses in the case of elastic
         * join. So only setting m_lastResponse when the message is not
         * a dummy will leave the variable as null.
         */
        if (m_lastResponse == null) {
            m_lastResponse = message;
        }

        m_expectedHSIds.remove(message.m_sourceHSId);
        if (!m_expectedHSIds.isEmpty()) {
            return WAITING;
        }

        determineResult();
        return DONE;
    }

    private void determineResult() {
        if (!m_forEverySite || m_responses.isEmpty()) {
            return;
        }
        ResponseResult leaderResponse = m_responses.remove(m_leaderHSID);
        assert (leaderResponse != null);
        m_responseHashes = leaderResponse.hashes;
        m_lastResponse = leaderResponse.message;
        int failedCount = m_responses.size();
        for (Iterator<Map.Entry<Long, ResponseResult>> it = m_responses.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, ResponseResult> entry = it.next();
            if (!m_replicas.contains(entry.getKey())) {
                it.remove();
                failedCount--;
            }
            ResponseResult res = entry.getValue();
            if (!res.success) {
                failedCount--;
            }
            int [] theHashes = res.hashes;
            int pos = -1;
            if (leaderResponse.success != entry.getValue().success) {
                tmLog.error(String.format(FAIL_MSG, getStoredProcedureName()));
                logRelevantMismatchInformation("HASH MISMATCH", theHashes, res.message, -1);
                m_allMatched = false;
            } else if ((pos = DeterminismHash.compareHashes(leaderResponse.hashes, theHashes)) >= 0) {
                tmLog.error(String.format(MISMATCH_MSG, getStoredProcedureName()));
                logRelevantMismatchInformation("HASH MISMATCH", theHashes, res.message, pos);
                m_allMatched = false;
            }
        }

        // All replicas, including leader, failed
        if (!leaderResponse.success && failedCount ==0) {
            m_allMatched = true;
        }
    }

    public boolean allResponsesMatched() {
        return m_allMatched;
    }

    int offer(VoltMessage message) {
        if (message instanceof FragmentResponseMessage ||
                message instanceof CompleteTransactionResponseMessage ||
                message instanceof DummyTransactionResponseMessage) {
            if (m_lastResponse == null) {
                m_lastResponse = message;
            }

            m_expectedHSIds.remove(message.m_sourceHSId);
            return (m_expectedHSIds.isEmpty()) ? DONE : WAITING;
        }
        if (message instanceof InitiateResponseMessage) {
            final InitiateResponseMessage msg = (InitiateResponseMessage)message;
            ClientResponseImpl r = msg.getClientResponseData();
            return checkCommon(r.getHashes(),
                    msg.isRecovering(),
                    message,
                    !(ClientResponseImpl.isTransactionallySuccessful(r.getStatus())));
        }
        return DONE;
    }

    VoltMessage getLastResponse() {
        return m_lastResponse;
    }

    public void dumpCounter(StringBuilder sb) {
        sb.append("DuplicateCounter: [");
        m_openMessage.toDuplicateCounterString(sb);
        sb.append(" outstanding HSIds: ");
        sb.append(CoreUtils.hsIdCollectionToString(m_expectedHSIds));
        sb.append("]\n");
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
