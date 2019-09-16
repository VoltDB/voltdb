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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
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

import com.google_voltpatches.common.collect.Lists;
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
    final List<Long> m_expectedHSIds;
    final long m_txnId;
    final long m_leaderHSID;
    final TransactionInfoBaseMessage m_openMessage;

    List<Long> m_misMatchedSites = Lists.newArrayList();
    int[] m_leaderResponseHashes = null;
    boolean m_leaderTxnSucceed = false;
    Map<Long, int[]> m_replicaResponseHashes = Maps.newTreeMap();
    Set<Long> m_replicas = Sets.newHashSet();
    Set<VoltMessage> m_failedReplicas = Sets.newHashSet();

    final boolean m_counterForEverySiteProc;

    DuplicateCounter(
            long destinationHSId,
            long realTxnId,
            List<Long> expectedHSIds,
            TransactionInfoBaseMessage openMessage,
            long leaderHSID,
            boolean counterForEverySiteProc) {
        m_destinationId = destinationHSId;
        m_txnId = realTxnId;
        m_expectedHSIds = new ArrayList<Long>(expectedHSIds);
        m_openMessage = openMessage;
        m_leaderHSID = leaderHSID;
        m_replicas.addAll(expectedHSIds);
        m_replicas.remove(m_leaderHSID);
        m_counterForEverySiteProc = counterForEverySiteProc;
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
        m_failedReplicas = m_failedReplicas.stream().filter(
                s->replicas.contains(s.m_sourceHSId)).collect(Collectors.toSet());
        if (m_expectedHSIds.isEmpty()) {
            if (!m_leaderTxnSucceed && !m_failedReplicas.isEmpty() && m_failedReplicas.size() == m_replicas.size()) {
                m_misMatchedSites.clear();
            }
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
                m_leaderResponseHashes[0],
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

    protected int checkCommon(int[] hashes, boolean rejoining, VoltTable resultTables[], VoltMessage message, boolean txnSucceed)
    {
        if (!rejoining) {
            int pos = -1;

            // Every partition sys proc InitiateResponseMessage
            if (m_counterForEverySiteProc) {
                if (m_responseHashes == null) {
                    m_responseHashes = hashes;
                    m_leaderTxnSucceed = txnSucceed;
                } else if (m_leaderTxnSucceed != txnSucceed) {
                    tmLog.error(String.format(FAIL_MSG, getStoredProcedureName()));
                    logRelevantMismatchInformation("PARTIAL ROLLBACK/ABORT", hashes, message, pos);
                    return ABORT;
                } else if ((pos = DeterminismHash.compareHashes(m_responseHashes, hashes)) >= 0) {
                    tmLog.error(String.format(MISMATCH_MSG, getStoredProcedureName()));
                    logRelevantMismatchInformation("HASH MISMATCH", hashes, message, pos);
                    return MISMATCH;
                }
            } else if (message.m_sourceHSId == m_leaderHSID) {
                m_leaderResponseHashes = hashes;
                m_leaderTxnSucceed = txnSucceed;
                // Responses from replicas may have arrived earlier than partition master
                for (Map.Entry<Long, int[]> entry : m_replicaResponseHashes.entrySet()) {
                    int [] theHashes = entry.getValue();
                    if ((pos = DeterminismHash.compareHashes(m_leaderResponseHashes, theHashes)) >= 0) {
                        tmLog.error(String.format(MISMATCH_MSG, getStoredProcedureName()));
                        logRelevantMismatchInformation("HASH MISMATCH", theHashes, message, pos);
                        m_misMatchedSites.add(entry.getKey());
                    }
                }
                m_replicaResponseHashes.clear();
                if (m_leaderTxnSucceed) {
                    if (!m_failedReplicas.isEmpty()) {
                        tmLog.error(String.format(FAIL_MSG, getStoredProcedureName()));
                        for (VoltMessage msg : m_failedReplicas) {
                            logRelevantMismatchInformation("HASH MISMATCH", ZERO_HASHES, msg, -1);
                        }
                    }
                }
            } else if (message.m_sourceHSId != m_leaderHSID) {
                if (m_leaderResponseHashes != null) {
                    if (m_leaderTxnSucceed != txnSucceed) {
                        tmLog.error(String.format(FAIL_MSG, getStoredProcedureName()));
                        logRelevantMismatchInformation("HASH MISMATCH", ZERO_HASHES, message, -1);
                    } else {
                        if ((pos = DeterminismHash.compareHashes(m_leaderResponseHashes, hashes)) >= 0) {
                            tmLog.error(String.format(MISMATCH_MSG, getStoredProcedureName()));
                            logRelevantMismatchInformation("HASH MISMATCH", hashes, message, pos);
                            m_misMatchedSites.add(message.m_sourceHSId);
                        }
                    }
                } else if (txnSucceed) {
                    m_replicaResponseHashes.put(message.m_sourceHSId, hashes);
                } else {
                    m_failedReplicas.add(message);
                }
            }
            m_lastResponse = message;
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

        // Clean up failed sites
        m_misMatchedSites.retainAll(m_replicas);
        m_failedReplicas = m_failedReplicas.stream().filter(
                s->m_replicas.contains(s.m_sourceHSId)).collect(Collectors.toSet());

        // Transaction fails on all replicas
        if (!m_leaderTxnSucceed && !m_failedReplicas.isEmpty() && m_failedReplicas.size() == m_replicas.size()) {
            m_misMatchedSites.clear();
        }
        return DONE;
    }

    protected int checkCommon(VoltMessage message) {
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
        return (m_expectedHSIds.isEmpty()) ? DONE : WAITING;
    }

    public boolean allResponsesMatched() {
        return m_misMatchedSites.isEmpty();
    }

    int offer(InitiateResponseMessage message)
    {
        ClientResponseImpl r = message.getClientResponseData();
        // get the hash of sql run
        int[] hashes = r.getHashes();

        boolean txnAbort = true;
        if (ClientResponseImpl.isTransactionallySuccessful(message.getClientResponseData().getStatus())) {
            txnAbort = false;
        }

        return checkCommon(hashes, message.isRecovering(), r.getResults(), message, txnAbort);
    }

    int offer(FragmentResponseMessage message) {
        // No check on fragment message
        return checkCommon(message);
    }

    int offer(CompleteTransactionResponseMessage message) {
        return checkCommon(message);
    }

    int offer(DummyTransactionResponseMessage message) {
        return checkCommon(message);
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
