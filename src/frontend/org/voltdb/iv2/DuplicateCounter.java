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
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.DummyTransactionResponseMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;
import org.voltdb.utils.CatalogUtil;

/**
 * Track responses from each partition. This should be subsumed
 * by proper response tracking for the usual replication case?
 */
public class DuplicateCounter
{
    public static final String HASHMISMATCH_MSG = "Hash mismatch occurred.";
    public static final String MISMATCH_RESPONSE_MSG = "The procedure generated different results on different copies of the partition. Please report the following information to support@voltactivedata.com.";
    public static final String MISMATCH_HASH_MSG = "The stored procedure generated different SQL queries on different copies of the partition because the procedure is non-deterministic. \n" +
            "The following information can be used to debug the procedure: ";

    public enum HashResult{
        MISMATCH(0),
        DONE(1),
        WAITING(2),
        ABORT(3);
        final int status;
        HashResult(int status) {
            this.status = status;
        }
        int get() {
            return status;
        }
        public boolean isDone() {
            return status == DONE.get();
        }
        public boolean isMismatch() {
            return status == MISMATCH.get();
        }
        public boolean isAbort() {
            return status == ABORT.get();
        }
    }

    protected static final VoltLogger tmLog = new VoltLogger("TM");

    static final int[] ZERO_HASHES = new int[] { 0, 0, 0 };
    final long m_destinationId;
    int[] m_responseHashes = null;
    protected VoltMessage m_lastResponse = null;
    // leader partition client response status
    byte m_status;
    String m_statusString;
    final List<Long> m_expectedHSIds;
    final long m_txnId;
    final long m_leaderHSID;
    final TransactionInfoBaseMessage m_openMessage;
    Map<Long, ResponseResult> m_responses = Maps.newTreeMap();

    // Flag indicating that the the hashes from replicas match with the hash from partition master
    private boolean m_hashMatched = true;

    Set<Long> m_replicas = Sets.newHashSet();

    // A placeholder for HSIDs of replicas whose hashes do not match with the one from partition master.
    Set<Long> m_misMatchedReplicas = Sets.newHashSet();

    // Track InitiateResponseMessage for run-every-site system procedure on MPI
    // Their hashes are compared between partitions, not between replicas of the same partition
    final boolean m_everySiteMPSysProc;

    // Used for transaction repair. In this case, Duplicate Counter may not have local site.
    boolean m_transactionRepair;

    static class ResponseResult {
        final int[] hashes;
        final byte status;
        final String statusString;
        public ResponseResult(int[] respHashes, byte respStatus, String respStatusString) {
            hashes = respHashes;
            status = respStatus;
            statusString = respStatusString;
        }
    }

    DuplicateCounter(
            long destinationHSId,
            long realTxnId,
            List<Long> expectedHSIds,
            TransactionInfoBaseMessage openMessage,
            long leaderHSID) {
        m_destinationId = destinationHSId;
        m_txnId = realTxnId;
        m_expectedHSIds = new ArrayList<Long>(expectedHSIds);
        m_openMessage = openMessage;
        m_leaderHSID = leaderHSID;
        m_everySiteMPSysProc = (TxnEgo.getPartitionId(realTxnId) == MpInitiator.MP_INIT_PID);
        m_replicas.addAll(expectedHSIds);
    }

    long getTxnId() {
        return m_txnId;
    }

    public void setTransactionRepair(boolean repair) {
        m_transactionRepair = repair;
    }

    HashResult updateReplicas(List<Long> replicas) {
        m_expectedHSIds.retainAll(replicas);
        m_replicas.retainAll(replicas);
        if (m_expectedHSIds.isEmpty()) {
            finalizeMatchResult();
            return HashResult.DONE;
        }
        return HashResult.WAITING;
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
    }

    void logRelevantMismatchInformation(int[] hashes, VoltMessage recentMessage, int misMatchPos, long replicaHSID) {
        if (misMatchPos >= 0) {
            if (recentMessage != null) {
                ((InitiateResponseMessage) recentMessage).setMismatchPos(misMatchPos);
            }
            ((InitiateResponseMessage) m_lastResponse).setMismatchPos(misMatchPos);
        }

        if (tmLog.isDebugEnabled()){
            String msg = String.format("COMPARING: %d to %d\n"
                            + "REQUEST MESSAGE: %s\n"
                            + "PREV RESPONSE MESSAGE: %s\n"
                            + "CURR RESPONSE MESSAGE: %s\n",
                    hashes[0],
                    m_responseHashes[0],
                    m_openMessage.toString(),
                    m_lastResponse.toString(),
                    recentMessage != null ? recentMessage.toString():"");
            tmLog.debug(msg);
        }
        // User friendly hash mismatch info
        String procName = getStoredProcedureName();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Procedure name: %s\n"
                                    + "Partition: %d\n"
                                    + "Host IDs: %d and %d\n"
                                    + "Procedure status: %d\n",
                                    procName,
                                    TxnEgo.getPartitionId(m_txnId),
                                    CoreUtils.getHostIdFromHSId(m_leaderHSID), CoreUtils.getHostIdFromHSId(replicaHSID),
                                    m_status));
        if (misMatchPos == DeterminismHash.HASH_CATALOG_VERSION_MISMATCH) {
            sb.append("Hash mismatch happened because catalog version differed between leader and replica.").
                    append(" Catalog version from leader:").append(m_responseHashes[1]).
                    append(" Catalog version from replica:").append(hashes[1]);
        } else if (misMatchPos == DeterminismHash.HASH_NOT_INCLUDE) {
            sb.append("Hash mismatch happened after ").append(DeterminismHash.MAX_HASHES_COUNT / 2 - DeterminismHash.HEADER_OFFSET + 1).append(" statements.\n").
                    append("For debugging purposes, use VOLTDB_OPTS=\"-DMAX_STATEMENTS_WITH_DETAIL=<hashcount>\" to set to a higher value, it could impact performance.");
        } else if (misMatchPos >= 0) {
            sb.append("Hash mismatch happened from statement ").append(misMatchPos/2).append("\n");
            Procedure proc = CatalogUtil.getProcedure(procName);
            if (proc == null) {
                sb.append("Unknown procedure: ").append(procName);
            } else if (proc.getSystemproc()) {
                sb.append(procName).append(" is system procedure. Please Contact VoltDB Support.");
            } else if (proc.getDefaultproc()) {
                sb.append(procName).append(" is auto-generated CRUD procedure. Please Contact VoltDB Support.");
            } else {
                sb.append("Procedure SQL Executions:\n");
                CatalogUtil.printUserProcedureDetailShort(proc, m_responseHashes, hashes, misMatchPos,
                        CoreUtils.getHostIdFromHSId(m_leaderHSID), CoreUtils.getHostIdFromHSId(replicaHSID), sb);
            }
        }
        tmLog.error(sb.toString());
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

    protected HashResult checkCommon(int[] hashes, boolean recovering, VoltMessage message, byte status, String statusString)
    {
        if (!recovering) {
            // Every partition sys proc InitiateResponseMessage
            if (m_everySiteMPSysProc || m_transactionRepair) {
                m_lastResponse = message;
                int pos = -1;
                if (m_responseHashes == null) {
                    m_responseHashes = hashes;
                    m_status = status;
                    m_statusString = statusString;
                } else if (ClientResponseImpl.isTransactionallySuccessful(m_status) != ClientResponseImpl.isTransactionallySuccessful(status)) {
                    tmLog.error(HASHMISMATCH_MSG);
                    tmLog.error(MISMATCH_RESPONSE_MSG);
                    tmLog.error("Procedure status from leader is: " + (m_statusString == null ? "Success" : m_statusString) + ", while from replica: " + (statusString == null ? "Success" : statusString));
                    logRelevantMismatchInformation(hashes, message, pos, message.m_sourceHSId);
                    return HashResult.ABORT;
                } else if ((pos = DeterminismHash.compareHashes(m_responseHashes, hashes)) != DeterminismHash.HASH_EQUAL) {
                    tmLog.error(HASHMISMATCH_MSG);
                    tmLog.error(MISMATCH_HASH_MSG);
                    logRelevantMismatchInformation(hashes, message, pos, message.m_sourceHSId);
                    return HashResult.MISMATCH;
                }
            } else {
                m_responses.put(message.m_sourceHSId, new ResponseResult(hashes, status, statusString));

                // Use the response message from local site
                if (m_leaderHSID == message.m_sourceHSId) {
                    m_lastResponse = message;
                    m_status = status;
                    m_statusString = statusString;
                }
            }
        } else {
            /*
             * Set m_lastResponse to a response once at least. It's possible
             * that all responses are dummy responses in the case of elastic
             * join. So only setting m_lastResponse when the message is not
             * a dummy will leave the variable as null.
             */
            if (m_lastResponse == null) {
                m_lastResponse = message;
                m_status = status;
                m_statusString = statusString;
            }
        }

        m_expectedHSIds.remove(message.m_sourceHSId);
        if (!m_expectedHSIds.isEmpty()) {
            return HashResult.WAITING;
        }

        finalizeMatchResult();
        return HashResult.DONE;
    }

    private void finalizeMatchResult() {

        // If the DuplicateCounter is used from MP run-every-site system procedure, hash mismatch is checked as responses come
        // in from every partition.
        if (m_everySiteMPSysProc || m_responses.isEmpty() || m_transactionRepair) {
            return;
        }

        // Compare the hash from partition leader with those from partition replicas
        ResponseResult leaderResponse = m_responses.remove(m_leaderHSID);
        assert (leaderResponse != null);
        m_responseHashes = leaderResponse.hashes;

        boolean misMatchLogged = false;
        for (Iterator<Map.Entry<Long, ResponseResult>> it = m_responses.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, ResponseResult> entry = it.next();

            // The replica is not present any more
            if (!m_replicas.contains(entry.getKey())) {
                it.remove();
                continue;
            }

            ResponseResult res = entry.getValue();
            if (ClientResponseImpl.isTransactionallySuccessful(m_status) != ClientResponseImpl.isTransactionallySuccessful(res.status)) {
                if (!misMatchLogged) {
                    tmLog.error(HASHMISMATCH_MSG);
                    tmLog.error(MISMATCH_RESPONSE_MSG);
                    tmLog.error("Procedure status from leader: " + (m_statusString == null ? "Success" : m_statusString) + " ,while from replica: " + (res.statusString == null ? "Success" : res.statusString) );
                    logRelevantMismatchInformation(res.hashes, null, -1, entry.getKey());
                    misMatchLogged = true;
                }
                m_hashMatched = false;
                m_misMatchedReplicas.add(entry.getKey());
                continue;
            }
            if (!ClientResponseImpl.isTransactionallySuccessful(m_status)) {
                // no need to check specific hashes if txn already failed
                continue;
            }
            int pos = -1;
            // Response hashes can be null from dummy or failed transaction responses.
            if (m_responseHashes != null && res.hashes != null &&
                    (pos = DeterminismHash.compareHashes(leaderResponse.hashes, res.hashes)) != DeterminismHash.HASH_EQUAL) {
                if (!misMatchLogged) {
                    tmLog.error(HASHMISMATCH_MSG);
                    tmLog.error(MISMATCH_HASH_MSG);
                    logRelevantMismatchInformation(res.hashes, null, pos, entry.getKey());
                    misMatchLogged = true;
                }
                m_hashMatched = false;
                m_misMatchedReplicas.add(entry.getKey());
            }
        }
    }

    public boolean isSuccess() {
        assert(m_expectedHSIds.isEmpty());
        return m_hashMatched;
    }

    HashResult offer(FragmentResponseMessage message) {
        // No check on fragment message
        return checkCommon(ZERO_HASHES, message.isRecovering(), message, ClientResponse.SUCCESS, null);
    }

    HashResult offer(CompleteTransactionResponseMessage message) {
        return checkCommon(ZERO_HASHES, message.isRecovering(), message, ClientResponse.SUCCESS, null);
    }

    HashResult offer(DummyTransactionResponseMessage message) {
        return checkCommon(ZERO_HASHES, false, message, ClientResponse.SUCCESS, null);
    }

    HashResult offer(InitiateResponseMessage message) {
        ClientResponseImpl r = message.getClientResponseData();
        return checkCommon(r.getHashes(),
                message.isRecovering(),
                message,
                r.getStatus(),
                r.getStatusString());
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

    Set<Long> getMisMatchedReplicas() {
        return m_misMatchedReplicas;
    }
}
