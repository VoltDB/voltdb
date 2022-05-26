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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.Future;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.ElasticHashinator;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.TheHashinator;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;

import com.google_voltpatches.common.util.concurrent.SettableFuture;

public class MpPromoteAlgo implements RepairAlgo
{
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final long m_requestId = System.nanoTime();
    private final List<Long> m_survivors;
    private final int m_deadHost;
    private long m_maxSeenTxnId = TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId();
    private final List<Iv2InitiateTaskMessage> m_interruptedTxns = new ArrayList<Iv2InitiateTaskMessage>();
    private Pair<Long, byte[]> m_newestHashinatorConfig = Pair.of(Long.MIN_VALUE,new byte[0]);
    // Each Term can process at most one promotion; if promotion fails, make
    // a new Term and try again (if that's your big plan...)
    private final SettableFuture<RepairResult> m_promotionResult = SettableFuture.create();
    // If leader change is caused by leader migration request, and no pending repair was cancelled previously,
    // the repair message is not necessarily needed
    private final boolean m_skipRepair;
    private final MpRestartSequenceGenerator m_restartSeqGenerator;
    private long m_repairTruncationHandle = TransactionInfoBaseMessage.UNUSED_TRUNC_HANDLE;
    long getRequestId()
    {
        return m_requestId;
    }

    // scoreboard for responding replica repair log responses (hsid -> response count)
    static class ReplicaRepairStruct
    {
        int m_receivedResponses = 0;
        int m_expectedResponses = -1; // (a log msg cares about this init. value)

        // update counters and return the number of outstanding messages.
        boolean update(Iv2RepairLogResponseMessage response)
        {
            m_receivedResponses++;
            m_expectedResponses = response.getOfTotal();
            return logsComplete();
        }

        // return 0 if all expected logs have been received.
        boolean logsComplete()
        {
            return (m_expectedResponses - m_receivedResponses) == 0;
        }
    }

    // replicas being processed and repaired.
    Map<Long, ReplicaRepairStruct> m_replicaRepairStructs =
        new HashMap<Long, ReplicaRepairStruct>();

    // Determine equal repair responses by the SpHandle of the response.
    Comparator<Iv2RepairLogResponseMessage> m_unionComparator =
        new Comparator<Iv2RepairLogResponseMessage>()
    {
        @Override
        public int compare(Iv2RepairLogResponseMessage o1, Iv2RepairLogResponseMessage o2)
        {
            if (o1.getTxnId() < o2.getTxnId()) {
                return -1;
            }
            else if (o1.getTxnId() > o2.getTxnId()) {
                return 1;
            }
            return 0;
        }
    };

    // Union of repair responses.
    TreeSet<Iv2RepairLogResponseMessage> m_repairLogUnion =
        new TreeSet<Iv2RepairLogResponseMessage>(m_unionComparator);

    /**
     * Setup a new RepairAlgo but don't take any action to take responsibility.
     */
    public MpPromoteAlgo(List<Long> survivors, int deadHost, InitiatorMailbox mailbox,
            MpRestartSequenceGenerator seqGen, String whoami)
    {
        this(survivors, deadHost, mailbox,seqGen, whoami, false);
    }

    /**
     * Setup a new RepairAlgo but don't take any action to take responsibility.
     */
    public MpPromoteAlgo(List<Long> survivors, int deadHost, InitiatorMailbox mailbox,
            MpRestartSequenceGenerator seqGen, String whoami, boolean skipRepair)
    {
        m_survivors = new ArrayList<Long>(survivors);
        m_deadHost = deadHost;
        m_mailbox = mailbox;
        m_skipRepair = skipRepair;
        m_whoami = whoami;
        m_restartSeqGenerator = seqGen;
    }

    @Override
    public Future<RepairResult> start()
    {
        try {
            prepareForFaultRecovery();
        } catch (Exception e) {
            repairLogger.error(m_whoami + "failed leader promotion:", e);
            m_promotionResult.setException(e);
        }
        return m_promotionResult;
    }

    @Override
    public boolean cancel()
    {
        return m_promotionResult.cancel(false);
    }

    @Override
    public boolean isCancelled() {
        return m_promotionResult.isCancelled();
    }

    /** Start fixing survivors: setup scoreboard and request repair logs. */
    void prepareForFaultRecovery()
    {
        for (Long hsid : m_survivors) {
            m_replicaRepairStructs.put(hsid, new ReplicaRepairStruct());
        }
        m_replicaRepairStructs.put(m_mailbox.getHSId(), new ReplicaRepairStruct());
        if (repairLogger.isDebugEnabled()) {
            repairLogger.debug(m_whoami + "found " + m_survivors.size()
            + " surviving leaders to repair. "
            + " Survivors: " + CoreUtils.hsIdCollectionToString(m_survivors) + " requested id:" + m_requestId);
        }
        VoltMessage logRequest = makeRepairLogRequestMessage(m_requestId, m_deadHost);
        m_mailbox.send(com.google_voltpatches.common.primitives.Longs.toArray(m_survivors), logRequest);
        m_mailbox.send(m_mailbox.getHSId(), logRequest);
    }

    /** Process a new repair log response */
    @Override
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2RepairLogResponseMessage) {
            Iv2RepairLogResponseMessage response = (Iv2RepairLogResponseMessage)message;
            if (response.getRequestId() != m_requestId) {
                if (repairLogger.isTraceEnabled()) {
                    repairLogger.trace(m_whoami + "rejecting stale repair response."
                            + " Current request id is: " + m_requestId
                            + " Received response for request id: " + response.getRequestId());
                }
                return;
            }

            // Step 1: if the msg has a known (not MAX VALUE) handle, update m_maxSeen.
            if (response.getTxnId() != Long.MAX_VALUE) {
                m_maxSeenTxnId = Math.max(m_maxSeenTxnId, response.getTxnId());
            }

            m_repairTruncationHandle = Math.max(m_repairTruncationHandle, response.getRepairTruncationHandle());

            // Step 2: track hashinator versions

            if (response.hasHashinatorConfig()) {
                Pair<Long,byte[]> proposed = response.getHashinatorVersionedConfig();
                if (proposed.getFirst() > m_newestHashinatorConfig.getFirst()) {
                    m_newestHashinatorConfig = proposed;
                }
            }

            // Step 3: offer to the union
            addToRepairLog(response);
            if (repairLogger.isDebugEnabled()) {
                repairLogger.debug(m_whoami + " collected from " + CoreUtils.hsIdToString(response.m_sourceHSId) +
                        ", message: " + response.getPayload());
            }

            // Step 4: update the corresponding replica repair struct.
            ReplicaRepairStruct rrs = m_replicaRepairStructs.get(response.m_sourceHSId);
            if (rrs.m_expectedResponses < 0) {
                if (repairLogger.isDebugEnabled()) {
                    repairLogger.debug(m_whoami + "collecting " + response.getOfTotal()
                    + " repair log entries from "
                    + CoreUtils.hsIdToString(response.m_sourceHSId));
                }
            }

            if (rrs.update(response)) {
                if (repairLogger.isDebugEnabled()) {
                    repairLogger.debug(m_whoami + "collected " + rrs.m_receivedResponses
                            + " responses for " + rrs.m_expectedResponses
                            + " repair log entries from " + CoreUtils.hsIdToString(response.m_sourceHSId));
                }

                if (areRepairLogsComplete()) {

                    TheHashinator.updateHashinator(ElasticHashinator.class,
                            m_newestHashinatorConfig.getFirst(), m_newestHashinatorConfig.getSecond(), true);

                    //no real transaction repair when triggered with MigratePartitionLeader. Theoretically it should not be here.
                    //MigratePartitionLeader should not trigger MP promotion.
                    if (m_skipRepair) {
                        m_promotionResult.set(new RepairResult(m_maxSeenTxnId, m_repairTruncationHandle));
                    } else {
                        repairSurvivors();
                    }
                }
            }
        }
    }

    /** Have all survivors supplied a full repair log? */
    public boolean areRepairLogsComplete()
    {
        for (Entry<Long, ReplicaRepairStruct> entry : m_replicaRepairStructs.entrySet()) {
            if (!entry.getValue().logsComplete()) {
                return false;
            }
        }
        return true;
    }

    public List<Iv2InitiateTaskMessage> getInterruptedTxns()
    {
        assert(m_interruptedTxns.isEmpty() || m_interruptedTxns.size() == 1);
        return m_interruptedTxns;
    }

    /** Send missed-messages to survivors. Exciting! */
    public void repairSurvivors()
    {
        // cancel() and repair() must be synchronized by the caller (the deliver lock,
        // currently). If cancelled and the last repair message arrives, don't send
        // out corrections!
        if (this.m_promotionResult.isCancelled()) {
            repairLogger.debug(m_whoami + "skipping repair message creation for cancelled Term.");
            return;
        }

        if (repairLogger.isDebugEnabled()) {
            repairLogger.debug(m_whoami + "received all repair logs and is repairing surviving replicas.");
        }
        for (Iv2RepairLogResponseMessage li : m_repairLogUnion) {
            // send the repair log union to all the survivors. SPIs will ignore
            // CompleteTransactionMessages for transactions which have already
            // completed, so this has the effect of making sure that any holes
            // in the repair log are filled without explicitly having to
            // discover and track them.
            VoltMessage repairMsg = createRepairMessage(li);
            if (repairLogger.isDebugEnabled()) {
                repairLogger.debug(m_whoami + "repairing: " + CoreUtils.hsIdCollectionToString(m_survivors) + " with: " + TxnEgo.txnIdToString(li.getTxnId()) +
                        " " + repairMsg);
            }
            if (repairMsg != null) {
                m_mailbox.repairReplicasWith(m_survivors, repairMsg);
            }
        }

        m_promotionResult.set(new RepairResult(m_maxSeenTxnId, m_repairTruncationHandle));
    }

    //
    //
    //  Specialization
    //
    //
    VoltMessage makeRepairLogRequestMessage(long requestId, int deadHost)
    {
        return new Iv2RepairLogRequestMessage(requestId, deadHost, Iv2RepairLogRequestMessage.MPIREQUEST);
    }

    // Always add the first message for a transaction id and always
    // replace old messages with complete transaction messages.
    void addToRepairLog(Iv2RepairLogResponseMessage msg)
    {
        // don't add the null payload from the first message ack to the repair log
        if (msg.getPayload() == null) {
            return;
        }

        Iv2RepairLogResponseMessage prev = m_repairLogUnion.floor(msg);
        if (prev != null && (prev.getTxnId() != msg.getTxnId())) {
            prev = null;
        }

        if (prev == null) {
            m_repairLogUnion.add(msg);
        } else if (msg.getPayload() instanceof CompleteTransactionMessage) {
            // prefer complete messages to fragment tasks.
            m_repairLogUnion.remove(prev);
            m_repairLogUnion.add(msg);
        }
    }

    VoltMessage createRepairMessage(Iv2RepairLogResponseMessage msg)
    {
        if (msg.getPayload() instanceof CompleteTransactionMessage) {
            CompleteTransactionMessage ctm = (CompleteTransactionMessage)msg.getPayload();
            // Repair log messages that originated from this host may still be in the scoreboard,
            // so build a new message to avoid corrupting the scoreboard's reference.
            CompleteTransactionMessage message = new CompleteTransactionMessage(ctm.getInitiatorHSId(),
                    ctm.getCoordinatorHSId(), ctm);
            message.setForReplica(false);
            message.setRequireAck(false);
            message.setTimestamp(m_restartSeqGenerator.getNextSeqNum());
            message.setTruncationHandle(TransactionInfoBaseMessage.UNUSED_TRUNC_HANDLE);
            if (repairLogger.isDebugEnabled()) {
                repairLogger.debug(m_whoami + "sending completion for txn " + TxnEgo.txnIdToString(message.getTxnId()) +
                        ", ts " + MpRestartSequenceGenerator.restartSeqIdToString(message.getTimestamp()));
            }
            return message;
        } else {
            FragmentTaskMessage ftm = (FragmentTaskMessage)msg.getPayload();
            // We currently don't want to restart read-only MP transactions because:
            // 1) We're not writing the Iv2InitiateTaskMessage to the first
            // FragmentTaskMessage in read-only case in the name of some unmeasured
            // performance impact,
            // 2) We don't want to perturb command logging and/or DR this close to the 3.0 release
            // 3) We don't guarantee the restarted results returned to the client
            // anyway, so not restarting the read is currently harmless.
            boolean restart = !ftm.isReadOnly();
            if (restart) {
                Iv2InitiateTaskMessage initTaskMsg = ftm.getInitiateTask();
                assert(initTaskMsg != null);
                initTaskMsg.setTruncationHandle(TransactionInfoBaseMessage.UNUSED_TRUNC_HANDLE);
                m_interruptedTxns.add(ftm.getInitiateTask());
            }
            CompleteTransactionMessage rollback = null;
            //A response for non-restartable proc will be sent to client immediately if it is restarted. Thus
            //the transaction is marked as done and the state is removed. But sites may still have fragments in the backlog or site queue
            //which may block Scoreboard to release downstream transactions. The message would help clean the transaction state.
            String procName = ftm.getProcedureName();
            if (procName != null) {
                final SystemProcedureCatalog.Config proc = SystemProcedureCatalog.listing.get(procName);
                if (proc != null && !proc.isRestartable()) {
                    rollback = new CompleteTransactionMessage(
                            ftm.getInitiatorHSId(),
                            ftm.getCoordinatorHSId(),
                            ftm.getTxnId(),
                            ftm.isReadOnly(),
                            0,
                            true,       // Force rollback as our repair operation.
                            false,      // no acks in iv2.
                            false,      // Indicate because this will not be restarted, the txn should be cleaned up
                            ftm.isForReplay(),
                            ftm.isNPartTxn(),
                            true,
                            false);
                    rollback.setTimestamp(m_restartSeqGenerator.getNextSeqNum());
                    rollback.setTruncationHandle(TransactionInfoBaseMessage.UNUSED_TRUNC_HANDLE);
                }
            }

            return rollback;
        }
    }
}
