/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import com.google_voltpatches.common.util.concurrent.SettableFuture;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;

public class SpPromoteAlgo implements RepairAlgo
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final long m_requestId = System.nanoTime();
    private final List<Long> m_survivors;
    private long m_maxSeenTxnId;

    // Each Term can process at most one promotion; if promotion fails, make
    // a new Term and try again (if that's your big plan...)
    private final SettableFuture<Long> m_promotionResult = SettableFuture.create();

    long getRequestId()
    {
        return m_requestId;
    }

    // scoreboard for responding replica repair log responses (hsid -> response count)
    static class ReplicaRepairStruct
    {
        int m_receivedResponses = 0;
        int m_expectedResponses = -1; // (a log msg cares about this init. value)
        long m_maxSpHandleSeen = Long.MIN_VALUE;

        // update counters and return the number of outstanding messages.
        boolean update(Iv2RepairLogResponseMessage response)
        {
            m_receivedResponses++;
            m_expectedResponses = response.getOfTotal();
            m_maxSpHandleSeen = Math.max(m_maxSpHandleSeen, response.getHandle());
            return logsComplete();
        }

        boolean logsComplete()
        {
            return (m_expectedResponses - m_receivedResponses) == 0;
        }

        // return true if this replica needs the message for spHandle.
        boolean needs(long spHandle)
        {
            return m_maxSpHandleSeen < spHandle;
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
            if (o1.getHandle() < o2.getHandle()) {
                return -1;
            }
            else if (o1.getHandle() > o2.getHandle()) {
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
    public SpPromoteAlgo(List<Long> survivors, InitiatorMailbox mailbox,
            String whoami, int partitionId)
    {
        m_mailbox = mailbox;
        m_survivors = survivors;

        m_whoami = whoami;
        m_maxSeenTxnId = TxnEgo.makeZero(partitionId).getTxnId();
    }

    @Override
    public Future<Long> start()
    {
        try {
            prepareForFaultRecovery();
        } catch (Exception e) {
            tmLog.error(m_whoami + "failed leader promotion:", e);
            m_promotionResult.setException(e);
        }
        return m_promotionResult;
    }

    @Override
    public boolean cancel()
    {
        return m_promotionResult.cancel(false);
    }

    /** Start fixing survivors: setup scoreboard and request repair logs. */
    void prepareForFaultRecovery()
    {
        for (Long hsid : m_survivors) {
            m_replicaRepairStructs.put(hsid, new ReplicaRepairStruct());
        }

        tmLog.info(m_whoami + "found (including self) " + m_survivors.size()
                 + " surviving replicas to repair. "
                 + " Survivors: " + CoreUtils.hsIdCollectionToString(m_survivors));
        VoltMessage logRequest =
            new Iv2RepairLogRequestMessage(m_requestId, Iv2RepairLogRequestMessage.SPREQUEST);
        m_mailbox.send(com.google_voltpatches.common.primitives.Longs.toArray(m_survivors), logRequest);
    }

    /** Process a new repair log response */
    @Override
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2RepairLogResponseMessage) {
            Iv2RepairLogResponseMessage response = (Iv2RepairLogResponseMessage)message;
            if (response.getRequestId() != m_requestId) {
                tmLog.debug(m_whoami + "rejecting stale repair response."
                          + " Current request id is: " + m_requestId
                          + " Received response for request id: " + response.getRequestId());
                return;
            }
            ReplicaRepairStruct rrs = m_replicaRepairStructs.get(response.m_sourceHSId);
            if (rrs.m_expectedResponses < 0) {
                tmLog.debug(m_whoami + "collecting " + response.getOfTotal()
                          + " repair log entries from "
                          + CoreUtils.hsIdToString(response.m_sourceHSId));
            }
            // Long.MAX_VALUE has rejoin semantics
            if (response.getHandle() != Long.MAX_VALUE) {
                m_maxSeenTxnId = Math.max(m_maxSeenTxnId, response.getHandle());
            }
            if (response.getPayload() != null) {
                m_repairLogUnion.add(response);
                if (tmLog.isTraceEnabled()) {
                    tmLog.trace(m_whoami + " collected from " + CoreUtils.hsIdToString(response.m_sourceHSId) +
                            ", message: " + response.getPayload());
                }
            }
            if (rrs.update(response)) {
                tmLog.debug(m_whoami + "collected " + rrs.m_receivedResponses
                          + " responses for " + rrs.m_expectedResponses
                          + " repair log entries from " + CoreUtils.hsIdToString(response.m_sourceHSId));
                if (areRepairLogsComplete()) {
                    repairSurvivors();
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

    /** Send missed-messages to survivors. */
    public void repairSurvivors()
    {
        // cancel() and repair() must be synchronized by the caller (the deliver lock,
        // currently). If cancelled and the last repair message arrives, don't send
        // out corrections!
        if (this.m_promotionResult.isCancelled()) {
            tmLog.debug(m_whoami + "Skipping repair message creation for cancelled Term.");
            return;
        }

        int queued = 0;
        tmLog.debug(m_whoami + "received all repair logs and is repairing surviving replicas.");
        for (Iv2RepairLogResponseMessage li : m_repairLogUnion) {
            List<Long> needsRepair = new ArrayList<Long>(5);
            for (Entry<Long, ReplicaRepairStruct> entry : m_replicaRepairStructs.entrySet()) {
                if  (entry.getValue().needs(li.getHandle())) {
                    ++queued;
                    tmLog.debug(m_whoami + "repairing " + CoreUtils.hsIdToString(entry.getKey()) +
                            ". Max seen " + entry.getValue().m_maxSpHandleSeen + ". Repairing with " +
                            li.getHandle());
                    needsRepair.add(entry.getKey());
                }
            }
            if (!needsRepair.isEmpty()) {
                if (tmLog.isTraceEnabled()) {
                    tmLog.trace(m_whoami + "repairing: " + CoreUtils.hsIdCollectionToString(needsRepair) +
                            " with message: " + li.getPayload());
                }
                m_mailbox.repairReplicasWith(needsRepair, li.getPayload());
            }
        }
        tmLog.debug(m_whoami + "finished queuing " + queued + " replica repair messages.");

        m_promotionResult.set(m_maxSeenTxnId);
    }
}
