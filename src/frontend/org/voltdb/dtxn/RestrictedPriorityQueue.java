/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.dtxn;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HeartbeatResponseMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

/**
 * <p>Extends a PriorityQueue such that is only stores transaction state
 * objects, and it only releases them (to a poll() call) if they are
 * ready to be processed.</p>
 *
 * <p>In this case, ready to be processed is determined by storing the
 * most recent transaction id from each initiator. The smallest transaction
 * id across all initiators is safe to run. Also any older transactions are
 * also safe to run.</p>
 *
 * <p>This class manages all that state.</p>
 */
public class RestrictedPriorityQueue extends PriorityQueue<OrderableTransaction> implements RPQInterface  {
    private static final long serialVersionUID = 1L;
    private final VoltLogger m_joinLog = new VoltLogger("JOIN");

    public enum QueueState {
        UNBLOCKED,
        BLOCKED_EMPTY,
        BLOCKED_ORDERING,
        BLOCKED_SAFETY,
        BLOCKED_CLOSED;    // terminal state.
    }

    class LastInitiatorData {
        LastInitiatorData() {
            m_lastSeenTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID; // -1
            m_lastSafeTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID; // -1
        }

        long m_lastSeenTxnId;
        long m_lastSafeTxnId;
    }

    final LinkedHashMap<Long, LastInitiatorData> m_initiatorData = new LinkedHashMap<Long, LastInitiatorData>();
    final LinkedList<RoadBlock> m_roadblocks = new LinkedList<RoadBlock>();

    /**
     * A future transaction point at which RPQ must send an action
     * message and stall indefinitely.
     */
    static class RoadBlock implements Comparable<RoadBlock> {
        final long m_transactionId;
        final QueueState m_reason;
        final VoltMessage m_action;

        RoadBlock(long id, QueueState reason, VoltMessage action) {
            m_transactionId = id;
            m_reason = reason;
            m_action = action;
        }

        @Override
        public int compareTo(RoadBlock o) {
            if (m_transactionId < o.m_transactionId) {
                return -1;
            } else if (m_transactionId > o.m_transactionId) {
                return 1;
            }
            return 0;
        }
    }

    public void makeRoadBlock(long blockAfter, QueueState blockReason, VoltMessage action) {
        action.m_sourceHSId = m_siteId;
        RoadBlock roadblock = new RoadBlock(blockAfter, blockReason, action);
        m_roadblocks.add(roadblock);
        Collections.sort(m_roadblocks);
    }

    QueueState checkRoadBlock(long txnId) {
        // System.out.println("Checking roadblock with txnId: " + txnId);
        RoadBlock roadblock = m_roadblocks.peek();
        if (roadblock != null && roadblock.m_transactionId < txnId) {
            roadblock = m_roadblocks.poll();
            m_joinLog.info("Delivering roadblock action: " +
                               roadblock.m_action + " for txnId: " +
                               roadblock.m_transactionId);
            if (roadblock.m_action != null) {
                m_mailbox.deliverFront(roadblock.m_action);
            }
            return roadblock.m_reason;
        }
        return QueueState.UNBLOCKED;
    }

    long m_newestCandidateTransaction = -1;
    final long m_siteId;
    long m_txnsPopped = 0;
    QueueState m_state = QueueState.BLOCKED_EMPTY;
    final Mailbox m_mailbox;
    final boolean m_useSafetyDance;

    /**
     * Tell this queue about all initiators. If any initiators
     * are later referenced that aren't in this list, trip
     * an assertion.
     */
    public RestrictedPriorityQueue(long[] initiatorHSIds, long siteId, Mailbox mbox, boolean useSafetyDance) {
        m_siteId = siteId;
        m_mailbox = mbox;
        for (long id : initiatorHSIds)
            m_initiatorData.put(id, new LastInitiatorData());
        m_useSafetyDance = useSafetyDance;
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public OrderableTransaction poll() {
        OrderableTransaction retval = null;
        updateQueueState();
        if (m_state == QueueState.UNBLOCKED) {
            retval = super.peek();
            if (!retval.isDurable()) {
                return null;
            }
            super.poll();
            m_txnsPopped++;
        }
        return retval;
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public OrderableTransaction peek() {
        OrderableTransaction retval = null;
        updateQueueState();
        if (m_state == QueueState.UNBLOCKED) {
            retval = super.peek();
            if (!retval.isDurable()) {
                return null;
            }
        }
        return retval;
    }

    /**
     * Drop data for unknown initiators. This is the only valid add interface.
     */
    @Override
    public boolean add(OrderableTransaction txnState) {
        if (m_initiatorData.containsKey(txnState.initiatorHSId) == false) {
            return false;
        }
        boolean retval = super.add(txnState);
        // update the queue state
        if (retval) updateQueueState();
        return retval;
    }

    @Override
    public boolean remove(Object txnState) {
        boolean retval = super.remove(txnState);
        updateQueueState();
        return retval;
    }

    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    @Override
    public long noteTransactionRecievedAndReturnLastSeen(long initiatorHSId, long txnId,
            boolean isHeartbeat, long lastSafeTxnIdFromInitiator)
    {
        // System.out.printf("Site %d got heartbeat message from initiator %d with txnid/safeid: %d/%d\n",
        //                   m_siteId, initiatorHSId, txnId, lastSafeTxnIdFromInitiator);

        // this doesn't exclude dummy txnid but is also a sanity check
        assert(txnId != 0);

        // Drop old data from already-failed initiators.
        if (m_initiatorData.containsKey(initiatorHSId) == false) {
            //hostLog.info("Dropping txn " + txnId + " data from failed initiatorHSId: " + initiatorSiteId);
            return DtxnConstants.DUMMY_LAST_SEEN_TXN_ID;
        }

        // update the latest transaction for the specified initiator
        LastInitiatorData lid = m_initiatorData.get(initiatorHSId);
        if (lid.m_lastSeenTxnId < txnId)
            lid.m_lastSeenTxnId = txnId;
        if (lid.m_lastSafeTxnId < lastSafeTxnIdFromInitiator)
            lid.m_lastSafeTxnId = lastSafeTxnIdFromInitiator;

        /*
         * Why aren't we asserting that the txnId is > then the last seen/last safe
         * It seems like this should be guaranteed by TCP ordering and we want to
         * know if it isn't!
         */

        // find the minimum value across all latest transactions
        long min = Long.MAX_VALUE;
        for (LastInitiatorData l : m_initiatorData.values())
            if (l.m_lastSeenTxnId < min) min = l.m_lastSeenTxnId;

        //  This transaction is the guaranteed minimum
        //  but is not yet necessarily 2PC'd to every site.
        m_newestCandidateTransaction = min;

        // this will update the state of the queue if needed
        updateQueueState();

        // return the last seen id for the originating initiator
        return lid.m_lastSeenTxnId;
    }

    /**
     * Used to poke the PartitionDRGateway with a number that should increase with
     * time as a lower bound on the txnid of the last real work the EE will see.
     * @return 0 if queue is non-empty, a valid txnid otherwise
     */
    public long getEarliestSeenTxnIdAcrossInitiatorsWhenEmpty() {
        if (m_state != QueueState.BLOCKED_EMPTY)
            return 0;
        long txnId = Long.MAX_VALUE;
        for (LastInitiatorData lid : m_initiatorData.values()) {
            if (txnId > lid.m_lastSeenTxnId)
                txnId = lid.m_lastSeenTxnId;
        }
        txnId = Math.max(0, txnId);
        return txnId;
    }

    /**
     * Remove all pending transactions from the specified initiator
     * and do not require heartbeats from that initiator to proceed.
     * @param initiatorId id of the failed initiator.
     */
    @Override
    public void gotFaultForInitiator(long initiatorId) {
        // calculate the next minimum transaction w/o our dead friend
        noteTransactionRecievedAndReturnLastSeen(initiatorId, Long.MAX_VALUE, true, DtxnConstants.DUMMY_LAST_SEEN_TXN_ID);

        // remove initiator from minimum. txnid scoreboard
        m_initiatorData.remove(initiatorId);
    }

    public void faultTransaction(OrderableTransaction txnState) {
        this.remove(txnState);
    }

    /**
     * After a catalog change, double check that all initators in the catalog
     * that are known to be "up" are here, in the RPQ's list.
     * @param initiatorId Initiator present in the catalog.
     * @return The number of initiators that weren't known
     */
    @Override
    public int ensureInitiatorIsKnown(long initiatorId) {
        int newInitiatorCount = 0;
        if (m_initiatorData.get(initiatorId) == null) {
            m_initiatorData.put(initiatorId, new LastInitiatorData());
            newInitiatorCount++;
        }
        return newInitiatorCount;
    }

    /**
     * @return The id of the newest safe transaction to run.
     */
    long getNewestSafeTransaction() {
        return m_newestCandidateTransaction;
    }

    /**
     * Return the largest confirmed txn id for the initiator given.
     * Used to figure out what to do after an initiator fails.
     * @param initiatorId The id of the initiator that has failed.
     */
    @Override
    public Long getNewestSafeTransactionForInitiator(long initiatorId) {
        LastInitiatorData lid = m_initiatorData.get(initiatorId);
        if (lid == null) {
            return null;
        }
        return lid.m_lastSafeTxnId;
    }

    public void shutdown() throws InterruptedException {
    }

    public QueueState getQueueState() {
        return m_state;
    }

    long m_blockTime = 0;

    QueueState updateQueueState() {
        QueueState newState = QueueState.UNBLOCKED;
        OrderableTransaction ts = super.peek();
        LastInitiatorData lid = null;

        // Terminal states (currently only BLOCKED_CLOSED)
        if (m_state == QueueState.BLOCKED_CLOSED) {
            return m_state;
        }
        assert (newState == QueueState.UNBLOCKED);

        // Empty queue
        if (ts == null) {
            // Roadblocks - heartbeats can drive an empty queue
            // to the BLOCKED_CLOSED state, too.
            QueueState checkRoadBlock = checkRoadBlock(m_newestCandidateTransaction);
            if (checkRoadBlock == QueueState.BLOCKED_CLOSED) {
                executeStateChange(checkRoadBlock, ts, lid);
                return m_state;
            } else {
                //No roadblock due to heartbeats, switch to BLOCKED_EMPTY
                newState = QueueState.BLOCKED_EMPTY;
                executeStateChange(newState, ts, lid);
                return m_state;
            }

        }
        assert (newState == QueueState.UNBLOCKED);

        // Roadblocks - txn drives queue to BLOCKED_CLOSED due to roadblock
        {
            newState = checkRoadBlock(ts.txnId);
            if (newState == QueueState.BLOCKED_CLOSED) {
                executeStateChange(newState, ts, lid);
                return m_state;
            }
        }

        assert (newState == QueueState.UNBLOCKED);

        // Sufficient ordering established?
        if (ts.txnId > m_newestCandidateTransaction) {
            newState = QueueState.BLOCKED_ORDERING;
            executeStateChange(newState, ts, lid);
            return m_state;
        }
        assert (newState == QueueState.UNBLOCKED);

        // Remember, an 'in recovery' response satisfies the safety dance
        lid = m_initiatorData.get(ts.initiatorHSId);
        if (lid == null) {
            // what does this mean???
        }
        // if the txn is newer than the last safe txn from initiatior, block
        //  except if this RPQ has safety turned off
        else if (m_useSafetyDance && (ts.txnId > lid.m_lastSafeTxnId)) {
            newState = QueueState.BLOCKED_SAFETY;
            executeStateChange(newState, ts, lid);
            return m_state;
        }
        assert (newState == QueueState.UNBLOCKED);

        // legitimately unblocked
        assert (ts != null);

        executeStateChange( newState, ts, lid);

        return newState;
    }

    private void executeStateChange(QueueState newState, OrderableTransaction ts,
            LastInitiatorData lid)
    {
        // Execute state changes
        if (newState != m_state) {
            // Count millis spent non-empty but blocked
            if ((newState == QueueState.BLOCKED_ORDERING) ||
                (newState == QueueState.BLOCKED_SAFETY))
            {
                m_blockTime = System.currentTimeMillis();
            }

            // Send a heartbeat response on blocked safety transitions
            // This side-effect is a little broken. It results in extra
            // heartbeat responses in some paths.
            if (newState == QueueState.BLOCKED_SAFETY) {
                assert(ts != null);
                assert(lid != null);
                sendHearbeatResponse(ts, lid);
            }

            m_state = newState;
        }
    }

    private void sendHearbeatResponse(OrderableTransaction ts, LastInitiatorData lid) {
        // mailbox might be null in testing
        if (m_mailbox == null) return;

        HeartbeatResponseMessage hbr =
            new HeartbeatResponseMessage(m_siteId, lid.m_lastSeenTxnId, true);
        m_mailbox.send(ts.initiatorHSId, hbr);
    }

    /**
     * Determine if it is safe to recover and if it is, what txnid it is safe to recover at.
     * Recovery is initiated by the recovering source partition. It can't be initiated until the recovering
     * partition has heard from every initiator. This is because it is not possible to pick a point
     * in the global txn ordering for the recovery to start at where all subsequent procedure invocations
     * that need to be applied after recovery are available unless every initiator has been heard from.
     *
     * Once the initiators have all been heard from it is necessary to pick the lowest txnid possible for all pending
     * work. This means taking the min of the newest candidate transaction | the txnid of the next txn in the queue.
     *
     * The newest candidate transaction is used if there are no pending txns so recovery can start when
     * the system is idle.
     */
    public Long safeToRecover() {
        boolean safe = true;
        for (LastInitiatorData data : m_initiatorData.values()) {
            final long lastSeenTxnId = data.m_lastSeenTxnId;
            if (lastSeenTxnId == DtxnConstants.DUMMY_LAST_SEEN_TXN_ID) {
                safe = false;
            }
        }
        if (!safe) {
            return null;
        }

        OrderableTransaction next = peek();
        if (next == null) {
            // no work - have heard from all initiators. use a heartbeat
            if (m_state == QueueState.BLOCKED_EMPTY) {
                return m_newestCandidateTransaction;
            }
            // waiting for some txn to be 2pc to this site.
            else if (m_state == QueueState.BLOCKED_SAFETY) {
                return null;
            } else if (m_state == QueueState.BLOCKED_ORDERING){
                return null;
            }
            m_joinLog.error("Unexpected RPQ state " + m_state + " when attempting to start recovery at " +
                    " the source site. Consider killing the recovering node and trying again");
            return null; // unreachable
        }
        else {
            // bingo - have a real transaction to return as the recovery point
            return next.txnId;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("State: ").append(m_state);
        for (Map.Entry<Long, LastInitiatorData> entry : m_initiatorData.entrySet()) {
            LastInitiatorData lid = entry.getValue();
            sb.append(' ');
            sb.append(CoreUtils.hsIdToString(entry.getKey()));
            sb.append("==");
            sb.append(lid.m_lastSeenTxnId);
            sb.append(':');
            sb.append(lid.m_lastSafeTxnId);
            sb.append(' ');
        }
        sb.append('\n');
        sb.append(super.toString());
        return sb.toString();
    }
}
