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

package org.voltdb.dtxn;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import org.voltdb.TransactionIdManager;
import org.voltdb.debugstate.ExecutorContext;
import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState;

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
public class RestrictedPriorityQueue extends PriorityQueue<TransactionState> {
    private static final long serialVersionUID = 1L;

    class LastInitiatorData {
        LastInitiatorData() {
            m_lastSeenTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID; // -1
            m_lastSafeTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID; // -1
        }

        long m_lastSeenTxnId;
        long m_lastSafeTxnId;
    }

    final LinkedHashMap<Integer, LastInitiatorData> m_initiatorData = new LinkedHashMap<Integer, LastInitiatorData>();

    long m_newestCandidateTransaction = -1;
    final int m_siteId;
    long m_txnsPopped = 0;
    long m_lastTxnPopped = 0;

    /**
     * Tell this queue about all initiators. If any initiators
     * are later referenced that aren't in this list, trip
     * an assertion.
     */
    public RestrictedPriorityQueue(int[] initiatorSiteIds, int siteId) {
        m_siteId = siteId;
        for (int id : initiatorSiteIds)
            m_initiatorData.put(id, new LastInitiatorData());
    }

    private boolean isSafe(TransactionState ts) {
        if (ts == null) return false;
        if (ts.txnId > m_newestCandidateTransaction) return false;
        LastInitiatorData lid = m_initiatorData.get(ts.initiatorSiteId);
        return (ts.txnId <= lid.m_lastSafeTxnId);
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public TransactionState poll() {
        TransactionState retval = super.peek();
        if (isSafe(retval)) {
            m_txnsPopped++;
            m_lastTxnPopped = retval.txnId;
            return super.poll();
        }

        return null;
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public TransactionState peek() {
        TransactionState retval = super.peek();
        if (isSafe(retval)) {
            m_lastTxnPopped = retval.txnId;
            return retval;
        }

        return null;
    }

    /**
     * Drop data for unknown initiators. This is the only valid add interface.
     */
    @Override
    public boolean add(TransactionState txnState) {
        if (m_initiatorData.containsKey(txnState.initiatorSiteId)) {
            return super.add(txnState);
        }
        return false;
    }

    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    public long noteTransactionRecievedAndReturnLastSeen(int initiatorId, long txnId, boolean isHeartbeat, long lastSafeTxnIdFromInitiator)
    {
        // this excludes dummy txnid but is also a sanity check
        assert(txnId > 0);

        // Drop old data from already-failed initiators.
        if (m_initiatorData.containsKey(initiatorId)) {
            // we've decided that this can happen, and it's fine... just ignore it
            if (m_lastTxnPopped > txnId) {
                StringBuilder msg = new StringBuilder();
                msg.append("Txn ordering deadlock (QUEUE) at site ").append(m_siteId).append(":\n");
                msg.append("   txn ").append(m_lastTxnPopped).append(" (");
                msg.append(TransactionIdManager.toString(m_lastTxnPopped)).append(" HB: ?) before\n");
                msg.append("   txn ").append(txnId).append(" (");
                msg.append(TransactionIdManager.toString(txnId)).append(" HB:");
                msg.append(isHeartbeat).append(").\n");
                System.err.print(msg.toString());
            }

            // update the latest transaction for the specified initiator
            LastInitiatorData lid = m_initiatorData.get(initiatorId);
            if (lid.m_lastSeenTxnId < txnId)
                lid.m_lastSeenTxnId = txnId;
            if (lid.m_lastSafeTxnId < lastSafeTxnIdFromInitiator)
                lid.m_lastSafeTxnId = lastSafeTxnIdFromInitiator;

            // find the minimum value across all latest transactions
            long min = Long.MAX_VALUE;
            for (LastInitiatorData l : m_initiatorData.values())
                if (l.m_lastSeenTxnId < min) min = l.m_lastSeenTxnId;

            // this minimum is the newest safe transaction to run
            // but you still need to check if a transaction has been confirmed
            //  by its initiator
            //  (note: this check is done when peeking/polling from the queue)
            m_newestCandidateTransaction = min;

            // return the last seen id for the originating initiator
            return lid.m_lastSeenTxnId;
        }

        return DtxnConstants.DUMMY_LAST_SEEN_TXN_ID;
    }

    /**
     * Remove all pending transactions from the specified initiator
     * and do not require heartbeats from that initiator to proceed.
     * @param initiatorId id of the failed initiator.
     */
    public void gotFaultForInitiator(int initiatorId) {
        // calculate the next minimum transaction w/o our dead friend
        noteTransactionRecievedAndReturnLastSeen(initiatorId, Long.MAX_VALUE, true, DtxnConstants.DUMMY_LAST_SEEN_TXN_ID);

        // remove initiator from minimum. txnid scoreboard
        LastInitiatorData remove = m_initiatorData.remove(initiatorId);
        assert(remove != null);
    }

    public void faultTransaction(TransactionState txnState) {
        this.remove(txnState);
    }

    /**
     * @return The id of the newest safe transaction to run.
     */
    long getNewestSafeTransaction() {
        return m_newestCandidateTransaction;
    }

    /**
     * TODO: implement this functionality.
     * @param context
     */
    public long getNewestSafeTransactionForInitiator(int initiatorId) {
        return Long.MAX_VALUE;
    }

    public void getDumpContents(ExecutorContext context) {
        // set misc scalars
        context.transactionsStarted = m_txnsPopped;

        // get all the queued transactions
        context.queuedTransactions = new ExecutorTxnState[size()];
        int i = 0;
        for (TransactionState txnState : this) {
            assert(txnState != null);
            context.queuedTransactions[i] = txnState.getDumpContents();
            context.queuedTransactions[i].ready = (txnState.txnId <= m_newestCandidateTransaction);
            i++;
        }
        Arrays.sort(context.queuedTransactions);

        // store the contact history
        context.contactHistory = new ExecutorContext.InitiatorContactHistory[m_initiatorData.size()];
        i = 0;
        for (Entry<Integer, LastInitiatorData> e : m_initiatorData.entrySet()) {
            context.contactHistory[i] = new ExecutorContext.InitiatorContactHistory();
            context.contactHistory[i].initiatorSiteId = e.getKey();
            context.contactHistory[i].transactionId = e.getValue().m_lastSeenTxnId;
            i++;
        }
    }

    public void shutdown() throws InterruptedException {
    }

}
