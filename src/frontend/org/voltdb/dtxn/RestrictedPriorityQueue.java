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

    final LinkedHashMap<Integer, Long> m_lastTxnFromEachInitiator = new LinkedHashMap<Integer, Long>();

    long m_newestSafeTransaction = -1;
    final int m_siteId;
    long m_txnsPopped = 0;
    long m_lastTxnPopped = 0;

    /**
     * Tell this queue about all initiators. If any initiators
     * are later referenced that aren't in this list, trip
     * an assertion.
     */
    RestrictedPriorityQueue(int[] initiatorSiteIds, int siteId) {
        m_siteId = siteId;
        for (int id : initiatorSiteIds)
            m_lastTxnFromEachInitiator.put(id, -1L);
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public TransactionState poll() {
        TransactionState retval = super.peek();
        if ((retval != null) && (retval.txnId <= m_newestSafeTransaction)) {
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
        if ((retval != null) && (retval.txnId <= m_newestSafeTransaction)) {
            m_txnsPopped++;
            m_lastTxnPopped = retval.txnId;
            return retval;
        }

        return null;
    }

    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    void gotTransaction(int initiatorId, long txnId, boolean isHeartbeat) {
        assert(m_lastTxnFromEachInitiator.containsKey(initiatorId));

        if (m_lastTxnPopped > txnId) {
            StringBuilder msg = new StringBuilder();
            msg.append("Txn ordering deadlock at site ").append(m_siteId).append(":\n");
            msg.append("   txn ").append(m_lastTxnPopped).append(" (");
            msg.append(TransactionIdManager.toString(m_lastTxnPopped)).append(" HB:");
            msg.append(isHeartbeat).append(") before\n");
            msg.append("   txn ").append(txnId).append(" (");
            msg.append(TransactionIdManager.toString(txnId)).append(" HB:");
            msg.append(isHeartbeat).append(").\n");
            throw new RuntimeException(msg.toString());
        }

        // update the latest transaction for the specified initiator
        long prevTxnId = m_lastTxnFromEachInitiator.get(initiatorId);
        if (prevTxnId < txnId)
            m_lastTxnFromEachInitiator.put(initiatorId, txnId);
        // find the minimum value across all latest transactions
        long min = Long.MAX_VALUE;
        for (long l : m_lastTxnFromEachInitiator.values())
            if (l < min) min = l;

        // this minimum is the newest safe transaction to run
        m_newestSafeTransaction = min;
    }

    /**
     * @return The id of the newest safe transaction to run.
     */
    long getNewestSafeTransaction() {
        return m_newestSafeTransaction;
    }

    void getDumpContents(ExecutorContext context) {
        // set misc scalars
        context.transactionsStarted = m_txnsPopped;

        // get all the queued transactions
        context.queuedTransactions = new ExecutorTxnState[size()];
        int i = 0;
        for (TransactionState txnState : this) {
            assert(txnState != null);
            context.queuedTransactions[i] = txnState.getDumpContents();
            context.queuedTransactions[i].ready = (txnState.txnId <= m_newestSafeTransaction);
            i++;
        }
        Arrays.sort(context.queuedTransactions);

        // store the contact history
        context.contactHistory = new ExecutorContext.InitiatorContactHistory[m_lastTxnFromEachInitiator.size()];
        i = 0;
        for (Entry<Integer, Long> e : m_lastTxnFromEachInitiator.entrySet()) {
            context.contactHistory[i] = new ExecutorContext.InitiatorContactHistory();
            context.contactHistory[i].initiatorSiteId = e.getKey();
            context.contactHistory[i].transactionId = e.getValue();
            i++;
        }
    }

    public void shutdown() throws InterruptedException {
    }
}
