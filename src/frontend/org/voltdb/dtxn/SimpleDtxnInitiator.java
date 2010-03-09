/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.debugstate.InitiatorContext;
import org.voltdb.messages.InitiateResponse;
import org.voltdb.messages.InitiateTask;
import org.voltdb.messages.MembershipNotice;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.messaging.impl.SiteMailbox;
import org.voltdb.network.Connection;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.VoltLoggerFactory;

/** Supports correct execution of multiple partition transactions by executing them one at a time. */
public class SimpleDtxnInitiator extends TransactionInitiator {
    final TransactionIdManager m_idManager;
    final SiteTracker m_siteTracker;

    private static final Logger transactionLog =
        Logger.getLogger("TRANSACTION", VoltLoggerFactory.instance());

    /**
     * Task to run when a backpressure condition starts
     */
    private final Runnable m_onBackPressure;

    /**
     * Task to run when a backpressure condition stops
     */
    private final Runnable m_offBackPressure;

    /**
     * Indicates if backpressure has been seen and reported
     */
    private boolean m_hadBackPressure = false;

    /**
     * Storage for initiator statistics
     */
    final InitiatorStats m_stats;

    // If an initiator handles a full node, it processes approximately 50,000 txns/sec.
    // That's about 50 txns/ms. Try not to keep more than 5 ms of work? Seems like a really
    // small number. On the other hand, backPressure() is just a hint to the ClientInterface.
    // CI will submit ClientPort.MAX_READ * clients / bytesPerStoredProcInvocation txns
    // on average if clients present constant uninterrupted load.
    private final static int MAX_DESIRED_PENDING_BYTES = 67108864;
    private final static int MAX_DESIRED_PENDING_TXNS = 15000;
    private long m_pendingTxnBytes = 0;
    private int m_pendingTxnCount = 0;
    private final Mailbox m_mailbox;
    private final int m_siteId;


    public SimpleDtxnInitiator(Mailbox mailbox, int hostId, int siteId,
                               int initiatorId,
                               Runnable onBackPressure,
                               Runnable offBackPressure)
    {
        assert(mailbox != null);
        System.out.printf("INITIALIZING INITIATOR ID: %d, SITEID: %d\n", initiatorId, siteId);
        System.out.flush();

        m_stats = new InitiatorStats("Initiator " + siteId + " stats", siteId);
        m_idManager = new TransactionIdManager(initiatorId);
        m_mailbox = mailbox;
        m_siteId = siteId;
        m_siteTracker = VoltDB.instance().getCatalogContext().siteTracker;
        m_onBackPressure = onBackPressure;
        m_offBackPressure = offBackPressure;
    }

    @Override
    public synchronized void createTransaction(
                                  int connectionId,
                                  StoredProcedureInvocation invocation,
                                  boolean isReadOnly,
                                  boolean isSinglePartition,
                                  int partitions[],
                                  int numPartitions,
                                  Object clientData,
                                  int messageSize,
                                  long now)
    {
        assert(invocation != null);
        assert(partitions != null);
        assert(numPartitions >= 1);

        if (isSinglePartition)
        {
            assert(numPartitions == 1);
            createSinglePartitionTxn(connectionId, invocation, isReadOnly,
                                     partitions[0], clientData, messageSize, now);
            return;
        }
        else
        {
            long txnId = m_idManager.getNextUniqueTransactionId();

            // store only partitions that are NOT the coordinator
            // this is a bit too slow
            int[] allSiteIds = m_siteTracker.getAllSitesForEachPartition(partitions);
            int coordinatorId = allSiteIds[0];
            int[] otherSiteIds = new int[allSiteIds.length - 1];

            for (int i = 1; i < allSiteIds.length; i++)
            {
                // if this site is in the list of sites for this partition
                // then take over as the coordinator
                if (allSiteIds[i] == m_siteId)
                {
                    otherSiteIds[i - 1] = coordinatorId;
                    coordinatorId = m_siteId;
                }
                else
                {
                    otherSiteIds[i - 1] = allSiteIds[i];
                }
            }

            InFlightTxnState txn = new InFlightTxnState(txnId,
                                                        coordinatorId,
                                                        otherSiteIds,
                                                        isReadOnly,
                                                        false,
                                                        invocation,
                                                        clientData,
                                                        messageSize,
                                                        now,
                                                        connectionId);
            dispatchMultiPartitionTxn(txn);
        }
    }

    @Override
    public synchronized void tick(long time, long interval) {
        long txnId = m_idManager.getNextUniqueTransactionId();
        MembershipNotice tickNotice = new MembershipNotice(m_siteId, -1, txnId, false);
        tickNotice.setIsHeartBeat(true);

        int[] outOfDateSites = m_siteTracker.getSitesWhichNeedAHeartbeat(time, interval);
        try {
            m_mailbox.send(outOfDateSites, 0, tickNotice);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getMostRecentTxnId() {
        return m_idManager.getLastTxnId();
    }

    private void
    createSinglePartitionTxn(int connectionId,
                             StoredProcedureInvocation invocation,
                             boolean isReadOnly,
                             int partition,
                             Object clientData,
                             int messageSize,
                             long now)
    {
        long txnId = m_idManager.getNextUniqueTransactionId();

        // split the list of partitions into coordinator and the set of participants
        ArrayList<Integer> site_ids = m_siteTracker.getAllSitesForPartition(partition);
        ArrayList<InFlightTxnState> txn_states = new ArrayList<InFlightTxnState>();

        m_pendingTxnBytes += messageSize;
        m_pendingTxnCount++;
        if (m_pendingTxnCount > MAX_DESIRED_PENDING_TXNS || m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES) {
            if (!m_hadBackPressure) {
                transactionLog.trace("DTXN back pressure began");
                m_hadBackPressure = true;
                m_onBackPressure.run();
            }
        }

        // create and register each replicated transaction with the pending
        // transaction structure.  do this before transmitting them to avoid
        // races where we get a reply from a replica before we finish
        // transmission
        for (int site_id : site_ids)
        {
            InFlightTxnState txn =
                new InFlightTxnState(txnId,
                                     site_id,
                                     null,
                                     isReadOnly,
                                     true,
                                     invocation.getShallowCopy(),
                                     clientData,
                                     messageSize,
                                     now,
                                     connectionId);
            txn_states.add(txn);
            m_pendingTxns.addTxn(txn.txnId, txn.coordinatorId, txn);

        }

        for (InFlightTxnState txn_state : txn_states)
        {
            sendTransactionToCoordinator(txn_state);
        }
    }

    /**
     * Notify all participating partitions that they will be
     * involved in a transaction. This message contains no work,
     * but allows txnids to get into a site's queue faster, thus
     * reducing the amount of time when things are blocked.
     *
     * @param txn Information about the transaction to send.
     */
    private void dispatchMultiPartitionTxn(InFlightTxnState txn)
    {
        m_pendingTxns.addTxn(txn.txnId, txn.coordinatorId, txn);
        m_pendingTxnBytes += txn.messageSize;
        m_pendingTxnCount++;
        if (m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES || m_pendingTxnCount > MAX_DESIRED_PENDING_TXNS) {
            if (!m_hadBackPressure) {
                transactionLog.trace("DTXN back pressure began");
                m_hadBackPressure = true;
                m_onBackPressure.run();
            }
        }

        MembershipNotice notice = new MembershipNotice(m_siteId,
                                                       txn.coordinatorId,
                                                       txn.txnId,
                                                       txn.isReadOnly);
        try {
            m_mailbox.send(txn.otherSiteIds, 0, notice);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

        sendTransactionToCoordinator(txn);
    }

    /**
     * Send the initiation notice to the coordinator for a
     * transaction. This message does contain work. In the case
     * of a single-partition transaction, this is all that is
     * needed to run a transaction.
     *
     * @param txn Information about the transaction to send.
     */
    private void sendTransactionToCoordinator(InFlightTxnState txn)
    {
        InitiateTask workRequest = new InitiateTask(
                m_siteId,
                txn.coordinatorId,
                txn.txnId,
                txn.isReadOnly,
                txn.isSinglePartition,
                txn.invocation);
        workRequest.setNonCoordinatorSites(txn.otherSiteIds);

        try {
            m_mailbox.send(txn.coordinatorId, 0, workRequest);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    /** Map of transaction ids to transaction information */
    private final PendingTxnList m_pendingTxns = new PendingTxnList();

    private static class PendingTxnList
    {
        HashMap<Long, HashMap<Integer, InFlightTxnState>> m_txnIdMap;

        PendingTxnList()
        {
            m_txnIdMap =
                new HashMap<Long,
                            HashMap<Integer, InFlightTxnState>>();
        }

        void addTxn(long txnId, int coordinatorSiteId, InFlightTxnState txn)
        {
            HashMap<Integer, InFlightTxnState> site_map = m_txnIdMap.get(txnId);
            if (site_map == null)
            {
                site_map = new HashMap<Integer, InFlightTxnState>();
                m_txnIdMap.put(txnId, site_map);
            }
            site_map.put(coordinatorSiteId, txn);
        }

        InFlightTxnState getTxn(long txnId, int coordinatorSiteId)
        {
            HashMap<Integer, InFlightTxnState> site_map = m_txnIdMap.get(txnId);
            if (site_map == null)
            {
                return null;
            }
            InFlightTxnState state = site_map.remove(coordinatorSiteId);
            return state;
        }

        void removeTxnId(long txnId)
        {
            if (m_txnIdMap.containsKey(txnId))
            {
                if (m_txnIdMap.get(txnId).size() == 0)
                {
                    m_txnIdMap.remove(txnId);
                }
                else
                {
                    assert(false) : "Don't remove non-empty txnId map for: " + txnId;
                    VoltDB.crashVoltDB();
                }
            }
            else
            {
                assert(false) : "Attempt to remove txnId that doesn't exist: " + txnId;
                VoltDB.crashVoltDB();
            }
        }

        int size()
        {
            return m_txnIdMap.size();
        }

        int getTxnIdSize(long txnId)
        {
            if (m_txnIdMap.containsKey(txnId))
            {
                return m_txnIdMap.get(txnId).size();
            }
            // txnId better exist
            assert(false) : "Transaction does not exist: " + txnId;
            return -1;
        }

        ArrayList<InFlightTxnState> getInFlightTxns()
        {
            ArrayList<InFlightTxnState> retval = new ArrayList<InFlightTxnState>();
            for (long txnId : m_txnIdMap.keySet())
            {
                // horrible hack to just get one InFlightTxnState out of the inner map
                for (InFlightTxnState txn_state : m_txnIdMap.get(txnId).values())
                {
                    retval.add(txn_state);
                    break;
                }
            }
            return retval;
        }
    }

    public static class DummyQueue implements Queue<VoltMessage> {

        private SimpleDtxnInitiator m_initiator;
        private final HashMap<Long, VoltTable[]> m_txnIdResponses;

        public DummyQueue()
        {
            m_txnIdResponses =
                new HashMap<Long, VoltTable[]>();
        }

        public void setInitiator(SimpleDtxnInitiator initiator) {
            m_initiator = initiator;
        }

        @Override
        public boolean add(VoltMessage arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(Object arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized boolean offer(VoltMessage message) {
            assert(message instanceof InitiateResponse);
            final InitiateResponse r = (InitiateResponse) message;

            InFlightTxnState state;
            int sites_left = -1;
            synchronized (m_initiator)
            {
                state =
                    m_initiator.m_pendingTxns.getTxn(r.getTxnId(), r.getCoordinatorSiteId());
                sites_left = m_initiator.m_pendingTxns.getTxnIdSize(r.getTxnId());
            }

            assert(state.coordinatorId == r.getCoordinatorSiteId());
            assert(m_initiator.m_siteId == r.getInitiatorSiteId());

            boolean first_response = false;
            VoltTable[] first_results = null;
            // XXX HACK I hacked this up to avoid optimistic return to the client
            // because then I could avoid cloning a table which was killing performance
            // Will need to revisit this soon.  --izzy
            if (!m_txnIdResponses.containsKey(r.getTxnId()))
            {
                ClientResponseImpl curr_response = (ClientResponseImpl) r.getClientResponseData();
                VoltTable[] curr_results = curr_response.getResults();
                VoltTable[] saved_results = new VoltTable[curr_results.length];
                // Create shallow copies of all the VoltTables to avoid
                // race conditions with the ByteBuffer metadata
                for (int i = 0; i < curr_results.length; ++i)
                {
                    saved_results[i] = new VoltTable(curr_results[i].getTableDataReference(), true);
                }
                m_txnIdResponses.put(r.getTxnId(), saved_results);
                first_response = true;
            }
            else
            {
                first_results = m_txnIdResponses.get(r.getTxnId());
            }
            if (first_response)
            {
                // If this is a read-only transaction then we'll return
                // the first response to the client
                if (state.isReadOnly)
                {
                    r.setClientHandle(state.invocation.getClientHandle());
                    // Horrible but so much more efficient.
                    final Connection c = (Connection)state.clientData;
                    assert(c != null);
                    c.writeStream().enqueue(r.getClientResponseData());
                }
            }
            else
            {
                assert(first_results != null);

                ClientResponseImpl curr_response = (ClientResponseImpl) r.getClientResponseData();
                VoltTable[] curr_results = curr_response.getResults();
                if (first_results.length != curr_results.length)
                {
                    String msg = "Mismatched result count received for transaction: " + r.getTxnId();
                    msg += "\n  from execution site: " + r.getCoordinatorSiteId();
                    msg += "\n  Expected number of results: " + first_results.length;
                    msg += "\n  Mismatched number of results: " + curr_results.length;
                    throw new RuntimeException(msg);
                }
                for (int i = 0; i < first_results.length; ++i)
                {
                    if (!curr_results[i].hasSameContents(first_results[i]))
                    {
                        String msg = "Mismatched results received for transaction: " + r.getTxnId();
                        msg += "\n  from execution site: " + r.getCoordinatorSiteId();
                        msg += "\n  Expected results: " + first_results[i].toString();
                        msg += "\n  Mismatched results: " + curr_results[i].toString();
                        throw new RuntimeException(msg);
                    }
                }
            }

            // XXX_K-SAFE if we never receive a response from a site,
            // this data structure is going to leak memory.  Need to ponder
            // where to jam a timeout.  Maybe just wait for failure detection
            // to tell us to clean up
            if (sites_left == 0)
            {
                synchronized (m_initiator)
                {
                    m_initiator.m_pendingTxns.removeTxnId(r.getTxnId());
                    m_initiator.m_pendingTxnBytes -= state.messageSize;
                    m_initiator.m_pendingTxnCount--;
                    if (m_initiator.m_pendingTxnBytes < (MAX_DESIRED_PENDING_BYTES * .8) &&
                            m_initiator.m_pendingTxnCount < (MAX_DESIRED_PENDING_TXNS * .8))
                    {
                        if (m_initiator.m_hadBackPressure)
                        {
                            transactionLog.trace("DTXN backpressure ended");
                            m_initiator.m_hadBackPressure = false;
                            m_initiator.m_offBackPressure.run();
                        }
                    }
                }
                m_txnIdResponses.remove(r.getTxnId());
                if (!state.isReadOnly)
                {
                    r.setClientHandle(state.invocation.getClientHandle());
                    //Horrible but so much more efficient.
                    final Connection c = (Connection)state.clientData;

                    assert(c != null) : "NULL connection in connection state client data.";
                    final long now = EstTime.currentTimeMillis();
                    final int delta = (int)(now - state.initiateTime);
                    final ClientResponseImpl response = r.getClientResponseData();
                    response.setClusterRoundtrip(delta);
                    m_initiator.m_stats.logTransactionCompleted(state.connectionId, state.invocation, delta);
                    c.writeStream().enqueue(response);
                }
            }

            return true;
        }


        @Override
        public boolean remove(Object arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public VoltMessage element() {
            throw new UnsupportedOperationException();
        }

        @Override
        public VoltMessage peek() {
            throw new UnsupportedOperationException();
        }

        @Override
        public VoltMessage poll() {
            throw new UnsupportedOperationException();
        }

        @Override
        public VoltMessage remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends VoltMessage> arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<VoltMessage> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T[] toArray(T[] arg0) {
            throw new UnsupportedOperationException();
        }

    }

    public void getDumpContents(StringBuilder sb) {
        sb.append("Transactions in Flight (").append(m_pendingTxns.size()).append("):\n");
        for (InFlightTxnState state : m_pendingTxns.getInFlightTxns())
        {
            if (state != null)
            {
                sb.append("emptyfornow\n");
            }
        }
    }

    public void getDumpContents(InitiatorContext context) {
        // add mailbox history
        if (m_mailbox instanceof SiteMailbox)
            context.mailboxHistory = ((SiteMailbox) m_mailbox).getHistory();

        // list transactions in flight
        ArrayList<InFlightTxnState> inFlightTxnList = new ArrayList<InFlightTxnState>();
        try {
            for (InFlightTxnState txn : m_pendingTxns.getInFlightTxns())
                inFlightTxnList.add(txn);
        }
        catch (Exception e) {
            // not much we can hope to do to fix an exception here
        }
        context.inFlightTxns = new InFlightTxnState[inFlightTxnList.size()];
        for (int i = 0; i < inFlightTxnList.size(); i++)
            context.inFlightTxns[i] = inFlightTxnList.get(i);
    }
}
