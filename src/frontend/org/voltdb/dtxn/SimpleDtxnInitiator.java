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
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.debugstate.InitiatorContext;
import org.voltdb.messages.InitiateTask;
import org.voltdb.messages.MembershipNotice;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.impl.SiteMailbox;
import org.voltdb.utils.VoltLoggerFactory;

/** Supports correct execution of multiple partition transactions by executing them one at a time. */
public class SimpleDtxnInitiator extends TransactionInitiator {
    final TransactionIdManager m_idManager;
    final SiteTracker m_siteTracker;

    private final DtxnInitiatorQueue m_queue;

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
    private final int m_hostId;

    public SimpleDtxnInitiator(Messenger messenger, int hostId, int siteId,
                               int initiatorId,
                               Runnable onBackPressure,
                               Runnable offBackPressure)
    {
        assert(messenger != null);
        System.out.printf("INITIALIZING INITIATOR ID: %d, SITEID: %d\n", initiatorId, siteId);
        System.out.flush();

        m_idManager = new TransactionIdManager(initiatorId);
        m_hostId = hostId;
        m_siteId = siteId;
        m_queue = new DtxnInitiatorQueue(siteId);
        m_mailbox = messenger.createMailbox(siteId, VoltDB.DTXN_MAILBOX_ID,
                                            m_queue);
        m_queue.setInitiator(this);
        m_siteTracker = VoltDB.instance().getCatalogContext().siteTracker;
        m_onBackPressure = onBackPressure;
        m_offBackPressure = offBackPressure;
    }

    @Override
    public synchronized void createTransaction(
                                  long connectionId,
                                  final String connectionHostname,
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
            createSinglePartitionTxn(connectionId, connectionHostname, invocation, isReadOnly,
                                     partitions[0], clientData, messageSize, now);
            return;
        }
        else
        {
            long txnId = m_idManager.getNextUniqueTransactionId();

            // store only partitions that are NOT the coordinator
            // this is a bit too slow
            int[] allSiteIds = m_siteTracker.getLiveSitesForEachPartition(partitions);
            int coordinatorId = allSiteIds[0];
            int[] otherSiteIds = new int[allSiteIds.length - 1];

            for (int i = 1; i < allSiteIds.length; i++)
            {
                // if this site is on the same host as the initiator
                // then take over as the coordinator
                if (m_hostId == allSiteIds[i] / VoltDB.SITES_TO_HOST_DIVISOR)
                {
                    otherSiteIds[i - 1] = coordinatorId;
                    coordinatorId = allSiteIds[i];
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
                                                        connectionId,
                                                        connectionHostname);
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
    createSinglePartitionTxn(long connectionId,
                             final String connectionHostname,
                             StoredProcedureInvocation invocation,
                             boolean isReadOnly,
                             int partition,
                             Object clientData,
                             int messageSize,
                             long now)
    {
        long txnId = m_idManager.getNextUniqueTransactionId();

        // split the list of partitions into coordinator and the set of participants
        ArrayList<Integer> site_ids = m_siteTracker.getLiveSitesForPartition(partition);
        ArrayList<InFlightTxnState> txn_states = new ArrayList<InFlightTxnState>();

        increaseBackpressure(messageSize);

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
                                     connectionId,
                                     connectionHostname);
            txn_states.add(txn);
            m_queue.addPendingTxn(txn);
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
        m_queue.addPendingTxn(txn);
        increaseBackpressure(txn.messageSize);

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

    void increaseBackpressure(int messageSize)
    {
        m_pendingTxnBytes += messageSize;
        m_pendingTxnCount++;
        if (m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES || m_pendingTxnCount > MAX_DESIRED_PENDING_TXNS) {
            if (!m_hadBackPressure) {
                transactionLog.trace("DTXN back pressure began");
                m_hadBackPressure = true;
                m_onBackPressure.run();
            }
        }
    }

    void reduceBackpressure(int messageSize)
    {
        m_pendingTxnBytes -= messageSize;
        m_pendingTxnCount--;
        if (m_pendingTxnBytes < (MAX_DESIRED_PENDING_BYTES * .8) &&
            m_pendingTxnCount < (MAX_DESIRED_PENDING_TXNS * .8))
        {
            if (m_hadBackPressure)
            {
                transactionLog.trace("DTXN backpressure ended");
                m_hadBackPressure = false;
                m_offBackPressure.run();
            }
        }
    }

    public void getDumpContents(StringBuilder sb) {
        List<InFlightTxnState> in_flight_txns = m_queue.getInFlightTxns();
        sb.append("Transactions in Flight (").append(in_flight_txns.size()).append("):\n");
        for (InFlightTxnState state : in_flight_txns)
        {
            if (state != null)
            {
                sb.append("emptyfornow\n");
            }
        }
    }

    public void getDumpContents(InitiatorContext context)
    {
        // add mailbox history
        if (m_mailbox instanceof SiteMailbox)
            context.mailboxHistory = ((SiteMailbox) m_mailbox).getHistory();

        // list transactions in flight
        List<InFlightTxnState> inFlightTxnList = m_queue.getInFlightTxns();
        context.inFlightTxns = new InFlightTxnState[inFlightTxnList.size()];
        for (int i = 0; i < inFlightTxnList.size(); i++)
        {
            context.inFlightTxns[i] = inFlightTxnList.get(i);
        }
    }
}
