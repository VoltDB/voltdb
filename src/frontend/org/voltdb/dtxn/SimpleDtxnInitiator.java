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

import org.voltdb.CatalogContext;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.debugstate.InitiatorContext;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.HeartbeatMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

/** Supports correct execution of multiple partition transactions by executing them one at a time. */
public class SimpleDtxnInitiator extends TransactionInitiator {
    final TransactionIdManager m_idManager;

    private final ExecutorTxnIdSafetyState m_safetyState;

    private static final VoltLogger transactionLog = new VoltLogger("TRANSACTION");
    private static final VoltLogger hostLog = new VoltLogger("HOST");

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
    private final DtxnInitiatorMailbox m_mailbox;
    private final int m_siteId;
    private final int m_hostId;

    public SimpleDtxnInitiator(CatalogContext context,
                               Messenger messenger, int hostId, int siteId,
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
        m_safetyState = new ExecutorTxnIdSafetyState(siteId, context.siteTracker);
        m_mailbox =
            new DtxnInitiatorMailbox(
                    siteId,
                    m_safetyState,
                    (org.voltdb.messaging.HostMessenger)messenger);
        messenger.createMailbox(siteId, VoltDB.DTXN_MAILBOX_ID,
                                            m_mailbox);
        m_mailbox.setInitiator(this);
        m_onBackPressure = onBackPressure;
        m_offBackPressure = offBackPressure;
    }

    @Override
    public synchronized void createTransaction(
                                  final long connectionId,
                                  final String connectionHostname,
                                  final StoredProcedureInvocation invocation,
                                  final boolean isReadOnly,
                                  final boolean isSinglePartition,
                                  final boolean isEveryPartition,
                                  final int partitions[],
                                  final int numPartitions,
                                  final Object clientData,
                                  final int messageSize,
                                  final long now)
    {
        assert(invocation != null);
        assert(partitions != null);
        assert(numPartitions >= 1);

        if (isSinglePartition || isEveryPartition)
        {
            createSinglePartitionTxn(connectionId, connectionHostname, invocation, isReadOnly,
                                     partitions, clientData, messageSize, now);
            return;
        }
        else
        {
            long txnId = m_idManager.getNextUniqueTransactionId();

            // store only partitions that are NOT the coordinator
            // this is a bit too slow
            int[] allSiteIds =
                VoltDB.instance().getCatalogContext().
                siteTracker.getLiveSitesForEachPartition(partitions);
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

    long m_lastTickTime = 0;

    @Override
    public synchronized long tick() {
        final long txnId = m_idManager.getNextUniqueTransactionId();
        final long now = m_idManager.getLastUsedTime();

        // SEMI-HACK: this list can become incorrect if there's a node
        // failure in between here and the Heartbeat transmission loop below.
        // Rather than add another synchronization point, we'll just make
        // that a survivable case, see ExecutorTxnIdSafetyState for more info.
        int[] outOfDateSites =
            VoltDB.instance().getCatalogContext().
            //siteTracker.getSitesWhichNeedAHeartbeat(time, interval);
            siteTracker.getUpExecutionSites();

        //long duration = now - m_lastTickTime;
        //System.out.printf("Sending tick after %d ms pause.\n", duration);
        //System.out.flush();

        try {
            // loop over all the sites that need a heartbeat and send one
            for (int siteId : outOfDateSites) {
                // tack on the last confirmed seen txn id for all sites with a particular partition
                long newestSafeTxnId = m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(siteId);
                HeartbeatMessage tickNotice = new HeartbeatMessage(m_siteId, txnId, newestSafeTxnId);
                m_mailbox.send(siteId, VoltDB.DTXN_MAILBOX_ID, tickNotice);
            }
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

        m_lastTickTime = now;
        return now;
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
                             int[] partitions,
                             Object clientData,
                             int messageSize,
                             long now)
    {
        long txnId = m_idManager.getNextUniqueTransactionId();

        ArrayList<Integer> siteIds;

        // Special case the common 1 partition case -- cheap via SiteTracker
        if (partitions.length == 1) {
            siteIds = VoltDB.instance().getCatalogContext().
            siteTracker.getLiveSitesForPartition(partitions[0]);
        }
        // need all sites for a set of partitions -- a little more expensive
        else {
            siteIds = VoltDB.instance().getCatalogContext().
            siteTracker.getLiveSitesForEachPartitionAsList(partitions);
        }

        increaseBackpressure(messageSize);

        // create and register each replicated transaction with the pending
        // transaction structure.  do this before transmitting them to avoid
        // races where we get a reply from a replica before we finish
        // transmission

        InFlightTxnState state =
            new InFlightTxnState(txnId,
                                 siteIds.get(0),
                                 null,
                                 isReadOnly,
                                 true,
                                 invocation.getShallowCopy(),
                                 clientData,
                                 messageSize,
                                 now,
                                 connectionId,
                                 connectionHostname);

        for (int siteId : siteIds) {
            state.addCoordinator(siteId);
        }
        m_mailbox.addPendingTxn(state);

        for (int coordId : state.outstandingCoordinators) {
            sendTransactionToCoordinator(state, coordId);
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
        m_mailbox.addPendingTxn(txn);
        increaseBackpressure(txn.messageSize);

        MultiPartitionParticipantMessage notice = new MultiPartitionParticipantMessage(
                m_siteId, txn.firstCoordinatorId, txn.txnId, txn.isReadOnly);
        try {
            m_mailbox.send(txn.otherSiteIds, 0, notice);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

        sendTransactionToCoordinator(txn, txn.firstCoordinatorId);
    }

    /**
     * Send the initiation notice to the coordinator for a
     * transaction. This message does contain work. In the case
     * of a single-partition transaction, this is all that is
     * needed to run a transaction.
     *
     * @param txn Information about the transaction to send.
     */
    private void sendTransactionToCoordinator(InFlightTxnState txn, int coordinatorId)
    {
        // figure out what the newest txnid seen by ALL partitions for this execution
        //  site id is and tack it on to this message
        long newestSafeTxnId = m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(coordinatorId);

        InitiateTaskMessage workRequest = new InitiateTaskMessage(
                m_siteId,
                coordinatorId,
                txn.txnId,
                txn.isReadOnly,
                txn.isSinglePartition,
                txn.invocation,
                newestSafeTxnId); // this will allow all transactions to run for now

        try {
            m_mailbox.send(coordinatorId, 0, workRequest);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
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

    @Override
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

    @Override
    public synchronized void notifyExecutionSiteRejoin(ArrayList<Integer> executorSiteIds) {
        for (int executorSiteId : executorSiteIds) {
            m_safetyState.addRejoinedState(executorSiteId);
        }
    }

    public void getDumpContents(StringBuilder sb) {
        List<InFlightTxnState> in_flight_txns = m_mailbox.getInFlightTxns();
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
//        if (m_mailbox instanceof SiteMailbox)
//            context.mailboxHistory = ((SiteMailbox) m_mailbox).getHistory();

        // list transactions in flight
        List<InFlightTxnState> inFlightTxnList = m_mailbox.getInFlightTxns();
        context.inFlightTxns = new InFlightTxnState[inFlightTxnList.size()];
        for (int i = 0; i < inFlightTxnList.size(); i++)
        {
            context.inFlightTxns[i] = inFlightTxnList.get(i);
        }
    }
}
