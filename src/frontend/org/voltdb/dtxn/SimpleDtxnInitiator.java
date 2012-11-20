/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HeartbeatMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.messaging.CoalescedHeartbeatMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

/** Supports correct execution of multiple partition transactions by executing them one at a time. */
public class SimpleDtxnInitiator extends TransactionInitiator {
    final TransactionIdManager m_idManager;

    private final ExecutorTxnIdSafetyState m_safetyState;
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");

    private ClientInterface m_clientInterface;
    public void setClientInterface(ClientInterface ci) {
        m_clientInterface = ci;
    }

    /**
     * Indicates if backpressure has been seen and reported
     */
    private final AtomicBoolean m_hadBackPressure = new AtomicBoolean(false);

    /*
     * Whether or not to send heartbeats. It's set to false by default, this
     * will be set to true once command-log replay has finished.
     */
    private volatile boolean m_sendHeartbeats = false;

    // If an initiator handles a full node, it processes approximately 50,000 txns/sec.
    // That's about 50 txns/ms. Try not to keep more than 5 ms of work? Seems like a really
    // small number. On the other hand, backPressure() is just a hint to the ClientInterface.
    // CI will submit ClientPort.MAX_READ * clients / bytesPerStoredProcInvocation txns
    // on average if clients present constant uninterrupted load.
    public final static int MAX_DESIRED_PENDING_BYTES = 67108864;
    public final static int MAX_DESIRED_PENDING_TXNS = 5000;
    private long m_pendingTxnBytes = 0;
    private int m_pendingTxnCount = 0;
    private final DtxnInitiatorMailbox m_mailbox;
    private final long m_siteId;
    private final int m_hostId;
    private long m_lastSeenOriginalTxnId = Long.MIN_VALUE;

    // Keeps track of site index for choosing sites via round robin discipline.
    private int m_roundRobinSiteIndex = 0;

    public SimpleDtxnInitiator(DtxnInitiatorMailbox mailbox,
                               CatalogContext context,
                               HostMessenger messenger, int hostId,
                               long initiatorId,
                               long timestampTestingSalt)
    {
        assert(messenger != null);

        m_mailbox = mailbox;
        m_safetyState = m_mailbox.getSafetyState();
        m_siteId = m_mailbox.getHSId();
        m_safetyState.setHSId(m_siteId);
        consoleLog.info("Initializing initiator ID: " + initiatorId  +
                ", SiteID: " + CoreUtils.hsIdToString(m_siteId));

        m_idManager = new TransactionIdManager(initiatorId, timestampTestingSalt);
        m_hostId = hostId;
        m_mailbox.setInitiator(this);
    }

    @Override
    public synchronized boolean createTransaction(
                                  final long connectionId,
                                  final String connectionHostname,
                                  final boolean adminConnection,
                                  final StoredProcedureInvocation invocation,
                                  final boolean isReadOnly,
                                  final boolean isSinglePartition,
                                  final boolean isEveryPartition,
                                  final int partitions[],
                                  final int numPartitions,
                                  final Object clientData,
                                  final int messageSize,
                                  final long now,
                                  final boolean allowMismatchedResults)
    {
        long txnId;
        txnId = m_idManager.getNextUniqueTransactionId();
        boolean retval =
            createTransaction(connectionId, connectionHostname, adminConnection, txnId,
                              TransactionIdManager.getTimestampFromTransactionId(txnId), invocation,
                              isReadOnly, isSinglePartition, isEveryPartition, partitions,
                              numPartitions, clientData, messageSize, now, allowMismatchedResults);
        return retval;
    }

    @Override
    public synchronized boolean createTransaction(
                                  final long connectionId,
                                  final String connectionHostname,
                                  final boolean adminConnection,
                                  final long txnId,
                                  final long timestamp,
                                  final StoredProcedureInvocation invocation,
                                  final boolean isReadOnly,
                                  final boolean isSinglePartition,
                                  final boolean isEveryPartition,
                                  final int partitions[],
                                  final int numPartitions,
                                  final Object clientData,
                                  final int messageSize,
                                  final long now,
                                  final boolean allowMismatchedResults)
    {
        assert(invocation != null);
        assert(partitions != null);
        assert(numPartitions >= 1);

        if (invocation.getType() == ProcedureInvocationType.REPLICATED)
        {
            /*
             * Ning - @LoadSinglepartTable and @LoadMultipartTable always have
             * the same txnId which is the txnId of the snapshot.
             */
            if (!(invocation.getProcName().equalsIgnoreCase("@LoadSinglepartitionTable") ||
                  invocation.getProcName().equalsIgnoreCase("@LoadMultipartitionTable")) &&
                invocation.getOriginalTxnId() <= m_lastSeenOriginalTxnId)
            {
                hostLog.debug("Dropping duplicate replicated transaction, txnid: " + invocation.getOriginalTxnId() + ", last seen: " + m_lastSeenOriginalTxnId);
                return false;
            }
            else
            {
                m_lastSeenOriginalTxnId = invocation.getOriginalTxnId();
            }
        }

        if (isSinglePartition || isEveryPartition)
        {
            createSinglePartitionTxn(connectionId, connectionHostname, adminConnection,
                                     txnId, invocation, isReadOnly,
                                     partitions, clientData, messageSize, now);
            return true;
        }
        else
        {
            SiteTracker tracker = VoltDB.instance().getSiteTracker();
            List<Long> sitesOnThisHost = tracker.getSitesForHost(m_hostId);
            // Choose coordinator using round robin technique.
            // Check for wrapping around before using the round robin index to
            // innoculate against size changes, etc..
            if (m_roundRobinSiteIndex >= sitesOnThisHost.size()) {
                m_roundRobinSiteIndex = 0;
            }
            long coordinatorId = sitesOnThisHost.get(m_roundRobinSiteIndex);
            m_roundRobinSiteIndex++;
            ArrayList<Long> replicaIds = new ArrayList<Long>();
            for (Long replica : tracker.getSitesForPartition(tracker.getPartitionForSite(coordinatorId))) {
                if (replica != coordinatorId) {
                    replicaIds.add(replica);
                }
            }

            ArrayList<Long> otherSiteIds = new ArrayList<Long>();

            // store only partitions that are NOT the coordinator or coordinator replica
            // this is a bit too slow
            long[] allSiteIds =
                tracker.getSitesForPartitionsAsArray(partitions);

            for (int i = 0; i < allSiteIds.length; i++)
            {
                // if this site is on the same host as the initiator
                // then take over as the coordinator
                if (allSiteIds[i] != coordinatorId && !replicaIds.contains(allSiteIds[i])) {
                    otherSiteIds.add(allSiteIds[i]);
                }
            }

            long otherSiteIdsArr[] = new long[otherSiteIds.size()];
            int ii = 0;
            for (Long otherSiteId : otherSiteIds) {
                otherSiteIdsArr[ii++] = otherSiteId;
            }

            InFlightTxnState txn = new InFlightTxnState(txnId,
                                                        coordinatorId,
                                                        replicaIds,
                                                        otherSiteIdsArr,
                                                        isReadOnly,
                                                        false,
                                                        invocation,
                                                        clientData,
                                                        messageSize,
                                                        now,
                                                        connectionId,
                                                        connectionHostname,
                                                        adminConnection,
                                                        allowMismatchedResults);
            dispatchMultiPartitionTxn(txn);
            return true;
        }
    }

    long m_lastTickTime = 0;

    @Override
    public synchronized long tick() {
        final long txnId = m_idManager.getNextUniqueTransactionId();
        final long now = m_idManager.getLastUsedTime();

        if (m_sendHeartbeats) {
            sendHeartbeat(txnId);
        }

        m_lastTickTime = now;
        return now;
    }

    @Override
    public void sendHeartbeat(final long txnId) {
        final SiteTracker st = VoltDB.instance().getSiteTracker();
        long remoteHeartbeatTargets[][] = st.getRemoteSites();
        long localHeartbeatTargets[] = st.getLocalSites();

        /*
         * For each host, create an array containing the safe txn ids for each
         * site. Then coalesce them into a single heartbeat message to send to the
         * initiator on that host who will then demux the heartbeats
         */
        for (long hostTargets[] : remoteHeartbeatTargets) {
            final long initiatorSiteId = st.getInitiatorsForHost(SiteTracker.getHostForSite(hostTargets[0])).get(0);
            assert(initiatorSiteId != 1L);//uninitialized value
            long safeTxnIds[] = new long[hostTargets.length];
            for (int ii = 0; ii < safeTxnIds.length; ii++) {
                safeTxnIds[ii] = m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(hostTargets[ii]);
            }

            CoalescedHeartbeatMessage heartbeat =
                new CoalescedHeartbeatMessage(m_siteId, txnId, hostTargets,safeTxnIds);
            m_mailbox.send(initiatorSiteId, heartbeat);
        }

        // loop over all the local sites that need a heartbeat and send each a message
        // no coalescing here
        for (long siteId : localHeartbeatTargets) {
            // tack on the last confirmed seen txn id for all sites with a particular partition
            long newestSafeTxnId = m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(siteId);
            HeartbeatMessage tickNotice = new HeartbeatMessage(m_siteId, txnId, newestSafeTxnId);
            m_mailbox.send(siteId, tickNotice);
        }
    }

    @Override
    public long getMostRecentTxnId() {
        return m_idManager.getLastTxnId();
    }

    private void
    createSinglePartitionTxn(long connectionId,
                             final String connectionHostname,
                             boolean adminConnection,
                             long txnId,
                             StoredProcedureInvocation invocation,
                             boolean isReadOnly,
                             int[] partitions,
                             Object clientData,
                             int messageSize,
                             long now)
    {
        List<Long> siteIds;
        SiteTracker siteTracker = VoltDB.instance().getSiteTracker();

        // Special case the common 1 partition case -- cheap via SiteTracker
        if (partitions.length == 1) {
            siteIds = siteTracker.getSitesForPartition(partitions[0]);
        }
        // need all sites for a set of partitions -- a little more expensive
        else {
            siteIds = siteTracker.getSitesForPartitions(partitions);
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
                                 null,
                                 isReadOnly,
                                 true,
                                 invocation.getShallowCopy(),
                                 clientData,
                                 messageSize,
                                 now,
                                 connectionId,
                                 connectionHostname,
                                 adminConnection,
                                 false);

        for (long siteId : siteIds) {
            state.addCoordinator(siteId);
        }
        m_mailbox.addPendingTxn(state);

        for (long coordId : state.outstandingCoordinators) {
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

        /*
         * Compose the set of non-coordinator sites and send it to the
         * coordinator, so that the coordinator will send fragment work to all
         * the sites that received the participant notice.
         */
        long[] nonCoordinatorSites = new long[txn.coordinatorReplicas.size() + txn.otherSiteIds.length];
        int i = 0;
        for (long hsId : txn.coordinatorReplicas) {
            nonCoordinatorSites[i++] = hsId;
        }
        for (long hsId : txn.otherSiteIds) {
            nonCoordinatorSites[i++] = hsId;
        }

        MultiPartitionParticipantMessage notice = new MultiPartitionParticipantMessage(
                m_siteId, txn.firstCoordinatorId, txn.txnId, txn.isReadOnly);
        m_mailbox.send(txn.otherSiteIds, notice);

        // figure out what the safely replicated txnid is for this execution site/partition id
        // in the multi-part case where we send this initiate task message to replicas of the coordinator,
        // the safe txn id is the same for all of them because they are all replicas of the same partition
        long newestSafeTxnId = m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(txn.firstCoordinatorId);

        InitiateTaskMessage workRequest = new InitiateTaskMessage(
                m_siteId,
                txn.firstCoordinatorId,
                txn.txnId,
                txn.isReadOnly,
                txn.isSinglePartition,
                txn.invocation,
                newestSafeTxnId, // this will allow all transactions to run for now
                nonCoordinatorSites);

        /*
         * Send the transaction to the coordinator as well as his replicas
         * so it is redundantly logged. The replicas that aren't listed
         * as the coordinator in the work request will treat it as
         * a participant notice
         */
        m_mailbox.send(txn.firstCoordinatorId, workRequest);
        for (Long replica : txn.coordinatorReplicas) {
            newestSafeTxnId = m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(replica);
            workRequest = new InitiateTaskMessage(
                    m_siteId,
                    txn.firstCoordinatorId,
                    txn.txnId,
                    txn.isReadOnly,
                    txn.isSinglePartition,
                    txn.invocation,
                    newestSafeTxnId); // this will allow all transactions to run for now
            m_mailbox.send(replica, workRequest);
        }
    }

    /**
     * Send the initiation notice to the coordinator for a
     * transaction. This message does contain work. In the case
     * of a single-partition transaction, this is all that is
     * needed to run a transaction. Only used for single parts at this point
     *
     * @param txn Information about the transaction to send.
     */
    private void sendTransactionToCoordinator(InFlightTxnState txn, long coordinatorId)
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

        m_mailbox.send(coordinatorId, workRequest);
    }

    @Override
    protected void increaseBackpressure(int messageSize)
    {
        m_pendingTxnBytes += messageSize;
        m_pendingTxnCount++;
        if (m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES || m_pendingTxnCount > MAX_DESIRED_PENDING_TXNS) {
            if (m_hadBackPressure.compareAndSet(false, true)) {
                hostLog.trace("DTXN back pressure began");
                m_clientInterface.onBackPressure();
            }
        }
    }

    @Override
    protected void reduceBackpressure(int messageSize)
    {
        m_pendingTxnBytes -= messageSize;
        m_pendingTxnCount--;
        if (m_pendingTxnBytes < (MAX_DESIRED_PENDING_BYTES * .8) &&
            m_pendingTxnCount < (MAX_DESIRED_PENDING_TXNS * .8))
        {
            if (m_hadBackPressure.compareAndSet(true, false))
            {
                hostLog.trace("DTXN backpressure ended");
                m_clientInterface.offBackPressure();
            }
        }
    }

    @Override
    public synchronized void notifyExecutionSiteRejoin(ArrayList<Long> executorSiteIds) {
        SiteTracker st = VoltDB.instance().getSiteTracker();
        for (Long executorSiteId : executorSiteIds) {
            m_safetyState.addState(executorSiteId, st.m_sitesToPartitionsImmutable.get(executorSiteId));
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

    @Override
    public synchronized Map<Long, long[]> getOutstandingTxnStats()
    {
        return m_mailbox.getOutstandingTxnStats();
    }

    @Override
    public void setSendHeartbeats(boolean val) {
        m_sendHeartbeats = val;
    }

    @Override
    public boolean isOnBackPressure() {
        return m_hadBackPressure.get();
    }

    @Override
    public void removeConnectionStats(long connectionId) {
        m_mailbox.removeConnectionStats(connectionId);
    }

    @Override
    public void sendSentinel(long txnId, int partitionId) {
        //Does nothing pre-IV2
    }

    @Override
    public void sendEOLMessage(int partitionId) {
        // Does nothing pre-IV2
    }
}
