/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.messaging.CoalescedHeartbeatMessage;
import org.voltdb.messaging.HeartbeatMessage;
import org.voltdb.messaging.HeartbeatResponseMessage;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.Subject;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.network.Connection;
import org.voltdb.utils.ResponseSampler;

/**
 * DtxnInitiatorQueue matches incoming result set responses to outstanding
 * transactions, performing duplicate suppression and consistency checking
 * for single-partition transactions when replication is enabled.
 *
 * It currently shares/uses m_initiator's intrinsic lock to maintain
 * thread-safety across callers into SimpleDtxnInitiator and threads which
 * provide InitiateResponses via offer().  This is a bit ugly but is identical
 * with the synchronization mechanism that existed before the extraction of
 * this class, so it should JustWork(tm).
 */
public class DtxnInitiatorMailbox implements Mailbox
{
    private class InitiatorNodeFailureFaultHandler implements FaultHandler
    {
        @Override
        public void faultOccured(Set<VoltFault> faults)
        {
            synchronized (m_initiator) {
                for (VoltFault fault : faults) {
                    if (fault instanceof NodeFailureFault)
                    {
                        NodeFailureFault node_fault = (NodeFailureFault)fault;
                        ArrayList<Integer> dead_sites =
                            VoltDB.instance().getCatalogContext().siteTracker.
                            getAllSitesForHost(node_fault.getHostId());
                        for (Integer site_id : dead_sites)
                        {
                            removeSite(site_id);
                            m_safetyState.removeState(site_id);
                        }
                    }
                    VoltDB.instance().getFaultDistributor().reportFaultHandled(this, fault);
                }
            }
        }

        @Override
        public void faultCleared(Set<VoltFault> faults) {
        }
    }

    /** Map of transaction ids to transaction information */
    private final int m_siteId;
    private final Map<Long, InFlightTxnState> m_pendingTxns =
        new HashMap<Long, InFlightTxnState>();
    private TransactionInitiator m_initiator;
    //private final HashMap<Long, InitiateResponseMessage> m_txnIdResponses;
    // need a separate copy of the VoltTables so that we can have
    // thread-safe meta-data
    //private final HashMap<Long, VoltTable[]> m_txnIdResults;
    private final HostMessenger m_hostMessenger;

    private final ExecutorTxnIdSafetyState m_safetyState;

    /**
     * Storage for initiator statistics
     */
    final InitiatorStats m_stats;
    /**
     * Latency distribution, stored in buckets.
     */
    final LatencyStats m_latencies;

    /**
     * Construct a new DtxnInitiatorQueue
     * @param siteId  The mailbox siteId for this initiator
     */
    public DtxnInitiatorMailbox(int siteId, ExecutorTxnIdSafetyState safetyState, HostMessenger hostMessenger)
    {
        assert(safetyState != null);
        assert(hostMessenger != null);
        m_hostMessenger = hostMessenger;
        m_siteId = siteId;
        m_safetyState = safetyState;
        m_stats = new InitiatorStats("Initiator " + siteId + " stats", siteId);
        m_latencies = new LatencyStats("Initiator " + siteId + " latency distribution", siteId);
        //m_txnIdResults =
        //    new HashMap<Long, VoltTable[]>();
        //m_txnIdResponses = new HashMap<Long, InitiateResponseMessage>();
        VoltDB.instance().getFaultDistributor().
        // For Node failure, the initiators need to be ordered after the catalog
        // but before everything else (to prevent any new work for bad sites)
        registerFaultHandler(NodeFailureFault.NODE_FAILURE_INITIATOR,
                             new InitiatorNodeFailureFaultHandler(),
                             FaultType.NODE_FAILURE);
    }

    public void setInitiator(TransactionInitiator initiator) {
        m_initiator = initiator;
    }

    public void addPendingTxn(InFlightTxnState txn)
    {
        m_pendingTxns.put(txn.txnId, txn);
    }

    public void removeSite(int siteId)
    {
        ArrayList<Long> txnIdsToRemove = new ArrayList<Long>();
        for (InFlightTxnState state : m_pendingTxns.values())
        {
            // skips txns that don't have this site as coordinator
            if (!state.siteIsCoordinator(siteId)) continue;

            // note that the site failed
            ClientResponseImpl toSend = state.addFailedOrRecoveringResponse(siteId);

            // send a response if the state wants to
            if (toSend != null) {
                enqueueResponse(toSend, state);
            }

            if (state.hasAllResponses()) {
                txnIdsToRemove.add(state.txnId);
                m_initiator.reduceBackpressure(state.messageSize);

                if (!state.hasSentResponse()) {
                    // TODO badness here...
                    assert(false);
                }
            }
        }

        for (long txnId : txnIdsToRemove) {
            m_pendingTxns.remove(txnId);
        }
    }

    private void enqueueResponse(ClientResponseImpl response,
                                 InFlightTxnState state)
    {
        response.setClientHandle(state.invocation.getClientHandle());
        //Horrible but so much more efficient.
        final Connection c = (Connection)state.clientData;

        assert(c != null) : "NULL connection in connection state client data.";
        final long now = System.currentTimeMillis();
        final int delta = (int)(now - state.initiateTime);
        response.setClusterRoundtrip(delta);
        m_stats.logTransactionCompleted(
                state.connectionId,
                state.connectionHostname,
                state.invocation,
                delta,
                response.getStatus());
        m_latencies.logTransactionCompleted(delta);
        c.writeStream().enqueue(response);
    }

    public void removeConnectionStats(long connectionId) {
        m_stats.removeConnectionStats(connectionId);
    }

    /**
     * Currently used to provide object state for the dump manager
     * @return A list of outstanding transaction state objects
     */
    public List<InFlightTxnState> getInFlightTxns()
    {
        List<InFlightTxnState> retval = new ArrayList<InFlightTxnState>();
        retval.addAll(m_pendingTxns.values());
        return retval;
    }

    @Override
    public void deliver(VoltMessage message) {
        if (message instanceof CoalescedHeartbeatMessage) {
            demuxCoalescedHeartbeatMessage((CoalescedHeartbeatMessage)message);
            return;
        }
        ClientResponseImpl toSend = null;
        InFlightTxnState state = null;
        synchronized (m_initiator) {
            // update the state of seen txnids for each executor
            if (message instanceof HeartbeatResponseMessage) {
                HeartbeatResponseMessage hrm = (HeartbeatResponseMessage) message;
                m_safetyState.updateLastSeenTxnIdFromExecutorBySiteId(hrm.getExecSiteId(), hrm.getLastReceivedTxnId(), hrm.isBlocked());
                return;
            }

            // only valid messages are this and heartbeatresponse
            assert(message instanceof InitiateResponseMessage);
            final InitiateResponseMessage r = (InitiateResponseMessage) message;

            state = m_pendingTxns.get(r.getTxnId());

            assert(m_siteId == r.getInitiatorSiteId());

            // if this is a dummy response, make sure the m_pendingTxns list thinks
            // the site has been removed from the list
            if (r.isRecovering()) {
                toSend = state.addFailedOrRecoveringResponse(r.getCoordinatorSiteId());
            }
            // otherwise update the InFlightTxnState with the response
            else {
                toSend = state.addResponse(r.getCoordinatorSiteId(), r.getClientResponseData());
            }

            if (state.hasAllResponses()) {
                m_initiator.reduceBackpressure(state.messageSize);
                m_pendingTxns.remove(r.getTxnId());

                // TODO make this send an error message on failure
                assert(state.hasSentResponse());
            }
        }
        //Stop moving the response send into the initiator locked section. It isn't necessary,
        //and several other locks need to be acquired in the network subsystem. Bad voodoo.
        //addResponse returning non-null means send the response to the client
        if (toSend != null) {
            // the next bit is usually a noop, unless we're sampling responses for test
            if (!state.isReadOnly)
                ResponseSampler.offerResponse(this.getSiteId(), state.txnId, state.invocation, toSend);
            // queue the response to be sent to the client
            enqueueResponse(toSend, state);
        }
    }

    private void demuxCoalescedHeartbeatMessage(
            CoalescedHeartbeatMessage message) {
        final int destinations[] = message.getHeartbeatDestinations();
        final HeartbeatMessage heartbeats[] = message.getHeartbeatsToDeliver();
        assert(destinations.length == heartbeats.length);

        try {
            for (int ii = 0; ii < destinations.length; ii++) {
                m_hostMessenger.send(destinations[ii], VoltDB.DTXN_MAILBOX_ID, heartbeats[ii]);
            }
        } catch (MessagingException e) {
            VoltDB.crashLocalVoltDB("Error attempting to demux and deliver coalesced heartbeat messages", true, e);
        }
    }

    @Override
    public void deliverFront(VoltMessage message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recv() {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recv(Subject[] s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recvBlocking() {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void send(int siteId, int mailboxId, VoltMessage message) throws MessagingException {
        m_hostMessenger.send(siteId, mailboxId, message);
    }

    @Override
    public void send(int[] siteIds, int mailboxId, VoltMessage message) throws MessagingException {
        assert(message != null);
        assert(siteIds != null);
        m_hostMessenger.send(siteIds, mailboxId, message);
    }

    @Override
    public VoltMessage recvBlocking(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s, long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSiteId() {
        return m_siteId;
    }

    Map<Long, long[]> getOutstandingTxnStats()
    {
        HashMap<Long, long[]> retval = new HashMap<Long, long[]>();
        for (InFlightTxnState state : m_pendingTxns.values())
        {
            if (!retval.containsKey(state.connectionId))
            {
                // Default new entries to not admin/no outstanding txns
                retval.put(state.connectionId, new long[]{0, 0});
            }
            retval.get(state.connectionId)[0] = (state.isAdmin ? 1 : 0);
            retval.get(state.connectionId)[1]++;
        }
        return retval;
    }
}
