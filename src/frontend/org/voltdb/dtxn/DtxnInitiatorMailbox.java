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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltcore.messaging.HeartbeatMessage;
import org.voltcore.messaging.HeartbeatResponseMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.messaging.HostMessenger;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.SiteFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.messaging.CoalescedHeartbeatMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltcore.network.Connection;
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
                    if (fault instanceof SiteFailureFault)
                    {
                        SiteFailureFault site_fault = (SiteFailureFault)fault;
                        for (Long site : site_fault.getSiteIds()) {
                            removeSite(site);
                            m_safetyState.removeState(site);
                        }
                    }
                }
            }
        }
    }

    /** Map of transaction ids to transaction information */
    private long m_hsId;
    private final Map<Long, InFlightTxnState> m_pendingTxns =
        new HashMap<Long, InFlightTxnState>();
    private TransactionInitiator m_initiator;
    private final HostMessenger m_hostMessenger;

    private final ExecutorTxnIdSafetyState m_safetyState;

    /**
     * Storage for initiator statistics
     */
    InitiatorStats m_stats = null;
    /**
     * Latency distribution, stored in buckets.
     */
    LatencyStats m_latencies = null;

    /**
     * Construct a new DtxnInitiatorQueue
     */
    public DtxnInitiatorMailbox(ExecutorTxnIdSafetyState safetyState, HostMessenger hostMessenger)
    {
        assert(safetyState != null);
        assert(hostMessenger != null);
        m_hostMessenger = hostMessenger;
        m_safetyState = safetyState;
        VoltDB.instance().getFaultDistributor().
        // For Node failure, the initiators need to be ordered after the catalog
        // but before everything else (to prevent any new work for bad sites)
        registerFaultHandler(SiteFailureFault.SITE_FAILURE_INITIATOR,
                             new InitiatorNodeFailureFaultHandler(),
                             FaultType.SITE_FAILURE);
    }

    public ExecutorTxnIdSafetyState getSafetyState() {
        return m_safetyState;
    }

    public void setInitiator(TransactionInitiator initiator) {
        m_initiator = initiator;
    }

    public void addPendingTxn(InFlightTxnState txn)
    {
        m_pendingTxns.put(txn.txnId, txn);
    }

    public void removeSite(long siteId)
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
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf).flip();
        c.writeStream().enqueue(buf);
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
                m_safetyState.updateLastSeenTxnIdFromExecutorBySiteId(
                        hrm.getExecHSId(), hrm.getLastReceivedTxnId(), hrm.isBlocked());
                return;
            }

            // only valid messages are this and heartbeatresponse
            assert(message instanceof InitiateResponseMessage);
            final InitiateResponseMessage r = (InitiateResponseMessage) message;

            state = m_pendingTxns.get(r.getTxnId());

            assert(m_hsId == r.getInitiatorHSId());

            // if this is a dummy response, make sure the m_pendingTxns list thinks
            // the site has been removed from the list
            if (r.isRecovering()) {
                toSend = state.addFailedOrRecoveringResponse(r.getCoordinatorHSId());
            }
            // otherwise update the InFlightTxnState with the response
            else {
                toSend = state.addResponse(r.getCoordinatorHSId(), r.getClientResponseData());
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
                ResponseSampler.offerResponse(this.getHSId(), state.txnId, state.invocation, toSend);
            // queue the response to be sent to the client
            enqueueResponse(toSend, state);
        }
    }

    private void demuxCoalescedHeartbeatMessage(
            CoalescedHeartbeatMessage message) {
        final long destinations[] = message.getHeartbeatDestinations();
        final HeartbeatMessage heartbeats[] = message.getHeartbeatsToDeliver();
        assert(destinations.length == heartbeats.length);

        for (int ii = 0; ii < destinations.length; ii++) {
            m_hostMessenger.send(destinations[ii], heartbeats[ii]);
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
    public void send(long hsId, VoltMessage message) {
        message.m_sourceHSId = m_hsId;
        m_hostMessenger.send(hsId, message);
    }

    @Override
    public void send(long[] hsIds, VoltMessage message) {
        assert(message != null);
        assert(hsIds != null);
        message.m_sourceHSId = m_hsId;
        m_hostMessenger.send(hsIds, message);
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
    public long getHSId() {
        return m_hsId;
    }

    @Override
    public void setHSId(long hsId) {
        this.m_hsId = hsId;
        m_stats = new InitiatorStats(m_hsId);
        m_latencies = new LatencyStats(m_hsId);
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
