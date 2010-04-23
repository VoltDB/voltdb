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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.messaging.HeartbeatResponseMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.network.Connection;
import org.voltdb.utils.EstTime;

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
public class DtxnInitiatorQueue implements Queue<VoltMessage>
{
    private class InitiatorNodeFailureFaultHandler implements FaultHandler
    {
        @Override
        public void faultOccured(VoltFault fault)
        {
            if (fault instanceof NodeFailureFault)
            {
                NodeFailureFault node_fault = (NodeFailureFault) fault;
                ArrayList<Integer> dead_sites =
                    VoltDB.instance().getCatalogContext().siteTracker.
                    getAllSitesForHost(node_fault.getHostId());
                for (Integer site_id : dead_sites)
                {
                    removeSite(site_id);
                    m_safetyState.removeState(site_id);
                }
            }
        }
    }

    /** Map of transaction ids to transaction information */
    private final int m_siteId;
    private final PendingTxnList m_pendingTxns = new PendingTxnList();
    private TransactionInitiator m_initiator;
    private final HashMap<Long, InitiateResponseMessage> m_txnIdResponses;
    // need a separate copy of the VoltTables so that we can have
    // thread-safe meta-data
    private final HashMap<Long, VoltTable[]> m_txnIdResults;

    private final ExecutorTxnIdSafetyState m_safetyState;

    /**
     * Storage for initiator statistics
     */
    final InitiatorStats m_stats;

    /**
     * Construct a new DtxnInitiatorQueue
     * @param siteId  The mailbox siteId for this initiator
     */
    public DtxnInitiatorQueue(int siteId, ExecutorTxnIdSafetyState safetyState)
    {
        assert(safetyState != null);

        m_siteId = siteId;
        m_safetyState = safetyState;
        m_stats = new InitiatorStats("Initiator " + siteId + " stats", siteId);
        m_txnIdResults =
            new HashMap<Long, VoltTable[]>();
        m_txnIdResponses = new HashMap<Long, InitiateResponseMessage>();
        VoltDB.instance().getFaultDistributor().
        // For Node failure, the initiators need to be ordered after the catalog
        // but before everything else (to prevent any new work for bad sites)
        registerFaultHandler(FaultType.NODE_FAILURE,
                             new InitiatorNodeFailureFaultHandler(),
                             NodeFailureFault.NODE_FAILURE_INITIATOR);
    }

    public void setInitiator(TransactionInitiator initiator) {
        m_initiator = initiator;
    }

    public void addPendingTxn(InFlightTxnState txn)
    {
        synchronized (m_initiator)
        {
            m_pendingTxns.addTxn(txn.txnId, txn.coordinatorId, txn);
        }
    }

    public void removeSite(int siteId)
    {
        synchronized (m_initiator)
        {
            Map<Long, InFlightTxnState> txn_ids_affected =
                m_pendingTxns.removeSite(siteId);
            for (Long txn_id : txn_ids_affected.keySet())
            {
                if (m_pendingTxns.getTxnIdSize(txn_id) == 0)
                {
                    InFlightTxnState state = txn_ids_affected.get(txn_id);
                    if (!m_txnIdResponses.containsKey(txn_id))
                    {
                        // No response was ever received for this TXN.
                        // What's the right thing to return to the client?
                        // XXX-FAILURE don't like this crashing long-term
                        VoltDB.crashVoltDB();
                    }
                    InitiateResponseMessage r = m_txnIdResponses.get(txn_id);
                    m_pendingTxns.removeTxnId(r.getTxnId());
                    m_initiator.reduceBackpressure(state.messageSize);
                    m_txnIdResponses.remove(r.getTxnId());
                    if (!state.isReadOnly)
                    {
                        enqueueResponse(r, state);
                    }
                }
            }
        }
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
    public boolean offer(VoltMessage message)
    {
        synchronized(m_initiator)
        {
            // update the state of seen txnids for each executor
            if (message instanceof HeartbeatResponseMessage) {
                HeartbeatResponseMessage hrm = (HeartbeatResponseMessage) message;
                m_safetyState.updateLastSeenTxnIdFromExecutorBySiteId(hrm.getExecSiteId(), hrm.getLastReceivedTxnId(), hrm.isBlocked());
                return true;
            }

            // only valid messages are this and heartbeatresponse
            assert(message instanceof InitiateResponseMessage);
            final InitiateResponseMessage r = (InitiateResponseMessage) message;
            // update the state of seen txnids for each executor
            m_safetyState.updateLastSeenTxnIdFromExecutorBySiteId(r.getCoordinatorSiteId(), r.getLastReceivedTxnId(), false);

            InFlightTxnState state;
            int sites_left = -1;
            state = m_pendingTxns.getTxn(r.getTxnId(), r.getCoordinatorSiteId());
            sites_left = m_pendingTxns.getTxnIdSize(r.getTxnId());

            assert(state.coordinatorId == r.getCoordinatorSiteId());
            assert(m_siteId == r.getInitiatorSiteId());

            boolean first_response = false;
            VoltTable[] first_results = null;
            if (!m_txnIdResults.containsKey(r.getTxnId()))
            {
                ClientResponseImpl curr_response = r.getClientResponseData();
                VoltTable[] curr_results = curr_response.getResults();
                VoltTable[] saved_results = new VoltTable[curr_results.length];
                // Create shallow copies of all the VoltTables to avoid
                // race conditions with the ByteBuffer metadata
                for (int i = 0; i < curr_results.length; ++i)
                {
                    saved_results[i] = new VoltTable(curr_results[i].getTableDataReference(), true);
                }
                m_txnIdResults.put(r.getTxnId(), saved_results);
                m_txnIdResponses.put(r.getTxnId(), r);
                first_response = true;
            }
            else
            {
                first_results = m_txnIdResults.get(r.getTxnId());
            }
            if (first_response)
            {
                // If this is a read-only transaction then we'll return
                // the first response to the client
                if (state.isReadOnly)
                {
                    enqueueResponse(r, state);
                }
            }
            else
            {
                assert(first_results != null);

                ClientResponseImpl curr_response = r.getClientResponseData();
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
                m_pendingTxns.removeTxnId(r.getTxnId());
                m_initiator.reduceBackpressure(state.messageSize);
                m_txnIdResults.remove(r.getTxnId());
                m_txnIdResponses.remove(r.getTxnId());
                if (!state.isReadOnly)
                {
                    enqueueResponse(r, state);
                }
            }

            return true;
        }
    }

    private void enqueueResponse(InitiateResponseMessage response,
                                 InFlightTxnState state)
    {
        response.setClientHandle(state.invocation.getClientHandle());
        //Horrible but so much more efficient.
        final Connection c = (Connection)state.clientData;

        assert(c != null) : "NULL connection in connection state client data.";
        final long now = EstTime.currentTimeMillis();
        final int delta = (int)(now - state.initiateTime);
        final ClientResponseImpl client_response =
            response.getClientResponseData();
        client_response.setClusterRoundtrip(delta);
        m_stats.logTransactionCompleted(
                state.connectionId,
                state.connectionHostname,
                state.invocation,
                delta,
                client_response.getStatus());
        c.writeStream().enqueue(client_response);
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

    /**
     * Currently used to provide object state for the dump manager
     * @return A list of outstanding transaction state objects
     */
    public List<InFlightTxnState> getInFlightTxns()
    {
        return m_pendingTxns.getInFlightTxns();
    }
}
