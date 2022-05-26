/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableMap.Builder;

/**
 * This manages per-partition handles used to identify responses for
 * work done in IV2.  Since the work generated for a partition at each client interface
 * (treating multi-part work as a separate partition) is deterministically
 * ordered and completed, we can use the per-partition lists to determine which
 * transactions have been dropped due to faults and potentially report that
 * back to the client.
 */
public class ClientInterfaceHandleManager
{
    private static final VoltLogger tmLog = new VoltLogger("TM");

    //Add an extra bit so compared to the 14-bits in txnids so there
    //can be a short circuit read partition id or NT procedure partition id.
    //
    //NT procedure partition id is also a fake partition id, it is used to
    //track NT procedure invocation separately because NT procedure
    //doesn't care about mastership change
    static final int PART_ID_BITS = 15;
    static final int MP_PART_ID = (1 << (PART_ID_BITS - 1)) - 1;
    static final int SHORT_CIRCUIT_PART_ID = MP_PART_ID + 1;
    static final int NT_PROC_PART_ID = SHORT_CIRCUIT_PART_ID + 1;
    static final long PART_ID_SHIFT = 48;
    static final long SEQNUM_MAX = (1L << PART_ID_SHIFT) - 1L;

    private long m_outstandingTxns;
    public final boolean isAdmin;
    public final Connection connection;
    public final ClientInterfaceRepairCallback repairCallback;
    private final long m_expectedThreadId = Thread.currentThread().getId();
    final AdmissionControlGroup m_acg;

    private volatile boolean m_wantsTopologyUpdates = false;

    private ImmutableMap<Integer, PartitionInFlightTracker> m_trackerMap
        = new Builder<Integer, PartitionInFlightTracker>().build();

    private static class HandleGenerator
    {
        private long m_sequence = 0;
        final private long m_partitionId;

        HandleGenerator(int partitionId)
        {
            m_partitionId = partitionId;
        }

        public long getNextHandle()
        {
            if (m_sequence > SEQNUM_MAX) {
                m_sequence = 0;
            }
            return ((m_partitionId << PART_ID_SHIFT) | m_sequence++);
        }
    }

    static class Iv2InFlight
    {
        final long m_ciHandle;
        final long m_clientHandle;
        final int m_messageSize;
        final long m_creationTimeNanos;
        final String m_procName;
        final long m_initiatorHSId;
        Iv2InFlight(long ciHandle, long clientHandle,
                int messageSize, long creationTimeNanos, String procName, long initiatorHSId)
        {
            m_ciHandle = ciHandle;
            m_clientHandle = clientHandle;
            m_messageSize = messageSize;
            m_creationTimeNanos = creationTimeNanos;
            m_procName = procName;
            m_initiatorHSId = initiatorHSId;
        }
    }

    static class PartitionInFlightTracker {
        private final HandleGenerator m_generator;
        private final Map<Long, Iv2InFlight> m_inFlights = new HashMap<Long, Iv2InFlight>();

        private PartitionInFlightTracker(int partitionId) {
            m_generator = new HandleGenerator(partitionId);
        }
    }

    ClientInterfaceHandleManager(boolean isAdmin, Connection connection, ClientInterfaceRepairCallback repairCallback, AdmissionControlGroup acg)
    {
        this.isAdmin = isAdmin;
        this.connection = connection;
        this.repairCallback = repairCallback;
        m_acg = acg;
    }

    /**
     * Factory to make a threadsafe version of CIHM. This is used
     * exclusively by some internal CI adapters that don't have
     * the natural thread-safety protocol/design of VoltNetwork.
     */
    public static ClientInterfaceHandleManager makeThreadSafeCIHM(
            boolean isAdmin, Connection connection, ClientInterfaceRepairCallback callback, AdmissionControlGroup acg)
    {
        return new ClientInterfaceHandleManager(isAdmin, connection, callback, acg) {
            @Override
            synchronized long getHandle(boolean isSinglePartition, int partitionId,
                    long clientHandle, int messageSize, long creationTimeNanos, String procName, long initiatorHSId,
                    boolean isShortCircuitRead) {
                return super.getHandle(isSinglePartition, partitionId,
                        clientHandle, messageSize, creationTimeNanos, procName, initiatorHSId, isShortCircuitRead);
            }
            @Override
            synchronized Iv2InFlight findHandle(long ciHandle) {
                return super.findHandle(ciHandle);
            }
            @Override
            synchronized Iv2InFlight removeHandle(long ciHandle) {
                return super.removeHandle(ciHandle);
            }
            @Override
            synchronized long getOutstandingTxns() {
                return super.getOutstandingTxns();
            }
            @Override
            synchronized void freeOutstandingTxns() {
                super.freeOutstandingTxns();
            }

            @Override
            synchronized List<Iv2InFlight> removeHandlesForPartitionAndInitiator(Integer partitionId,
                    Long initiatorHSId) {
                return super.removeHandlesForPartitionAndInitiator(partitionId, initiatorHSId);
            }

            @Override
            synchronized boolean shouldCheckThreadIdAssertion()
            {
                return false;
            }
        };
    }

    /**
     * Create a new handle for a transaction and store the client information
     * for that transaction in the internal structures.
     * ClientInterface handles have the partition ID encoded in them as the 10
     * high-order non-sign bits (where the SHORT_CIRCUIT_PART_ID is the max value),
     * and a 53 bit sequence number in the low 53 bits.
     */
    long getHandle(
            boolean isSinglePartition,
            int partitionId,
            long clientHandle,
            int messageSize,
            long creationTimeNanos,
            String procName,
            long initiatorHSId,
            boolean isShortCircuitRead)
    {
        assert(!shouldCheckThreadIdAssertion() || m_expectedThreadId == Thread.currentThread().getId());
        if (isShortCircuitRead) {
            partitionId = SHORT_CIRCUIT_PART_ID;
        } else if (!isSinglePartition) {
            partitionId = MP_PART_ID;
        } else if (initiatorHSId == ClientInterface.NTPROC_JUNK_ID) {
            partitionId = NT_PROC_PART_ID;
        }

        PartitionInFlightTracker tracker = m_trackerMap.get(partitionId);
        if (tracker == null) {
            tracker = new PartitionInFlightTracker(partitionId);
            m_trackerMap =
                    new Builder<Integer, PartitionInFlightTracker>().
                    putAll(m_trackerMap).
                    put(partitionId, tracker).build();
        }

        long ciHandle = tracker.m_generator.getNextHandle();
        Iv2InFlight inFlight = new Iv2InFlight(ciHandle, clientHandle, messageSize,
                                               creationTimeNanos, procName, initiatorHSId);

        tracker.m_inFlights.put(ciHandle, inFlight);

        m_outstandingTxns++;
        m_acg.increaseBackpressure(messageSize);
        return ciHandle;
    }

    /**
     * Retrieve the client information for the specified handle
     */
    Iv2InFlight findHandle(long ciHandle)
    {
        assert(!shouldCheckThreadIdAssertion() || m_expectedThreadId == Thread.currentThread().getId());

        /*
         * Check the partition specific queue of handles
         */
        int partitionId = getPartIdFromHandle(ciHandle);
        PartitionInFlightTracker partitionStuff = m_trackerMap.get(partitionId);
        if (partitionStuff == null) {
            // whoa, bad
            tmLog.error("Unable to find handle list for partition: " + partitionId +
                ", client interface handle: " + ciHandle);
            return null;
        }

        Iv2InFlight inFlight = partitionStuff.m_inFlights.remove(ciHandle);
        if (inFlight != null) {
            m_acg.reduceBackpressure(inFlight.m_messageSize);
            m_outstandingTxns--;
            return inFlight;
        }

        return null;
    }

    /** Remove a specific handle without destroying any handles ordered before it */
    Iv2InFlight removeHandle(long ciHandle)
    {
        assert(!shouldCheckThreadIdAssertion() || m_expectedThreadId == Thread.currentThread().getId());

        /*
         * Check the partition specific queue of handles
         */
        int partitionId = getPartIdFromHandle(ciHandle);
        PartitionInFlightTracker partitionStuff = m_trackerMap.get(partitionId);
        if (partitionStuff == null) {
            // whoa, bad
            tmLog.error("Unable to find handle list for removal for partition: " + partitionId +
                    ", client interface handle: " + ciHandle);
            return null;
        }

        Iv2InFlight inFlight = partitionStuff.m_inFlights.remove(ciHandle);
        if (inFlight != null) {
            m_acg.reduceBackpressure(inFlight.m_messageSize);
            m_outstandingTxns--;
            return inFlight;
        }
        tmLog.error("Unable to find Client data to remove client interface handle: " + ciHandle);
        return null;
    }

    /** Return a map of ConnectionId::(adminmode, txn count) */
    long getOutstandingTxns()
    {
        return m_outstandingTxns;
    }

    /**
     * When a connection goes away, free all resources held by that connection
     * This opens a small window of opportunity for mischief in that work may
     * still be outstanding in the cluster, but once the client goes away so does
     * does the mapping to the resources allocated to it.
     */
    void freeOutstandingTxns() {
        assert(!shouldCheckThreadIdAssertion() || m_expectedThreadId == Thread.currentThread().getId());
        for (PartitionInFlightTracker tracker : m_trackerMap.values()) {
            for (Iv2InFlight inflight : tracker.m_inFlights.values()) {
                m_outstandingTxns--;
                m_acg.reduceBackpressure(inflight.m_messageSize);
            }
        }
    }

    private void collectAndRemovePartitionInFlightRequests(Integer partitionId, Long initiatorHSId, List<Iv2InFlight> retval) {
        PartitionInFlightTracker partitionStuff = m_trackerMap.get(partitionId);
        if (partitionStuff != null) {
            Iterator<Entry<Long, Iv2InFlight>> iter = partitionStuff.m_inFlights.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<Long, Iv2InFlight> entry = iter.next();
                if (entry.getValue().m_initiatorHSId != initiatorHSId) {
                    if (tmLog.isTraceEnabled()) {
                        tmLog.trace("cleared response for handle " + entry.getKey());
                    }
                    iter.remove();
                    retval.add(entry.getValue());
                    m_outstandingTxns--;
                    m_acg.reduceBackpressure(entry.getValue().m_messageSize);
                }
            }
        }
    }

    List<Iv2InFlight> removeHandlesForPartitionAndInitiator(Integer partitionId, Long initiatorHSId) {
        assert(!shouldCheckThreadIdAssertion() || m_expectedThreadId == Thread.currentThread().getId());
        List<Iv2InFlight> retval = new ArrayList<Iv2InFlight>();

        /*
         * Clear pending responses
         */
        collectAndRemovePartitionInFlightRequests(partitionId, initiatorHSId, retval);
        if (partitionId == MP_PART_ID) {
            collectAndRemovePartitionInFlightRequests(SHORT_CIRCUIT_PART_ID, initiatorHSId, retval);
        }

        // No need to clear NP_PROC_PART_ID bucket during mastership change,
        // because all NT procedure handles are from local invocations

        return retval;
    }

    // Coward's way out...the thread-safe override of this class will return false for this,
    // which will enable us to keep the thread ID assertions in all of the method calls and
    // not bomb when using the thread-safe version.
    boolean shouldCheckThreadIdAssertion() {
        return true;
    }

    public void setWantsTopologyUpdates(boolean wantsTopologyUpdates) {
        m_wantsTopologyUpdates = wantsTopologyUpdates;
    }

    public boolean wantsTopologyUpdates() {
        return m_wantsTopologyUpdates;
    }

    public static int getPartIdFromHandle(long handle)
    {
        // SHORT_CIRCUIT_PART_ID has 15 bits
        return (int)((handle >> PART_ID_SHIFT) & ((MP_PART_ID << 1) + 1));
    }

    public static long getSeqNumFromHandle(long handle)
    {
        return handle & SEQNUM_MAX;
    }

    public static String handleToString(long handle)
    {
        return "(pid " + getPartIdFromHandle(handle) + " seq " + getSeqNumFromHandle(handle) + ")";
    }
}
