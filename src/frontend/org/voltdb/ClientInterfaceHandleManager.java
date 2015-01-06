/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltdb.iv2.MpInitiator;

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

    static final long READ_BIT = 1L << 63;
    //Add an extra bit so compared to the 14-bits in txnids so there
    //can be a short circuit read partition id
    static final int PART_ID_BITS = 15;
    static final int MP_PART_ID = (1 << (PART_ID_BITS - 1)) - 1;
    static final int SHORT_CIRCUIT_PART_ID = MP_PART_ID + 1;
    static final long PART_ID_SHIFT = 48;
    static final long SEQNUM_MAX = (1L << PART_ID_SHIFT) - 1L;

    private long m_outstandingTxns;
    public final boolean isAdmin;
    public final Connection connection;
    private final long m_expectedThreadId = Thread.currentThread().getId();
    final AdmissionControlGroup m_acg;

    private volatile boolean m_wantsTopologyUpdates = false;

    private HandleGenerator m_shortCircuitHG = new HandleGenerator(SHORT_CIRCUIT_PART_ID);

    private final Map<Long, Iv2InFlight> m_shortCircuitReads = new HashMap<Long, Iv2InFlight>();

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

    public static int getPartIdFromHandle(long handle)
    {
        return (int)((handle >> PART_ID_SHIFT) & MP_PART_ID);
    }

    public static long getSeqNumFromHandle(long handle)
    {
        return handle & SEQNUM_MAX;
    }

    public static String handleToString(long handle)
    {
        return "(pid " + getPartIdFromHandle(handle) + " seq " + getSeqNumFromHandle(handle) + ")";
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

    static class PartitionData {
        private final HandleGenerator m_generator;
        private final Deque<Iv2InFlight> m_reads = new ArrayDeque<Iv2InFlight>();
        private final Deque<Iv2InFlight> m_writes = new ArrayDeque<Iv2InFlight>();

        private PartitionData(int partitionId) {
            m_generator = new HandleGenerator(partitionId);
        }
    }

    private ImmutableMap<Integer, PartitionData> m_partitionStuff =
            new Builder<Integer, PartitionData>().build();

    ClientInterfaceHandleManager(boolean isAdmin, Connection connection, AdmissionControlGroup acg)
    {
        this.isAdmin = isAdmin;
        this.connection = connection;
        m_acg = acg;
    }

    /**
     * Factory to make a threadsafe version of CIHM. This is used
     * exclusively by some internal CI adapters that don't have
     * the natural thread-safety protocol/design of VoltNetwork.
     */
    public static ClientInterfaceHandleManager makeThreadSafeCIHM(
            boolean isAdmin, Connection connection, AdmissionControlGroup acg)
    {
        return new ClientInterfaceHandleManager(isAdmin, connection, acg) {
            @Override
            synchronized long getHandle(boolean isSinglePartition, int partitionId,
                    long clientHandle, int messageSize, long creationTimeNanos, String procName, long initiatorHSId,
                    boolean readOnly, boolean isShortCircuitRead) {
                return super.getHandle(isSinglePartition, partitionId,
                        clientHandle, messageSize, creationTimeNanos, procName, initiatorHSId, readOnly, isShortCircuitRead);
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
     * high-order non-sign bits (where the MP partition ID is the max value),
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
            boolean readOnly,
            boolean isShortCircuitRead)
    {
        assert(!shouldCheckThreadIdAssertion() || m_expectedThreadId == Thread.currentThread().getId());
        if (!isSinglePartition) {
            partitionId = MP_PART_ID;
        }

        PartitionData partitionStuff = m_partitionStuff.get(partitionId);
        if (partitionStuff == null) {
            partitionStuff = new PartitionData(partitionId);
            m_partitionStuff =
                    new Builder<Integer, PartitionData>().
                        putAll(m_partitionStuff).
                        put(partitionId, partitionStuff).build();
        }

        long ciHandle =
                isShortCircuitRead ? m_shortCircuitHG.getNextHandle() : partitionStuff.m_generator.getNextHandle();
        Iv2InFlight inFlight =
                new Iv2InFlight(ciHandle, clientHandle, messageSize, creationTimeNanos, procName, initiatorHSId);

        if (isShortCircuitRead) {
            /*
             * Short circuit reads don't use a handle that is partition specific
             * because ordering doesn't really matter since it isn't used for failure handling
             * because the read is local to this process
             */
            m_shortCircuitReads.put(ciHandle, inFlight);
        } else {
            /*
             * Reads are not ordered with writes, writes might block due to command logging
             * so track them separately because they will come back in mixed order
             */
            if (readOnly) {
                /*
                 * Encode the read only-ness into the handle
                 */
                ciHandle = setReadBit(ciHandle);
                partitionStuff.m_reads.offer(inFlight);
            } else {
                partitionStuff.m_writes.offer(inFlight);
            }
        }

        m_outstandingTxns++;
        m_acg.increaseBackpressure(messageSize);
        return ciHandle;
    }

    private static boolean getReadBit(long handle) {
        return (handle & READ_BIT) != 0;
    }

    private static long unsetReadBit(long handle) {
        return handle & ~READ_BIT;
    }

    private static long setReadBit(long handle) {
        return (handle |= READ_BIT);
    }


    /**
     * Retrieve the client information for the specified handle
     */
    Iv2InFlight findHandle(long ciHandle)
    {
        assert(!shouldCheckThreadIdAssertion() || m_expectedThreadId == Thread.currentThread().getId());
        //Check read only encoded bit
        final boolean readOnly = getReadBit(ciHandle);
        //Remove read only encoding so comparison works
        ciHandle = unsetReadBit(ciHandle);

        /*
         * Check for a short circuit read
         */
        Iv2InFlight inflight = m_shortCircuitReads.remove(ciHandle);
        if (inflight != null) {
            m_acg.reduceBackpressure(inflight.m_messageSize);
            m_outstandingTxns--;
            return inflight;
        }

        /*
         * Not a short circuit read, check the partition specific
         * queue of handles
         */
        int partitionId = getPartIdFromHandle(ciHandle);
        PartitionData partitionStuff = m_partitionStuff.get(partitionId);
        if (partitionStuff == null) {
            // whoa, bad
            tmLog.error("Unable to find handle list for partition: " + partitionId);
            return null;
        }

        final Deque<Iv2InFlight> perPartDeque = readOnly ? partitionStuff.m_reads : partitionStuff.m_writes;
        while (perPartDeque.peekFirst() != null) {
            Iv2InFlight inFlight = perPartDeque.pollFirst();
            if (inFlight.m_ciHandle < ciHandle) {
                // lost txn, do something eventually
                tmLog.debug("CI found dropped transaction with handle: " + inFlight.m_ciHandle +
                        " for partition: " + partitionId + " while searching for handle " +
                        ciHandle);
                ClientResponseImpl errorResponse =
                    new ClientResponseImpl(
                            ClientResponseImpl.RESPONSE_UNKNOWN,
                            new VoltTable[0], "Transaction dropped during fault recovery",
                            inFlight.m_clientHandle);
                ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
                buf.putInt(buf.capacity() - 4);
                errorResponse.flattenToBuffer(buf);
                buf.flip();
                connection.writeStream().enqueue(buf);
                m_outstandingTxns--;
                m_acg.reduceBackpressure(inFlight.m_messageSize);
            }
            else if (inFlight.m_ciHandle > ciHandle) {
                // we've gone too far, need to jam this back into the front of the deque and run away.
                tmLog.debug("CI clientData lookup missing handle: " + ciHandle
                        + ". Next expected client data handle is: " + inFlight.m_ciHandle);
                perPartDeque.addFirst(inFlight);
                break;
            }
            else {
                m_acg.reduceBackpressure(inFlight.m_messageSize);
                m_outstandingTxns--;
                return inFlight;
            }
        }
        tmLog.debug("Unable to find Client data for client interface handle: " + ciHandle);
        return null;
    }

    /** Remove a specific handle without destroying any handles ordered before it */
    Iv2InFlight removeHandle(long ciHandle)
    {
        assert(!shouldCheckThreadIdAssertion() || m_expectedThreadId == Thread.currentThread().getId());
        //Check read only encoded bit
        final boolean readOnly = getReadBit(ciHandle);
        //Remove read only encoding so comparison works
        ciHandle = unsetReadBit(ciHandle);

        // Shouldn't see any reads in this path, since the whole point of this
        // method is to remove writes during replay which aren't going to get
        // done.  However, this is logically correct, so go ahead and allow it.
        Iv2InFlight inflight = m_shortCircuitReads.remove(ciHandle);
        if (inflight != null) {
            m_acg.reduceBackpressure(inflight.m_messageSize);
            m_outstandingTxns--;
            return inflight;
        }

        /*
         * Not a short circuit read, check the partition specific
         * queue of handles
         */
        int partitionId = getPartIdFromHandle(ciHandle);
        PartitionData partitionStuff = m_partitionStuff.get(partitionId);
        if (partitionStuff == null) {
            // whoa, bad
            tmLog.error("Unable to find handle list for partition: " + partitionId);
            return null;
        }

        final Deque<Iv2InFlight> perPartDeque = readOnly ? partitionStuff.m_reads : partitionStuff.m_writes;
        Iterator<Iv2InFlight> iter = perPartDeque.iterator();
        while (iter.hasNext()) {
            Iv2InFlight inFlight = iter.next();
            if (inFlight.m_ciHandle > ciHandle) {
                // we've gone too far, this handle doesn't exist
                tmLog.error("CI clientData lookup for remove missing handle: " + ciHandle
                        + ". Next expected client data handle is: " + inFlight.m_ciHandle);
                break;
            }
            else if (inFlight.m_ciHandle == ciHandle) {
                m_acg.reduceBackpressure(inFlight.m_messageSize);
                m_outstandingTxns--;
                iter.remove();
                return inFlight;
            }
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
        for (PartitionData pd : m_partitionStuff.values()) {
            for (Iv2InFlight inflight : pd.m_reads) {
                m_outstandingTxns--;
                m_acg.reduceBackpressure(inflight.m_messageSize);
            }
            for (Iv2InFlight inflight : pd.m_writes) {
                m_outstandingTxns--;
                m_acg.reduceBackpressure(inflight.m_messageSize);
            }
        }
        for (Iv2InFlight inflight : m_shortCircuitReads.values()) {
            m_outstandingTxns--;
            m_acg.reduceBackpressure(inflight.m_messageSize);
        }
    }

    List<Iv2InFlight> removeHandlesForPartitionAndInitiator(Integer partitionId,
            Long initiatorHSId) {
        assert(!shouldCheckThreadIdAssertion() || m_expectedThreadId == Thread.currentThread().getId());
        List<Iv2InFlight> retval = new ArrayList<Iv2InFlight>();

        if (!m_partitionStuff.containsKey(partitionId)) return retval;

        /*
         * First clear the pending reads
         */
        PartitionData partitionStuff = m_partitionStuff.get(partitionId);
        Deque<Iv2InFlight> inFlight = partitionStuff.m_reads;
        Iterator<Iv2InFlight> i = inFlight.iterator();
        while (i.hasNext()) {
            Iv2InFlight entry = i.next();
            if (entry.m_initiatorHSId != initiatorHSId) {
                i.remove();
                retval.add(entry);
                m_outstandingTxns--;
                m_acg.reduceBackpressure(entry.m_messageSize);
            }
        }

        /*
         * MP short circuit reads can be remote, which necessitate repair
         */
        if (partitionId == MpInitiator.MP_INIT_PID) {
            Iterator<Map.Entry<Long, Iv2InFlight>> itr =
                    m_shortCircuitReads.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry<Long, Iv2InFlight> e = itr.next();
                Iv2InFlight entry = e.getValue();

                if (entry.m_initiatorHSId != initiatorHSId) {
                    itr.remove();
                    retval.add(entry);
                    m_outstandingTxns--;
                    m_acg.reduceBackpressure(entry.m_messageSize);
                }
            }
        }

        /*
         * Then clear the pending writes
         */
        inFlight = partitionStuff.m_writes;
        i = inFlight.iterator();
        while (i.hasNext()) {
            Iv2InFlight entry = i.next();
            if (entry.m_initiatorHSId != initiatorHSId) {
                i.remove();
                retval.add(entry);
                m_outstandingTxns--;
                m_acg.reduceBackpressure(entry.m_messageSize);
            }
        }
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
}
