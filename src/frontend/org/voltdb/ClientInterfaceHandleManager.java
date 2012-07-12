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

package org.voltdb;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.utils.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

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
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger tmLog = new VoltLogger("TM");
    static final int PART_ID_BITS = 10;
    static final int MP_PART_ID = (1 << PART_ID_BITS) - 1;
    static final int PART_ID_SHIFT = 54;
    static final int SEQNUM_MAX = (1 << PART_ID_SHIFT) - 1;

    private long m_outstandingTxns;
    public final boolean isAdmin;
    public final Connection connection;

    private static class HandleGenerator
    {
        private long m_sequence = 0;
        final private long m_partitionId;

        HandleGenerator(int partitionId)
        {
            m_partitionId = partitionId;
            hostLog.info("Constructing HandleGenerator for partition: " +
                    m_partitionId);
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
        final long m_creationTime;
        Iv2InFlight(long ciHandle, long clientHandle,
                int messageSize, long creationTime)
        {
            m_ciHandle = ciHandle;
            m_clientHandle = clientHandle;
            m_messageSize = messageSize;
            m_creationTime = creationTime;
        }
    }

    private ImmutableMap<Integer, Pair<HandleGenerator, Deque<Iv2InFlight>>> m_partitionStuff =
            new Builder<Integer, Pair<HandleGenerator, Deque<Iv2InFlight>>>().build();

    ClientInterfaceHandleManager(boolean isAdmin, Connection connection)
    {
        this.isAdmin = isAdmin;
        this.connection = connection;
    }

    /**
     * Create a new handle for a transaction and store the client information
     * for that transaction in the internal structures.
     * ClientInterface handles have the partition ID encoded in them as the 10
     * high-order non-sign bits (where the MP partition ID is the max value),
     * and a 53 bit sequence number in the low 53 bits.
     */
    long getHandle(boolean isSinglePartition, int partitionId,
            long clientHandle,
            int messageSize, long creationTime)
    {
        if (!isSinglePartition) {
            partitionId = MP_PART_ID;
        }
        HandleGenerator generator;
        Deque<Iv2InFlight> perPartDeque;
        Pair<HandleGenerator, Deque<Iv2InFlight>> partitionStuff = m_partitionStuff.get(partitionId);
        if (partitionStuff == null) {
            perPartDeque = new ArrayDeque<Iv2InFlight>();
            generator = new HandleGenerator(partitionId);
            m_partitionStuff =
                    new Builder<Integer, Pair<HandleGenerator, Deque<Iv2InFlight>>>().
                        putAll(m_partitionStuff).
                        put(partitionId, Pair.of(generator, perPartDeque)).build();
        } else {
            generator = partitionStuff.getFirst();
            perPartDeque = partitionStuff.getSecond();
        }
        long ciHandle = generator.getNextHandle();
        Iv2InFlight inFlight = new Iv2InFlight(ciHandle, clientHandle, messageSize, creationTime);
        perPartDeque.addLast(inFlight);
        m_outstandingTxns++;
        return ciHandle;
    }

    /**
     * Remove the specified handle from internal storage.  Used for the 'oops'
     * cases.  Returns true or false depending on whether the given handle was
     * actually present and removed.
     */
    boolean removeHandle(long ciHandle)
    {
        int partitionId = getPartIdFromHandle(ciHandle);
        Pair<HandleGenerator, Deque<Iv2InFlight>> partitionStuff = m_partitionStuff.get(partitionId);
        if (partitionStuff == null) {
            return false;
        }

        Deque<Iv2InFlight> perPartDeque = partitionStuff.getSecond();
        Iterator<Iv2InFlight> iter = perPartDeque.iterator();
        while (iter.hasNext())
        {
            Iv2InFlight inflight = iter.next();
            if (inflight.m_ciHandle == ciHandle) {

                iter.remove();
                m_outstandingTxns--;
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieve the client information for the specified handle
     */
    Iv2InFlight findHandle(long ciHandle)
    {
        int partitionId = getPartIdFromHandle(ciHandle);
        Pair<HandleGenerator, Deque<Iv2InFlight>> partitionStuff = m_partitionStuff.get(partitionId);
        if (partitionStuff == null) {
            // whoa, bad
            tmLog.error("Unable to find handle list for partition: " + partitionId);
            return null;
        }

        Deque<Iv2InFlight> perPartDeque = partitionStuff.getSecond();

        while (perPartDeque.peekFirst() != null) {
            Iv2InFlight inFlight = perPartDeque.pollFirst();
            if (inFlight.m_ciHandle < ciHandle) {
                // lost txn, do something eventually
                tmLog.info("CI found dropped transaction with handle: " + inFlight.m_ciHandle +
                        " for partition: " + partitionId + " while searching for handle " +
                        ciHandle);
                m_outstandingTxns--;
            }
            else if (inFlight.m_ciHandle > ciHandle) {
                // we've gone too far, need to jam this back into the front of the deque and run away.
                tmLog.error("CI clientData lookup missing handle: " + ciHandle
                        + ". Next expected client data handle is: " + inFlight.m_ciHandle);
                perPartDeque.addFirst(inFlight);
                break;
            }
            else {
                m_outstandingTxns--;
                return inFlight;
            }
        }
        tmLog.error("Unable to find Client data for client interface handle: " + ciHandle);
        return null;
    }

    /** Return a map of ConnectionId::(adminmode, txn count) */
    long getOutstandingTxns()
    {
        return m_outstandingTxns;
    }
}
