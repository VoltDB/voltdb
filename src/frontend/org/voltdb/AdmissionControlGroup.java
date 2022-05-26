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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.voltcore.logging.VoltLogger;
import org.voltdb.dtxn.InitiatorStats.InvocationInfo;
import org.voltdb.dtxn.LatencyHistogramStats;

/**
 * Manage admission control for incoming requests by tracking the size of outstanding requests
 * as well as the number of outstanding requests in order to bound the total amount of memory
 * used to service the requests.
 *
 * Responses are also tracked by size although not by count and use the same resource pool
 * as incoming requests size wise. Count of responses is not tracked because they are flattened
 * and coalesced to byte buffers so the metadata overhead is minimal and not worth tracking separately
 *
 * Admission control only limits the amount of work each group is willing to accept into the cluster.
 * Because there is no coordination between groups it is possible for all the work to end up at one node.
 * This is guaranteed to happen if one node is slow enough that it can't keep up with the workload.
 */
public class AdmissionControlGroup implements org.voltcore.network.QueueMonitor
{
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");


    /*
     * Maximum values for each group are configured when the group is constructed
     */
    final private int MAX_DESIRED_PENDING_BYTES;
    final private int LESS_THAN_MAX_DESIRED_PENDING_BYTES;
    final private int MAX_DESIRED_PENDING_TXNS;
    final private int LESS_THAN_MAX_DESIRED_PENDING_TXNS;

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private int m_pendingTxnCount = 0;
    private long m_pendingTxnBytes = 0;
    private boolean m_hadBackPressure = false;

    /*
     * If for some reason ACG logs a negative transaction count or outstanding bytes,
     * only do it once to avoid flooding. Not going to make it a fatal error, but
     * don't want it to fail silently either.
     */
    private boolean m_haveLoggedACGNegativeFailure = false;

    /*
     * The ACG is accessed lock free from the network thread that owns the group.
     * Track the thread id of callers of this class to ensure that it is always the right
     * thread using assertions.
     */
    private final long m_expectedThreadId = Thread.currentThread().getId();

    /*
     * Members of the admission control group implement this interface and are expected
     * to stop accepting new requests when after onBackpressure is invoked and are allowed
     * to start accepting requests again once offBackpressure is invoked
     */
    public interface ACGMember
    {
        public void onBackpressure();
        public void offBackpressure();
        public long connectionId();
    }



    private final HashSet<ACGMember> m_members = new HashSet<ACGMember>();

    /*
     * Reads/writes to the actual InvocationInfo are unsynchronized. There is a single writer
     * so no issues there, but the reader is unprotected.
     *
     * There is only one writer so a single stripe is fine
     */
    private final ConcurrentHashMap<Long, Map<String, org.voltdb.dtxn.InitiatorStats.InvocationInfo>> m_connectionStates =
                 new ConcurrentHashMap<Long, Map<String, org.voltdb.dtxn.InitiatorStats.InvocationInfo>>(1024, .75f, 1);

    private final AbstractHistogram m_latencyInfo = LatencyHistogramStats.constructHistogram(true);

    public AdmissionControlGroup(int maxBytes, int maxRequests)
    {
        MAX_DESIRED_PENDING_BYTES = maxBytes;
        LESS_THAN_MAX_DESIRED_PENDING_BYTES = (int)(MAX_DESIRED_PENDING_BYTES * .8);
        MAX_DESIRED_PENDING_TXNS = maxRequests;
        LESS_THAN_MAX_DESIRED_PENDING_TXNS = (int)(MAX_DESIRED_PENDING_TXNS * .8);
    }

    public static AdmissionControlGroup getDummy() {
        return new AdmissionControlGroup(Integer.MAX_VALUE, Integer.MAX_VALUE) {
            @Override
            public void addMember(ACGMember member) {}
            @Override
            public void removeMember(ACGMember member) {}
            @Override
            public void increaseBackpressure(int messageSize) {}
            @Override
            public void reduceBackpressure(int messageSize) {}
            @Override
            public boolean queue(int bytes) { return false; }
        };
    }

    public void addMember(ACGMember member)
    {
        assert(m_expectedThreadId == Thread.currentThread().getId());
        m_members.add(member);
    }

    public void removeMember(ACGMember member)
    {
        assert(m_expectedThreadId == Thread.currentThread().getId());
        m_members.remove(member);
        m_connectionStates.remove(member.connectionId());
    }

    /*
     * Invoked when accepting a new transaction. Increments pending txn count in addition
     * to tracking the number of request bytes accepted. Can invoke onBackpressure
     * on all group members if there isn't already a backpressure condition.
     */
    public void increaseBackpressure(int messageSize)
    {
        assert(m_expectedThreadId == Thread.currentThread().getId());
        if (messageSize < 1) {
            throw new IllegalArgumentException("Message size must be > 0 but was " + messageSize);
        }
        m_pendingTxnBytes += messageSize;
        m_pendingTxnCount++;

        checkAndLogInvariants();
        if (m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES || m_pendingTxnCount > MAX_DESIRED_PENDING_TXNS) {
            if (!m_hadBackPressure) {
                hostLog.debug("TXN back pressure began");
                m_hadBackPressure = true;
                for (ACGMember m : m_members) {
                    m.onBackpressure();
                }
            }
        }
    }

    /*
     * Check that various invariants are maintained. If they aren't log the error at most once,
     * and take corrective action to maintain the invariants
     */
    private void checkAndLogInvariants() {
        if (m_pendingTxnCount < 0 || m_pendingTxnBytes < 0) {
            boolean badTxnCount = m_pendingTxnCount < 0 ? true : false;
            boolean badPendingBytes = m_pendingTxnBytes < 0 ? true : false;
            if (!m_haveLoggedACGNegativeFailure) {
                m_haveLoggedACGNegativeFailure = true;
                if (badTxnCount) {
                    networkLog.error("Admission control error, negative outstanding transaction count. " +
                            "This is error is not fatal, but it does indicate that admission control " +
                            "is not correctly tracking transaction resource usage. This message will not repeat " +
                            "the next time the condition occurs to avoid log spam");
                }
                if (badPendingBytes) {
                    networkLog.error(
                            "Backpressure reports a negative outstanding transaction byte count (" +
                            m_pendingTxnBytes +
                            "). No action required.", new RuntimeException("for stack trace purposes"));
                }
            }

            /*
             * Repair both. It's possible that repairing it will trigger a repair cascade
             * effectively rendering the ACG always permissive, but it should right itself
             * once all requests associated with the ACG have left the system and the correct values are indeed 0.
             */
            m_pendingTxnCount = 0;
            m_pendingTxnBytes = 0;
        }
    }

    /*
     * Invoked when receiving a response to a transaction. Decrements pending txn count in addition
     * to tracking the number of request bytes accepted. Can invoke offBackpressure
     * on all group members if there is a backpressure condition that has ended
     */
    public void reduceBackpressure(int messageSize)
    {
        assert(m_expectedThreadId == Thread.currentThread().getId());
        if (messageSize < 1) {
            throw new IllegalArgumentException("Message size must be > 0 but was " + messageSize);
        }

        m_pendingTxnBytes -= messageSize;
        m_pendingTxnCount--;
        checkAndLogInvariants();

        if ((m_pendingTxnBytes < LESS_THAN_MAX_DESIRED_PENDING_BYTES) &&
            (m_pendingTxnCount < LESS_THAN_MAX_DESIRED_PENDING_TXNS))
        {
            if (m_hadBackPressure) {
                hostLog.debug("TXN backpressure ended");
                m_hadBackPressure = false;
                for (ACGMember m : m_members) {
                    m.offBackpressure();
                }
            }
        }
    }

    /*
     * When accepting new connections this flag is used to decide whether read selection should be enabled.
     * Read selection should not be enabled if there is currently backpressure
     */
    public boolean hasBackPressure() {
        return m_hadBackPressure;
    }

    /**
     * Used by tests.
     * @return
     */
    public long getPendingBytes() {
        return m_pendingTxnBytes;
    }

    /*
     * Invoked when queueing response bytes back to a connection. Can be invoked with positive/negative
     * values to indicate whether data is being flushed or added. The same resource pool counter is used
     * to track pending responses as is used to track outstanding requests although only the bytes are being
     * counted.
     *
     * Can signal start/stop backpressure when appropriate. The return value is not used and will be removed
     * in the future.
     */
    @Override
    public boolean queue(int bytes) {
        m_pendingTxnBytes += bytes;
        checkAndLogInvariants();

        if (m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES) {
            if (!m_hadBackPressure) {
                hostLog.debug("TXN back pressure began");
                m_hadBackPressure = true;
                for (ACGMember m : m_members) {
                    m.onBackpressure();
                }
            }
        }
        else if ((m_pendingTxnBytes < LESS_THAN_MAX_DESIRED_PENDING_BYTES) &&
                (m_pendingTxnCount < LESS_THAN_MAX_DESIRED_PENDING_TXNS)) {
            if (m_hadBackPressure) {
                hostLog.debug("TXN backpressure ended");
                m_hadBackPressure = false;
                for (ACGMember m : m_members) {
                    m.offBackpressure();
                }
            }
        }

        return false;
    }

    public void logTransactionCompleted(
            long connectionId,
            String connectionHostname,
            String procedureName,
            long deltaNanos,
            byte status) {
        boolean needToInsert = false;
        Map<String, InvocationInfo> procInfoMap = m_connectionStates.get(connectionId);
        if (procInfoMap == null) {
            procInfoMap = new NonBlockingHashMap<String, InvocationInfo>();
            needToInsert = true;
        }
        InvocationInfo info = procInfoMap.get(procedureName);
        if(info == null){
            info = new InvocationInfo(connectionHostname);
            procInfoMap.put(procedureName, info);
        }
        info.processInvocation((int)TimeUnit.NANOSECONDS.toMillis(deltaNanos), status);
        // ENG-7209 This is to not log the latency value for a snapshot restore, as this just creates
        // a large initial value in the graph which is not actually relevant to the user.
        if (!procedureName.equals("@SnapshotRestore")) {
            m_latencyInfo.recordValue(Math.max(1, Math.min(TimeUnit.NANOSECONDS.toMicros(deltaNanos), m_latencyInfo.getHighestTrackableValue())));
        }
        if (needToInsert) {
            m_connectionStates.put(connectionId, procInfoMap);
        }
    }

    public Iterator<Map.Entry<Long, Map<String, InvocationInfo>>> getInitiationStatsIterator() {
        return m_connectionStates.entrySet().iterator();
    }

    public AbstractHistogram getLatencyInfo() {
        return m_latencyInfo;
    }
}
