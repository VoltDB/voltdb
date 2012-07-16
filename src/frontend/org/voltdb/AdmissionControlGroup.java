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


import java.util.HashSet;
import org.voltcore.logging.VoltLogger;

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
    final private int MAX_DESIRED_PENDING_TXNS;

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
    }

    private final HashSet<ACGMember> m_members = new HashSet<ACGMember>();

    public AdmissionControlGroup(int maxBytes, int maxRequests)
    {
        MAX_DESIRED_PENDING_BYTES = maxBytes;
        MAX_DESIRED_PENDING_TXNS = maxRequests;
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
                    networkLog.error("Admission control error, negative outstanding transaction byte count (" +
                            m_pendingTxnBytes + "). " +
                            "This is error is not fatal, but it does indicate that admission control " +
                            "is not correctly tracking transaction resource usage. This message will not repeat " +
                            "the next time the condition occurs to avoid log spam");
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
        if (m_pendingTxnBytes < (MAX_DESIRED_PENDING_BYTES * .8) &&
            m_pendingTxnCount < (MAX_DESIRED_PENDING_TXNS * .8))
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
        if (bytes > 0) {
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
        } else {
            m_pendingTxnBytes += bytes;
            checkAndLogInvariants();
            if (m_pendingTxnBytes < (MAX_DESIRED_PENDING_BYTES * .8)) {
                if (m_hadBackPressure) {
                    hostLog.debug("TXN backpressure ended");
                    m_hadBackPressure = false;
                    for (ACGMember m : m_members) {
                        m.offBackpressure();
                    }
                }
            }
        }

        return false;
    }
}
