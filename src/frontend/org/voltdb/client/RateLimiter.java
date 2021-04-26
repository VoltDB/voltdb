/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
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

package org.voltdb.client;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Provide the {@link Client} with a way to throttle throughput in one
 * of several ways. First, it can cap outstanding transactions or
 * limit the rate of new transactions. Second, it can auto-tune the
 * send rate to get a good balance of througput and latency on the
 * server.
 */
class RateLimiter {

    final static int BLOCK_SIZE = 100; // ms
    final static int HISTORY_SIZE = 25;
    final static int RECENT_HISTORY_SIZE = 5;
    final static int MINIMUM_MOVEMENT = 5;

    // If true, we're doing actual rate limiting.
    // If false, we're just limiting outstanding txns.
    protected boolean m_doesAnyTuning = false;

    // Data used in rate limiting
    protected int m_maxOutstandingTxns = 10;
    protected int m_outstandingTxns = 0;
    protected long m_currentBlockTimestamp = -1;
    protected int m_currentBlockSendCount = 0;
    protected int m_currentBlockRecvSuccessCount = 0;
    protected long m_currentBlockTotalInternalLatency = 0;
    protected Deque<Double> m_prevInternalLatencyAvgs = new ArrayDeque<>();

    // Autotuning-specific data
    protected boolean m_autoTune = false;
    protected int m_targetTxnsPerSecond = Integer.MAX_VALUE;
    protected int m_latencyTarget = 5;

    // Non-rate-limiting data
    protected Semaphore m_outstandingTxnsSemaphore = new Semaphore(10);

    /*
     * Implements the autotune mechanisms.
     */
    private void autoTuneTargetFromHistory() {
        double recentLatency = 0, mediumTermLatency = 0;
        if (! m_prevInternalLatencyAvgs.isEmpty()) {
            int i = 0;
            for (double value : m_prevInternalLatencyAvgs) {
                if (i < RECENT_HISTORY_SIZE) recentLatency += value;
                mediumTermLatency += value;
                ++i;
            }
            recentLatency /= Math.min(m_prevInternalLatencyAvgs.size(), RECENT_HISTORY_SIZE);
            mediumTermLatency /= m_prevInternalLatencyAvgs.size();
        }

        if (mediumTermLatency > m_latencyTarget && recentLatency > m_latencyTarget) {
            m_maxOutstandingTxns -= Math.max(0.1 * m_maxOutstandingTxns, MINIMUM_MOVEMENT);
        } else if (mediumTermLatency < m_latencyTarget && recentLatency > m_latencyTarget) {
            --m_maxOutstandingTxns;
        } else if (mediumTermLatency > m_latencyTarget && recentLatency < m_latencyTarget) {
            m_maxOutstandingTxns++;
        } else { // if ((mediumTermLatency < m_latencyTarget) && (recentLatency < m_latencyTarget)) {
            m_maxOutstandingTxns += Math.max(0.1 * m_maxOutstandingTxns, MINIMUM_MOVEMENT);
        }

        // don't let this go to 0, latency be damned
        if (m_maxOutstandingTxns <= 0) {
            m_maxOutstandingTxns = 1;
        }
    }

    /*
     * Given the timestamp of a transaction, checks whether the timestamp is
     * represented within the current block of transactions, and if not, starts
     * a new block. Autotuning is applied at this time if desired.
     */
    private void ensureCurrentBlockIsKosher(long timestamp) {
        long thisBlock = timestamp - (timestamp % BLOCK_SIZE);

        // handle first time initialization
        if (m_currentBlockTimestamp == -1) {
            m_currentBlockTimestamp = thisBlock;
        }

        // handle time moving backwards (a bit)
        if (thisBlock < m_currentBlockTimestamp) {
            thisBlock = m_currentBlockTimestamp;
        }

        // check for new block
        if (thisBlock > m_currentBlockTimestamp) {
            // need to deal with 100ms skips here TODO
            m_currentBlockTimestamp = thisBlock;
            m_prevInternalLatencyAvgs.addFirst(
                    m_currentBlockTotalInternalLatency / (double) m_currentBlockRecvSuccessCount);
            while (m_prevInternalLatencyAvgs.size() > HISTORY_SIZE) {
                m_prevInternalLatencyAvgs.pollLast();
            }
            m_currentBlockSendCount = 0;
            m_currentBlockRecvSuccessCount = 0;
            m_currentBlockTotalInternalLatency = 0;

            if (m_autoTune) {
                autoTuneTargetFromHistory();
            }
        }
    }

    /**
     * Unconditionally enables autotuning based on using some target
     * latency to dynamically adjust the limit of outstandind txns.
     * May not be reflected until the next 100ms.
     */
    synchronized void enableAutoTuning(int latencyTarget) {
        m_autoTune = true;
        m_doesAnyTuning = true;
        m_targetTxnsPerSecond = Integer.MAX_VALUE;
        m_maxOutstandingTxns = 20;
        m_latencyTarget = latencyTarget;
    }

    /**
     * Sets static limits on transaction rate and outstanding txns.
     * The txnsPerSec limit must be 'reasonably low' to enable
     * rate limiting (if it's set unachievably high then the limit
     * will never be exceeded, so there's no point in tracking).
     * May not be reflected until the next 100ms.
     */
    synchronized void setLimits(int txnsPerSec, int maxOutstanding) {
        m_autoTune = false;
        m_doesAnyTuning = (txnsPerSec < Integer.MAX_VALUE / 2);
        m_targetTxnsPerSecond = txnsPerSec;
        m_maxOutstandingTxns = maxOutstanding;
        m_outstandingTxnsSemaphore.drainPermits();
        m_outstandingTxnsSemaphore.release(maxOutstanding);
    }

    /**
     * Get the instantaneous values of the rate limiting values for this client.
     * @return A length-2 array of integers representing max throughput/sec and
     * max outstanding txns.
     */
    synchronized int[] getLimits() {
        int[] limits = new int[2];
        limits[0] = m_targetTxnsPerSecond;
        limits[1] = m_maxOutstandingTxns;
        return limits;
    }

    /**
     * Do housekeeping on rate-limiting mechanism when response is received
     * or we time out on awaiting a response.
     *
     * @param timestampNanos The time as measured when the call is made.
     * @param internalLatency Latency measurement of this transaction in millis (-1 for no response)
     * @param ignoreBackpressure Don't return a permit for backpressure purposes since none was ever taken
     */
    void transactionResponseReceived(long timestampNanos, int internalLatency, boolean ignoreBackpressure) {
        if (m_doesAnyTuning) {
            synchronized (this) {
                ensureCurrentBlockIsKosher(TimeUnit.NANOSECONDS.toMillis(timestampNanos));
                --m_outstandingTxns;
                assert(m_outstandingTxns >= 0);
                if (internalLatency != -1) {
                    ++m_currentBlockRecvSuccessCount;
                    m_currentBlockTotalInternalLatency += internalLatency;
                }
            }
        } else if (!ignoreBackpressure) {
            m_outstandingTxnsSemaphore.release();
        }
    }

    /**
     * We're about to send a request; this method handles rate-limiting.
     * Two major modes of operation are possible:
     *
     * 1. Normal rate-limiting mode ('doesAnyTuning' is true)
     *    We do rate calculations and then either return so that the
     *    request can be sent, or else we wait a tick and then retry.
     *    We'll keep retrying until the rate drops below the limit,
     *    or until the specified timeout expires.
     *
     * 2. Limit based on outstanding transaction count only, not
     *    on rate limiting. We wait on a semaphore, but not longer
     *    than the specified timeout.
     *
     * A 'true' value for ignoreBackpressure flag modifies execution
     * of the two modes as follows:
     *
     * 1. We do not wait for an acceptable rate, we just do the
     *    rate computations once and return.
     *
     * 2. We do not wait on the semaphore, but return immediately.
     *    Note that it's essential that in this case, ignoreBackPressure
     *    is also true on transactionResponseReceived, in order to
     *    avoid incrementing the semaphore we did not decrement.
     *
     * When ignoreBackpressure is true, timeoutNanos has no effect.
     * TODO - this may be a bug.
     *
     * @param callTimeNanos The time as measured when the call is made.
     * @param timeoutNanos Limit on time waiting for permission to send
     * @param ignoreBackpressure If true, send permission immediately granted
     * @return The time as measured when the call returns.
     */
    long prepareToSendTransaction(long callTimeNanos, long timeoutNanos, boolean ignoreBackpressure)
        throws TimeoutException, InterruptedException {

        long timestampNanos = callTimeNanos; // updated if we block

        // Rate-limiting mode
        if (m_doesAnyTuning) {
            while (!rateWithinLimit(timestampNanos, ignoreBackpressure)) {

                // timed out? (TODO - surely this is required ?)
                //if (timestampNanos - callTimeNanos >= timeoutNanos) {
                //    throw new TimeoutException("timed out in rate limiter");
                //}

                // If the rate is above target, pause for the smallest time possible
                // (we specify 1 ms, but actual delay depends on scheduling granularity)
                Thread.sleep(1);
                timestampNanos = System.nanoTime();
            }
        }

        // Outstanding transaction count only
        else if (!ignoreBackpressure) {
            boolean acquired = m_outstandingTxnsSemaphore.tryAcquire();
            if (!acquired) {
                acquired = m_outstandingTxnsSemaphore.tryAcquire(timeoutNanos, TimeUnit.NANOSECONDS);
                if (!acquired) {
                    throw new TimeoutException("timed out awaiting send permit");
                }
                timestampNanos = System.nanoTime();
            }
        }

        // This time may have changed if this call blocked
        return timestampNanos;
    }

    /*
     * Checks current transaction rate against the target limit, to determine
     * if a further send is now possible.  If so, updates the outstanding
     * transaction and blocks-sent count on the assumption we will in
     * fact send.
     */
    private synchronized boolean rateWithinLimit(long timestampNanos, boolean ignoreBackpressure) {
        long timestamp = TimeUnit.NANOSECONDS.toMillis(timestampNanos);

        // Switch to a new block if 100ms has passed, and
        // possibly compute a new target rate.
        ensureCurrentBlockIsKosher(timestamp);
        assert(timestamp - m_currentBlockTimestamp <= BLOCK_SIZE);

        // Check current rate, but Skip calculations if we're going to
        // ignore the result anyway
        if (ignoreBackpressure || checkRate(timestamp)) {
            ++m_currentBlockSendCount;
            ++m_outstandingTxns;
            return true;
        }

        // Can't send yet
        return false;
    }

    private boolean checkRate(long timestamp) {
        // Don't let the time be before the start of the current block
        // Also ensure faketime - m_currentBlockTimestamp is positive
        long faketime = Math.max(timestamp, m_currentBlockTimestamp);
        long targetTxnsPerBlock = m_targetTxnsPerSecond / (1000 / BLOCK_SIZE);

        // Compute the percentage of the current 100ms block that has passed
        double expectedTxnsSent =
            targetTxnsPerBlock * (faketime - m_currentBlockTimestamp + 1.0) / BLOCK_SIZE;
        expectedTxnsSent = Math.ceil(expectedTxnsSent);
        assert(expectedTxnsSent <= targetTxnsPerBlock); // stupid fp math
        assert(expectedTxnsSent >= 1.0 || targetTxnsPerBlock == 0);

        // If the rate is under target, we can send immediately
        return (m_currentBlockSendCount < expectedTxnsSent && m_outstandingTxns < m_maxOutstandingTxns);
    }

    /**
     * Debug aid, dump rate-limit stats to standard out
     */
    synchronized void debug() {
        System.out.printf("Target throughput/s is %d and max outstanding txns is %d\n",
                m_targetTxnsPerSecond, m_maxOutstandingTxns);
        System.out.printf("Current outstanding is %d and recent internal latency is %.2f\n",
                m_outstandingTxns, m_prevInternalLatencyAvgs.peekFirst());
    }
}
