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

package org.voltdb.client;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.voltcore.utils.EstTime;

/**
 * Provide the {@link Client} with a way to throttle throughput in one
 * of several ways. First, it can cap outstanding transactions or
 * limit the rate of new transactions. Second, it can auto-tune the
 * send rate to get a good balance of througput and latency on the
 * server.
 */
class RateLimiter {

    final static int BLOCK_SIZE = 100; // ms
    final static int DEFAULT_TXN_LIMIT = 10;

    // If true, we're doing actual rate limiting.
    // If false, we're just limiting outstanding txns.
    protected boolean m_doesRateLimiting = false;

    // Data used in rate limiting
    protected int m_maxOutstandingTxns = DEFAULT_TXN_LIMIT;
    protected int m_outstandingTxns = 0;
    protected long m_currentBlockTimestamp = -1;
    protected int m_currentBlockSendCount = 0;
    protected int m_currentBlockRecvSuccessCount = 0;
    protected long m_currentBlockTotalInternalLatency = 0;
    protected int m_targetTxnsPerSecond = Integer.MAX_VALUE;

    // Non-rate-limiting data
    protected Semaphore m_outstandingTxnsSemaphore = new Semaphore(DEFAULT_TXN_LIMIT);

    // Flow maintenance in non-blocking mode
    private Runnable m_resumeSendCallback = null;
    private boolean m_needResume = false;
    private long m_resumeWaitTimeout = 0;
    private int m_nonblockingOutCount = 0;
    private int m_nonblockingResumeLevel = Math.round(DEFAULT_TXN_LIMIT * RESUME_THRESHOLD);
    private final static float RESUME_THRESHOLD = 0.25f;
    private final static int RESUME_TIMEOUT_FACTOR = 5;

    /*
     * Given the timestamp of a transaction, checks whether the timestamp is
     * represented within the current block of transactions, and if not, starts
     * a new block.
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
            m_currentBlockSendCount = 0;
            m_currentBlockRecvSuccessCount = 0;
            m_currentBlockTotalInternalLatency = 0;
        }
    }

    /**
     * Sets static limits on transaction rate and outstanding txns.
     * The txnsPerSec limit must be 'reasonably low' to enable
     * rate limiting (if it's set unachievably high then the limit
     * will never be exceeded, so there's no point in tracking).
     * May not be reflected until the next 100ms.
     */
    synchronized void setLimits(int txnsPerSec, int maxOutstanding) {
        m_doesRateLimiting = (txnsPerSec < Integer.MAX_VALUE / 2);
        m_targetTxnsPerSecond = txnsPerSec;
        m_maxOutstandingTxns = maxOutstanding;
        m_outstandingTxnsSemaphore.drainPermits();
        m_outstandingTxnsSemaphore.release(maxOutstanding);
        m_nonblockingOutCount = 0;
        m_nonblockingResumeLevel = Math.round(maxOutstanding * RESUME_THRESHOLD);
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
     * Sets the "resume callback" needed for non-blocking mode (see the
     * transactionResponseReceived method below) and implicitly declares
     * the intent to use non-blocking sends.
     */
    synchronized void setNonblockingResumeHook(Runnable callbk) {
        m_resumeSendCallback = callbk;
    }

    /**
     * Do housekeeping on rate-limiting mechanism when response is received
     * or we time out on awaiting a response.
     *
     * @param endNanos The time as measured when the response was received
     * @param internalLatency Latency measurement of this transaction in millis (-1 for no response)
     * @param ignoreBackpressure Don't return a permit for backpressure purposes since none was ever taken
     */
    void transactionResponseReceived(long endNanos, int internalLatency, boolean ignoreBackpressure) {
        if (m_doesRateLimiting) {
            synchronized (this) {
                ensureCurrentBlockIsKosher(TimeUnit.NANOSECONDS.toMillis(endNanos));
                --m_outstandingTxns;
                assert(m_outstandingTxns >= 0);
                if (internalLatency != -1) {
                    ++m_currentBlockRecvSuccessCount;
                    m_currentBlockTotalInternalLatency += internalLatency;
                }
            }
        } else if (!ignoreBackpressure) {
            m_outstandingTxnsSemaphore.release();
            if (m_resumeSendCallback != null && shouldResumeSending()) {
                m_resumeSendCallback.run();
            }
        }
    }

    /*
     * When the client is using non-blocking sends and has previously failed
     * to acquire a send permit (too many outstanding txns), we need to tell
     * it when it can resume. This is done when the outstanding txn count
     * drops below some threshold, currently set to 25% of the available
     * send permits. Note, nonblocking count can be wrong if client mixes
     * non-blocking and blocking sends. We don't expect it to be frequent.
     * We also implement a (fairly long) timeout that, in practice, we
     * expect to never be needed. It's there to protect against unexpected
     * problems, which will probably indicate bugs.
     */
    private synchronized boolean shouldResumeSending() {
        if (m_nonblockingOutCount > 0) {
            --m_nonblockingOutCount;
        }
        if (m_needResume && (m_nonblockingOutCount <= m_nonblockingResumeLevel ||
                             EstTime.currentTimeMillis() >= m_resumeWaitTimeout)) {
            m_needResume = false;
            m_resumeWaitTimeout = 0;
            return true;
        }
        return false;
    }

    /**
     * We're about to send a request; this method handles rate-limiting.
     * Two major modes of operation are possible:
     *
     * 1. Normal rate-limiting mode ('doesRateLimiting' is true)
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
     *
     * @param startNanos The time as measured when the call is made.
     * @param timeoutNanos Limit on time waiting for permission to send
     * @param ignoreBackpressure If true, send permission immediately granted
     */
    void prepareToSendTransaction(long startNanos, long timeoutNanos, boolean ignoreBackpressure)
        throws TimeoutException, InterruptedException {

        // Rate-limiting mode
        if (m_doesRateLimiting) {
            long timestampNanos = startNanos; // updated if we block
            while (!rateWithinLimit(timestampNanos, ignoreBackpressure)) {

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
            }
        }
    }

    /**
     * We're about to send a request for a non-blocking client;
     * this method handles rate-limiting. Unlike the blocking case,
     * this only supports a limit based on an outstanding transaction
     * count. We try to acquire the semaphore, but never wait.
     *
     * @return true for permission to send, false for not
     */
    boolean prepareToSendTransactionNonblocking() {

        // Rate-limiting mode not supported. The problem lies in
        // having an efficient mechanism to determine when to resume.
        if (m_doesRateLimiting) {
            throw new IllegalStateException("Nonblocking not available with rate-limiting");
        }

        // Outstanding transaction count mode
        boolean acquired = m_outstandingTxnsSemaphore.tryAcquire();
        synchronized (this) {
            if (!acquired) {
                if (!m_needResume) {
                    m_needResume = true;
                    m_resumeWaitTimeout = EstTime.currentTimeMillis() +
                                         (m_maxOutstandingTxns * RESUME_TIMEOUT_FACTOR);
                }
                return false;
            }
            ++m_nonblockingOutCount;
        }

        // Can send now
        return true;
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

        // Check current rate, but skip calculations if we're going to
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
        System.out.printf("Target txns/sec is %d, max outstanding txns is %d, current outstanding is %d\n",
                          m_targetTxnsPerSecond, m_maxOutstandingTxns, m_outstandingTxns);
    }
}
