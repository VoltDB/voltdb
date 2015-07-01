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

package org.voltdb.client;

import java.util.ArrayDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google_voltpatches.common.base.Throwables;

/**
 * Provide the {@link Client} with a way to throttle throughput in one
 * of several ways. First, it can cap outstanding transactions or
 * limit the rate of new transactions. Second, it can auto-tune the
 * send rate to get a good balance of througput and latency on the
 * server.
 *
 */
class RateLimiter {

    final int BLOCK_SIZE = 100; // ms
    final int HISTORY_SIZE = 25;
    final int RECENT_HISTORY_SIZE = 5;
    final int MINIMUM_MOVEMENT = 5;

    //Boolean indicating whether the only thing being tracked is max outstanding
    protected boolean m_doesAnyTuning = false;
    protected boolean m_autoTune = false;
    protected int m_targetTxnsPerSecond = Integer.MAX_VALUE;
    //protected int m_targetTxnsPerBlock = Integer.MAX_VALUE;
    protected int m_latencyTarget = 5;

    protected int m_currentBlockSendCount = 0;
    protected int m_currentBlockRecvSuccessCount = 0;
    protected int m_outstandingTxns = 0;
    protected Semaphore m_outstandingTxnsSemaphore = new Semaphore(10);

    protected int m_maxOutstandingTxns = 10;

    protected long m_currentBlockTimestamp = -1;

    protected long m_currentBlockTotalInternalLatency = 0;

    protected ArrayDeque<Double> m_prevInternalLatencyAvgs = new ArrayDeque<Double>();

    protected void autoTuneTargetFromHistory() {
        double recentLatency = 0, mediumTermLatency = 0;
        if (m_prevInternalLatencyAvgs.size() > 0) {
            int i = 0;
            for (double value : m_prevInternalLatencyAvgs) {
                if (i < RECENT_HISTORY_SIZE) recentLatency += value;
                mediumTermLatency += value;
                ++i;
            }
            recentLatency /= Math.min(m_prevInternalLatencyAvgs.size(), RECENT_HISTORY_SIZE);
            mediumTermLatency /= m_prevInternalLatencyAvgs.size();
        }

        if ((mediumTermLatency > m_latencyTarget) && (recentLatency > m_latencyTarget)) {
            m_maxOutstandingTxns -= Math.max(0.1 * m_maxOutstandingTxns, MINIMUM_MOVEMENT);
        }
        else if ((mediumTermLatency < m_latencyTarget) && (recentLatency > m_latencyTarget)) {
            --m_maxOutstandingTxns;
        }
        else if ((mediumTermLatency > m_latencyTarget) && (recentLatency < m_latencyTarget)) {
            m_maxOutstandingTxns++;
        }
        else { // if ((mediumTermLatency < m_latencyTarget) && (recentLatency < m_latencyTarget)) {
            m_maxOutstandingTxns += Math.max(0.1 * m_maxOutstandingTxns, MINIMUM_MOVEMENT);
        }

        // don't let this go to 0, latency be damned
        if (m_maxOutstandingTxns <= 0) {
            m_maxOutstandingTxns = 1;
        }
    }

    protected void ensureCurrentBlockIsKosher(long timestamp) {
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
     * May not be reflected until the next 100ms.
     */
    synchronized void setLimits(int txnsPerSec, int maxOutstanding) {
        m_autoTune = false;
        /*
         * If the rate limit is some reasonably low value then go through the effort
         * of rate limiting
         */
        if (txnsPerSec < Integer.MAX_VALUE / 2) {
            m_doesAnyTuning = true;
        }
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
     *
     * @param timestampNanos The time as measured when the call is made.
     * @param internalLatency Latency measurement of this transaction in millis
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
        } else {
            if (ignoreBackpressure) return;
            m_outstandingTxnsSemaphore.release();
        }
    }

    /**
     *
     *
     * @param timestamp The time as measured when the call is made.
     * @param ignoreBackpressure If true, never block.
     * @return The time as measured when the call returns.
     */
    long sendTxnWithOptionalBlockAndReturnCurrentTime(long timestampNanos, long timeoutNanos, boolean ignoreBackpressure) throws TimeoutException {
        if (m_doesAnyTuning) {
            long timestamp = TimeUnit.NANOSECONDS.toMillis(timestampNanos);
            while (true) {
                synchronized(this) {
                    // switch to a new block if 100ms has passed
                    // possibly compute a new target rate
                    ensureCurrentBlockIsKosher(timestamp);

                    assert((timestamp - m_currentBlockTimestamp) <= BLOCK_SIZE);

                    // don't let the time be before the start of the current block
                    // also ensure faketime - m_currentBlockTimestamp is positive
                    long faketime = timestamp < m_currentBlockTimestamp ? m_currentBlockTimestamp : timestamp;

                    long targetTxnsPerBlock = m_targetTxnsPerSecond / (1000 / BLOCK_SIZE);

                    // compute the percentage of the current 100ms block that has passed
                    double expectedTxnsSent =
                            targetTxnsPerBlock * (faketime - m_currentBlockTimestamp + 1.0) / BLOCK_SIZE;
                    expectedTxnsSent = Math.ceil(expectedTxnsSent);

                    assert(expectedTxnsSent <= targetTxnsPerBlock); // stupid fp math
                    assert((expectedTxnsSent >= 1.0) || (targetTxnsPerBlock == 0));

                    // if the rate is under target, no problems
                    if (((m_currentBlockSendCount < expectedTxnsSent) &&
                         (m_outstandingTxns < m_maxOutstandingTxns)) ||
                        (ignoreBackpressure == true)) {

                        // bookkeeping
                        ++m_currentBlockSendCount;
                        ++m_outstandingTxns;

                        // exit the while loop
                        break;
                    }
                }

                // if the rate is above target, pause for the smallest time possible
                try { Thread.sleep(1); } catch (InterruptedException e) {}
                timestampNanos = System.nanoTime();
                timestamp = TimeUnit.NANOSECONDS.toMillis(timestampNanos);
            }
        } else {
            if (ignoreBackpressure) return timestampNanos;
            boolean acquired = m_outstandingTxnsSemaphore.tryAcquire();

            if (!acquired) {
                try {
                    if (!m_outstandingTxnsSemaphore.tryAcquire(timeoutNanos, TimeUnit.NANOSECONDS)) {
                        throw new TimeoutException();
                    }
                } catch (InterruptedException e) {
                    Throwables.propagate(e);
                }
                return System.nanoTime();
            }
        }

        // this time may have changed if this call blocked during a sleep
        return timestampNanos;
    }

    public synchronized void debug() {
        System.out.printf("Target throughput/s is %d and max outstanding txns is %d\n",
                m_targetTxnsPerSecond, m_maxOutstandingTxns);
        System.out.printf("Current outstanding is %d and recent internal latency is %.2f\n",
                m_outstandingTxns, m_prevInternalLatencyAvgs.peekFirst());
    }
}
