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
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.iv2.PriorityPolicy;
import org.voltdb.iv2.SiteTasker;

/**
 * A class to track and generate statistics regarding queue depth.
 * Generate information on instantaneous queue depth and number of tasks
 * pulled from queue, average wait time and max wait time (in microseconds)
 * within a 5-second window.
 *
 * The statistics are accessible via two selectors, QUEUE for
 * stats without regard to task priority, and QUEUEPRIORITY,
 * for stats broken down by priority.
 *
 * 'Public' for the VoltDB prometheus agent.
 */
public class QueueDepthTracker {

    private static final VoltLogger s_logger = new VoltLogger("HOST");

    private static final long s_maxWaitTimeWindowSize = 5_000_000_000L; // window size set to 5 seconds
    private static final long s_recentWindowSize = s_maxWaitTimeWindowSize / 10; // recent window size set to 0.5 second

    private final long m_siteId;
    private final StatsSet m_aggregateStats;
    private final StatsSet[] m_perPrioStats;
    private final boolean m_prioSupport;

    /*
     * Single set of stats; we keep one set for the entire queue
     * of tasks, and optionally one per priority level.
     */
    private static class StatsSet {
        private final int m_priority; // -1 for aggregate
        private final AtomicInteger m_depth;
        private long m_lastWaitTime; // duration
        private final ArrayBlockingQueue<QueueStatus> m_historicalData;
        private BlockingQueue<SiteTasker> m_tasks;
        private long m_maxWaitLastLogTime; // instant
        private volatile long m_recentMaxWaitTime; // duration
        private long m_recentTotalWaitTime;
        private long m_recentPollCount;
        private volatile boolean m_active; // set if this level ever used

        StatsSet(long startTime, int priority, BlockingQueue<SiteTasker> tasks) {
            m_priority = priority;
            assert tasks.size() == 0 : "queue not empty";
            m_depth = new AtomicInteger(0);
            m_historicalData = new ArrayBlockingQueue<>(10);
            m_tasks = tasks;
            m_maxWaitLastLogTime = startTime;
        }

        boolean isActive() {
            return m_active;
        }

        void offerUpdate() {
            if (m_depth.getAndIncrement() == 0) {
                m_active = true;
            }
        }

        void pollUpdate(long currentTime, long offerTime) {
            m_depth.decrementAndGet();
            m_lastWaitTime = currentTime - offerTime;

            // if max wait time was last logged in current time window
            // then update current window stats
            if (currentTime - m_maxWaitLastLogTime < s_recentWindowSize) {
                if (m_recentMaxWaitTime < m_lastWaitTime) {
                    m_recentMaxWaitTime = m_lastWaitTime;
                }
                m_recentTotalWaitTime += m_lastWaitTime;
                m_recentPollCount++;
            }

            // otherwise remove out of date historical data
            // and start new window of time
            else {
                while (!m_historicalData.isEmpty() &&
                       m_historicalData.peek().timestamp < currentTime - s_maxWaitTimeWindowSize) {
                    m_historicalData.poll();
                }
                if (!m_historicalData.offer(new QueueStatus(currentTime,
                                                            m_recentMaxWaitTime,
                                                            m_recentTotalWaitTime,
                                                            m_recentPollCount))) {
                    //This should never happen...
                    s_logger.warn("Could not insert queue stats data. Current data size: " + m_historicalData.size());
                }
                m_recentMaxWaitTime = m_lastWaitTime;
                m_recentTotalWaitTime = m_lastWaitTime;
                m_recentPollCount = 1;
                m_maxWaitLastLogTime = currentTime;
            }
        }

        void getStatsRow(long currentTime, int offset, Object[] rowValues) {
            // check if current wait time exceeds the maxWaitTime
            // but only for the aggregate queue (don't know next task
            // for our particular priority, so we'll just ignore the
            // possibility that the current head of queue has been waiting
            // for a longer time than any completed task)
            long currentWaitTime = 0;
            if (m_priority < 0) {
                SiteTasker nextTask = m_tasks.peek();
                if (nextTask != null) {
                    currentWaitTime = currentTime - nextTask.getQueueOfferTime();
                }
            }
            long maxWaitTimeInWindow = Math.max(currentWaitTime, m_recentMaxWaitTime);

            // check historicalMaxWaitTime, report max wait time and mean wait time in window
            long totalWaitTimeInWindow = 0;
            long totalPollCountInWindow = 0;
            if (!m_historicalData.isEmpty()) {
                // iterate through all past max wait times
                // only process those within the window
                for (QueueStatus status : m_historicalData) {
                    if (status.timestamp >= currentTime - s_maxWaitTimeWindowSize) {
                        maxWaitTimeInWindow = Math.max(maxWaitTimeInWindow, status.maxWait);
                        totalWaitTimeInWindow += status.totalWait;
                        totalPollCountInWindow += status.pollCount;
                    }
                }
            }

            // fields common to Queue and QueuePriority are assumed to have the
            // same order relative to the supplied offset
            rowValues[offset + Queue.CURRENT_DEPTH.ordinal()] = m_depth;
            rowValues[offset + Queue.POLL_COUNT.ordinal()] = totalPollCountInWindow;
            // wait times are in microseconds
            rowValues[offset + Queue.AVG_WAIT.ordinal()] = (totalWaitTimeInWindow / Math.max(1, totalPollCountInWindow)) / 1000;
            rowValues[offset + Queue.MAX_WAIT.ordinal()] = maxWaitTimeInWindow / 1000;
       }
    }

    /*
     * Rows for QUEUE and PRIORITYQUEUE stats. The former
     * is a subset of the latter, and any additions for
     * priorities must be before the common columns.
     * Common columns must be in the same order in both.
     * getStatsRow() assumes these requirements.
     *
     * 'Public' for the VoltDB prometheus agent.
     */
    public enum Queue {
        CURRENT_DEPTH           (VoltType.INTEGER),
        POLL_COUNT              (VoltType.BIGINT),
        AVG_WAIT                (VoltType.BIGINT),
        MAX_WAIT                (VoltType.BIGINT);
        public final VoltType m_type;
        Queue(VoltType type) { m_type = type; }
    }

    public enum QueuePriority {
        PRIORITY                (VoltType.SMALLINT),
        CURRENT_DEPTH           (VoltType.INTEGER),
        POLL_COUNT              (VoltType.BIGINT),
        AVG_WAIT                (VoltType.BIGINT),
        MAX_WAIT                (VoltType.BIGINT);
        public final VoltType m_type;
        QueuePriority(VoltType type) { m_type = type; }
    }

    /*
     * Single queue data entry in historical data
     */
    private static class QueueStatus {
        long timestamp;
        long maxWait;
        long totalWait;
        long pollCount;

        QueueStatus(long timestamp, long max, long total, long count) {
            this.timestamp = timestamp;
            this.maxWait = max;
            this.totalWait = total;
            this.pollCount = count;
        }
    }

    /*
     * Implementation
     */
    public QueueDepthTracker(long siteId, BlockingQueue<SiteTasker> tasks) {
        m_siteId = siteId;
        long currentTime = System.nanoTime();
        m_aggregateStats = new StatsSet(currentTime, -1, tasks);
        m_prioSupport = PriorityPolicy.isEnabled();
        if (m_prioSupport) {
            m_perPrioStats = new StatsSet[PriorityPolicy.getLowestPriority()+1];
            for (int i=0; i<m_perPrioStats.length; i++) {
                m_perPrioStats[i] = new StatsSet(currentTime, i, tasks);
            }
        }
        else {
            m_perPrioStats = null;
        }
    }

    public void offerUpdate(int priority) {
        m_aggregateStats.offerUpdate();
        if (m_prioSupport) {
            m_perPrioStats[priority].offerUpdate();
        }
    }

    public void pollUpdate(long offerTime, int priority) {
        long currentTime = System.nanoTime();
        m_aggregateStats.pollUpdate(currentTime, offerTime);
        if (m_prioSupport) {
            m_perPrioStats[priority].pollUpdate(currentTime, offerTime);
        }
    }

    public SiteStatsSource newQueueStats() {
        return new QueueStats(this, m_siteId);
    }

    public SiteStatsSource newQueuePriorityStats() {
        return new QueuePriorityStats(this, m_siteId);
    }

    void getStatsRow(int priority, int offset, Object[] rowValues) {
        long currentTime = System.nanoTime();
        if (priority < 0) {
            m_aggregateStats.getStatsRow(currentTime, offset, rowValues);
        }
        else {
            final int prioOrd = QueuePriority.PRIORITY.ordinal();
            assert prioOrd == 0;
            rowValues[offset + prioOrd] = priority;
            offset += prioOrd + 1;
            m_perPrioStats[priority].getStatsRow(currentTime, offset, rowValues);
        }
    }

    Iterator<Object> getStatsIterator(boolean wantPrio) {
        return new Iterator<Object>() {
            // range [-1,0) for aggregate, [0,len) for prio stats, empty for no prio support
            final int end = wantPrio & m_prioSupport ? m_perPrioStats.length : 0;
            int next = wantPrio ? 0 : -1;
            @Override
            public boolean hasNext() {
                while (next >= 0 && next < end && !m_perPrioStats[next].isActive()) {
                    next++; // skip never-used priority entries
                }
                return next < end;
            }
            @Override
            public Object next() {
                if (next >= end) {
                    throw new java.util.NoSuchElementException("no next stats");
                }
                return next++;
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}

/*
 * Aggregate stats, most meaningful in non-priority
 * queue disciplines.
 */
class QueueStats extends SiteStatsSource {

    private final QueueDepthTracker m_qdt;

    public QueueStats(QueueDepthTracker qdt, long siteId) {
        super(siteId, false);
        m_qdt = qdt;
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, QueueDepthTracker.Queue.class);
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        m_qdt.getStatsRow((Integer)rowKey, offset, rowValues);
        return offset + QueueDepthTracker.Queue.values().length;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return m_qdt.getStatsIterator(false);
    }
}

/*
 * Per-priority stats. Empty if priorities are not enabled
 * on this server (iterator will return empty collection).
 */
class QueuePriorityStats extends SiteStatsSource {

    private final QueueDepthTracker m_qdt;

    public QueuePriorityStats(QueueDepthTracker qdt, long siteId) {
        super(siteId, false);
        m_qdt = qdt;
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, QueueDepthTracker.QueuePriority.class);
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        m_qdt.getStatsRow((Integer)rowKey, offset, rowValues);
        return offset + QueueDepthTracker.QueuePriority.values().length;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return m_qdt.getStatsIterator(true);
    }
}
