/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.iv2.SiteTasker;

/**
 * A class to track and generate statistics regarding queue depth.
 * Generate information on instantaneous queue depth and number of tasks
 * pulled from queue, average wait time and max wait time (in microseconds)
 * within a 5-second window
 */
public class QueueDepthTracker extends SiteStatsSource {

    private static final VoltLogger s_logger = new VoltLogger("HOST");

    private final AtomicInteger m_depth;
    private long m_lastWaitTime;
    private final ArrayBlockingQueue<QueueStatus> m_historicalData;
    private LinkedTransferQueue<SiteTasker> m_tasks;
    private long m_maxWaitTimeWindowSize = 5_000_000_000L; // window size set to 5 seconds
    private long m_maxWaitLastLogTime;
    private volatile long m_recentMaxWaitTime;
    private long m_recentTotalWaitTime;
    private long m_recentPollCount;
    private long m_recentWindowSize = m_maxWaitTimeWindowSize / 10; // recent window size set to 0.5 second

    public class QueueStatus {
        public long timestamp;
        public long maxWait;
        public long totalWait;
        public long pollCount;

        public QueueStatus(long timestamp, long max, long total, long count) {
            this.timestamp = timestamp;
            this.maxWait = max;
            this.totalWait = total;
            this.pollCount = count;
        }
    }

    public QueueDepthTracker(long siteId, LinkedTransferQueue<SiteTasker> tasks) {
        super(siteId, false);
        m_historicalData = new ArrayBlockingQueue<>(10);
        m_depth = new AtomicInteger(tasks.size());
        m_lastWaitTime = 0;
        m_maxWaitLastLogTime = System.nanoTime();
        m_recentMaxWaitTime = 0;
        m_recentTotalWaitTime = 0;
        m_recentPollCount = 0;
        m_tasks = tasks;
    }

    public void offerUpdate() {
        m_depth.incrementAndGet();
    }

    public void pollUpdate(long offerTime) {
        m_depth.decrementAndGet();
        long currentTime = System.nanoTime();
        m_lastWaitTime = currentTime - offerTime;
        // if max wait time was last logged less than m_recentWindowSize ago
        // keep the max wait time in m_recentMaxWaitTime
        // or log and reset the recentMaxWaitTime, update last log time
        if (currentTime - m_maxWaitLastLogTime < m_recentWindowSize) {
            if (m_recentMaxWaitTime < m_lastWaitTime) m_recentMaxWaitTime = m_lastWaitTime;
            m_recentTotalWaitTime += m_lastWaitTime;
            m_recentPollCount++;
        } else {
            // remove out of date historical data
            while (!m_historicalData.isEmpty() &&
                    m_historicalData.peek().timestamp <
                    currentTime - m_maxWaitTimeWindowSize) {
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

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("CURRENT_DEPTH", VoltType.INTEGER));
        columns.add(new ColumnInfo("POLL_COUNT", VoltType.BIGINT));
        columns.add(new ColumnInfo("AVG_WAIT", VoltType.BIGINT));
        columns.add(new ColumnInfo("MAX_WAIT", VoltType.BIGINT));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        long currentTime = System.nanoTime();
        // check if current wait time exceeds the maxWaitTime
        long currentWaitTime;
        SiteTasker nextTask = m_tasks.peek();
        if (nextTask == null) {
            currentWaitTime = 0;
        } else {
            currentWaitTime = currentTime - nextTask.getQueueOfferTime();
        }
        // check historicalMaxWaitTime, report max wait time and mean wait time in window
        long maxWaitTimeInWindow = Math.max(currentWaitTime, m_recentMaxWaitTime);
        long totalWaitTimeInWindow = 0;
        long totalPollCountInWindow = 0;
        if (!m_historicalData.isEmpty()) {
            // iterate through all past max wait times
            // only process those within the window
            for (QueueStatus status : m_historicalData) {
                if (status.timestamp >= currentTime - m_maxWaitTimeWindowSize) {
                    maxWaitTimeInWindow = Math.max(maxWaitTimeInWindow, status.maxWait);
                    totalWaitTimeInWindow += status.totalWait;
                    totalPollCountInWindow += status.pollCount;
                }
            }
        }
        rowValues[columnNameToIndex.get("CURRENT_DEPTH")] = m_depth;
        rowValues[columnNameToIndex.get("POLL_COUNT")] = totalPollCountInWindow;
        // wait times are in microseconds
        rowValues[columnNameToIndex.get("AVG_WAIT")] = (totalWaitTimeInWindow / Math.max(1, totalPollCountInWindow)) / 1000;
        rowValues[columnNameToIndex.get("MAX_WAIT")] = maxWaitTimeInWindow / 1000;

        super.updateStatsRow(rowKey, rowValues);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(final boolean interval) {
        return new Iterator<Object>() {
            boolean returnRow = true;
            @Override
            public boolean hasNext() {
                return returnRow;
            }

            @Override
            public Object next() {
                if (returnRow) {
                    returnRow = false;
                    return new Object();
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

}
