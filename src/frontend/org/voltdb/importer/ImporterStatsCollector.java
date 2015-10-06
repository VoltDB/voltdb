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

package org.voltdb.importer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.SiteStatsSource;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/**
 * Maintains success, failure, pending and other relevant counts per importer.
 */

public class ImporterStatsCollector extends SiteStatsSource {

    public static final String IMPORTER_NAME_COL = "IMPORTER_NAME";
    public static final String PROC_NAME_COL = "PROCEDURE_NAME";
    public static final String SUCCESS_COUNT_COL = "SUCCESSES";
    public static final String FAILURE_COUNT_COL = "FAILURES";
    public static final String PENDING_COUNT_COL = "OUTSTANDING_REQUESTS";
    public static final String RETRY_COUNT_COL = "RETRIES";

    // Holds stats info for each known importer-procname combination
    private ConcurrentMap<String, ConcurrentMap<String, StatsInfo>> m_importerStats = new ConcurrentHashMap<>();
    private boolean m_isInterval;

    public ImporterStatsCollector(long siteId)
    {
        super(siteId, false);
    }

    // An insert request was queued
    public void reportQueued(String importerName, String procName) {
        StatsInfo statsInfo = getStatsInfo(importerName, procName);
        statsInfo.m_pendingCount.incrementAndGet();
    }

    // One insert failed
    public void reportFailure(String importerName, String procName) {
        reportFailure(importerName, procName, true);
    }

    // Use this when the insert fails even before the request is queued by the InternalConnectionHandler
    public void reportFailure(String importerName, String procName, boolean decrementPending) {
        StatsInfo statsInfo = getStatsInfo(importerName, procName);
        if (decrementPending) {
            statsInfo.m_pendingCount.decrementAndGet();
        }
        statsInfo.m_failureCount.incrementAndGet();
    }

    // One insert succeeded
    public void reportSuccess(String importerName, String procName) {
        StatsInfo statsInfo = getStatsInfo(importerName, procName);
        statsInfo.m_pendingCount.decrementAndGet();
        statsInfo.m_successCount.incrementAndGet();
    }

    // One insert was retried
    public void reportRetry(String importerName, String procName) {
        StatsInfo statsInfo = getStatsInfo(importerName, procName);
        statsInfo.m_retryCount.incrementAndGet();
    }

    private StatsInfo getStatsInfo(String importerName, String procName) {
        ConcurrentMap<String, StatsInfo> statsByProc = m_importerStats.get(importerName);
        if (statsByProc==null) {
            statsByProc = new ConcurrentHashMap<String, StatsInfo>();
            ConcurrentMap<String, StatsInfo> existing = m_importerStats.putIfAbsent(importerName, statsByProc);
            if (existing!=null) {
                statsByProc = existing;
            }
        }
        StatsInfo statsInfo = statsByProc.get(procName);
        if (statsInfo==null) {
            StatsInfo newValue = new StatsInfo(importerName, procName);
            StatsInfo existing = statsByProc.putIfAbsent(procName, newValue);
            if (existing!=null) {
                statsInfo = existing;
            } else {
                statsInfo = newValue;
            }
        }

        return statsInfo;
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        StatsInfo stats = (StatsInfo) rowKey;
        rowValues[columnNameToIndex.get(IMPORTER_NAME_COL)] = stats.m_importerName;
        rowValues[columnNameToIndex.get(PROC_NAME_COL)] = stats.m_procName;
        rowValues[columnNameToIndex.get(SUCCESS_COUNT_COL)] = getSuccessCountUpdateLast(stats);
        rowValues[columnNameToIndex.get(FAILURE_COUNT_COL)] = getFailureCountUpdateLast(stats);
        rowValues[columnNameToIndex.get(PENDING_COUNT_COL)] = getPendingCountUpdateLast(stats);
        rowValues[columnNameToIndex.get(RETRY_COUNT_COL)] = getRetryCountUpdateLast(stats);

        super.updateStatsRow(rowKey, rowValues);
    }

    private long getSuccessCountUpdateLast(StatsInfo stats) {
        long currentSuccess = stats.m_successCount.get();
        long successValue = currentSuccess;
        if (m_isInterval) {
            successValue = currentSuccess - stats.m_lastSuccessCount;
            stats.m_lastSuccessCount = currentSuccess;
        }

        return successValue;
    }

    private long getFailureCountUpdateLast(StatsInfo stats) {
        long current = stats.m_failureCount.get();
        long value = current;
        if (m_isInterval) {
            value = current - stats.m_lastFailureCount;
            stats.m_lastFailureCount = current;
        }

        return value;
    }

    private long getRetryCountUpdateLast(StatsInfo stats) {
        long current = stats.m_retryCount.get();
        long value = current;
        if (m_isInterval) {
            value = current - stats.m_lastRetryCount;
            stats.m_lastRetryCount = current;
        }

        return value;
    }

    private long getPendingCountUpdateLast(StatsInfo stats) {
        long current = stats.m_pendingCount.get();
        current = (current<0) ? 0 : current; // pending could be -ve if we get callback responses
                                             // before callProcedure returns to ImportHandlerProxy
        long value = current;
        if (m_isInterval) {
            value = current - stats.m_lastPendingCount;
            stats.m_lastPendingCount = current;
        }

        return value;
    }


    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        m_isInterval = interval;
        return new Iterator<Object>() {
            private Iterator<Map.Entry<String, ConcurrentMap<String, StatsInfo>>> m_outerItr = m_importerStats.entrySet().iterator();
            private Iterator<Map.Entry<String, StatsInfo>> m_innerItr;

            @Override
            public boolean hasNext() {
                if (m_innerItr == null || !m_innerItr.hasNext()) {
                    if (!m_outerItr.hasNext()) {
                        return false;
                    } else {
                        m_innerItr = m_outerItr.next().getValue().entrySet().iterator();
                    }
                }

                return m_innerItr.hasNext();
            }

            @Override
            public Object next() {
                // If next is called when there are no more next elements,
                // this will throw error, which is the expected correct behaviour.

                if (m_innerItr == null || !m_innerItr.hasNext()) {
                    m_innerItr = m_outerItr.next().getValue().entrySet().iterator();
                }

                return m_innerItr.next().getValue();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove operation is not supported for ImporterStats iterator implementation");
            }
        };
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo(IMPORTER_NAME_COL, VoltType.STRING));
        columns.add(new ColumnInfo(PROC_NAME_COL, VoltType.STRING));
        columns.add(new ColumnInfo(SUCCESS_COUNT_COL, VoltType.BIGINT));
        columns.add(new ColumnInfo(FAILURE_COUNT_COL, VoltType.BIGINT));
        columns.add(new ColumnInfo(PENDING_COUNT_COL, VoltType.BIGINT));
        columns.add(new ColumnInfo(RETRY_COUNT_COL, VoltType.BIGINT));
    }

    private class StatsInfo
    {
        String m_importerName;
        String m_procName;
        AtomicLong m_successCount = new AtomicLong(0);
        AtomicLong m_failureCount = new AtomicLong(0);
        AtomicLong m_pendingCount = new AtomicLong(0);
        AtomicLong m_retryCount = new AtomicLong(0);
        long m_lastSuccessCount = 0;
        long m_lastFailureCount = 0;
        long m_lastPendingCount = 0;
        long m_lastRetryCount = 0;

        public StatsInfo(String importerName, String procName) {
            m_importerName = importerName;
            m_procName = procName;
        }

        @Override
        public String toString() {
            return "StatsInfo(" + m_importerName + "." + m_procName + ")";
        }
    }
}
