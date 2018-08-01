/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.InternalConnectionStatsCollector;
import org.voltdb.SiteStatsSource;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Maintains success, failure, pending and other relevant counts per importer.
 */

public class ImporterStatsCollector extends SiteStatsSource
    implements InternalConnectionStatsCollector {

    public static final String IMPORTER_NAME_COL = "IMPORTER_NAME";
    public static final String PROC_NAME_COL = "PROCEDURE_NAME";
    public static final String SUCCESS_COUNT_COL = "SUCCESSES";
    public static final String FAILURE_COUNT_COL = "FAILURES";
    public static final String PENDING_COUNT_COL = "OUTSTANDING_REQUESTS";
    public static final String RETRY_COUNT_COL = "RETRIES";

    // Holds stats info for each known importer-procname combination.
    // Using AtomicReferences with ImmutableMap to avoid locking and faster access
    private AtomicReference<ImmutableMap<String, AtomicReference<ImmutableMap<String, StatsInfo>>>> m_importerStats = new AtomicReference<>();
    private boolean m_isInterval;

    public ImporterStatsCollector(long siteId)
    {
        super(siteId, false);
    }

    @Override
    public void reportCompletion(String importerName, String procName, ClientResponse response) {
            switch(response.getStatus()) {
            case ClientResponse.RESPONSE_UNKNOWN :
                reportRetry(importerName, procName);
                break;
            case ClientResponse.SUCCESS:
                reportSuccess(importerName, procName);
                break;
            default:
                reportFailure(importerName, procName);
                break;
            }
    }

    // An insert request was queued
    public void reportQueued(String importerName, String procName) {
        StatsInfo statsInfo = getStatsInfo(importerName, procName);
        statsInfo.m_pendingCount.incrementAndGet();
    }

    // One insert failed
    private void reportFailure(String importerName, String procName) {
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

    // Report that the importer was successfully initialized
    public void reportInitialized(String importerName, String procName) {
        getStatsInfo(importerName, procName);
    }

    // One insert succeeded
    private void reportSuccess(String importerName, String procName) {
        StatsInfo statsInfo = getStatsInfo(importerName, procName);
        statsInfo.m_pendingCount.decrementAndGet();
        statsInfo.m_successCount.incrementAndGet();
    }

    // One insert was retried
    private void reportRetry(String importerName, String procName) {
        StatsInfo statsInfo = getStatsInfo(importerName, procName);
        statsInfo.m_retryCount.incrementAndGet();
    }

    private StatsInfo getStatsInfo(String importerName, String procName) {
        ImmutableMap<String, AtomicReference<ImmutableMap<String, StatsInfo>>> existingMap;
        ImmutableMap<String, AtomicReference<ImmutableMap<String, StatsInfo>>> newMap;
        do {
            existingMap = m_importerStats.get();
            if (existingMap != null && existingMap.containsKey(importerName)) {
                break;
            }
            if (existingMap == null) {
                newMap = ImmutableMap.of(importerName, new AtomicReference<ImmutableMap<String, StatsInfo>>());
            } else {
                newMap = ImmutableMap.<String, AtomicReference<ImmutableMap<String, StatsInfo>>> builder()
                .putAll(existingMap)
                .put(importerName, new AtomicReference<ImmutableMap<String, StatsInfo>>())
                .build();
            }
        }
        while(!m_importerStats.compareAndSet(existingMap, newMap));

        AtomicReference<ImmutableMap<String, StatsInfo>> existingProcMapRef = m_importerStats.get().get(importerName);
        ImmutableMap<String, StatsInfo> existingProcMap;
        ImmutableMap<String, StatsInfo> newProcMap;
        do {
            existingProcMap = existingProcMapRef.get();
            if (existingProcMap != null && existingProcMap.containsKey(procName)) {
                break;
            }
            StatsInfo newStatValue = new StatsInfo(importerName, procName);
            if (existingProcMap == null) {
                newProcMap = ImmutableMap.of(procName, newStatValue);
            } else {
                newProcMap = ImmutableMap.<String, StatsInfo> builder()
                .putAll(existingProcMap)
                .put(procName, newStatValue)
                .build();
            }
        } while(!existingProcMapRef.compareAndSet(existingProcMap, newProcMap));

        return existingProcMapRef.get().get(procName);
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
        return new StatsInfoIterator();
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

    private class StatsInfoIterator implements Iterator<Object> {
        private Iterator<AtomicReference<ImmutableMap<String, StatsInfo>>> m_outerItr;
        private Iterator<StatsInfo> m_innerItr;

        public StatsInfoIterator() {
            ImmutableMap<String, AtomicReference<ImmutableMap<String, StatsInfo>>> importerMap = m_importerStats.get();
            m_outerItr = (importerMap == null) ? null : importerMap.values().iterator();
        }

        @Override
        public boolean hasNext() {
            if (m_outerItr == null) { // no stats yet
                return false;
            }

            if (m_innerItr == null || !m_innerItr.hasNext()) {
                if (!m_outerItr.hasNext()) {
                    return false;
                } else {
                    ImmutableMap<String, StatsInfo> innerMap = m_outerItr.next().get();
                    // Referenced inner map may be null
                    while (innerMap==null && m_outerItr.hasNext()) {
                        innerMap = m_outerItr.next().get();
                    }

                    if (innerMap==null) {
                        return false;
                    } else {
                        m_innerItr = innerMap.values().iterator(); // we never put empty map
                    }
                }
            }

            return m_innerItr.hasNext();
        }

        @Override
        public Object next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more importer stats elements");
            }

            // hasNext call above sets everything up
            return m_innerItr.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove operation is not supported for ImporterStats iterator implementation");
        }
    }
}
