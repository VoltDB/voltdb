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

package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable.ColumnInfo;

/**
 * Collects global cache use stats
 */
public class PlannerStatsCollector extends StatsSource {

    private static final VoltLogger log = new VoltLogger("HOST");

    /**
     * Whether to return results in intervals since polling or since the beginning
     */
    private boolean m_interval = false;

    /**
     * Record timings every N invocations
     */
    final long m_collectionFrequency = 20;

    /**
     * Flag indicating cache disposition of a planned statement.
     */
    public enum CacheUse {
        /// Plan came from cache #1.
        HIT1,
        /// Plan came from cache #2.
        HIT2,
        /// Plan not found in either cache.
        MISS,
        /// An unexpected failure interrupted cache lookup or planning.
        FAIL
    }

    /**
     * Site ID
     */
    final long m_siteId;

    /**
     * Partition ID
     */
    long m_partitionId;

    /**
     * Cache 1 level
     */
    long m_cache1Level = 0;
    long m_lastCache1Level = 0;

    /**
     * Cache 2 level
     */
    long m_cache2Level = 0;
    long m_lastCache2Level = 0;

    /**
     * Cache 1 hits
     */
    long m_cache1Hits = 0;
    long m_lastCache1Hits = 0;

    /**
     * Cache 2 hits
     */
    long m_cache2Hits = 0;
    long m_lastCache2Hits = 0;

    /**
     * Cache misses
     */
    long m_cacheMisses = 0;
    long m_lastCacheMisses = 0;

    /**
     * Time of last planning start
     */
    Long m_currentStartTime = null;

    /**
     * Total amount of planning time
     */
    long m_totalPlanningTime = 0;
    long m_lastTimedPlanningTime = 0;

    /**
     * Shortest amount of time used for planning
     */
    long m_minPlanningTime = Long.MAX_VALUE;
    long m_lastMinPlanningTime = Long.MAX_VALUE;

    /**
     * Longest amount of time used for planning
     */
    long m_maxPlanningTime = Long.MIN_VALUE;
    long m_lastMaxPlanningTime = Long.MIN_VALUE;

    /**
     * Count of the number of errors that occured during procedure execution
     */
    long m_failures = 0;
    long m_lastFailures = 0;


    /**
     * Count of the number of invocations = m_cache1Hits + m_cache2Hits + m_cacheMisses + m_failures;
     */
    long m_invocations = 0;
    long m_lastInvocations = 0;

    /**
     * Calculate the invocation count based on the cache hit/miss counts.
     * @return  invocation count
     */
    long getInvocations() {
        return m_invocations;
    }

    /**
     * Calculate the last invocation count based on the last cache hit/miss counts.
     * @return  last invocation count
     */
    long getLastInvocations() {
        return m_lastInvocations;
    }

    /**
     * Calculate the sample count based on the invocation count and the collection frequency.
     * @return  sample count
     */
    long getSampleCount() {
        return getInvocations() / m_collectionFrequency;
    }

    /**
     * Constructor
     *
     * @param siteId  site id
     */
    public PlannerStatsCollector(long siteId) {
        super(false);
        m_siteId = siteId;
    }

    /**
     * Used to update EE cache stats without changing tracked time
     */
    public void updateEECacheStats(long eeCacheSize, long hits, long misses, int partitionId) {
        m_cache1Level = eeCacheSize;
        m_cache1Hits += hits;
        m_cacheMisses += misses;

        m_invocations += hits + misses;
        m_partitionId = partitionId;
    }

    /**
     * Called before doing planning. Starts timer.
     */
    public void startStatsCollection() {
        if (getInvocations() % m_collectionFrequency == 0) {
            m_currentStartTime = System.nanoTime();
        }
    }

    /**
     * Called after planning or failing to plan. Records timer and cache stats.
     *
     * @param cache1Size   number of entries in level 1 cache
     * @param cache2Size   number of entries in level 2 cache
     * @param cacheUse     where the planned statement came from
     * @param partitionId  partition id
     */
    public void endStatsCollection(long cache1Size, long cache2Size, CacheUse cacheUse, long partitionId) {
        if (m_currentStartTime != null) {
            long delta = System.nanoTime() - m_currentStartTime;
            if (delta < 0) {
                if (Math.abs(delta) > 1000000000) {
                    log.info("Planner statistics recorded a negative planning time larger than one second: " +
                             delta);
                }
            }
            else {
                m_totalPlanningTime += delta;
                m_minPlanningTime = Math.min(delta, m_minPlanningTime);
                m_maxPlanningTime = Math.max(delta, m_maxPlanningTime);
                m_lastMinPlanningTime = Math.min(delta, m_lastMinPlanningTime);
                m_lastMaxPlanningTime = Math.max(delta, m_lastMaxPlanningTime);
            }
            m_currentStartTime = null;
        }

        m_cache1Level = cache1Size;
        m_cache2Level = cache2Size;

        switch(cacheUse) {
          case HIT1:
            m_cache1Hits++;
            break;
          case HIT2:
            m_cache2Hits++;
            break;
          case MISS:
            m_cacheMisses++;
            break;
          case FAIL:
            m_failures++;
            break;
        }
        m_invocations++;

        m_partitionId = partitionId;
    }

    /**
     * Update the rowValues array with the latest statistical information.
     * This method is overrides the super class version
     * which must also be called so that it can update its columns.
     * @param values Values of each column of the row of stats. Used as output.
     */
    @Override
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        super.updateStatsRow(rowKey, rowValues);

        rowValues[columnNameToIndex.get("PARTITION_ID")] = m_partitionId;
        long totalTimedExecutionTime = m_totalPlanningTime;
        long minExecutionTime = m_minPlanningTime;
        long maxExecutionTime = m_maxPlanningTime;
        long cache1Level = m_cache1Level;
        long cache2Level = m_cache2Level;
        long cache1Hits  = m_cache1Hits;
        long cache2Hits  = m_cache2Hits;
        long cacheMisses = m_cacheMisses;
        long failureCount = m_failures;

        if (m_interval) {
            totalTimedExecutionTime = m_totalPlanningTime - m_lastTimedPlanningTime;
            m_lastTimedPlanningTime = m_totalPlanningTime;

            minExecutionTime = m_lastMinPlanningTime;
            maxExecutionTime = m_lastMaxPlanningTime;
            m_lastMinPlanningTime = Long.MAX_VALUE;
            m_lastMaxPlanningTime = Long.MIN_VALUE;

            cache1Level = m_cache1Level - m_lastCache1Level;
            m_lastCache1Level = m_cache1Level;

            cache2Level = m_cache2Level - m_lastCache2Level;
            m_lastCache2Level = m_cache2Level;

            cache1Hits = m_cache1Hits - m_lastCache1Hits;
            m_lastCache1Hits = m_cache1Hits;

            cache2Hits = m_cache2Hits - m_lastCache2Hits;
            m_lastCache2Hits = m_cache2Hits;

            cacheMisses = m_cacheMisses - m_lastCacheMisses;
            m_lastCacheMisses = m_cacheMisses;

            failureCount = m_failures - m_lastFailures;
            m_lastFailures = m_failures;

            m_lastInvocations = m_invocations;
        }

        rowValues[columnNameToIndex.get(VoltSystemProcedure.CNAME_SITE_ID)] = CoreUtils.getSiteIdFromHSId(m_siteId);
        rowValues[columnNameToIndex.get("PARTITION_ID")] = m_partitionId;
        rowValues[columnNameToIndex.get("CACHE1_LEVEL")] = cache1Level;
        rowValues[columnNameToIndex.get("CACHE2_LEVEL")] = cache2Level;
        rowValues[columnNameToIndex.get("CACHE1_HITS" )] = cache1Hits;
        rowValues[columnNameToIndex.get("CACHE2_HITS" )] = cache2Hits;
        rowValues[columnNameToIndex.get("CACHE_MISSES")] = cacheMisses;
        rowValues[columnNameToIndex.get("PLAN_TIME_MIN")] = minExecutionTime;
        rowValues[columnNameToIndex.get("PLAN_TIME_MAX")] = maxExecutionTime;
        if (getSampleCount() != 0) {
            rowValues[columnNameToIndex.get("PLAN_TIME_AVG")] =
                 (totalTimedExecutionTime / getSampleCount());
        } else {
            rowValues[columnNameToIndex.get("PLAN_TIME_AVG")] = 0L;
        }
        rowValues[columnNameToIndex.get("FAILURES")] = failureCount;
    }

    /**
     * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
     * @param columns List of columns that are in a stats row.
     */
    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo(VoltSystemProcedure.CNAME_SITE_ID, VoltSystemProcedure.CTYPE_ID));
        columns.add(new ColumnInfo("PARTITION_ID",  VoltType.INTEGER));
        columns.add(new ColumnInfo("CACHE1_LEVEL",  VoltType.INTEGER));
        columns.add(new ColumnInfo("CACHE2_LEVEL",  VoltType.INTEGER));
        columns.add(new ColumnInfo("CACHE1_HITS",   VoltType.INTEGER));
        columns.add(new ColumnInfo("CACHE2_HITS",   VoltType.INTEGER));
        columns.add(new ColumnInfo("CACHE_MISSES",  VoltType.INTEGER));
        columns.add(new ColumnInfo("PLAN_TIME_MIN", VoltType.BIGINT));
        columns.add(new ColumnInfo("PLAN_TIME_MAX", VoltType.BIGINT));
        columns.add(new ColumnInfo("PLAN_TIME_AVG", VoltType.BIGINT));
        columns.add(new ColumnInfo("FAILURES",      VoltType.BIGINT));
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        m_interval = interval;
        return new Iterator<Object>() {
            boolean givenNext = false;
            @Override
            public boolean hasNext() {
                if (!isInterval()) {
                    if (getInvocations() == 0) {
                        return false;
                    }
                }
                else if (getInvocations() - getLastInvocations() == 0) {
                    return false;
                }
                return !givenNext;
            }

            @Override
            public Object next() {
                if (!givenNext) {
                    givenNext = true;
                    return new Object();
                }
                return null;
            }

            @Override
            public void remove() {}

        };
    }

    /**
     * @return the m_interval
     */
    public boolean isInterval() {
        return m_interval;
    }

    /**
     * @param m_interval the m_interval to set
     */
    public void setInterval(boolean m_interval) {
        this.m_interval = m_interval;
    }
}
