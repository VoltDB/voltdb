/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.voltcore.logging.VoltLogger;
import org.voltdb.catalog.Procedure;

/**
 * Derivation of StatsSource to expose timing information of procedure invocations.
 *
 */
public class ProcedureStatsCollector extends SiteStatsSource {

    private static final VoltLogger log = new VoltLogger("HOST");

    /**
     * Record procedure execution time every N invocations
     */
    final int timeCollectionInterval = 20;

    /**
     * Record statistics for each statement in the stored procedure.
     */
    class ProcedureStmtStat {
        /**
         * The name of the statement.
         * If it's for the statistics of the whole procedure, the name will be <ALL>.
         */
        private String m_stmtName;

        /**
         * Number of times this procedure has been invoked.
         */
        private long m_invocations = 0;
        private long m_lastInvocations = 0;

        /**
         * Number of timed invocations
         */
        private long m_timedInvocations = 0;
        private long m_lastTimedInvocations = 0;

        /**
         * Total amount of timed execution time
         */
        private long m_totalTimedExecutionTime = 0;
        private long m_lastTotalTimedExecutionTime = 0;

        /**
         * Shortest amount of time this procedure has executed in
         */
        private long m_minExecutionTime = Long.MAX_VALUE;
        private long m_lastMinExecutionTime = Long.MAX_VALUE;

        /**
         * Longest amount of time this procedure has executed in
         */
        private long m_maxExecutionTime = Long.MIN_VALUE;
        private long m_lastMaxExecutionTime = Long.MIN_VALUE;

        /**
         * Time the procedure was last started
         */
        private long m_currentStartTime = -1;

        /**
         * Count of the number of aborts (user initiated or DB initiated)
         */
        private long m_abortCount = 0;
        private long m_lastAbortCount = 0;

        /**
         * Count of the number of errors that occured during procedure execution
         */
        private long m_failureCount = 0;
        private long m_lastFailureCount = 0;

        /**
         * Smallest result size
         */
        private int m_minResultSize = Integer.MAX_VALUE;
        private int m_lastMinResultSize = Integer.MAX_VALUE;

        /**
         * Largest result size
         */
        private int m_maxResultSize = Integer.MIN_VALUE;
        private int m_lastMaxResultSize = Integer.MIN_VALUE;

        /**
         * Total result size for calculating averages
         */
        private long m_totalResultSize = 0;
        private long m_lastTotalResultSize = 0;

        /**
         * Smallest parameter set size
         */
        private int m_minParameterSetSize = Integer.MAX_VALUE;
        private int m_lastMinParameterSetSize = Integer.MAX_VALUE;

        /**
         * Largest parameter set size
         */
        private int m_maxParameterSetSize = Integer.MIN_VALUE;
        private int m_lastMaxParameterSetSize = Integer.MIN_VALUE;

        /**
         * Total parameter set size for calculating averages
         */
        private long m_totalParameterSetSize = 0;
        private long m_lastTotalParameterSetSize = 0;

        public ProcedureStmtStat(String stmtName) {
            m_stmtName = stmtName;
        }
    }

    /**
     * Whether to return results in intervals since polling or since the beginning
     */
    private boolean m_interval = false;

    private final String m_procName;
    private final int m_partitionId;

    private Map<String, ProcedureStmtStat> m_stats;
    private ProcedureStmtStat m_procStat;

    /**
     * Constructor requires no args because it has access to the enclosing classes members.
     */
    public ProcedureStatsCollector(long siteId, int partitionId, Procedure catProc,
                                   ArrayList<String> stmtNames) {
        super(siteId, false);
        m_partitionId = partitionId;
        m_procName = catProc.getClassname();
        // Use LinkedHashMap to have a fixed element order.
        m_stats = new LinkedHashMap<String, ProcedureStmtStat>();
        // Use one ProcedureStmtStat instance to hold the procedure-wide statistics.
        m_procStat = new ProcedureStmtStat("<ALL>");
        // The NULL key entry is reserved for the procedure-wide statistics.
        m_stats.put(null, m_procStat);
        // Add statistics for the individual SQL statements.
        if (stmtNames != null) {
            for (String stmtName : stmtNames) {
                m_stats.put(stmtName, new ProcedureStmtStat(stmtName));
            }
        }
    }

    /**
     * Called when a procedure begins executing. Caches the time the procedure starts.
     */
    public final void beginProcedure(boolean isSystemProc) {
        if (m_procStat.m_invocations % timeCollectionInterval == 0 || (isSystemProc && isProcedureUAC())) {
            m_procStat.m_currentStartTime = System.nanoTime();
        }
    }

    public final boolean recording() {
        return m_procStat.m_currentStartTime > 0;
    }

    public final void finishStatement(String stmtName,
                                      boolean granularStatsRequested,
                                      boolean failed,
                                      long duration,
                                      VoltTable result,
                                      ParameterSet parameterSet) {
        ProcedureStmtStat stat = m_stats.get(stmtName);
        if (stat == null) {
            return;
        }
        if (granularStatsRequested) {
            // This is a sampled invocation.
            // Update timings and size statistics.
            if (duration < 0)
            {
                if (Math.abs(duration) > 1000000000)
                {
                    log.info("Statement: " + stat.m_stmtName + " in procedure: " + m_procName +
                             " recorded a negative execution time larger than one second: " +
                             duration);
                }
            }
            else
            {
                stat.m_totalTimedExecutionTime += duration;
                stat.m_timedInvocations++;

                // sampled timings
                stat.m_minExecutionTime = Math.min( duration, stat.m_minExecutionTime);
                stat.m_maxExecutionTime = Math.max( duration, stat.m_maxExecutionTime);
                stat.m_lastMinExecutionTime = Math.min( duration, stat.m_lastMinExecutionTime);
                stat.m_lastMaxExecutionTime = Math.max( duration, stat.m_lastMaxExecutionTime);

                // sampled size statistics
                int resultSize = 0;
                if (result != null) {
                    resultSize = result.getSerializedSize();
                }
                stat.m_totalResultSize += resultSize;
                stat.m_minResultSize = Math.min(resultSize, stat.m_minResultSize);
                stat.m_maxResultSize = Math.max(resultSize, stat.m_maxResultSize);
                stat.m_lastMinResultSize = Math.min(resultSize, stat.m_lastMinResultSize);
                stat.m_lastMaxResultSize = Math.max(resultSize, stat.m_lastMaxResultSize);
                int parameterSetSize = (
                        parameterSet != null ? parameterSet.getSerializedSize() : 0);
                stat.m_totalParameterSetSize += parameterSetSize;
                stat.m_minParameterSetSize = Math.min(parameterSetSize, stat.m_minParameterSetSize);
                stat.m_maxParameterSetSize = Math.max(parameterSetSize, stat.m_maxParameterSetSize);
                stat.m_lastMinParameterSetSize = Math.min(parameterSetSize, stat.m_lastMinParameterSetSize);
                stat.m_lastMaxParameterSetSize = Math.max(parameterSetSize, stat.m_lastMaxParameterSetSize);
            }
        }
        if (failed) {
            stat.m_failureCount++;
        }
        stat.m_invocations++;
    }

    /**
     * Called after a procedure is finished executing. Compares the start and end time and calculates
     * the statistics.
     */
    public final void endProcedure(
            boolean aborted,
            boolean failed,
            VoltTable[] results,
            ParameterSet parameterSet) {
        if (recording()) {
            // This is a sampled invocation.
            // Update timings and size statistics.
            final long endTime = System.nanoTime();
            final long delta = endTime - m_procStat.m_currentStartTime;
            if (delta < 0)
            {
                if (Math.abs(delta) > 1000000000)
                {
                    log.info("Procedure: " + m_procName +
                             " recorded a negative execution time larger than one second: " +
                             delta);
                }
            }
            else
            {
                m_procStat.m_totalTimedExecutionTime += delta;
                m_procStat.m_timedInvocations++;

                // sampled timings
                m_procStat.m_minExecutionTime = Math.min( delta, m_procStat.m_minExecutionTime);
                m_procStat.m_maxExecutionTime = Math.max( delta, m_procStat.m_maxExecutionTime);
                m_procStat.m_lastMinExecutionTime = Math.min( delta, m_procStat.m_lastMinExecutionTime);
                m_procStat.m_lastMaxExecutionTime = Math.max( delta, m_procStat.m_lastMaxExecutionTime);

                // sampled size statistics
                int resultSize = 0;
                if (results != null) {
                    for (VoltTable result : results ) {
                        resultSize += result.getSerializedSize();
                    }
                }
                m_procStat.m_totalResultSize += resultSize;
                m_procStat.m_minResultSize = Math.min(resultSize, m_procStat.m_minResultSize);
                m_procStat.m_maxResultSize = Math.max(resultSize, m_procStat.m_maxResultSize);
                m_procStat.m_lastMinResultSize = Math.min(resultSize, m_procStat.m_lastMinResultSize);
                m_procStat.m_lastMaxResultSize = Math.max(resultSize, m_procStat.m_lastMaxResultSize);
                int parameterSetSize = (
                        parameterSet != null ? parameterSet.getSerializedSize() : 0);
                m_procStat.m_totalParameterSetSize += parameterSetSize;
                m_procStat.m_minParameterSetSize = Math.min(parameterSetSize, m_procStat.m_minParameterSetSize);
                m_procStat.m_maxParameterSetSize = Math.max(parameterSetSize, m_procStat.m_maxParameterSetSize);
                m_procStat.m_lastMinParameterSetSize = Math.min(parameterSetSize, m_procStat.m_lastMinParameterSetSize);
                m_procStat.m_lastMaxParameterSetSize = Math.max(parameterSetSize, m_procStat.m_lastMaxParameterSetSize);
            }
            m_procStat.m_currentStartTime = -1;
        }
        if (aborted) {
            m_procStat.m_abortCount++;
        }
        if (failed) {
            m_procStat.m_failureCount++;
        }
        m_procStat.m_invocations++;
    }

    /**
     * Update the rowValues array with the latest statistical information.
     * This method overrides the super class version
     * which must also be called so that it can update its columns.
     * @param rowKey The corresponding ProcedureStmtStat structure for this row.
     * @param rowValues Values of each column of the row of stats. Used as output.
     */
    @Override
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        super.updateStatsRow(rowKey, rowValues);
        rowValues[columnNameToIndex.get("PARTITION_ID")] = m_partitionId;
        rowValues[columnNameToIndex.get("PROCEDURE")] = m_procName;
        ProcedureStmtStat currRow = (ProcedureStmtStat)rowKey;
        assert(currRow != null);
        rowValues[columnNameToIndex.get("STATEMENT")] = currRow.m_stmtName;
        long invocations = currRow.m_invocations;
        long totalTimedExecutionTime = currRow.m_totalTimedExecutionTime;
        long timedInvocations = currRow.m_timedInvocations;
        long minExecutionTime = currRow.m_minExecutionTime;
        long maxExecutionTime = currRow.m_maxExecutionTime;
        long abortCount = currRow.m_abortCount;
        long failureCount = currRow.m_failureCount;
        int minResultSize = currRow.m_minResultSize;
        int maxResultSize = currRow.m_maxResultSize;
        long totalResultSize = currRow.m_totalResultSize;
        int minParameterSetSize = currRow.m_minParameterSetSize;
        int maxParameterSetSize = currRow.m_maxParameterSetSize;
        long totalParameterSetSize = currRow.m_totalParameterSetSize;

        if (m_interval) {
            invocations = currRow.m_invocations - currRow.m_lastInvocations;
            currRow.m_lastInvocations = currRow.m_invocations;

            totalTimedExecutionTime = currRow.m_totalTimedExecutionTime - currRow.m_lastTotalTimedExecutionTime;
            currRow.m_lastTotalTimedExecutionTime = currRow.m_totalTimedExecutionTime;

            timedInvocations = currRow.m_timedInvocations - currRow.m_lastTimedInvocations;
            currRow.m_lastTimedInvocations = currRow.m_timedInvocations;

            abortCount = currRow.m_abortCount - currRow.m_lastAbortCount;
            currRow.m_lastAbortCount = currRow.m_abortCount;

            failureCount = currRow.m_failureCount - currRow.m_lastFailureCount;
            currRow.m_lastFailureCount = currRow.m_failureCount;

            minExecutionTime = currRow.m_lastMinExecutionTime;
            maxExecutionTime = currRow.m_lastMaxExecutionTime;
            currRow.m_lastMinExecutionTime = Long.MAX_VALUE;
            currRow.m_lastMaxExecutionTime = Long.MIN_VALUE;

            minResultSize = currRow.m_lastMinResultSize;
            maxResultSize = currRow.m_lastMaxResultSize;
            currRow.m_lastMinResultSize = Integer.MAX_VALUE;
            currRow.m_lastMaxResultSize = Integer.MIN_VALUE;

            totalResultSize = currRow.m_totalResultSize - currRow.m_lastTotalResultSize;
            currRow.m_lastTotalResultSize = currRow.m_totalResultSize;

            totalParameterSetSize = currRow.m_totalParameterSetSize - currRow.m_lastTotalParameterSetSize;
            currRow.m_lastTotalParameterSetSize = currRow.m_totalParameterSetSize;
        }

        rowValues[columnNameToIndex.get("INVOCATIONS")] = invocations;
        rowValues[columnNameToIndex.get("TIMED_INVOCATIONS")] = timedInvocations;
        rowValues[columnNameToIndex.get("MIN_EXECUTION_TIME")] = minExecutionTime;
        rowValues[columnNameToIndex.get("MAX_EXECUTION_TIME")] = maxExecutionTime;
        if (timedInvocations != 0) {
            rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] =
                 (totalTimedExecutionTime / timedInvocations);
            rowValues[columnNameToIndex.get("AVG_RESULT_SIZE")] =
                    (totalResultSize / timedInvocations);
            rowValues[columnNameToIndex.get("AVG_PARAMETER_SET_SIZE")] =
                    (totalParameterSetSize / timedInvocations);
        } else {
            rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] = 0L;
            rowValues[columnNameToIndex.get("AVG_RESULT_SIZE")] = 0L;
            rowValues[columnNameToIndex.get("AVG_PARAMETER_SET_SIZE")] = 0L;
        }
        rowValues[columnNameToIndex.get("ABORTS")] = abortCount;
        rowValues[columnNameToIndex.get("FAILURES")] = failureCount;
        rowValues[columnNameToIndex.get("MIN_RESULT_SIZE")] = minResultSize;
        rowValues[columnNameToIndex.get("MAX_RESULT_SIZE")] = maxResultSize;
        rowValues[columnNameToIndex.get("MIN_PARAMETER_SET_SIZE")] = minParameterSetSize;
        rowValues[columnNameToIndex.get("MAX_PARAMETER_SET_SIZE")] = maxParameterSetSize;
    }

    /**
     * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
     * @param columns List of columns that are in a stats row.
     */
    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo("PARTITION_ID", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("PROCEDURE", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("STATEMENT", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("INVOCATIONS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MIN_RESULT_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("MAX_RESULT_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("AVG_RESULT_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("MIN_PARAMETER_SET_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("MAX_PARAMETER_SET_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("AVG_PARAMETER_SET_SIZE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("ABORTS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("FAILURES", VoltType.BIGINT));
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        m_interval = interval;
        return new Iterator<Object>() {
            Iterator<Entry<String, ProcedureStmtStat>> iter = m_stats.entrySet().iterator();
            ProcedureStmtStat nextToReturn = null;
            @Override
            public boolean hasNext() {
                if (nextToReturn != null) {
                    return true;
                }
                if ( ! iter.hasNext()) {
                    return false;
                }
                // Find the next element to return.
                do {
                    nextToReturn = iter.next().getValue();
                    if (getInterval()) {
                        if (nextToReturn.m_timedInvocations - nextToReturn.m_lastTimedInvocations == 0) {
                            nextToReturn = null;
                            continue;
                        }
                    }
                    else {
                        if (nextToReturn.m_timedInvocations == 0) {
                            nextToReturn = null;
                            continue;
                        }
                    }
                } while (nextToReturn == null && iter.hasNext());
                return nextToReturn != null;
            }

            @Override
            public Object next() {
                hasNext();
                Object ret = nextToReturn;
                nextToReturn = null;
                return ret;
            }

            @Override
            public void remove() {}

        };
    }

    @Override
    public String toString() {
        return m_procName;
    }

    /**
     * Accessor
     * @return the m_interval
     */
    public boolean getInterval() {
        return m_interval;
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public String getProcName() {
        return m_procName;
    }

    @Override
    public boolean equals(Object obj) {
        if(super.equals(obj) == false) return false;
        if (obj instanceof ProcedureStatsCollector == false) return false;

        ProcedureStatsCollector stats = (ProcedureStatsCollector) obj;
        if (stats.getPartitionId() != m_partitionId) return false;
        if (! m_procName.equals(stats.getProcName())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + m_partitionId + m_procName.hashCode();
    }

    /**
     * @return true if this procedure statistics should be reset
     */
    public boolean resetAfterCatalogChange() {
        // UpdateApplicationCatalog system procedure statistics should be kept
        if (isProcedureUAC()) {
            return false;
        }

        // TODO: we want want to keep other system procedure statistics ?
        // TODO: we may want to only reset updated user procedure statistics but keeping others.
        return true;
    }

    private boolean isProcedureUAC() {
        if (m_procName == null) return false;
        return m_procName.startsWith("org.voltdb.sysprocs.UpdateApplicationCatalog");
    }
}
