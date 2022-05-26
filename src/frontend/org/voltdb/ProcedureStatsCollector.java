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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.StatementStats.SingleCallStatsToken;
import org.voltdb.StatementStats.StatsData;
import org.voltdb.catalog.Procedure;
import org.voltdb.sysprocs.UpdateCore;

/**
 * Derivation of StatsSource to expose timing information of procedure invocations.
 */
public class ProcedureStatsCollector extends SiteStatsSource {

    private static final VoltLogger log = new VoltLogger("HOST");

    /**
     * Record statistics of procedure execution every N procedure invocations.
     */
    private int m_procSamplingInterval = 20;
    /**
     * Record statistics of procedure statement execution every N procedure invocations.
     */
    private int m_stmtSamplingInterval = 200;

    protected void setProcSamplingInterval(int timeCollectionInterval) {
        m_procSamplingInterval = timeCollectionInterval;
    }

    protected void setStmtSamplingInterval(int timeCollectionInterval) {
        m_stmtSamplingInterval = timeCollectionInterval;
    }

    /**
     * Whether to return incremental results since polling or since the beginning
     */
    private boolean m_incremental = false;

    private final String m_procName;
    private final int m_partitionId;
    // Mapping from the variable name of the user-defined SQLStmts to its stats.
    private final Map<String, StatementStats> m_stmtStatsMap;
    private final StatsData m_procStatsData;
    private final ProcType m_procType;
    private final boolean m_isUAC;

    public enum ProcedureColumns {
        PARTITION_ID            (VoltType.INTEGER),
        PROCEDURE               (VoltType.STRING),
        STATEMENT               (VoltType.STRING),
        INVOCATIONS             (VoltType.BIGINT),
        TIMED_INVOCATIONS       (VoltType.BIGINT),
        MIN_EXECUTION_TIME      (VoltType.BIGINT),
        MAX_EXECUTION_TIME      (VoltType.BIGINT),
        AVG_EXECUTION_TIME      (VoltType.BIGINT),
        MIN_RESULT_SIZE         (VoltType.INTEGER),
        MAX_RESULT_SIZE         (VoltType.INTEGER),
        AVG_RESULT_SIZE         (VoltType.INTEGER),
        MIN_PARAMETER_SET_SIZE  (VoltType.INTEGER),
        MAX_PARAMETER_SET_SIZE  (VoltType.INTEGER),
        AVG_PARAMETER_SET_SIZE  (VoltType.INTEGER),
        ABORTS                  (VoltType.BIGINT),
        FAILURES                (VoltType.BIGINT),
        TRANSACTIONAL           (VoltType.TINYINT),
        COMPOUND                (VoltType.TINYINT);

        public final VoltType m_type;
        ProcedureColumns(VoltType type) { m_type = type; }
    }

    public enum ProcType { TRANS, NONTRANS, COMPOUND };

    public ProcedureStatsCollector(long siteId,
                                   int partitionId,
                                   Procedure catProc,
                                   ArrayList<String> stmtNames,
                                   ProcType procType) {
        this(siteId, partitionId, catProc.getClassname(), catProc.getSinglepartition(), stmtNames, procType);
    }

    public ProcedureStatsCollector(long siteId,
                                   int partitionId,
                                   String procName,
                                   boolean singlePartition,
                                   ArrayList<String> stmtNames,
                                   ProcType procType) {
        super(siteId, false);
        m_partitionId = partitionId;
        m_procName = procName;

        m_stmtStatsMap = new HashMap<String, StatementStats>();
        // Use one StatementStats instance to hold the procedure-wide statistics.
        // The statement name for this StatementStats is "<ALL>".
        // It does not have coordinator task to track.
        StatementStats procedureWideStats = new StatementStats("<ALL>", false);
        m_procStatsData = procedureWideStats.m_workerTask;
        // The NULL key entry is reserved for the procedure-wide statistics.
        m_stmtStatsMap.put(null, procedureWideStats);
        // Add stats entry for each of the individual SQL statements.
        if (stmtNames != null) {
            for (String stmtName : stmtNames) {
                // If the procedure is a multi-partition one, its statements will have coordinator tasks.
                boolean hasCoordinatorTask = ! singlePartition;
                m_stmtStatsMap.put(stmtName, new StatementStats(stmtName, hasCoordinatorTask));
            }
        }
        m_procType = procType;

        // check if this proc is UpdateCore for 100% sampling rate
        m_isUAC = (m_procName != null) && (m_procName.startsWith(UpdateCore.class.getName()));
    }

    // This is not the *real* invocation count, but a fuzzy one we keep to sample 5% of
    // calls without modifying any state. We *only* modify state when a procedure completes.
    AtomicLong fuzzyInvocationCounter = new AtomicLong(0);

    /**
     * Called when a procedure begins executing. Caches the time the procedure starts.
     * Note: This does not touch internal mutable state besides fuzzyInvocationCounter.
     */
    public final SingleCallStatsToken beginProcedure() {
        long invocations = fuzzyInvocationCounter.getAndIncrement();

        boolean samplingProcedure = (invocations % m_procSamplingInterval == 0) || m_isUAC;
        boolean samplingStmts = invocations % m_stmtSamplingInterval == 0;

        long startTimeNanos = samplingProcedure ? System.nanoTime() : 0;

        return new SingleCallStatsToken(startTimeNanos, samplingStmts);
    }

    /**
     * Called after a procedure is finished executing. Compares the start and end time and calculates
     * the statistics.
     *
     * Synchronized because it modifies internal state and (for NT procs) can be called from multiple
     * threads. For transactional procs the lock should be uncontended.,
     */
    public final synchronized void endProcedure(boolean aborted, boolean failed, SingleCallStatsToken statsToken) {
        if (aborted) {
            m_procStatsData.m_abortCount++;
        }
        if (failed) {
            m_procStatsData.m_failureCount++;
        }
        m_procStatsData.m_invocations++;

        // this means additional stats were not recorded
        if (!statsToken.samplingProcedure()) {
            return;
        }

        // This is a sampled invocation.
        // Update timings and size statistics.
        final long endTime = System.nanoTime();
        final long duration = endTime - statsToken.startTimeNanos;
        if (duration < 0) {
            if (duration < -1000000000) {
                log.info("Procedure: " + m_procName +
                         " recorded a negative execution time larger than one second: " + duration);
            }
            return;
        }

        // sampled timings
        m_procStatsData.m_totalTimedExecutionTime += duration;
        m_procStatsData.m_minExecutionTime = Math.min(duration, m_procStatsData.m_minExecutionTime);
        m_procStatsData.m_maxExecutionTime = Math.max(duration, m_procStatsData.m_maxExecutionTime);
        m_procStatsData.m_incrMinExecutionTime = Math.min(duration, m_procStatsData.m_incrMinExecutionTime);
        m_procStatsData.m_incrMaxExecutionTime = Math.max(duration, m_procStatsData.m_incrMaxExecutionTime);

        m_procStatsData.m_totalResultSize += statsToken.resultSize;
        m_procStatsData.m_minResultSize = Math.min(statsToken.resultSize, m_procStatsData.m_minResultSize);
        m_procStatsData.m_maxResultSize = Math.max(statsToken.resultSize, m_procStatsData.m_maxResultSize);
        m_procStatsData.m_incrMinResultSize = Math.min(statsToken.resultSize, m_procStatsData.m_incrMinResultSize);
        m_procStatsData.m_incrMaxResultSize = Math.max(statsToken.resultSize, m_procStatsData.m_incrMaxResultSize);

        m_procStatsData.m_totalParameterSetSize += statsToken.parameterSetSize;
        m_procStatsData.m_minParameterSetSize = Math.min(statsToken.parameterSetSize, m_procStatsData.m_minParameterSetSize);
        m_procStatsData.m_maxParameterSetSize = Math.max(statsToken.parameterSetSize, m_procStatsData.m_maxParameterSetSize);
        m_procStatsData.m_incrMinParameterSetSize = Math.min(statsToken.parameterSetSize, m_procStatsData.m_incrMinParameterSetSize);
        m_procStatsData.m_incrMaxParameterSetSize = Math.max(statsToken.parameterSetSize, m_procStatsData.m_incrMaxParameterSetSize);

        // update this after the above, in the hope that the unsynchronized stats row
        // iterator will tend to skip rows that have no valid min/max
        m_procStatsData.m_timedInvocations++;

        // stop here if no statements
        if (statsToken.stmtStats == null) {
            return;
        }

        for (SingleCallStatsToken.PerStmtStats pss : statsToken.stmtStats) {
            long stmtDuration = 0;
            int stmtResultSize = 0;
            int stmtParameterSetSize = 0;
            if (pss.measurements != null) {
                stmtDuration = pss.measurements.stmtDuration;
                stmtResultSize = pss.measurements.stmtResultSize;
                stmtParameterSetSize = pss.measurements.stmtParameterSetSize;
            }

            endFragment(pss.stmtName,
                        pss.isCoordinatorTask,
                        pss.stmtFailed,
                        pss.measurements != null,
                        stmtDuration,
                        stmtResultSize,
                        stmtParameterSetSize);
        }
    }

    /**
     * This function will be called after a statement finish running.
     * It updates the data structures to maintain the statistics.
     */
    public final synchronized void endFragment(String stmtName,
                                               boolean isCoordinatorTask,
                                               boolean failed,
                                               boolean sampledStmt,
                                               long duration,
                                               int resultSize,
                                               int parameterSetSize)
    {
        if (stmtName == null) {
            return;
        }
        StatementStats stmtStats = m_stmtStatsMap.get(stmtName);
        if (stmtStats == null) {
            return;
        }
        StatsData dataToUpdate = isCoordinatorTask ? stmtStats.m_coordinatorTask : stmtStats.m_workerTask;
        // m_failureCount and m_invocations need to be updated even if the current invocation is not sampled.
        if (failed) {
            dataToUpdate.m_failureCount++;
        }
        dataToUpdate.m_invocations++;

        // If the current invocation is not sampled, we can stop now.
        // Notice that this function can be called by a FragmentTask from a multi-partition procedure.
        // Cannot use the isRecording() value here because SP sites can have values different from the MP Site.
        if (!sampledStmt) {
            return;
        }
        // This is a sampled invocation.
        // Update timings and size statistics below.
        if (duration < 0) {
            if (duration < -1000000000) {
                log.info("Statement: " + stmtStats.m_stmtName + " in procedure: " + m_procName +
                         " recorded a negative execution time larger than one second: " +
                         duration);
            }
            return;
        }

        // sampled timings
        dataToUpdate.m_totalTimedExecutionTime += duration;
        dataToUpdate.m_minExecutionTime = Math.min(duration, dataToUpdate.m_minExecutionTime);
        dataToUpdate.m_maxExecutionTime = Math.max(duration, dataToUpdate.m_maxExecutionTime);
        dataToUpdate.m_incrMinExecutionTime = Math.min(duration, dataToUpdate.m_incrMinExecutionTime);
        dataToUpdate.m_incrMaxExecutionTime = Math.max(duration, dataToUpdate.m_incrMaxExecutionTime);

        // sampled size statistics
        dataToUpdate.m_totalResultSize += resultSize;
        dataToUpdate.m_minResultSize = Math.min(resultSize, dataToUpdate.m_minResultSize);
        dataToUpdate.m_maxResultSize = Math.max(resultSize, dataToUpdate.m_maxResultSize);
        dataToUpdate.m_incrMinResultSize = Math.min(resultSize, dataToUpdate.m_incrMinResultSize);
        dataToUpdate.m_incrMaxResultSize = Math.max(resultSize, dataToUpdate.m_incrMaxResultSize);

        dataToUpdate.m_totalParameterSetSize += parameterSetSize;
        dataToUpdate.m_minParameterSetSize = Math.min(parameterSetSize, dataToUpdate.m_minParameterSetSize);
        dataToUpdate.m_maxParameterSetSize = Math.max(parameterSetSize, dataToUpdate.m_maxParameterSetSize);
        dataToUpdate.m_incrMinParameterSetSize = Math.min(parameterSetSize, dataToUpdate.m_incrMinParameterSetSize);
        dataToUpdate.m_incrMaxParameterSetSize = Math.max(parameterSetSize, dataToUpdate.m_incrMaxParameterSetSize);

        // update this after the above, in the hope that the unsynchronized stats row
        // iterator will tend to skip rows that have no valid min/max
        dataToUpdate.m_timedInvocations++;
    }

    /**
     * Update the rowValues array with the latest statistical information.
     * This method overrides the super class version
     * which must also be called so that it can update its columns.
     * @param rowKey The corresponding StatementStats structure for this row.
     * @param rowValues Values of each column of the row of stats. Used as output.
     * @return next column index to write
     */
    @Override
    protected int updateStatsRow(Object rowKey, Object rowValues[]) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        rowValues[offset + ProcedureColumns.PARTITION_ID.ordinal()] = m_partitionId;
        rowValues[offset + ProcedureColumns.PROCEDURE.ordinal()] = m_procName;
        StatementStats currRow = (StatementStats)rowKey;
        assert(currRow != null);
        rowValues[offset + ProcedureColumns.STATEMENT.ordinal()] = currRow.m_stmtName;

        long invocations = currRow.getInvocations();
        long timedInvocations = currRow.getTimedInvocations();
        long totalTimedExecutionTime = currRow.getTotalTimedExecutionTime();
        long minExecutionTime = currRow.getMinExecutionTime();
        long maxExecutionTime = currRow.getMaxExecutionTime();
        long abortCount = currRow.getAbortCount();
        long failureCount = currRow.getFailureCount();
        int minResultSize = currRow.getMinResultSize();
        int maxResultSize = currRow.getMaxResultSize();
        long totalResultSize = currRow.getTotalResultSize();
        int minParameterSetSize = currRow.getMinParameterSetSize();
        int maxParameterSetSize = currRow.getMaxParameterSetSize();
        long totalParameterSetSize = currRow.getTotalParameterSetSize();

        if (m_incremental) {
            abortCount -= currRow.getLastAbortCountAndReset();
            failureCount -= currRow.getLastFailureCountAndReset();
            totalTimedExecutionTime -= currRow.getLastTotalTimedExecutionTimeAndReset();
            totalResultSize -= currRow.getLastTotalResultSizeAndReset();
            totalParameterSetSize -= currRow.getLastTotalParameterSetSizeAndReset();
            minExecutionTime = currRow.getIncrementalMinExecutionTimeAndReset();
            maxExecutionTime = currRow.getIncrementalMaxExecutionTimeAndReset();
            minResultSize = currRow.getIncrementalMinResultSizeAndReset();
            maxResultSize = currRow.getIncrementalMaxResultSizeAndReset();
            minParameterSetSize = currRow.getIncrementalMinParameterSetSizeAndReset();
            maxParameterSetSize = currRow.getIncrementalMaxParameterSetSizeAndReset();
            // Notice that invocation numbers must be updated in the end.
            // Other numbers depend on them for correct behavior.
            invocations -= currRow.getLastInvocationsAndReset();
            timedInvocations -= currRow.getLastTimedInvocationsAndReset();
        }

        rowValues[offset + ProcedureColumns.INVOCATIONS.ordinal()] = invocations;
        rowValues[offset + ProcedureColumns.TIMED_INVOCATIONS.ordinal()] = timedInvocations;
        rowValues[offset + ProcedureColumns.MIN_EXECUTION_TIME.ordinal()] = minExecutionTime;
        rowValues[offset + ProcedureColumns.MAX_EXECUTION_TIME.ordinal()] = maxExecutionTime;
        if (timedInvocations != 0) {
            rowValues[offset + ProcedureColumns.AVG_EXECUTION_TIME.ordinal()] = (totalTimedExecutionTime / timedInvocations);
            rowValues[offset + ProcedureColumns.AVG_RESULT_SIZE.ordinal()] = (totalResultSize / timedInvocations);
            rowValues[offset + ProcedureColumns.AVG_PARAMETER_SET_SIZE.ordinal()] = (totalParameterSetSize / timedInvocations);
        } else {
            rowValues[offset + ProcedureColumns.AVG_EXECUTION_TIME.ordinal()] = 0L;
            rowValues[offset + ProcedureColumns.AVG_RESULT_SIZE.ordinal()] = 0;
            rowValues[offset + ProcedureColumns.AVG_PARAMETER_SET_SIZE.ordinal()] = 0;
        }
        rowValues[offset + ProcedureColumns.ABORTS.ordinal()] = abortCount;
        rowValues[offset + ProcedureColumns.FAILURES.ordinal()] = failureCount;
        rowValues[offset + ProcedureColumns.MIN_RESULT_SIZE.ordinal()] = minResultSize;
        rowValues[offset + ProcedureColumns.MAX_RESULT_SIZE.ordinal()] = maxResultSize;
        rowValues[offset + ProcedureColumns.MIN_PARAMETER_SET_SIZE.ordinal()] = minParameterSetSize;
        rowValues[offset + ProcedureColumns.MAX_PARAMETER_SET_SIZE.ordinal()] = maxParameterSetSize;
        rowValues[offset + ProcedureColumns.TRANSACTIONAL.ordinal()] = (byte) (m_procType == ProcType.TRANS ? 1 : 0);
        rowValues[offset + ProcedureColumns.COMPOUND.ordinal()] = (byte) (m_procType == ProcType.COMPOUND ? 1 : 0);

        return offset + ProcedureColumns.values().length;
    }

    /**
     * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
     * @param columns List of columns that are in a stats row.
     */
    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns, ProcedureColumns.class);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        m_incremental = interval;
        return new Iterator<Object>() {
            Iterator<Entry<String, StatementStats>> iter = m_stmtStatsMap.entrySet().iterator();
            StatementStats nextToReturn = null;
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
                    if (m_incremental) {
                        if (nextToReturn.getTimedInvocations() - nextToReturn.getLastTimedInvocations() == 0) {
                            nextToReturn = null;
                        }
                    }
                    else {
                        if (nextToReturn.getTimedInvocations() == 0) {
                            nextToReturn = null;
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
}
