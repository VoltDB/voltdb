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
import java.util.List;

/**
 * Record statistics for each statement in the stored procedure.
 */
public final class StatementStats {
    /**
     * The name of the statement.
     * If it's for the statistics of the whole procedure, the name will be <ALL>.
     */
    String m_stmtName;
    StatsData m_coordinatorTask = null;
    StatsData m_workerTask;

    public StatementStats(String stmtName, boolean hasCoordinatorTask) {
        m_stmtName = stmtName;
        m_workerTask = new StatsData();
        if (hasCoordinatorTask) {
            m_coordinatorTask = new StatsData();
        }
    }

    // Maybe the worker task got executed and timed multiple times, but all failed.
    // The coordinator task never got executed.
    // So we do not only need to check if m_coordinatorTask != null, but also need to check
    // if any coordinator task is executed at all.
    private boolean isCoordinatorStatsUsable(boolean incremental) {
        if (m_coordinatorTask == null) {
            return false;
        }
        if (incremental) {
            return m_coordinatorTask.m_timedInvocations - m_coordinatorTask.m_lastTimedInvocations > 0;
        }
        return m_coordinatorTask.m_timedInvocations > 0;
    }

    // Below is a bunch of access functions to help merging the numbers from the coordinator task and the worker task.
    // ===============================================================================================================

    // m_coordinatorTask may have fewer m_invocations than m_workerTask because m_workerTask
    // failures can prevent m_coordinatorTask from further execution.
    // So m_workerTask.m_invocations is accurate for the m_invocations number of the whole statement.
    // Same for the m_lastInvocations.
    // However, this does not mean the invocation counts for the coordinator task is useless.
    // We need to use them when calculating the min/max times and sizes. See below.
    public long getInvocations() {
        return m_workerTask.m_invocations;
    }

    public long getLastInvocationsAndReset() {
        long retval = m_workerTask.m_lastInvocations;
        m_workerTask.m_lastInvocations = m_workerTask.m_invocations;
        if (m_coordinatorTask != null) {
            m_coordinatorTask.m_lastInvocations = m_coordinatorTask.m_invocations;
        }
        return retval;
    }

    public long getTimedInvocations() {
        return m_workerTask.m_timedInvocations;
    }

    public long getLastTimedInvocations() {
        return m_workerTask.m_lastTimedInvocations;
    }

    public long getLastTimedInvocationsAndReset() {
        long retval = m_workerTask.m_lastTimedInvocations;
        m_workerTask.m_lastTimedInvocations = m_workerTask.m_timedInvocations;
        if (m_coordinatorTask != null) {
            m_coordinatorTask.m_lastTimedInvocations = m_coordinatorTask.m_timedInvocations;
        }
        return retval;
    }

    public long getTotalTimedExecutionTime() {
        return m_workerTask.m_totalTimedExecutionTime +
                (m_coordinatorTask == null ? 0 : m_coordinatorTask.m_totalTimedExecutionTime);
    }

    public long getLastTotalTimedExecutionTimeAndReset() {
        long retval = m_workerTask.m_lastTotalTimedExecutionTime;
        m_workerTask.m_lastTotalTimedExecutionTime = m_workerTask.m_totalTimedExecutionTime;
        if (m_coordinatorTask != null) {
            retval += m_coordinatorTask.m_lastTotalTimedExecutionTime;
            m_coordinatorTask.m_lastTotalTimedExecutionTime = m_coordinatorTask.m_totalTimedExecutionTime;
        }
        return retval;
    }

    // Notice that does min(worker + coord) == min(worker) + min(coord)?
    // The answer is NO. This is an approximation.
    public long getMinExecutionTime() {
        if (isCoordinatorStatsUsable(false)) {
            return m_workerTask.m_minExecutionTime + m_coordinatorTask.m_minExecutionTime;
        }
        return m_workerTask.m_minExecutionTime;
    }

    public long getIncrementalMinExecutionTimeAndReset() {
        long retval = m_workerTask.m_incrMinExecutionTime;
        if (isCoordinatorStatsUsable(true)) {
            retval += m_coordinatorTask.m_incrMinExecutionTime;
            m_coordinatorTask.m_incrMinExecutionTime = Long.MAX_VALUE;
        }
        m_workerTask.m_incrMinExecutionTime = Long.MAX_VALUE;
        return retval;
    }

    public long getMaxExecutionTime() {
        if (isCoordinatorStatsUsable(false)) {
            return m_workerTask.m_maxExecutionTime + m_coordinatorTask.m_maxExecutionTime;
        }
        return m_workerTask.m_maxExecutionTime;
    }

    public long getIncrementalMaxExecutionTimeAndReset() {
        long retval = m_workerTask.m_incrMaxExecutionTime;
        if (isCoordinatorStatsUsable(true)) {
            retval += m_coordinatorTask.m_incrMaxExecutionTime;
            m_coordinatorTask.m_incrMaxExecutionTime = Long.MIN_VALUE;
        }
        m_workerTask.m_incrMaxExecutionTime = Long.MIN_VALUE;
        return retval;
    }

    public long getAbortCount() {
        return m_workerTask.m_abortCount;
    }

    public long getLastAbortCountAndReset() {
        // Only the whole procedure can abort and the procedure stats does not have a coordinator task.
        long retval = m_workerTask.m_lastAbortCount;
        m_workerTask.m_lastAbortCount = m_workerTask.m_abortCount;
        return retval;
    }

    public long getFailureCount() {
        return m_workerTask.m_failureCount +
                (m_coordinatorTask == null ? 0 : m_coordinatorTask.m_failureCount);
    }

    public long getLastFailureCountAndReset() {
        long retval = m_workerTask.m_lastFailureCount;
        m_workerTask.m_lastFailureCount = m_workerTask.m_failureCount;
        if (m_coordinatorTask != null) {
            retval += m_coordinatorTask.m_lastFailureCount;
            m_coordinatorTask.m_lastFailureCount = m_coordinatorTask.m_failureCount;
        }
        return retval;
    }

    public int getMinResultSize() {
        return m_workerTask.m_minResultSize;
    }

    // The result size should be taken from the final output, coming from the coordinator task.
    public int getIncrementalMinResultSizeAndReset() {
        int retval = m_workerTask.m_incrMinResultSize;
        m_workerTask.m_incrMinResultSize = Integer.MAX_VALUE;
        if (isCoordinatorStatsUsable(true)) {
            m_coordinatorTask.m_incrMinResultSize = Integer.MAX_VALUE;
        }
        return retval;
    }

    public int getMaxResultSize() {
        return m_workerTask.m_maxResultSize;
    }

    public int getIncrementalMaxResultSizeAndReset() {
        int retval = m_workerTask.m_incrMaxResultSize;
        m_workerTask.m_incrMaxResultSize = Integer.MIN_VALUE;
        if (isCoordinatorStatsUsable(true)) {
            m_coordinatorTask.m_incrMaxResultSize = Integer.MIN_VALUE;
        }
        return retval;
    }

    public long getTotalResultSize() {
        return m_workerTask.m_totalResultSize;
    }

    public long getLastTotalResultSizeAndReset() {
        long retval = m_workerTask.m_lastTotalResultSize;
        m_workerTask.m_lastTotalResultSize = m_workerTask.m_totalResultSize;
        if (isCoordinatorStatsUsable(true)) {
            m_coordinatorTask.m_lastTotalResultSize = m_coordinatorTask.m_totalResultSize;
        }
        return retval;
    }

    public int getMinParameterSetSize() {
        return m_workerTask.m_minParameterSetSize;
    }

    public int getIncrementalMinParameterSetSizeAndReset() {
        int retval = m_workerTask.m_incrMinParameterSetSize;
        m_workerTask.m_incrMinResultSize = Integer.MAX_VALUE;
        if (m_coordinatorTask != null) {
            m_coordinatorTask.m_incrMinResultSize = Integer.MAX_VALUE;
        }
        return retval;
    }

    public int getMaxParameterSetSize() {
        return m_workerTask.m_maxParameterSetSize;
    }

    public int getIncrementalMaxParameterSetSizeAndReset() {
        int retval = m_workerTask.m_incrMaxParameterSetSize;
        m_workerTask.m_incrMaxResultSize = Integer.MIN_VALUE;
        if (m_coordinatorTask != null) {
            m_coordinatorTask.m_incrMaxResultSize = Integer.MIN_VALUE;
        }
        return retval;
    }

    public long getTotalParameterSetSize() {
        return m_workerTask.m_totalParameterSetSize;
    }

    public long getLastTotalParameterSetSizeAndReset() {
        long retval = m_workerTask.m_lastTotalParameterSetSize;
        m_workerTask.m_lastTotalParameterSetSize = m_workerTask.m_totalParameterSetSize;
        if (m_coordinatorTask != null) {
            m_coordinatorTask.m_lastTotalParameterSetSize = m_coordinatorTask.m_totalParameterSetSize;
        }
        return retval;
    }

    /**
     * This is a token the ProcedureRunner holds onto while it's running.
     * It collects stats information during the procedure run without needing
     * to touch the actual stats source.
     * When the procedure is done (commit/abort/whatever), this token is given
     * to the ProcedureStatsCollector in a single (thread-safe) call.
     *
     */
    public static final class SingleCallStatsToken {
        class PerStmtStats {
            final String stmtName;
            final boolean isCoordinatorTask;
            final boolean stmtFailed;
            final MeasuredStmtStats measurements;

            PerStmtStats(String stmtName,
                    boolean isCoordinatorTask,
                    boolean failed,
                    MeasuredStmtStats measurements)
            {
                this.stmtName = stmtName;
                this.isCoordinatorTask = isCoordinatorTask;
                this.stmtFailed = failed;
                this.measurements = measurements;
            }
        }

        /**
         * Per-statment stats
         */
        class MeasuredStmtStats {
            final long stmtDuration;
            final int stmtResultSize;
            final int stmtParameterSetSize;

            MeasuredStmtStats(long duration,
                              int resultSize,
                              int paramSetSize)
            {
                this.stmtDuration = duration;
                this.stmtResultSize = resultSize;
                this.stmtParameterSetSize = paramSetSize;
            }
        }

        final long startTimeNanos;
        final boolean samplingStatements;
        // stays null until used
        List<PerStmtStats> stmtStats = null;

        int parameterSetSize = 0;
        int resultSize = 0;

        public SingleCallStatsToken(long startTimeNanos, boolean samplingStatements) {
            this.startTimeNanos = startTimeNanos;
            this.samplingStatements = samplingStatements;
        }

        public boolean samplingProcedure() {
            return startTimeNanos != 0;
        }

        public boolean samplingStmts() {
            return samplingStatements;
        }

        public void setParameterSize(int size) {
            parameterSetSize = size;
        }

        public void setResultSize(VoltTable[] results) {
            resultSize = 0;
            if (results != null) {
                for (VoltTable result : results ) {
                    resultSize += result.getSerializedSize();
                }
            }
        }

        /**
         * Called when a statement completes.
         * Adds a record to the parent stats token.
         */
        public void recordStatementStats(String stmtName,
                                         boolean isCoordinatorTask,
                                         boolean failed,
                                         long duration,
                                         VoltTable result,
                                         ParameterSet parameterSet)
        {
            if (stmtStats == null) {
                stmtStats = new ArrayList<>();
            }

            MeasuredStmtStats measuredStmtStats = null;
            if (samplingStatements) {
                int stmtResultSize = 0;
                if (result != null) {
                    stmtResultSize = result.getSerializedSize();
                }
                int stmtParamSize = 0;
                if (parameterSet != null) {
                    stmtParamSize += parameterSet.getSerializedSize();
                }
                measuredStmtStats = new MeasuredStmtStats(duration, stmtResultSize, stmtParamSize);
            }

            stmtStats.add(new PerStmtStats(stmtName,
                                           isCoordinatorTask,
                                           failed,
                                           measuredStmtStats));
        }
    }

    static final class StatsData {
        /**
         * Number of times this procedure has been invoked.
         */
        long m_invocations = 0;
        long m_lastInvocations = 0;

        /**
         * Number of timed invocations
         */
        long m_timedInvocations = 0;
        long m_lastTimedInvocations = 0;

        /**
         * Total amount of timed execution time
         */
        long m_totalTimedExecutionTime = 0;
        long m_lastTotalTimedExecutionTime = 0;

        /**
         * Shortest amount of time this procedure has executed in
         */
        long m_minExecutionTime = Long.MAX_VALUE;
        long m_incrMinExecutionTime = Long.MAX_VALUE;

        /**
         * Longest amount of time this procedure has executed in
         */
        long m_maxExecutionTime = Long.MIN_VALUE;
        long m_incrMaxExecutionTime = Long.MIN_VALUE;

        /**
         * Count of the number of aborts (user initiated or DB initiated)
         */
        long m_abortCount = 0;
        long m_lastAbortCount = 0;

        /**
         * Count of the number of errors that occurred during procedure execution
         */
        long m_failureCount = 0;
        long m_lastFailureCount = 0;

        /**
         * Smallest result size
         */
        int m_minResultSize = Integer.MAX_VALUE;
        int m_incrMinResultSize = Integer.MAX_VALUE;

        /**
         * Largest result size
         */
        int m_maxResultSize = Integer.MIN_VALUE;
        int m_incrMaxResultSize = Integer.MIN_VALUE;

        /**
         * Total result size for calculating averages
         */
        long m_totalResultSize = 0;
        long m_lastTotalResultSize = 0;

        /**
         * Smallest parameter set size
         */
        int m_minParameterSetSize = Integer.MAX_VALUE;
        int m_incrMinParameterSetSize = Integer.MAX_VALUE;

        /**
         * Largest parameter set size
         */
        int m_maxParameterSetSize = Integer.MIN_VALUE;
        int m_incrMaxParameterSetSize = Integer.MIN_VALUE;

        /**
         * Total parameter set size for calculating averages
         */
        long m_totalParameterSetSize = 0;
        long m_lastTotalParameterSetSize = 0;
    }
}
