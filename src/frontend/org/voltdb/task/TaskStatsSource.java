/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.StatsSource;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Iterators;

/**
 * {@link StatsSource} used by {@link TaskManager}. A single instance of this stats source will be created for each
 * running instance of a Scheduler.
 */
public class TaskStatsSource extends StatsSource {
    private static final String PREFIX_SCHEDULER = "SCHEDULER_";
    private static final String PREFIX_PROCEDURE = "PROCEDURE_";

    private static final int s_sharedSubSelectorColumnCount = 7;

    // Metadata to basically select a subset of columns from the full stats
    private static List<ColumnAs> s_schedulersConvert;
    private static List<ColumnAs> s_procedursConvert;

    private final StatsSelector m_selector;
    private final String m_name;
    private final TaskScope m_scope;
    private final int m_partitionId;

    private String m_state;

    // Stats for scheduler execution
    private final TimingStats m_schedulerStats;
    private String m_schedulerStatus;

    // Stats for procedure executions
    private final TimingStats m_procedureStats;
    private long m_procedureFailures = 0;

    public enum Task {
        PARTITION_ID                (VoltType.INTEGER),
        TASK_NAME                   (VoltType.STRING),
        STATE                       (VoltType.STRING),
        SCOPE                       (VoltType.STRING),
        SCHEDULER_INVOCATIONS       (VoltType.BIGINT),
        SCHEDULER_TOTAL_EXECUTION   (VoltType.BIGINT),
        SCHEDULER_MIN_EXECUTION     (VoltType.BIGINT),
        SCHEDULER_MAX_EXECUTION     (VoltType.BIGINT),
        SCHEDULER_AVG_EXECUTION     (VoltType.BIGINT),
        SCHEDULER_TOTAL_WAIT_TIME   (VoltType.BIGINT),
        SCHEDULER_MIN_WAIT_TIME     (VoltType.BIGINT),
        SCHEDULER_MAX_WAIT_TIME     (VoltType.BIGINT),
        SCHEDULER_AVG_WAIT_TIME     (VoltType.BIGINT),
        SCHEDULER_STATUS            (VoltType.STRING),
        PROCEDURE_INVOCATIONS       (VoltType.BIGINT),
        PROCEDURE_TOTAL_EXECUTION   (VoltType.BIGINT),
        PROCEDURE_MIN_EXECUTION     (VoltType.BIGINT),
        PROCEDURE_MAX_EXECUTION     (VoltType.BIGINT),
        PROCEDURE_AVG_EXECUTION     (VoltType.BIGINT),
        PROCEDURE_TOTAL_WAIT_TIME   (VoltType.BIGINT),
        PROCEDURE_MIN_WAIT_TIME     (VoltType.BIGINT),
        PROCEDURE_MAX_WAIT_TIME     (VoltType.BIGINT),
        PROCEDURE_AVG_WAIT_TIME     (VoltType.BIGINT),
        PROCEDURE_FAILURES          (VoltType.BIGINT);

        public final VoltType m_type;
        Task(VoltType type) { m_type = type; }
    }

    /**
     * Convert stats collected under {@link StatsSelector#TASK} to either {@link StatsSelector#TASK_SCHEDULER} or
     * {@link StatsSelector#TASK_PROCEDURE}
     *
     * @param subselector to format to convert to
     * @param tables      generated from the stats colleciton
     */
    public static void convert(StatsSelector subselector, VoltTable[] tables) {
        assert tables.length == 1;

        List<ColumnAs> columnConversion;
        VoltTable source = tables[0];

        switch (subselector) {
        case TASK_PROCEDURE:
            columnConversion = getProceduresConverter(source);
            break;
        case TASK_SCHEDULER:
            columnConversion = getSchedulersConverter(source);
            break;
        case TASK:
        case SYSTEM_TASK:
            return;
        default:
            throw new IllegalArgumentException("Unsupported selector: " + subselector);
        }

        VoltTable dest = new VoltTable(
                columnConversion.stream().map(ColumnAs::getColumnInfo).toArray(ColumnInfo[]::new));

        source.resetRowPosition();
        while (source.advanceRow()) {
            Object[] columns = new Object[columnConversion.size()];
            for (int i = 0; i < columns.length; ++i) {
                ColumnAs columnAs = columnConversion.get(i);
                columns[i] = source.get(columnAs.m_fromIndex, columnAs.m_type);
            }
            dest.addRow(columns);
        }

        tables[0] = dest;
    }


    private static List<ColumnAs> getSchedulersConverter(VoltTable source) {
        if (s_schedulersConvert == null) {
            s_schedulersConvert = initializeConverter(source, PREFIX_SCHEDULER);
        }
        return s_schedulersConvert;
    }

    private static List<ColumnAs> getProceduresConverter(VoltTable source) {
        if (s_procedursConvert == null) {
            s_procedursConvert = initializeConverter(source, PREFIX_PROCEDURE);
        }
        return s_procedursConvert;
    }

    /**
     * @param source     {@link VoltTable} which will be the source of the stats
     * @param prefixKeep If a column has this prefix it will be kept in the converted table
     * @return {@link List} of {@link ColumnAs} for selecting out desired stats
     */
    private static List<ColumnAs> initializeConverter(VoltTable source, String prefixKeep) {
        ImmutableList.Builder<ColumnAs> builder = ImmutableList.<ColumnAs>builder();

        for (int i = 0; i < source.getColumnCount(); ++i) {
            String columnName = source.getColumnName(i);
            if (i < s_sharedSubSelectorColumnCount || columnName.startsWith(prefixKeep)) {
                builder.add(new ColumnAs(source, i, columnName));
            }
        }

        return builder.build();
    }

    static TaskStatsSource createDummy(boolean system) {
        return new TaskStatsSource(null, null, -1, system);
    }

    static TaskStatsSource create(String name, TaskScope scope, int partitionId, boolean systemTask) {
        return new TaskStatsSource(Objects.requireNonNull(name), Objects.requireNonNull(scope), partitionId,
                systemTask);
    }

    private TaskStatsSource(String name, TaskScope scope, int partitionId, boolean systemTask) {
        super(false);
        m_selector = systemTask ? StatsSelector.SYSTEM_TASK : StatsSelector.TASK;
        m_name = name;
        m_scope = scope;
        m_partitionId = partitionId;

        if (name == null) {
            m_schedulerStats = null;
            m_procedureStats = null;
        } else {
            m_schedulerStats = new TimingStats();
            m_procedureStats = new TimingStats();
        }
    }

    void register(StatsAgent agent) {
        agent.registerStatsSource(m_selector, m_partitionId, this);
    }

    void deregister(StatsAgent agent) {
        agent.deregisterStatsSource(m_selector, m_partitionId, this);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return m_name == null ? Collections.emptyIterator() : Iterators.singletonIterator(null);
    }

    @Override
    protected synchronized int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);

        // Header state info
        rowValues[offset + Task.PARTITION_ID.ordinal()] = m_partitionId;
        rowValues[offset + Task.TASK_NAME.ordinal()] = m_name;
        rowValues[offset + Task.STATE.ordinal()] = m_state;
        rowValues[offset + Task.SCOPE.ordinal()] = m_scope.name();

        // Scheduler stats
        m_schedulerStats.pupulateStats(rowValues, offset + Task.SCHEDULER_INVOCATIONS.ordinal());
        rowValues[offset + Task.SCHEDULER_STATUS.ordinal()] = m_schedulerStatus;

        // Procedure stats
        m_procedureStats.pupulateStats(rowValues, offset + Task.PROCEDURE_INVOCATIONS.ordinal());

        rowValues[offset + Task.PROCEDURE_FAILURES.ordinal()] = m_procedureFailures;

        int totalColumn = offset + Task.values().length;

        assert totalColumn == rowValues.length : "Column count off should be " + rowValues.length + " inserted " + totalColumn;
        return totalColumn;
     }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, Task.class);
    }

    /**
     * Update scheduler execution stats
     *
     * @param schedulerExecutionNs Time in NS which it took the scheduler to execute
     * @param schedulerWaitNs      Time in NS which the scheduler waited to be started
     */
    synchronized void addSchedulerCall(long schedulerExecutionNs, long schedulerWaitNs, String status) {
        m_schedulerStats.addCall(schedulerExecutionNs, schedulerWaitNs);
        m_schedulerStatus = status;
    }

    /**
     * Update procedure execution stats
     *
     * @param procedureExecutionNs Time in NS which it took the procedure to return a result
     * @param procedureWaitNs      Time in NS which the procedure waited to be started
     * @param failed               If {@code true} the number of procedure errors will be increased
     */
    synchronized void addProcedureCall(long procedureExecutionNs, long procedureWaitNs, boolean failed) {
        m_procedureStats.addCall(procedureExecutionNs, procedureWaitNs);
        if (failed) {
            ++m_procedureFailures;
        }
    }

    synchronized void setSchedulerStatus(String status) {
        m_schedulerStatus = status;
    }

    /**
     * @param state Updated state of the scheduler
     */
    synchronized void setState(String state) {
        m_state = state;
    }

    /**
     * Class to deduplicate the handling of the stats which are common between schedulers and procedures.
     */
    private static final class TimingStats {
        // Number of times the operation has been invoked
        private long m_invocations = 0;
        // Time in ns which the operation took to execute
        private long m_totalExecutionNs = 0;
        private long m_minExecutionNs = Long.MAX_VALUE;
        private long m_maxExecutionNs = 0;

        // Time between inserting operation runnable into executer to when execution started
        private long m_totalWaitNs = 0;
        private long m_minWaitNs = Long.MAX_VALUE;
        private long m_maxWaitNs = 0;

        private static long average(long value, long count) {
            return count == 0 ? 0 : value / count;
        }

        void addCall(long executionNs, long waitNs) {
            ++m_invocations;

            m_totalExecutionNs += executionNs;
            m_minExecutionNs = Math.min(m_minExecutionNs, executionNs);
            m_maxExecutionNs = Math.max(m_maxExecutionNs, executionNs);

            m_totalWaitNs += waitNs;
            m_minWaitNs = Math.min(m_minWaitNs, waitNs);
            m_maxWaitNs = Math.max(m_maxWaitNs, waitNs);
        }

        void pupulateStats(Object[] rowValues, int offset) {
            rowValues[offset++] = m_invocations;

            rowValues[offset++] = m_totalExecutionNs;
            rowValues[offset++] = m_minExecutionNs == Long.MAX_VALUE ? 0 : m_minExecutionNs;
            rowValues[offset++] = m_maxExecutionNs;
            rowValues[offset++] = average(m_totalExecutionNs, m_invocations);

            rowValues[offset++] = m_totalWaitNs;
            rowValues[offset++] = m_minWaitNs == Long.MAX_VALUE ? 0 : m_minWaitNs;
            rowValues[offset++] = m_maxWaitNs;
            rowValues[offset++] = average(m_totalWaitNs, m_invocations);
        }
    }

    /**
     * Simple class to hold the metadata needed to essentially convert a source column into a destination column.
     * Similar to {@code SELECT column2 AS column1}
     */
    private static final class ColumnAs {
        final int m_fromIndex;
        final VoltType m_type;
        final ColumnInfo m_toColumn;

        ColumnAs(VoltTable source, int fromIndex, String toName) {
            m_fromIndex = fromIndex;
            m_type = source.getColumnType(m_fromIndex);
            m_toColumn = new ColumnInfo(toName, m_type);
        }

        ColumnInfo getColumnInfo() {
            return m_toColumn;
        }
    }
}
