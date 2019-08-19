/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

package org.voltdb.sched;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.StatsSource;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Iterators;

/**
 * {@link StatsSource} used by {@link SchedulerManager}. A single instance of this stats source will be created for each
 * running instance of a Scheduler.
 */
public class ScheduleStatsSource extends StatsSource {
    private static final String PREFIX_SCHEDULER = "SCHEDULER_";
    private static final String PREFIX_PROCEDURE = "PROCEDURE_";

    private static final Collection<ColumnInfo> s_columns = ImmutableList.of(new ColumnInfo("NAME", VoltType.STRING),
            new ColumnInfo("STATE", VoltType.STRING),
            new ColumnInfo("SCOPE", VoltType.STRING),
            new ColumnInfo(VoltSystemProcedure.CNAME_PARTITION_ID, VoltSystemProcedure.CTYPE_ID),
            new ColumnInfo(PREFIX_SCHEDULER + "INVOCATIONS", VoltType.BIGINT),
            new ColumnInfo(PREFIX_SCHEDULER + "TOTAL_EXECUTION", VoltType.BIGINT),
            new ColumnInfo(PREFIX_SCHEDULER + "MIN_EXECUTION", VoltType.BIGINT),
            new ColumnInfo(PREFIX_SCHEDULER + "MAX_EXECUTION", VoltType.BIGINT),
            new ColumnInfo(PREFIX_SCHEDULER + "AVG_EXECUTION", VoltType.BIGINT),
            new ColumnInfo(PREFIX_SCHEDULER + "TOTAL_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo(PREFIX_SCHEDULER + "MIN_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo(PREFIX_SCHEDULER + "MAX_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo(PREFIX_SCHEDULER + "AVG_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo(PREFIX_SCHEDULER + "STATUS", VoltType.STRING),
            new ColumnInfo(PREFIX_PROCEDURE + "INVOCATIONS", VoltType.BIGINT),
            new ColumnInfo(PREFIX_PROCEDURE + "TOTAL_EXECUTION", VoltType.BIGINT),
            new ColumnInfo(PREFIX_PROCEDURE + "MIN_EXECUTION", VoltType.BIGINT),
            new ColumnInfo(PREFIX_PROCEDURE + "MAX_EXECUTION", VoltType.BIGINT),
            new ColumnInfo(PREFIX_PROCEDURE + "AVG_EXECUTION", VoltType.BIGINT),
            new ColumnInfo(PREFIX_PROCEDURE + "TOTAL_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo(PREFIX_PROCEDURE + "MIN_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo(PREFIX_PROCEDURE + "MAX_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo(PREFIX_PROCEDURE + "AVG_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo(PREFIX_PROCEDURE + "FAILURES", VoltType.BIGINT));

    // Metadata to basically select a subset of columns from the full stats
    private static List<ColumnAs> s_schedulersConvert;
    private static List<ColumnAs> s_procedursConvert;

    private final String m_name;
    private final String m_scope;
    private final int m_partitionId;

    private String m_state;

    // Stats for scheduler execution
    private final TimingStats m_schedulerStats;
    private String m_schedulerStatus;

    // Stats for procedure executions
    private final TimingStats m_procedureStats;
    private long m_procedureFailures = 0;

    /**
     * Convert stats collected under {@link StatsSelector#SCHEDULES} to either {@link StatsSelector#SCHEDULERS} or
     * {@link StatsSelector#SCHEDULED_PROCEDURES}
     *
     * @param subselector to format to convert to
     * @param tables      generated from the stats colleciton
     */
    public static void convert(StatsSelector subselector, VoltTable[] tables) {
        assert tables.length == 1;

        List<ColumnAs> columnConversion;
        VoltTable source = tables[0];

        switch (subselector) {
        case SCHEDULED_PROCEDURES:
            columnConversion = getProceduresConverter(source);
            break;
        case SCHEDULERS:
            columnConversion = getSchedulersConverter(source);
            break;
        case SCHEDULES:
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
            s_schedulersConvert = initializeConverter(source, PREFIX_SCHEDULER, PREFIX_PROCEDURE);
        }
        return s_schedulersConvert;
    }

    private static List<ColumnAs> getProceduresConverter(VoltTable source) {
        if (s_procedursConvert == null) {
            s_procedursConvert = initializeConverter(source, PREFIX_PROCEDURE, PREFIX_SCHEDULER);
        }
        return s_procedursConvert;
    }

    /**
     * @param source       {@link VoltTable} which will be the source of the stats
     * @param prefixRemove If a column has this prefix the column will be kept but the prefix will be removed from the
     *                     name
     * @param prefixSkip   If a column has this prefix it will be skipped
     * @return {@link List} of {@link ColumnAs} for selecting out desired stats
     */
    private static List<ColumnAs> initializeConverter(VoltTable source, String prefixRemove, String prefixSkip) {
        ImmutableList.Builder<ColumnAs> builder = ImmutableList.<ColumnAs>builder();

        for (int i = 0; i < source.getColumnCount(); ++i) {
            String columnName = source.getColumnName(i);
            if (columnName.startsWith(prefixSkip)) {
                continue;
            }
            if (columnName.startsWith(prefixRemove)) {
                columnName = columnName.substring(prefixRemove.length());
            }
            builder.add(new ColumnAs(source, i, columnName));
        }

        return builder.build();
    }

    static ScheduleStatsSource createDummy() {
        return new ScheduleStatsSource(null, null, -1);
    }

    static ScheduleStatsSource create(String name, String scope, int partitionId) {
        return new ScheduleStatsSource(Objects.requireNonNull(name), Objects.requireNonNull(scope), partitionId);
    }

    private ScheduleStatsSource(String name, String scope, int partitionId) {
        super(false);
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
        agent.registerStatsSource(StatsSelector.SCHEDULES, m_partitionId, this);
    }

    void deregister(StatsAgent agent) {
        agent.deregisterStatsSource(StatsSelector.SCHEDULES, m_partitionId, this);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return m_name == null ? Collections.emptyIterator() : Iterators.singletonIterator(null);
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        super.updateStatsRow(rowKey, rowValues);
        int column = 3;

        // Header state info
        rowValues[column++] = m_name;
        rowValues[column++] = m_state;
        rowValues[column++] = m_scope;
        rowValues[column++] = m_partitionId;

        // Scheduler stats
        column = m_schedulerStats.pupulateStats(rowValues, column);
        rowValues[column++] = m_schedulerStatus;

        // Procedure stats
        column = m_procedureStats.pupulateStats(rowValues, column);

        rowValues[column++] = m_procedureFailures;

        assert column == rowValues.length : "Column count off should be " + rowValues.length + " inserted " + column;
     }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.addAll(s_columns);
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

        int pupulateStats(Object[] rowValues, int column) {
            rowValues[column++] = m_invocations;

            rowValues[column++] = m_totalExecutionNs;
            rowValues[column++] = m_minExecutionNs == Long.MAX_VALUE ? 0 : m_minExecutionNs;
            rowValues[column++] = m_maxExecutionNs;
            rowValues[column++] = average(m_totalExecutionNs, m_invocations);

            rowValues[column++] = m_totalWaitNs;
            rowValues[column++] = m_minWaitNs == Long.MAX_VALUE ? 0 : m_minWaitNs;
            rowValues[column++] = m_maxWaitNs;
            rowValues[column++] = average(m_totalWaitNs, m_invocations);
            return column;
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
