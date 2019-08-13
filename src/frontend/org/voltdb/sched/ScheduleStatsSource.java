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
import java.util.Iterator;

import org.voltdb.StatsSource;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

/**
 * {@link StatsSource} used by {@link SchedulerManager}. A single instance of this stats source will be created for each
 * running instance of a Scheduler.
 */
class ScheduleStatsSource extends StatsSource {
    static final Collection<ColumnInfo> s_columns = ImmutableList.of(new ColumnInfo("NAME", VoltType.STRING),
            new ColumnInfo("STATE", VoltType.STRING),
            new ColumnInfo("SCOPE", VoltType.STRING),
            new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
            new ColumnInfo("PARTITION_ID", VoltType.INTEGER),
            new ColumnInfo("SCHEDULER_INVOCATIONS", VoltType.BIGINT),
            new ColumnInfo("SCHEDULER_TOTAL_EXECUTION", VoltType.BIGINT),
            new ColumnInfo("SCHEDULER_AVG_EXECUTION", VoltType.BIGINT),
            new ColumnInfo("SCHEDULER_TOTAL_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo("SCHEDULER_AVG_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo("PROCEDURE_INVOCATIONS", VoltType.BIGINT),
            new ColumnInfo("PROCEDURE_TOTAL_EXECUTION", VoltType.BIGINT),
            new ColumnInfo("PROCEDURE_AVG_EXECUTION", VoltType.BIGINT),
            new ColumnInfo("PROCEDURE_TOTAL_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo("PROCEDURE_AVG_WAIT_TIME", VoltType.BIGINT),
            new ColumnInfo("PROCEDURE_ERRORS", VoltType.BIGINT));

    private final String m_name;
    private final String m_scope;
    private final int m_hostId;
    private final int m_partitionId;

    private String m_state;

    // Stats for scheduler execution
    // Number of times the scheduler has been invoked
    private long m_schedulerInvocations = 0;
    // Time in ns which the scheduler took to execute
    private long m_schedulerTotalExecutionNs = 0;
    // Time between inserting scheduler runnable into executer to when execution started
    private long m_schedulerTotalWaitNs = 0;

    // Stats for procedure executions
    // Number of times a procedure was invoked
    private long m_procedureInvocations = 0;
    // Total time it took from starting the procedure to returning the result
    private long m_procedureTotalExecutionNs = 0;
    // Total time it took from when procedure should have started to when it actually started
    private long m_procedureTotalWaitNs = 0;
    private long m_procedureErrors = 0;

    ScheduleStatsSource(String name, String scope, int hostId, int partitionId) {
        super(false);
        m_name = name;
        m_scope = scope;
        m_hostId = hostId;
        m_partitionId = partitionId;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return Iterators.singletonIterator(null);
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        int column = 0;

        // Header state info
        rowValues[column++] = m_name;
        rowValues[column++] = m_state;
        rowValues[column++] = m_scope;
        rowValues[column++] = m_hostId;
        rowValues[column++] = m_partitionId;

        // Scheduler stats
        column = addStates(rowValues, column, m_schedulerInvocations, m_schedulerTotalExecutionNs,
                m_schedulerTotalWaitNs);

        // Procedure stats
        column = addStates(rowValues, column, m_procedureInvocations, m_procedureTotalExecutionNs,
                m_procedureTotalWaitNs);

        rowValues[column++] = m_procedureErrors;

        assert column == s_columns.size() : "Column count off should be " + s_columns.size() + " inserted " + column;
     }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        columns.addAll(s_columns);
    }

    /**
     * Update scheduler execution stats
     *
     * @param schedulerExecutionNs Time in NS which it took the scheduler to execute
     * @param schedulerWaitNs      Time in NS which the scheduler waited to be started
     */
    synchronized void addSchedulerCall(long schedulerExecutionNs, long schedulerWaitNs) {
        ++m_schedulerInvocations;
        m_schedulerTotalExecutionNs += schedulerExecutionNs;
        m_schedulerTotalWaitNs += schedulerWaitNs;
    }

    /**
     * Update procedure execution stats
     *
     * @param procedureExecutionNs Time in NS which it took the procedure to return a result
     * @param procedureWaitNs      Time in NS which the procedure waited to be started
     * @param failed               If {@code true} the number of procedure errors will be increased
     */
    synchronized void addProcedureCall(long procedureExecutionNs, long procedureWaitNs, boolean failed) {
        ++m_procedureInvocations;
        m_procedureTotalExecutionNs += procedureExecutionNs;
        m_procedureTotalWaitNs += procedureWaitNs;
        if (failed) {
            ++m_procedureErrors;
        }
    }

    /**
     * @param state Updated state of the scheduler
     */
    synchronized void setState(String state) {
        m_state = state;
    }

    private static int addStates(Object[] rowValues, int column, long invocations, long executionTime, long waitTime) {
        rowValues[column++] = invocations;
        rowValues[column++] = executionTime;
        rowValues[column++] = average(executionTime, invocations);
        rowValues[column++] = waitTime;
        rowValues[column++] = average(waitTime, invocations);
        return column;
    }

    private static long average(long value, long count) {
        return count == 0 ? 0 : value / count;
    }
}
