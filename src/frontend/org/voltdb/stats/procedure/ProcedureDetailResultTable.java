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
package org.voltdb.stats.procedure;

import java.util.ArrayList;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/**
 * This class is used to re-arrange the output of the PROCEDUREDETAIL selector.
 * It orders the output by:
 * procedure_name, statement_name, host_id, site_id, partition_id, timestamp (descending);
 */
public class ProcedureDetailResultTable {

    private final VoltTable m_sortedResultTable = new VoltTable(
            new ColumnInfo("TIMESTAMP", VoltType.BIGINT),
            new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
            new ColumnInfo("HOSTNAME", VoltType.STRING),
            new ColumnInfo(VoltSystemProcedure.CNAME_SITE_ID, VoltSystemProcedure.CTYPE_ID),
            new ColumnInfo("PARTITION_ID", VoltType.INTEGER),
            new ColumnInfo("PROCEDURE", VoltType.STRING),
            new ColumnInfo("STATEMENT", VoltType.STRING),
            new ColumnInfo("INVOCATIONS", VoltType.BIGINT),
            new ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT),
            new ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT),
            new ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT),
            new ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT),
            new ColumnInfo("MIN_RESULT_SIZE", VoltType.INTEGER),
            new ColumnInfo("MAX_RESULT_SIZE", VoltType.INTEGER),
            new ColumnInfo("AVG_RESULT_SIZE", VoltType.INTEGER),
            new ColumnInfo("MIN_PARAMETER_SET_SIZE", VoltType.INTEGER),
            new ColumnInfo("MAX_PARAMETER_SET_SIZE", VoltType.INTEGER),
            new ColumnInfo("AVG_PARAMETER_SET_SIZE", VoltType.INTEGER),
            new ColumnInfo("ABORTS", VoltType.BIGINT),
            new ColumnInfo("FAILURES", VoltType.BIGINT));

    public ProcedureDetailResultTable(VoltTable table) {
        assert (table != null);

        ArrayList<ProcedureDetailResultRow> m_rows = new ArrayList<>(table.getRowCount());
        table.resetRowPosition();
        while (table.advanceRow()) {
            m_rows.add(
                    new ProcedureDetailResultRow(
                            table.getLong("TIMESTAMP"),
                            table.getLong(VoltSystemProcedure.CNAME_HOST_ID),
                            table.getString("HOSTNAME"),
                            table.getLong(VoltSystemProcedure.CNAME_SITE_ID),
                            table.getLong("PARTITION_ID"),
                            table.getString("PROCEDURE"),
                            table.getString("STATEMENT"),
                            table.getLong("INVOCATIONS"),
                            table.getLong("TIMED_INVOCATIONS"),
                            table.getLong("MIN_EXECUTION_TIME"),
                            table.getLong("MAX_EXECUTION_TIME"),
                            table.getLong("AVG_EXECUTION_TIME"),
                            table.getLong("MIN_RESULT_SIZE"),
                            table.getLong("MAX_RESULT_SIZE"),
                            table.getLong("AVG_RESULT_SIZE"),
                            table.getLong("MIN_PARAMETER_SET_SIZE"),
                            table.getLong("MAX_PARAMETER_SET_SIZE"),
                            table.getLong("AVG_PARAMETER_SET_SIZE"),
                            table.getLong("ABORTS"),
                            table.getLong("FAILURES")
                    )
            );
        }
        m_rows.sort(ProcedureDetailResultRow::compareTo);

        for (ProcedureDetailResultRow row : m_rows) {
            m_sortedResultTable.addRow(row.m_timestamp,
                                       row.m_hostId,
                                       row.m_hostName,
                                       row.m_siteId,
                                       row.m_partitionId,
                                       row.m_procedure,
                                       row.m_statement,
                                       row.m_invocations,
                                       row.m_timedInvocations,
                                       row.m_minExecutionTime,
                                       row.m_maxExecutionTime,
                                       row.m_avgExecutionTime,
                                       row.m_minResultSize,
                                       row.m_maxResultSize,
                                       row.m_avgResultSize,
                                       row.m_minParameterSetSize,
                                       row.m_maxParameterSetSize,
                                       row.m_avgParameterSetSize,
                                       row.m_aborts,
                                       row.m_failures);
        }
    }

    public VoltTable[] getSortedResultTable() {
        return new VoltTable[]{m_sortedResultTable};
    }

    private static class ProcedureDetailResultRow implements Comparable<ProcedureDetailResultRow> {
        long m_timestamp, m_hostId, m_siteId, m_partitionId;
        String m_hostName, m_procedure, m_statement;
        long m_invocations, m_timedInvocations, m_aborts, m_failures;
        long m_minExecutionTime, m_maxExecutionTime, m_avgExecutionTime;
        long m_minResultSize, m_maxResultSize, m_avgResultSize;
        long m_minParameterSetSize, m_maxParameterSetSize, m_avgParameterSetSize;

        public ProcedureDetailResultRow(long timestamp, long hostId, String hostName,
                                        long siteId, long partitionId, String procedure, String statement,
                                        long invocations, long timedInvocations,
                                        long minExecutionTime, long maxExecutionTime, long avgExecutionTime,
                                        long minResultSize, long maxResultSize, long avgResultSize,
                                        long minParameterSetSize, long maxParameterSetSize, long avgParameterSetSize,
                                        long aborts, long failures) {
            m_timestamp = timestamp;
            m_hostId = hostId;
            m_hostName = hostName;
            m_siteId = siteId;
            m_partitionId = partitionId;
            m_procedure = procedure;
            m_statement = statement;
            m_invocations = invocations;
            m_timedInvocations = timedInvocations;
            m_minExecutionTime = minExecutionTime;
            m_maxExecutionTime = maxExecutionTime;
            m_avgExecutionTime = avgExecutionTime;
            m_minResultSize = minResultSize;
            m_maxResultSize = maxResultSize;
            m_avgResultSize = avgResultSize;
            m_minParameterSetSize = minParameterSetSize;
            m_maxParameterSetSize = maxParameterSetSize;
            m_avgParameterSetSize = avgParameterSetSize;
            m_aborts = aborts;
            m_failures = failures;
        }

        @Override
        public int compareTo(ProcedureDetailResultRow other) {
            long diff = m_procedure.compareTo(other.m_procedure);
            if (diff != 0) {return Long.signum(diff);}

            diff = m_statement.compareTo(other.m_statement);
            if (diff != 0) {return Long.signum(diff);}

            diff = m_hostId - other.m_hostId;
            if (diff != 0) {return Long.signum(diff);}

            diff = m_siteId - other.m_siteId;
            if (diff != 0) {return Long.signum(diff);}

            diff = m_partitionId - other.m_partitionId;
            if (diff != 0) {return Long.signum(diff);}

            diff = other.m_timestamp - m_timestamp;
            if (diff != 0) {return Long.signum(diff);}

            return 0;
        }
    }
}
