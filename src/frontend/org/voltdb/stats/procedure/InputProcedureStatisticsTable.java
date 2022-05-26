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

import org.voltdb.TableShorthand;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

/**
 * This class computes data for PROCEDUREINPUT @Statistics selector.
 */
public class InputProcedureStatisticsTable extends ProcedureStatisticsTable {

    private static final long BYTES_IN_MEGABYTE = 1024 * 1024;
    private static final VoltTable TABLE_TEMPLATE = TableShorthand.tableFromShorthand(
            "PROCEDURE_INPUT" +
            "(" +
            "TIMESTAMP:BIGINT," +
            "PROCEDURE:VARCHAR," +
            "WEIGHTED_PERC:BIGINT," +
            "INVOCATIONS:BIGINT," +
            "MIN_PARAMETER_SET_SIZE:BIGINT," +
            "MAX_PARAMETER_SET_SIZE:BIGINT," +
            "AVG_PARAMETER_SET_SIZE:BIGINT," +
            "TOTAL_PARAMETER_SET_SIZE_MB:BIGINT" +
            ")"
    );

    public InputProcedureStatisticsTable() {
        super(TABLE_TEMPLATE);
    }

    @Override
    public void updateTable(boolean shouldDeduplicate, String procedureName, VoltTableRow row) {
        updateTable(
                shouldDeduplicate,
                procedureName,
                row.getLong("PARTITION_ID"),
                row.getLong("TIMESTAMP"),
                row.getLong("INVOCATIONS"),
                row.getLong("MIN_PARAMETER_SET_SIZE"),
                row.getLong("MAX_PARAMETER_SET_SIZE"),
                row.getLong("AVG_PARAMETER_SET_SIZE"),
                0,
                0
        );
    }

    // Return table ordered by total bytes out
    public void fillSingleRow(VoltTable result, StatisticRow statRow, long totalInvocations) {
        long percentOfAllInvocations = calculatePercent(statRow.avg * statRow.invocations, totalInvocations);

        result.addRow(
                statRow.timestamp,
                statRow.procedure,
                percentOfAllInvocations,
                statRow.invocations,
                statRow.min,
                statRow.max,
                statRow.avg,
                (statRow.avg * statRow.invocations) / BYTES_IN_MEGABYTE
        );
    }
}
