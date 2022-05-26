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
 * This class computes data for PROCEDUREPROFILE @Statistics selector.
 */
public class ProcedureProfileStatisticsTable extends ProcedureStatisticsTable {

    private static final VoltTable TABLE_TEMPLATE = TableShorthand.tableFromShorthand(
            "EXECUTION_TIME" +
            "(" +
            "TIMESTAMP:BIGINT," +
            "PROCEDURE:VARCHAR," +
            "WEIGHTED_PERC:BIGINT," +
            "INVOCATIONS:BIGINT," +
            "AVG:BIGINT," +
            "MIN:BIGINT," +
            "MAX:BIGINT," +
            "ABORTS:BIGINT," +
            "FAILURES:BIGINT" +
            ")"
    );

    public ProcedureProfileStatisticsTable() {
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
                row.getLong("MIN_EXECUTION_TIME"),
                row.getLong("MAX_EXECUTION_TIME"),
                row.getLong("AVG_EXECUTION_TIME"),
                row.getLong("FAILURES"),
                row.getLong("ABORTS")
        );
    }

    // Return table sorted by weighted avg
    public void fillSingleRow(VoltTable result, StatisticRow statRow, long totalInvocations) {
        long percentOfAllInvocations = calculatePercent(statRow.avg * statRow.invocations, totalInvocations);

        result.addRow(
                statRow.timestamp,
                statRow.procedure,
                percentOfAllInvocations,
                statRow.invocations,
                statRow.avg,
                statRow.min,
                statRow.max,
                statRow.aborts,
                statRow.failures
        );
    }
}
