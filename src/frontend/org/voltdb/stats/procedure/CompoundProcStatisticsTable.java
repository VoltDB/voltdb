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
package org.voltdb.stats.procedure;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.TableShorthand;

/**
 * Helper class for building compound procedure statistics
 * tables.
 *
 * We are presented with the rows of a PROCEDURE table,
 * as built by the ProcedureStatsCollector, that correspond
 * to compound procedures, one row per procedure per host.
 * We build a COMPOUNDPROC table where each row corresponds
 * with each of the rows we are given (i.e. there is no
 * actual aggregation in this case), but containing a subset
 * of the columns. Column headings may differ in order that
 * they are more appropriate to this case, for example the
 * use of "elapsed" rather than "execution" times.
 */
public class CompoundProcStatisticsTable {

    // This must match the template below, and is used by
    // the prometheus agent to get column names.
    public enum CompoundProcColumns {
        PROCEDURE       (VoltType.STRING),
        INVOCATIONS     (VoltType.BIGINT),
        MIN_ELAPSED     (VoltType.BIGINT),
        MAX_ELAPSED     (VoltType.BIGINT),
        AVG_ELAPSED     (VoltType.BIGINT),
        ABORTS          (VoltType.BIGINT),
        FAILURES        (VoltType.BIGINT);

        public final VoltType m_type;
        CompoundProcColumns(VoltType type) { m_type = type; }
    }

    // The template is what we actually use to construct our results
    // table. The duplication is unfortunate, but we have a clash of
    // conventions here.
    private static final VoltTable TABLE_TEMPLATE =
        TableShorthand.tableFromShorthand("COMPOUND_PROC_STATS" +
                                          " (TIMESTAMP:BIGINT, HOST_ID:INTEGER, HOSTNAME:STRING," +
                                          " PROCEDURE:VARCHAR, INVOCATIONS:BIGINT," +
                                          " MIN_ELAPSED:BIGINT, MAX_ELAPSED:BIGINT, AVG_ELAPSED:BIGINT," +
                                          " ABORTS:BIGINT, FAILURES:BIGINT)");

    // The statistics table we are constructing.
    private VoltTable result = TABLE_TEMPLATE.clone(0);

    // Update the result table with a subset of data from the
    // orignal VoltTable row. Caller already filtered out
    // non-compound-proc rows. Column names here are for the
    // input PROCEDURE stats, not the result COMPOUNDPROC stats.
    public void updateTable(VoltTableRow row) {
        result.addRow(row.getLong("TIMESTAMP"),
                      (int) row.getLong("HOST_ID"),
                      row.getString("HOSTNAME"),
                      row.getString("PROCEDURE"),
                      row.getLong("INVOCATIONS"),
                      row.getLong("MIN_EXECUTION_TIME"),
                      row.getLong("MAX_EXECUTION_TIME"),
                      row.getLong("AVG_EXECUTION_TIME"),
                      row.getLong("ABORTS"),
                      row.getLong("FAILURES"));
    }

    // Return table "sorted" - but we actually keep it in
    // the original order.
    public VoltTable getSortedTable() {
        return result;
    }
}
