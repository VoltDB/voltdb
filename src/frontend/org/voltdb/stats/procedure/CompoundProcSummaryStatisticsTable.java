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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.TableShorthand;

/**
 * Helper class for aggregation of compound procedure
 * summary statistics.
 */
public class CompoundProcSummaryStatisticsTable {

    private static final VoltTable TABLE_TEMPLATE =
        TableShorthand.tableFromShorthand("COMPOUND_PROC_SUMMARY" +
                                          " (TIMESTAMP:BIGINT, PROCEDURE:VARCHAR, INVOCATIONS:BIGINT," +
                                          " MIN_ELAPSED:BIGINT, MAX_ELAPSED:BIGINT, AVG_ELAPSED:BIGINT," +
                                          " ABORTS:BIGINT, FAILURES:BIGINT)");

    // ProcRows keyed by procedure name
    Map<String,ProcRow> rowMap = new HashMap<>();

    // One row (procedure) of min/max/avg data aggregated across hosts
    static class ProcRow {
        long timestamp;
        String procedure;
        long invocations;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        double avg;
        long aborts;
        long failures;

        ProcRow(long ts, String proc) {
            timestamp = ts;
            procedure = proc;
        }

        void update(long invocations, long min, long max, long avg, long aborts,  long failures) {
            this.avg = updatedAverage(avg, invocations); // before other updates!
            this.invocations += invocations;
            this.min = Math.min(this.min, min);
            this.max = Math.max(this.max, max);
            this.aborts += aborts;
            this.failures += failures;
        }

        private double updatedAverage(double avg, long invocations) {
            double currTime = this.avg * this.invocations;
            double rowTime = avg * invocations;
            long totalInv = this.invocations + invocations;
            return totalInv != 0 ? (currTime + rowTime) / totalInv : 0;
        }
    }

    // Add or update the corresponding internal ProcRow
    // with a subset of data from the orignal VoltTable row
    public void updateTable(VoltTableRow row) {
        updateTable(row.getLong("TIMESTAMP"),
                    row.getString("PROCEDURE"),
                    row.getLong("INVOCATIONS"),
                    row.getLong("MIN_EXECUTION_TIME"),
                    row.getLong("MAX_EXECUTION_TIME"),
                    row.getLong("AVG_EXECUTION_TIME"),
                    row.getLong("ABORTS"),
                    row.getLong("FAILURES"));
    }

    // Package access for unit test
    void updateTable(long timestamp, String proc, long invocations,
                     long min, long max, long avg, long aborts, long failures) {
        ProcRow row = rowMap.computeIfAbsent(proc, k -> new ProcRow(timestamp, proc));
        row.update(invocations, min, max, avg, aborts, failures);
    }

    // Return table sorted by average elapsed time
    public VoltTable getSortedTable() {
        List<ProcRow> sorted = new ArrayList<>(rowMap.values());
        sorted.sort((lhs, rhs) -> -Double.compare(lhs.avg * lhs.invocations, rhs.avg * rhs.invocations));
        VoltTable result = TABLE_TEMPLATE.clone(0);
        for (ProcRow row : sorted) {
            result.addRow(row.timestamp, row.procedure, row.invocations,
                          row.min, row.max, row.avg, row.aborts, row.failures);
        }
        return result;
    }
}
