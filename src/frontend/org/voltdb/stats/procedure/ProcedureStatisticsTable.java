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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

public abstract class ProcedureStatisticsTable {

    private final VoltTable tableTemplate;

    // A table of StatisticRows: set of unique procedure names
    protected TreeSet<StatisticRow> rowsTable = new TreeSet<>();

    protected ProcedureStatisticsTable(VoltTable tableTemplate) {
        this.tableTemplate = tableTemplate;
    }

    // One row (procedure) of min/max/avg data aggregated across sites and hosts
    static class StatisticRow implements Comparable<StatisticRow> {
        String procedure;
        long partition;
        long timestamp;
        long invocations;

        long min;
        long max;
        double avg;
        long failures;
        long aborts;

        // track which partitions have been witnessed.
        private final Set<Long> seenPartitions;

        public StatisticRow(
                String procedure,
                long partition,
                long timestamp,
                long invocations,
                long min,
                long max,
                long avg,
                long failures,
                long aborts
        ) {
            this.procedure = procedure;
            this.partition = partition;
            this.timestamp = timestamp;
            this.invocations = invocations;
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.failures = failures;
            this.aborts = aborts;

            seenPartitions = new TreeSet<>();
            seenPartitions.add(partition);
        }

        @Override
        public int compareTo(StatisticRow other) {
            return procedure.compareTo(other.procedure);
        }

        // Augment this StatisticRow with a new input row.
        // dedup flag indicates if we should dedup data based on partition for proc.
        void updateWith(boolean dedup, StatisticRow in) {
            // adjust the min, max and avg across all replicas.
            this.avg = calculateAverage(
                    this.avg,
                    this.invocations,
                    in.avg,
                    in.invocations
            );

            this.min = Math.min(this.min, in.min);
            this.max = Math.max(this.max, in.max);

            if (!dedup) {
                //Not deduping so add up all values.
                this.invocations += in.invocations;
                this.failures += in.failures;
                this.aborts += in.aborts;
            } else {
                // invocations, failures and aborts per-logical-partition
                if (!seenPartitions.contains(in.partition)) {
                    this.invocations += in.invocations;
                    this.failures += in.failures;
                    this.aborts += in.aborts;
                    seenPartitions.add(in.partition);
                }
            }
        }
    }

    /**
     * Given a running average and the running invocation total as well as a new
     * row's average and invocation total, return a new running average
     */
    static double calculateAverage(
            double currentAverage,
            long currentInvocations,
            double rowAverage,
            long rowInvocations
    ) {
        double currentTotal = currentAverage * currentInvocations;
        double rowTotal = rowAverage * rowInvocations;

        // If both are 0, then currentTotal, rowTotal are also 0.
        if ((currentInvocations + rowInvocations) == 0L) {
            return 0L;
        }

        return (currentTotal + rowTotal) / (currentInvocations + rowInvocations);
    }

    /**
     * Safe division that assumes x/0 = 100%
     */
    static long calculatePercent(double numerator, long denominator) {
        if (denominator == 0L) {
            return 100L;
        }

        return Math.round(100.0 * numerator / denominator);
    }

    // Sort by average, weighting the sampled average by the real invocation count.
    public int compare(StatisticRow lhs, StatisticRow rhs) {
        return Double.compare(lhs.avg * lhs.invocations, rhs.avg * rhs.invocations);
    }

    public abstract void updateTable(boolean shouldDeduplicate, String procedureName, VoltTableRow row);

    // Add or update the corresponding row. dedup flag indicates if we should dedup data based on partition for proc.
    public void updateTable(
            boolean dedup,
            String procedure,
            long partition,
            long timestamp,
            long invocations,
            long min,
            long max,
            long avg,
            long failures,
            long aborts
    ) {
        StatisticRow in = new StatisticRow(procedure, partition, timestamp,
                                           invocations, min, max, avg, failures,
                                           aborts);
        StatisticRow exists = rowsTable.ceiling(in);
        if (exists != null && in.procedure.equals(exists.procedure)) {
            exists.updateWith(dedup, in);
        } else {
            rowsTable.add(in);
        }
    }

    // Return table sorted by weighted avg
    public VoltTable getSortedTable() {
        List<StatisticRow> sortedStats = new ArrayList<>(rowsTable);
        sortedStats.sort((lhs, rhs) -> {
            return compare(rhs, lhs);  // sort desc
        });

        long totalInvocations = 0L;
        for (StatisticRow row : sortedStats) {
            totalInvocations += (row.avg * row.invocations);
        }

        VoltTable result = tableTemplate.clone(0);
        for (StatisticRow statRow : sortedStats) {
            fillSingleRow(result, statRow, totalInvocations);
        }

        return result;
    }

    public abstract void fillSingleRow(VoltTable result, StatisticRow statRow, long totalInvocations);
}
