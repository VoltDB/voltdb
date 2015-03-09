/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class StatsProcOutputTable
{

    // A table of ProcOutputRows: set of unique procedure names
    TreeSet<ProcOutputRow> m_rowsTable = new TreeSet<ProcOutputRow>();

    // A row for a procedure on a single host aggregating invocations and
    // min/max/avg bytes I/O across partitions
    static class ProcOutputRow implements Comparable<ProcOutputRow>
    {
        String procedure;
        long partition;
        long timestamp;
        long invocations;

        long minOUT;
        long maxOUT;
        long avgOUT;

        // track which partitions and hosts have been witnessed.
        private final Set<Long> seenPartitions;

        public ProcOutputRow(String procedure, long partition, long timestamp,
            long invocations, long minOUT, long maxOUT, long avgOUT)
        {
            this.procedure = procedure;
            this.partition = partition;
            this.timestamp = timestamp;
            this.invocations = invocations;
            this.minOUT = minOUT;
            this.maxOUT = maxOUT;
            this.avgOUT = avgOUT;

            seenPartitions = new TreeSet<Long>();
            seenPartitions.add(partition);

        }

        @Override
        public int compareTo(ProcOutputRow other)
        {
            return procedure.compareTo(other.procedure);

        }

        // Augment this ProcOutputRow with a new input row
        // dedup flag indicates if we should dedup data based on partition for proc.
        void updateWith(boolean dedup, ProcOutputRow in)        {
            this.avgOUT = calculateAverage(this.avgOUT, this.invocations,
                in.avgOUT, in.invocations);
            this.minOUT = Math.min(this.minOUT, in.minOUT);
            this.maxOUT = Math.max(this.maxOUT, in.maxOUT);

            if (!dedup) {
                //Not deduping so add up all values.
                this.invocations += in.invocations;
            } else {
                if (!seenPartitions.contains(in.partition)) {
                    this.invocations += in.invocations;
                    seenPartitions.add(in.partition);
                }
            }
        }
    }

    /**
     * Given a running average and the running invocation total as well as a new
     * row's average and invocation total, return a new running average
     */
    static long calculateAverage(long currAvg, long currInvoc, long rowAvg, long rowInvoc)
    {
        long currTtl = currAvg * currInvoc;
        long rowTtl = rowAvg * rowInvoc;

        // If both are 0, then currTtl, rowTtl are also 0.
        if ((currInvoc + rowInvoc) == 0L) {
            return 0L;
        } else {
            return (currTtl + rowTtl) / (currInvoc + rowInvoc);
        }
    }

    /**
     * Safe division that assumes x/0 = 100%
     */
    static long calculatePercent(long num, long denom)
    {
        if (denom == 0L) {
            return 100L;
        } else {
            return Math.round(100.0 * num / denom);
        }
    }

    // Sort by total bytes out
    public int compareByOutput(ProcOutputRow r1, ProcOutputRow r2)
    {
        if (r1.avgOUT * r1.invocations > r2.avgOUT * r2.invocations) {
            return 1;
        } else if (r1.avgOUT * r1.invocations < r2.avgOUT * r2.invocations) {
            return -1;
        } else {
            return 0;
        }
    }

    // Add or update the corresponding row. dedup flag indicates if we should dedup data based on partition for proc.
    public void updateTable(boolean dedup, String procedure, long partition, long timestamp,
            long invocations, long minOUT, long maxOUT, long avgOUT)
    {
        ProcOutputRow in = new ProcOutputRow(procedure, partition, timestamp,
            invocations, minOUT, maxOUT, avgOUT);
        ProcOutputRow exists = m_rowsTable.ceiling(in);
        if (exists != null && in.procedure.equals(exists.procedure)) {
            exists.updateWith(dedup, in);
        } else {
            m_rowsTable.add(in);
        }
    }

    // Return table ordered by total bytes out
    public VoltTable sortByOutput(String tableName)
    {
        List<ProcOutputRow> sorted = new ArrayList<ProcOutputRow>(m_rowsTable);
        Collections.sort(sorted, new Comparator<ProcOutputRow>() {
            @Override
            public int compare(ProcOutputRow r1, ProcOutputRow r2) {
                return compareByOutput(r2, r1); // sort descending
            }
        });

        long totalOutput = 0L;
        for (ProcOutputRow row : sorted) {
            totalOutput += (row.avgOUT * row.invocations);
        }

        int kB = 1024;
        int mB = 1024 * kB;
        int gB = 1024 * mB;

        VoltTable result = TableShorthand.tableFromShorthand(
            tableName +
            "(TIMESTAMP:BIGINT," +
                "PROCEDURE:VARCHAR," +
                "WEIGHTED_PERC:BIGINT," +
                "INVOCATIONS:BIGINT," +
                "MIN_RESULT_SIZE:BIGINT," +
                "MAX_RESULT_SIZE:BIGINT," +
                "AVG_RESULT_SIZE:BIGINT," +
                "TOTAL_RESULT_SIZE_MB:BIGINT)"
                );

        for (ProcOutputRow row : sorted) {
            result.addRow(
                row.timestamp,
                row.procedure,
                calculatePercent((row.avgOUT * row.invocations), totalOutput), //% total out
                row.invocations,
                row.minOUT,
                row.maxOUT,
                row.avgOUT,
                (row.avgOUT * row.invocations) / mB //total out
                );
        }
        return result;
    }
}