/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.stats.procedure;

import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;
import org.voltdb.VoltTable;

public class TestProcedureProfileProcedureStatisticsTable {

    // result row in java form for test
    static class ResultRow {
        long timestamp;
        String procedure;
        long weighted_perc;
        long invocations;
        long avg;
        long min;
        long max;
        long aborts;
        long failures;

        public ResultRow(long timestamp, String procedure, long weighted_perc,
                         long invocations, long avg, long min, long max, long aborts,
                         long failures) {
            this.timestamp = timestamp;
            this.procedure = procedure;
            this.weighted_perc = weighted_perc;
            this.invocations = invocations;
            this.avg = avg;
            this.min = min;
            this.max = max;
            this.aborts = aborts;
            this.failures = failures;
        }
    }

    // push rows from data in to the table.
    void loadData(ProcedureProfileStatisticsTable dut, ProcedureStatisticsTable.StatisticRow[] data) {
        for (ProcedureStatisticsTable.StatisticRow row : data) {
            dut.updateTable(
                    true,
                    row.procedure,
                    row.partition,
                    row.timestamp,
                    row.invocations,
                    row.min,
                    row.max,
                    (long) row.avg,
                    row.failures,
                    row.aborts
            );
        }
    }

    // push rows from data in to the table.
    void loadDataNoDedup(ProcedureProfileStatisticsTable dut, ProcedureStatisticsTable.StatisticRow[] data) {
        for (ProcedureStatisticsTable.StatisticRow row : data) {
            dut.updateTable(
                    false,
                    row.procedure,
                    row.partition,
                    row.timestamp,
                    row.invocations,
                    row.min,
                    row.max,
                    (long) row.avg,
                    row.failures,
                    row.aborts
            );
        }
    }

    // validate contents of sorted dut vs. expectation of ResultRow[]
    void assertEquals(String testname, ProcedureProfileStatisticsTable dut, ResultRow[] data) {
        VoltTable actual = dut.getSortedTable();
        Assert.assertEquals(testname + " has wrong number of result rows in test.",
                            actual.getRowCount(), data.length);

        for (ResultRow row : data) {
            assertTrue(actual.advanceRow());
            System.out.printf("%s: validating row %d\n", testname, actual.getActiveRowIndex());

            Assert.assertEquals(row.timestamp, actual.getLong("TIMESTAMP"));
            Assert.assertEquals(row.procedure, actual.getString("PROCEDURE"));
            Assert.assertEquals(row.weighted_perc, actual.getLong("WEIGHTED_PERC"));
            Assert.assertEquals(row.invocations, actual.getLong("INVOCATIONS"));
            Assert.assertEquals(row.avg, actual.getLong("AVG"));
            Assert.assertEquals(row.min, actual.getLong("MIN"));
            Assert.assertEquals(row.max, actual.getLong("MAX"));
            Assert.assertEquals(row.aborts, actual.getLong("ABORTS"));
            Assert.assertEquals(row.failures, actual.getLong("FAILURES"));
        }
    }

    @Test
    public void testBaseCase() {
        // Given
        // validate sensical round-trip of one row.
        ProcedureStatisticsTable.StatisticRow[] data = {
                new ProcedureStatisticsTable.StatisticRow("A", 0L, 1371587140278L, 100L, 1L, 2L, 3L, 4L, 5L)
        };

        ResultRow[] result = {
                new ResultRow(1371587140278L, "A", 100L, 100L, 3L, 1L, 2L, 5L, 4L)
        };

        // When
        ProcedureProfileStatisticsTable dut = new ProcedureProfileStatisticsTable();
        loadData(dut, data);

        // Then
        assertEquals("testBaseCase", dut, result);
    }

    @Test
    public void testAllZeros() {
        // Given
        // validate paranoia about an all zero row - just in case.
        ProcedureStatisticsTable.StatisticRow[] data = {
                new ProcedureStatisticsTable.StatisticRow("B", 0L, 1371587140278L, 0L, 0L, 0L, 0L, 0L, 0L)
        };

        ResultRow[] result = {
                new ResultRow(1371587140278L, "B", 100L, 0L, 0L, 0L, 0L, 0L, 0L)
        };

        // When
        ProcedureProfileStatisticsTable dut = new ProcedureProfileStatisticsTable();
        loadData(dut, data);

        // Then
        assertEquals("testAllZeros", dut, result);
    }

    @Test
    public void testMultipleProcs() {
        // Given
        // 2 procs, 2 partitions - make sure min,max,avg,wtd works
        ProcedureStatisticsTable.StatisticRow[] data = {
                //                          TS/Proc/Part/invok/min/max/avg/fail/abort
                new ProcedureStatisticsTable.StatisticRow("B", 0L, 1371587140278L, 100L, 2L, 5L, 4L, 17L, 18L),
                new ProcedureStatisticsTable.StatisticRow("A", 1L, 1371587140278L, 1L, 10L, 20L, 30L, 0L, 18L),
                new ProcedureStatisticsTable.StatisticRow("B", 1L, 1371587140278L, 100L, 1L, 2L, 3L, 17L, 18L)
        };

        ResultRow[] result = {
                //                         TS/Proc/wtd/invok/avg/min/max/abort/fail
                new ResultRow(1371587140278L, "B", 96L, 200L, 3L, 1L, 5L, 36L, 34L),
                new ResultRow(1371587140278L, "A", 4L, 1L, 30L, 10L, 20L, 18L, 0L)
        };

        // When
        ProcedureProfileStatisticsTable dut = new ProcedureProfileStatisticsTable();
        loadData(dut, data);

        // Then
        assertEquals("testMultipleProcs", dut, result);
    }

    @Test
    public void testSiteDedupe() {
        // Given
        // need to not double count invocations at replicas, but do look at
        // min, max, avg, fail, abort
        ProcedureStatisticsTable.StatisticRow[] data = {
                //                          TS/Proc/Part/invok/min/max/avg/fail/abort
                new ProcedureStatisticsTable.StatisticRow("B", 0L, 1371587140278L, 100L, 2L, 5L, 4L, 17L, 18L),
                new ProcedureStatisticsTable.StatisticRow("A", 1L, 1371587140278L, 1L, 10L, 20L, 30L, 0L, 18L),
                new ProcedureStatisticsTable.StatisticRow("B", 0L, 1371587140278L, 100L, 1L, 2L, 2L, 17L, 18L),
                new ProcedureStatisticsTable.StatisticRow("B", 1L, 1371587140278L, 100L, 4L, 4L, 3L, 17L, 18L)
        };

        ResultRow[] result = {
                //                         TS/Proc/wtd/invok/avg/min/max/abort/fail
                new ResultRow(1371587140278L, "B", 95L, 200L, 3L, 1L, 5L, 36L, 34L),
                new ResultRow(1371587140278L, "A", 5L, 1L, 30L, 10L, 20L, 18L, 0L)
        };

        // When
        ProcedureProfileStatisticsTable dut = new ProcedureProfileStatisticsTable();
        loadData(dut, data);

        // Then
        assertEquals("testSiteDedupe", dut, result);
    }

    @Test
    public void testSiteNoDedupe() {
        // Given
        // need to not double count invocations at replicas, but do look at
        // min, max, avg, fail, abort
        ProcedureStatisticsTable.StatisticRow[] data = {
                //                          TS/Proc/Part/invok/min/max/avg/fail/abort
                new ProcedureStatisticsTable.StatisticRow("B", 0L, 1371587140278L, 100L, 2L, 5L, 4L, 17L, 18L),
                new ProcedureStatisticsTable.StatisticRow("A", 1L, 1371587140278L, 1L, 10L, 20L, 30L, 0L, 18L),
                new ProcedureStatisticsTable.StatisticRow("B", 0L, 1371587140278L, 100L, 1L, 2L, 2L, 17L, 18L),
                new ProcedureStatisticsTable.StatisticRow("B", 1L, 1371587140278L, 100L, 4L, 4L, 3L, 17L, 18L)
        };
        ResultRow[] result = {
                //               TS/         Proc /wtd/ invok/avg/ min/max/abort/fail
                new ResultRow(1371587140278L, "B", 97L, 300L, 3L, 1L, 5L, 54L, 51L),
                new ResultRow(1371587140278L, "A", 3L, 1L, 30L, 10L, 20L, 18L, 0L)
        };

        // When
        ProcedureProfileStatisticsTable dut = new ProcedureProfileStatisticsTable();
        loadDataNoDedup(dut, data);

        // Then
        assertEquals("testSiteNoDedupe", dut, result);
    }

    @Test
    public void testRounding() {
        // Given
        // need to not double count invocations at replicas, but do look at
        // min, max, avg, fail, abort
        ProcedureStatisticsTable.StatisticRow[] data = {
                //                          TS/Proc/Part/invok/min/max/avg/fail/abort
                new ProcedureStatisticsTable.StatisticRow("B", 0L, 1371587140278L, 10000000L, 2L, 5L, 4L, 17L, 18L),
                new ProcedureStatisticsTable.StatisticRow("A", 0L, 1371587140278L, 1L, 10L, 20L, 30L, 0L, 18L)
        };

        ResultRow[] result = {
                //               TS/         Proc /wtd/ invok/avg/ min/max/fail/abort
                new ResultRow(1371587140278L, "B", 100L, 10000000L, 4L, 2L, 5L, 18L, 17L),
                new ResultRow(1371587140278L, "A", 0L, 1L, 30L, 10L, 20L, 18L, 0L)
        };

        // When
        ProcedureProfileStatisticsTable dut = new ProcedureProfileStatisticsTable();
        loadData(dut, data);

        // Then
        assertEquals("testRounding", dut, result);
    }
}
