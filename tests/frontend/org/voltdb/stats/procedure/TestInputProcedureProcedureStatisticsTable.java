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

public class TestInputProcedureProcedureStatisticsTable {

    long mB = 1024 * 1024;

    // result row in java form for test
    static class ResultRow {
        long timestamp;
        String procedure;
        long percent;
        long invocations;
        long minIN;
        long maxIN;
        long avgIN;
        long totalIN;

        public ResultRow(long timestamp, String procedure, long percent, long invocations,
                         long minIN, long maxIN, long avgIN, long totalIN) {
            this.timestamp = timestamp;
            this.procedure = procedure;
            this.percent = percent;
            this.invocations = invocations;
            this.minIN = minIN;
            this.maxIN = maxIN;
            this.avgIN = avgIN;
            this.totalIN = totalIN;
        }
    }

    // push rows from data in to the table.
    void loadData(InputProcedureStatisticsTable dut, ProcedureStatisticsTable.StatisticRow[] data) {
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
                    0,
                    0
            );
        }
    }

    void loadDataNoDedup(InputProcedureStatisticsTable dut, ProcedureStatisticsTable.StatisticRow[] data) {
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
                    0,
                    0
            );
        }
    }

    // validate contents of sorted dut vs. expectation of ResultRow[]
    void assertEquals(String testname, InputProcedureStatisticsTable dut, ResultRow[] data) {
        VoltTable actual = dut.getSortedTable();
        Assert.assertEquals(
                testname + " has wrong number of result rows in test.",
                actual.getRowCount(),
                data.length
        );

        for (ResultRow row : data) {
            assertTrue(actual.advanceRow());
            System.out.printf("%s: validating row %d\n", testname, actual.getActiveRowIndex());

            Assert.assertEquals(row.percent, actual.getLong("WEIGHTED_PERC"));
            Assert.assertEquals(row.timestamp, actual.getLong("TIMESTAMP"));
            Assert.assertEquals(row.procedure, actual.getString("PROCEDURE"));
            Assert.assertEquals(row.invocations, actual.getLong("INVOCATIONS"));
            Assert.assertEquals(row.minIN, actual.getLong("MIN_PARAMETER_SET_SIZE"));
            Assert.assertEquals(row.maxIN, actual.getLong("MAX_PARAMETER_SET_SIZE"));
            Assert.assertEquals(row.avgIN, actual.getLong("AVG_PARAMETER_SET_SIZE"));
            Assert.assertEquals(row.totalIN, actual.getLong("TOTAL_PARAMETER_SET_SIZE_MB"));
        }
    }

    @Test
    public void testBaseCase() throws Exception {
        // Given
        // validate sensical round-trip of one row.
        ProcedureStatisticsTable.StatisticRow[] data = {
                //proc/part/time/invok/min/max/avg
                new ProcedureStatisticsTable.StatisticRow("proc", 0L, 12345L, 100 * mB, 2L, 4L, 3L, 0L, 0L)
        };

        ResultRow[] result = {
                //time/proc/perc/inok/min/max/avg/tot
                new ResultRow(12345L, "proc", 100L, 100 * mB, 2L, 4L, 3L, 300L)
        };

        // When
        InputProcedureStatisticsTable dut = new InputProcedureStatisticsTable();
        loadData(dut, data);

        // Then
        assertEquals("testBaseCase", dut, result);
    }

    @Test
    public void testAllZeros() {
        // Given
        // validate paranoia about an all zero row - just in case.
        ProcedureStatisticsTable.StatisticRow[] data = {     //proc/part/time/invok/min/max/avg
                new ProcedureStatisticsTable.StatisticRow("proc", 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)

        };
        ResultRow[] result = {  //time/proc/perc/inok/min/max/avg/tot
                new ResultRow(0L, "proc", 100L, 0L, 0L, 0L, 0L, 0L)
        };

        // When
        InputProcedureStatisticsTable dut = new InputProcedureStatisticsTable();
        loadData(dut, data);

        // Then
        assertEquals("testAllZeros", dut, result);
    }

    @Test
    public void testMultipleProcs() {
        // Given
        ProcedureStatisticsTable.StatisticRow[] data = {     //proc/part/time/invok/min/max/avg
                new ProcedureStatisticsTable.StatisticRow("A", 0L, 12345L, 300 * mB, 3L, 5L, 4L, 0L, 0L),
                new ProcedureStatisticsTable.StatisticRow("B", 0L, 12345L, 100 * mB, 1L, 4L, 2L, 0L, 0L),
                new ProcedureStatisticsTable.StatisticRow("B", 1L, 12345L, 100 * mB, 1L, 3L, 2L, 0L, 0L)
        };

        ResultRow[] result = {  //time/proc/perc/inok/min/max/avg/tot
                new ResultRow(12345L, "A", 75L, 300 * mB, 3L, 5L, 4L, 1200L),
                new ResultRow(12345L, "B", 25L, 200 * mB, 1L, 4L, 2L, 400L)
        };

        // When
        InputProcedureStatisticsTable dut = new InputProcedureStatisticsTable();
        loadData(dut, data);

        // Then
        assertEquals("testMulipleProcs", dut, result);
    }

    @Test
    public void testSiteDedupe() {
        // Given
        // need to not double count invocations at replicas, but do look at
        // min, max, avg
        ProcedureStatisticsTable.StatisticRow[] data = { //proc/part/time/invok/min/max/avg
                new ProcedureStatisticsTable.StatisticRow("proc", 0L, 12345L, 200 * mB, 4L, 10L, 6L, 0L, 0L),
                new ProcedureStatisticsTable.StatisticRow("proc", 0L, 12345L, 100 * mB, 4L, 25L, 10L, 0L, 0L),
                new ProcedureStatisticsTable.StatisticRow("proc", 0L, 12345L, 100 * mB, 1L, 4L, 2L, 0L, 0L),
                new ProcedureStatisticsTable.StatisticRow("proc", 1L, 12345L, 400 * mB, 2L, 8L, 4L, 0L, 0L)
        };

        ResultRow[] result = { //time/proc/perc/inok/min/max/avg/tot
                new ResultRow(12345L, "proc", 100L, 800 * mB, 1L, 25L, 5L, 4000L)
        };

        // When
        InputProcedureStatisticsTable dut = new InputProcedureStatisticsTable();
        loadDataNoDedup(dut, data);

        // Then
        assertEquals("testSiteDedupe", dut, result);
    }

    @Test
    public void testRounding() {
        // Given
        ProcedureStatisticsTable.StatisticRow[] data = {     //proc/part/time/invok/min/max/avg
                new ProcedureStatisticsTable.StatisticRow("A", 0L, 12345L, 10000000 * mB, 3L, 5L, 4L, 0L, 0L),
                new ProcedureStatisticsTable.StatisticRow("B", 0L, 12345L, 1 * mB, 1L, 4L, 2L, 0L, 0L)
        };

        ResultRow[] result = {  //time/proc/perc/inok/min/max/avg/tot
                new ResultRow(12345L, "A", 100L, 10000000 * mB, 3L, 5L, 4L, 40000000L),
                new ResultRow(12345L, "B", 0L, 1 * mB, 1L, 4L, 2L, 2L)
        };

        // When
        InputProcedureStatisticsTable dut = new InputProcedureStatisticsTable();
        loadData(dut, data);

        //Then
        assertEquals("testRounding", dut, result);
    }
}
