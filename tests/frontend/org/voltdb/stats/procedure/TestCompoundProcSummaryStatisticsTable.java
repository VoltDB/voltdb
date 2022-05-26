/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.voltdb.VoltTable;

public class TestCompoundProcSummaryStatisticsTable {

    @Rule
    public final TestName testname = new TestName();

    // The basic input to CompoundProcSummaryStatisticsTable is a
    // VoltTableRow in the form produced by ProcedureStatsCollector.
    //
    //  TIMESTAMP               (VoltType.BIGINT),
    //  HOST_ID                 (VoltType.INTEGER),  don't care
    //  HOSTNAME                (VoltType.STRING),   don't care
    //  SITE_ID                 (VoltType.INTEGER),  n/a
    //  PARTITION_ID            (VoltType.INTEGER),  n/a
    //  PROCEDURE               (VoltType.STRING),
    //  STATEMENT               (VoltType.STRING),   n/a
    //  INVOCATIONS             (VoltType.BIGINT),
    //  TIMED_INVOCATIONS       (VoltType.BIGINT),   don't care
    //  MIN_EXECUTION_TIME      (VoltType.BIGINT),
    //  MAX_EXECUTION_TIME      (VoltType.BIGINT),
    //  AVG_EXECUTION_TIME      (VoltType.BIGINT),
    //  MIN_RESULT_SIZE         (VoltType.INTEGER),  don't care
    //  MAX_RESULT_SIZE         (VoltType.INTEGER),  don't care
    //  AVG_RESULT_SIZE         (VoltType.INTEGER),  don't care
    //  MIN_PARAMETER_SET_SIZE  (VoltType.INTEGER),  don't care
    //  MAX_PARAMETER_SET_SIZE  (VoltType.INTEGER),  don't care
    //  AVG_PARAMETER_SET_SIZE  (VoltType.INTEGER),  don't care
    //  ABORTS                  (VoltType.BIGINT),
    //  FAILURES                (VoltType.BIGINT),
    //  TRANSACTIONAL           (VoltType.TINYINT),  always 0 for non-transactional
    //  COMPOUND                (VoltType.TINYINT);  always 1 for compound
    //
    // n/a = not applicable to compound-proc case
    // don't care = ignored by compound-proc summary statistics
    //
    // The 8 columns of interest are extracted from the VoltTableRow
    // and passed as separate arguments to the updateTable method.
    // For ease of coding this test, we skip dealing with VoltTables
    // and use a private row structure.
    //
    // The result has essentially the same columns. We use the
    // same structure to hold expected values. Counts are accumulated
    // across all rows with the same procedure name; the average
    // is suitable computed based on the average and invocation
    // count from each such row.
    //
    static class Row {
        long timestamp;
        String procedure; // input: class; result: short name
        long invocations;
        long min, max, avg;
        long aborts, failures;

        Row(long timestamp, String procedure, long invocations,
            long min, long max, long avg,
            long aborts, long failures) {
            this.timestamp = timestamp;
            this.procedure = procedure;
            this.invocations = invocations;
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.aborts = aborts;
            this.failures = failures;
        }
    }

    // push rows from data in to the table.
    void loadData(CompoundProcSummaryStatisticsTable dut, Row[] inputTable) {
        for (Row in : inputTable) {
            dut.updateTable(in.timestamp, in.procedure, in.invocations,
                            in.min, in.max, in.avg, in.aborts, in.failures);
        }
    }

    // validate contents of sorted dut vs. expectation of ResultRow[]
    void assertEquals(CompoundProcSummaryStatisticsTable dut, Row[] data) {
        VoltTable actual = dut.getSortedTable();
        String test = testname.getMethodName();
        Assert.assertEquals(test + " has wrong number of result rows in test.",
                            actual.getRowCount(), data.length);
        for (Row row : data) {
            assertTrue(actual.advanceRow());
            System.out.printf("%s: validating row %d\n", test, actual.getActiveRowIndex());
            Assert.assertEquals(row.timestamp, actual.getLong("TIMESTAMP"));
            Assert.assertEquals(row.procedure, actual.getString("PROCEDURE"));
            Assert.assertEquals(row.invocations, actual.getLong("INVOCATIONS"));
            Assert.assertEquals(row.min, actual.getLong("MIN_ELAPSED"));
            Assert.assertEquals(row.max, actual.getLong("MAX_ELAPSED"));
            Assert.assertEquals(row.avg, actual.getLong("AVG_ELAPSED"));
            Assert.assertEquals(row.aborts, actual.getLong("ABORTS"));
            Assert.assertEquals(row.failures, actual.getLong("FAILURES"));
        }
    }

    @Test
    public void testBaseCase() {
        // Given
        // validate round-trip of one row.
        Row[] input = {
            new Row(1371587140278L, "org.banana.A", 100L, 1L, 2L, 3L, 4L, 5L)
        };
        Row[] result = {
            new Row(1371587140278L, "org.banana.A", 100L, 1L, 2L, 3L, 4L, 5L)
        };

        // When
        CompoundProcSummaryStatisticsTable dut = new CompoundProcSummaryStatisticsTable();
        loadData(dut, input);

        // Then
        assertEquals(dut, result);
    }

    @Test
    public void testAllZeros() {
        // Given
        // validate paranoia about an all zero row - just in case.
        Row[] input = {
            new Row(1371587140278L, "org.banana.B", 0L, 0L, 0L, 0L, 0L, 0L)
        };

        Row[] result = {
            new Row(1371587140278L, "org.banana.B", 0L, 0L, 0L, 0L, 0L, 0L)
        };

        // When
        CompoundProcSummaryStatisticsTable dut = new CompoundProcSummaryStatisticsTable();
        loadData(dut, input);

        // Then
        assertEquals(dut, result);
    }

    @Test
    public void testMultipleProcs() {
        // Given
        // 2 procs, 2 partitions - make sure min,max,avg works
        Row[] input = {
            //                                      inv   min  max  avg  abo  err
            new Row(1371587140278L, "org.banana.B", 100L, 2L,  5L,  4L,  17L, 18L),
            new Row(1371587140279L, "org.banana.A", 1L,   20L, 30L, 10L, 0L,  18L),
            new Row(1371587140280L, "org.banana.B", 100L, 1L,  3L,  2L,  17L, 18L)
        };

        Row[] result = {
            //                                      inv   min  max  avg  abo  err
            new Row(1371587140278L, "org.banana.B", 200L, 1L,  5L,  3L,  34L, 36L),
            new Row(1371587140279L, "org.banana.A", 1L,   20L, 30L, 10L, 0L,  18L)
        };

        // When
        CompoundProcSummaryStatisticsTable dut = new CompoundProcSummaryStatisticsTable();
        loadData(dut, input);

        // Then
        assertEquals(dut, result);
    }

    @Test
    public void testNoOverflow() {
        // Given
        // paranoia about overflow when computing average of big numbers
        long yuge = 2_000_000_000_000_000_000L; // Long.MAX_VALUE is 9_223_372_036_854_775_807
        Row[] input = {
            //                                      inv   min  max      avg     abo  err
            new Row(1371587140278L, "org.banana.Y", yuge, 1L,  yuge+10, yuge+1, 0L,  0L),
            new Row(1371587140279L, "org.banana.Y", yuge, 1L,  yuge+20, yuge+2, 0L,  0L),
            new Row(1371587140280L, "org.banana.Y", yuge, 1L,  yuge+30, yuge+1, 0L,  0L),
            new Row(1371587140281L, "org.banana.Y", yuge, 1L,  yuge+40, yuge+4, 0L,  0L),
         };
        Row[] result = {
            //                                      inv     min  max      avg   abo  err
            new Row(1371587140278L, "org.banana.Y", yuge*4, 1L,  yuge+40, yuge, 0L,  0L)
            // average is reported as yuge, not yuge+2, because double-precision has
            // only 15 to 16 decimal digits of significance. once your average latency
            // has exceeded 2 years, though, I don't think you'll care about rounding.
        };

        // When
        CompoundProcSummaryStatisticsTable dut = new CompoundProcSummaryStatisticsTable();
        loadData(dut, input);

        // Then
        assertEquals(dut, result);
    }
}
