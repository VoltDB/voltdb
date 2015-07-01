/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package org.voltdb;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.voltdb.StatsProcProfTable.ProcProfRow;

public class TestStatsProcProfTable {

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
    void loadEmUp(StatsProcProfTable dut, ProcProfRow[] data) {
        for (int ii = 0; ii < data.length; ++ii) {
            dut.updateTable(true,
                    data[ii].timestamp,
                    data[ii].procedure,
                    data[ii].partition,
                    data[ii].invocations,
                    data[ii].min,
                    data[ii].max,
                    data[ii].avg,
                    data[ii].failures,
                    data[ii].aborts);
        }
    }

    // push rows from data in to the table.
    void loadEmUpNoDedup(StatsProcProfTable dut, ProcProfRow[] data) {
        for (int ii = 0; ii < data.length; ++ii) {
            dut.updateTable(false,
                    data[ii].timestamp,
                    data[ii].procedure,
                    data[ii].partition,
                    data[ii].invocations,
                    data[ii].min,
                    data[ii].max,
                    data[ii].avg,
                    data[ii].failures,
                    data[ii].aborts);
        }
    }

    // validate contents of sorted dut vs. expectation of ResultRow[]
    void validateEmGood(String testname, StatsProcProfTable dut, ResultRow[] data) {
        VoltTable vt = dut.sortByAverage(testname);
        assertEquals(testname + " has wrong number of result rows in test.",
                vt.getRowCount(), data.length);
        int ii = 0;
        while (vt.advanceRow()) {
            System.out.printf("%s: validating row %d\n", testname, ii);
            assertEquals(data[ii].timestamp, vt.getLong("TIMESTAMP"));
            assertEquals(data[ii].procedure, vt.getString("PROCEDURE"));
            assertEquals(data[ii].weighted_perc, vt.getLong("WEIGHTED_PERC"));
            assertEquals(data[ii].invocations, vt.getLong("INVOCATIONS"));
            assertEquals(data[ii].avg, vt.getLong("AVG"));
            assertEquals(data[ii].min, vt.getLong("MIN"));
            assertEquals(data[ii].max, vt.getLong("MAX"));
            assertEquals(data[ii].aborts, vt.getLong("ABORTS"));
            assertEquals(data[ii].failures, vt.getLong("FAILURES"));
            ++ii;
        }
    }

    @Test
    public void testBaseCase() throws Exception {
        // validate sensical round-trip of one row.
        ProcProfRow[] data = {
            new ProcProfRow(1371587140278L, "A", 0L, 100L, 1L, 2L, 3L, 4L, 5L)
        };

        ResultRow[] result = {
            new ResultRow(1371587140278L, "A", 100L, 100L, 3L, 1L, 2L, 5L, 4L)
        };

        StatsProcProfTable dut = new StatsProcProfTable();
        loadEmUp(dut, data);
        validateEmGood("testBaseCase", dut, result);
    }

    @Test
    public void testAllZeros() throws Exception {
        // validate paranoia about an all zero row - just in case.
        ProcProfRow data[] = {
            new ProcProfRow(1371587140278L, "B", 0L, 0L, 0L, 0L, 0L, 0L, 0L)
        };
        ResultRow result[] = {
            new ResultRow(1371587140278L, "B", 100L, 0L, 0L, 0L, 0L, 0L, 0L)
        };
        StatsProcProfTable dut = new StatsProcProfTable();
        loadEmUp(dut, data);
        validateEmGood("testAllZeros", dut, result);
    }

    @Test
    public void testMultipleProcs() throws Exception {
        // 2 procs, 2 partitions - make sure min,max,avg,wtd works
        ProcProfRow data[] = {
            //                          TS/Proc/Part/invok/min/max/avg/fail/abort
            new ProcProfRow(1371587140278L, "B", 0L, 100L, 2L, 5L, 4L, 17L, 18L),
            new ProcProfRow(1371587140278L, "A", 1L, 1L,  10L, 20L, 30L, 0L, 18L),
            new ProcProfRow(1371587140278L, "B", 1L, 100L,  1L, 2L, 3L, 17L, 18L)
        };
        ResultRow result[] = {
            //                         TS/Proc/wtd/invok/avg/min/max/abort/fail
            new ResultRow(1371587140278L, "B", 95L, 200L, 3L, 1L, 5L, 36L, 34L),
            new ResultRow(1371587140278L, "A", 5L, 1L, 30L, 10L, 20L, 18L, 0L)
        };
        StatsProcProfTable dut = new StatsProcProfTable();
        loadEmUp(dut, data);
        validateEmGood("testMultipleProcs", dut, result);
    }


    @Test
    public void testSiteDedupe() throws Exception {
        // need to not double count invocations at replicas, but do look at
        // min, max, avg, fail, abort
        ProcProfRow data[] = {
            //                          TS/Proc/Part/invok/min/max/avg/fail/abort
            new ProcProfRow(1371587140278L, "B", 0L, 100L, 2L, 5L, 4L, 17L, 18L),
            new ProcProfRow(1371587140278L, "A", 1L, 1L,  10L, 20L, 30L, 0L, 18L),
            new ProcProfRow(1371587140278L, "B", 0L, 100L, 1L, 2L, 2L, 17L, 18L),
            new ProcProfRow(1371587140278L, "B", 1L, 100L,  4L, 4L, 3L, 17L, 18L)
        };
        ResultRow result[] = {
            //                         TS/Proc/wtd/invok/avg/min/max/abort/fail
            new ResultRow(1371587140278L, "B", 95L, 200L, 3L, 1L, 5L, 36L, 34L),
            new ResultRow(1371587140278L, "A", 5L, 1L, 30L, 10L, 20L, 18L, 0L)
        };
        StatsProcProfTable dut = new StatsProcProfTable();
        loadEmUp(dut, data);
        validateEmGood("testSiteDedupe", dut, result);
    }

    @Test
    public void testSiteNoDedupe() throws Exception {
        // need to not double count invocations at replicas, but do look at
        // min, max, avg, fail, abort
        ProcProfRow data[] = {
            //                          TS/Proc/Part/invok/min/max/avg/fail/abort
            new ProcProfRow(1371587140278L, "B", 0L, 100L, 2L, 5L, 4L, 17L, 18L),
            new ProcProfRow(1371587140278L, "A", 1L, 1L, 10L, 20L, 30L, 0L, 18L),
            new ProcProfRow(1371587140278L, "B", 0L, 100L, 1L, 2L, 2L, 17L, 18L),
            new ProcProfRow(1371587140278L, "B", 1L, 100L, 4L, 4L, 3L, 17L, 18L)
        };
        ResultRow result[] = {
            //               TS/         Proc /wtd/ invok/avg/ min/max/abort/fail
            new ResultRow(1371587140278L, "B", 97L, 300L, 3L, 1L, 5L, 54L, 51L),
            new ResultRow(1371587140278L, "A", 3L, 1L, 30L, 10L, 20L, 18L, 0L)
        };
        StatsProcProfTable dut = new StatsProcProfTable();
        loadEmUpNoDedup(dut, data);
        validateEmGood("testSiteNoDedupe", dut, result);
    }

    @Test
    public void testRounding() throws Exception {
        // need to not double count invocations at replicas, but do look at
        // min, max, avg, fail, abort
        ProcProfRow data[] = {
            //                          TS/Proc/Part/invok/min/max/avg/fail/abort
            new ProcProfRow(1371587140278L, "B", 0L, 10000000L, 2L, 5L, 4L, 17L, 18L),
            new ProcProfRow(1371587140278L, "A", 0L, 1L, 10L, 20L, 30L, 0L, 18L)
        };
        ResultRow result[] = {
            //               TS/         Proc /wtd/ invok/avg/ min/max/fail/abort
            new ResultRow(1371587140278L, "B", 100L, 10000000L, 4L, 2L, 5L, 18L, 17L),
            new ResultRow(1371587140278L, "A", 0L, 1L, 30L, 10L, 20L, 18L, 0L)
        };
        StatsProcProfTable dut = new StatsProcProfTable();
        loadEmUp(dut, data);
        validateEmGood("testRounding", dut, result);
    }
}
