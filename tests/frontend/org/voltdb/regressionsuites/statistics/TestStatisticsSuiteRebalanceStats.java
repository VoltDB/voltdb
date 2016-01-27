/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.regressionsuites.statistics;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;

import org.voltdb.join.BalancePartitionsStatistics;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;

public class TestStatisticsSuiteRebalanceStats extends StatisticsTestSuiteBase {

    public TestStatisticsSuiteRebalanceStats(String name) {
        super(name);
    }

    class RebalanceStatsChecker
    {
        final double fuzzFactor;
        final int rangesToMove;

        long tStartMS;
        long rangesMoved = 0;
        long bytesMoved = 0;
        long rowsMoved = 0;
        long invocations = 0;
        long totalInvTimeMS = 0;

        RebalanceStatsChecker(int rangesToMove, double fuzzFactor)
        {
            this.fuzzFactor = fuzzFactor;
            this.rangesToMove = rangesToMove;
            this.tStartMS = System.currentTimeMillis();
        }

        void update(int ranges, int bytes, int rows)
        {
            rangesMoved += ranges;
            bytesMoved += bytes;
            rowsMoved += rows;
            invocations++;
        }

        void checkFuzz(double expected, double actual)
        {
            double delta = Math.abs((expected - actual) / expected);
            if (delta > fuzzFactor) {
                assertFalse(Math.abs((expected - actual) / expected) > fuzzFactor);
            }
        }

        void check(BalancePartitionsStatistics.StatsPoint stats)
        {
            double totalTimeS = (System.currentTimeMillis() - tStartMS) / 1000.0;
            double statsRangesMoved1 = (stats.getPercentageMoved() / 100.0) * rangesToMove;
            checkFuzz(rangesMoved, statsRangesMoved1);
            double statsRangesMoved2 = stats.getRangesPerSecond() * totalTimeS;
            checkFuzz(rangesMoved, statsRangesMoved2);
            double statsBytesMoved = stats.getMegabytesPerSecond() * 1000000.0 * totalTimeS;
            checkFuzz(bytesMoved, statsBytesMoved);
            double statsRowsMoved = stats.getRowsPerSecond() * totalTimeS;
            checkFuzz(rowsMoved, statsRowsMoved);
            double statsInvocations = stats.getInvocationsPerSecond() * totalTimeS;
            checkFuzz(invocations, statsInvocations);
            double statsInvTimeMS = stats.getAverageInvocationTime() * invocations;
            assertTrue(Math.abs((totalInvTimeMS - statsInvTimeMS) / totalInvTimeMS) <= fuzzFactor);
            checkFuzz(totalInvTimeMS, statsInvTimeMS);
            double estTimeRemainingS = totalTimeS * (rangesToMove / (double)rangesMoved - 1.0);
            double statsEstTimeRemainingS = stats.getEstimatedRemaining() / 1000.0;
            checkFuzz(estTimeRemainingS, statsEstTimeRemainingS);
        }
    }

    public void testRebalanceStats() throws Exception {
        System.out.println("testRebalanceStats");
        // Test constants
        final int DURATION_SECONDS = 10;
        final int INVOCATION_SLEEP_MILLIS = 500;
        final int IDLE_SLEEP_MILLIS = 200;
        final int RANGES_TO_MOVE = Integer.MAX_VALUE;
        final int BYTES_TO_MOVE = 10000000;
        final int ROWS_TO_MOVE = 1000000;
        final double FUZZ_FACTOR = .1;

        RebalanceStatsChecker checker = new RebalanceStatsChecker(RANGES_TO_MOVE, FUZZ_FACTOR);
        BalancePartitionsStatistics bps = new BalancePartitionsStatistics(RANGES_TO_MOVE);
        Random r = new Random(2222);
        // Random numbers are between zero and the constant, so everything will average out
        // to half the time and quantities. Nothing will be exhausted by the test.
        final int loopCount = (DURATION_SECONDS * 1000) / (INVOCATION_SLEEP_MILLIS + IDLE_SLEEP_MILLIS);
        for (int i = 0; i < loopCount; i++) {
            bps.logBalanceStarts();
            int invocationTimeMS = r.nextInt(INVOCATION_SLEEP_MILLIS);
            Thread.sleep(invocationTimeMS);
            checker.totalInvTimeMS += invocationTimeMS;
            int ranges = r.nextInt(RANGES_TO_MOVE / loopCount);
            int bytes = r.nextInt(BYTES_TO_MOVE / loopCount);
            int rows = r.nextInt(ROWS_TO_MOVE / loopCount);
            bps.logBalanceEnds(ranges, bytes, TimeUnit.MILLISECONDS.toNanos(invocationTimeMS), TimeUnit.MILLISECONDS.toNanos(invocationTimeMS), rows);
            checker.update(ranges, bytes, rows);
            checker.check(bps.getLastStatsPoint());
            int idleTimeMS = r.nextInt(IDLE_SLEEP_MILLIS);
            Thread.sleep(idleTimeMS);
        }
        // Check the results with fuzzing to avoid rounding errors.
        checker.check(bps.getOverallStats());
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        return StatisticsTestSuiteBase.suite(TestStatisticsSuiteRebalanceStats.class, false);
    }
}
