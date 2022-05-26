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

package org.voltdb.regressionsuites.statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;

import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;

public class TestStatisticsSuitePlannerStats extends StatisticsTestSuiteBase {

    public TestStatisticsSuitePlannerStats(String name) {
        super(name);
    }

    //
    // planner statistics
    //
    public void testPlannerStatistics() throws Exception {
        System.out.println("\n\nTESTING PLANNER STATS\n\n\n");
        Client client  = getClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[14];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("PARTITION_ID", VoltType.INTEGER);
        expectedSchema[5] = new ColumnInfo("CACHE1_LEVEL", VoltType.INTEGER);
        expectedSchema[6] = new ColumnInfo("CACHE2_LEVEL", VoltType.INTEGER);
        expectedSchema[7] = new ColumnInfo("CACHE1_HITS", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("CACHE2_HITS", VoltType.BIGINT);
        expectedSchema[9] = new ColumnInfo("CACHE_MISSES", VoltType.BIGINT);
        expectedSchema[10] = new ColumnInfo("PLAN_TIME_MIN", VoltType.BIGINT);
        expectedSchema[11] = new ColumnInfo("PLAN_TIME_MAX", VoltType.BIGINT);
        expectedSchema[12] = new ColumnInfo("PLAN_TIME_AVG", VoltType.BIGINT);
        expectedSchema[13] = new ColumnInfo("FAILURES", VoltType.BIGINT);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;

        // Clear the interval statistics
        results = client.callProcedure("@Statistics", "planner", 1).getResults();
        assertEquals(1, results.length);

        // Invoke a few select queries a few times to get some cache hits and misses,
        // and to exceed the sampling frequency.
        // This does not use level 2 cache (parameterization) or trigger failures.
        for (String query : new String[] {
                "select * from warehouse",
                "select * from new_order",
                "select * from item",
                }) {
            for (int i = 0; i < 10; i++) {
                client.callProcedure("@AdHoc", query).getResults();
                assertEquals(1, results.length);
            }
        }

        // Get the final interval statistics
        results = client.callProcedure("@Statistics", "planner", 1).getResults();
        assertEquals(1, results.length);
        System.out.println("Test planner table: " + results[0].toString());
        validateSchema(results[0], expectedTable);
        VoltTable stats = results[0];

        // Sample the statistics
        List<Long> siteIds = new ArrayList<Long>();
        int cache1_level = 0;
        int cache2_level = 0;
        int cache1_hits  = 0;
        int cache2_hits  = 0;
        int cache_misses = 0;
        long plan_time_min_min = Long.MAX_VALUE;
        long plan_time_max_max = Long.MIN_VALUE;
        long plan_time_avg_tot = 0;
        int failures = 0;
        while (stats.advanceRow()) {
            cache1_level += (Integer)stats.get("CACHE1_LEVEL", VoltType.INTEGER);
            cache2_level += (Integer)stats.get("CACHE2_LEVEL", VoltType.INTEGER);
            cache1_hits  += (Integer)stats.get("CACHE1_HITS", VoltType.INTEGER);
            cache2_hits  += (Integer)stats.get("CACHE2_HITS", VoltType.INTEGER);
            cache_misses += (Integer)stats.get("CACHE_MISSES", VoltType.INTEGER);
            plan_time_min_min = Math.min(plan_time_min_min, (Long)stats.get("PLAN_TIME_MIN", VoltType.BIGINT));
            plan_time_max_max = Math.max(plan_time_max_max, (Long)stats.get("PLAN_TIME_MAX", VoltType.BIGINT));
            plan_time_avg_tot += (Long)stats.get("PLAN_TIME_AVG", VoltType.BIGINT);
            failures += (Integer)stats.get("FAILURES", VoltType.INTEGER);
            siteIds.add((Long)stats.get("SITE_ID", VoltType.BIGINT));
        }

        // Check for reasonable results
        int globalPlanners = 0;
        assertTrue("Failed siteIds count >= 2", siteIds.size() >= 2);
        for (long siteId : siteIds) {
            if (siteId == -1) {
                globalPlanners++;
            }
        }
        assertTrue("Global planner sites not 1, value was: " + globalPlanners, globalPlanners == 1);
        assertTrue("Failed total CACHE1_LEVEL > 0, value was: " + cache1_level, cache1_level > 0);
        assertTrue("Failed total CACHE1_LEVEL < 1,000,000, value was: " + cache1_level, cache1_level < 1000000);
        assertTrue("Failed total CACHE2_LEVEL >= 0, value was: " + cache2_level, cache2_level >= 0);
        assertTrue("Failed total CACHE2_LEVEL < 1,000,000, value was: " + cache2_level, cache2_level < 1000000);
        assertTrue("Failed total CACHE1_HITS > 0, value was: " + cache1_hits, cache1_hits > 0);
        assertTrue("Failed total CACHE1_HITS < 1,000,000, value was: " + cache1_hits, cache1_hits < 1000000);
        assertTrue("Failed total CACHE2_HITS == 0, value was: " + cache2_hits, cache2_hits == 0);
        assertTrue("Failed total CACHE2_HITS < 1,000,000, value was: " + cache2_hits, cache2_hits < 1000000);
        assertTrue("Failed total CACHE_MISSES > 0, value was: " + cache_misses, cache_misses > 0);
        assertTrue("Failed total CACHE_MISSES < 1,000,000, value was: " + cache_misses, cache_misses < 1000000);
        assertTrue("Failed min PLAN_TIME_MIN > 0, value was: " + plan_time_min_min, plan_time_min_min > 0);
        assertTrue("Failed total PLAN_TIME_MIN < 100,000,000,000, value was: " + plan_time_min_min, plan_time_min_min < 100000000000L);
        assertTrue("Failed max PLAN_TIME_MAX > 0, value was: " + plan_time_max_max, plan_time_max_max > 0);
        assertTrue("Failed total PLAN_TIME_MAX < 100,000,000,000, value was: " + plan_time_max_max, plan_time_max_max < 100000000000L);
        assertTrue("Failed total PLAN_TIME_AVG > 0, value was: " + plan_time_avg_tot, plan_time_avg_tot > 0);
        assertTrue("Failed total FAILURES == 0, value was: " + failures, failures == 0);
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        return StatisticsTestSuiteBase.suite(TestStatisticsSuitePlannerStats.class, false);
    }
}
