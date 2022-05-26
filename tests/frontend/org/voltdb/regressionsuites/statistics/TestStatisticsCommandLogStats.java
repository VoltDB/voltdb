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
import java.util.HashMap;
import java.util.Map;

import org.voltdb.CommandLogStats.CommandLogCols;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;
import org.voltdb.utils.MiscUtils;

import junit.framework.Test;

public class TestStatisticsCommandLogStats extends StatisticsTestSuiteBase {

    public TestStatisticsCommandLogStats(String name) {
        super(name);
    }

    public void testCommandLogStats() throws Exception {
        System.out.println("\n\nTESTING COMMANDLOG STATS\n\n\n");

        Client client  = getFullyConnectedClient();

        VoltTable.ColumnInfo[] expectedSchema = new VoltTable.ColumnInfo[8];
        expectedSchema[0] = new VoltTable.ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new VoltTable.ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new VoltTable.ColumnInfo("HOSTNAME", VoltType.STRING);
        int index = 3;
        for (CommandLogCols col : CommandLogCols.values()) {
            expectedSchema[index++] = new VoltTable.ColumnInfo(col.name(), col.m_type);
        }
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;

        // Test table schema
        results = client.callProcedure("@Statistics", "COMMANDLOG", 0).getResults();
        System.out.println("Node commandlog statistics table: " + results[0].toString());
        assertEquals(1, results.length);
        validateSchema(results[0], expectedTable);
        results[0].advanceRow();
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, true);

        // Enough for community version
        if (!MiscUtils.isPro()) {
            return;
        }

        // Inject some transactions
        for (int i = 0; i < 2; i++) {
            long start = System.currentTimeMillis();
            for (int j = 0; j < 16000; j++) {
                client.callProcedure("NEW_ORDER.insert", (i * 16000 + j) % 32768);
            }
            long end = System.currentTimeMillis();
            System.out.println("Insertion took " + (end - start) + " ms");
            if (end - start < FSYNC_INTERVAL_GOLD) {
                System.out.println("Insertion took " + (end - start) + " ms, sleeping..");
                Thread.sleep(FSYNC_INTERVAL_GOLD - (end - start));
            }

            // Issue commandlog stats query
            Thread.sleep(1000);
            results = client.callProcedure("@Statistics", "COMMANDLOG", 0).getResults();
            System.out.println("commandlog statistics: " + results[0].toString());

            // Check every row
            while (results[0].advanceRow()) {
                // Print fsync interval
                int actualFsyncInterval = (int) results[0].getLong(CommandLogCols.FSYNC_INTERVAL.name());
                System.out.println("Actual fsync interval is " + actualFsyncInterval + "ms, specified interval is " + FSYNC_INTERVAL_GOLD + "ms");

                // Test segment counts
                if (i == 1) {
                    int actualLoanedSegmentCount = (int) results[0].getLong(CommandLogCols.IN_USE_SEGMENT_COUNT.name());
                    int actualSegmentCount = (int) results[0].getLong(CommandLogCols.SEGMENT_COUNT.name());
                    String message = "Unexpected segment count: should be 2";
                    assertTrue(message, actualSegmentCount == 2);
                    message = "Unexpected segment count: loaned segment count should be less than total count";
                    assertTrue(message, actualLoanedSegmentCount <= actualSegmentCount);
                }
            }
        }
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        return StatisticsTestSuiteBase.suite(TestStatisticsCommandLogStats.class, true);
    }
}
