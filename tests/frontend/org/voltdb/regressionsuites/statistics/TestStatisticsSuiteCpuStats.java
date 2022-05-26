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

import junit.framework.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class TestStatisticsSuiteCpuStats extends StatisticsTestSuiteBase {
    public TestStatisticsSuiteCpuStats(String name) {
        super(name);
    }

    public void testCpuStatistics() throws Exception {
        System.out.println("\n\nTESTING CPU STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[4];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("PERCENT_USED", VoltType.BIGINT);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;

        //
        // cpu
        //
        // give time to seed the stats cache?
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        results = client.callProcedure("@Statistics", "cpu", 0).getResults();
        System.out.println("Node cpu statistics table: " + results[0].toString());
        // one aggregate table returned
        assertEquals(1, results.length);
        validateSchema(results[0], expectedTable);
        results[0].advanceRow();
        // Hacky, on a single local cluster make sure that all 'nodes' are present.
        // CPU stats lacks a common string across nodes, but we can hijack the hostname in this case.
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, true);
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        return StatisticsTestSuiteBase.suite(TestStatisticsSuiteCpuStats.class, false);
    }
}
