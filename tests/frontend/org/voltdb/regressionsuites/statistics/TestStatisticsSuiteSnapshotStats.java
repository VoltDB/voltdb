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

import junit.framework.Test;

import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;

public class TestStatisticsSuiteSnapshotStats extends StatisticsTestSuiteBase {

    public TestStatisticsSuiteSnapshotStats(String name) {
        super(name);
    }

    public void testSnapshotStatus() throws Exception {
        System.out.println("\n\nTESTING SNAPSHOTSTATUS\n\n\n");
        if (KFACTOR == 0) {
            // SnapshotSave is a PRO feature starting from 4.0
            return;
        }

        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[15];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("TABLE", VoltType.STRING);
        expectedSchema[4] = new ColumnInfo("PATH", VoltType.STRING);
        expectedSchema[5] = new ColumnInfo("FILENAME", VoltType.STRING);
        expectedSchema[6] = new ColumnInfo("NONCE", VoltType.STRING);
        expectedSchema[7] = new ColumnInfo("TXNID", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("START_TIME", VoltType.BIGINT);
        expectedSchema[9] = new ColumnInfo("END_TIME", VoltType.BIGINT);
        expectedSchema[10] = new ColumnInfo("SIZE", VoltType.BIGINT);
        expectedSchema[11] = new ColumnInfo("DURATION", VoltType.BIGINT);
        expectedSchema[12] = new ColumnInfo("THROUGHPUT", VoltType.FLOAT);
        expectedSchema[13] = new ColumnInfo("RESULT", VoltType.STRING);
        expectedSchema[14] = new ColumnInfo("TYPE", VoltType.STRING);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        // Finagle a snapshot
        client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, 1);

        VoltTable[] results = null;
        //
        // SNAPSHOTSTATUS
        //
        results = client.callProcedure("@Statistics", "SNAPSHOTSTATUS", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Test SNAPSHOTSTATUS table: " + results[0].toString());
        validateSchema(results[0], expectedTable);
        // One row per table per node, test existence of manually added snapshot
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("NONCE", TESTNONCE);
        columnTargets.put("TABLE", "WAREHOUSE");
        validateRowSeenAtAllHosts(results[0], columnTargets, true);
        columnTargets.put("TABLE", "NEW_ORDER");
        validateRowSeenAtAllHosts(results[0], columnTargets, true);
        columnTargets.put("TABLE", "ITEM");
        validateRowSeenAtAllHosts(results[0], columnTargets, true);
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        return StatisticsTestSuiteBase.suite(TestStatisticsSuiteSnapshotStats.class, false);
    }
}
