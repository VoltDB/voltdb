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
import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;

import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;

public class TestStatisticsSuiteDRStats extends StatisticsTestSuiteBase {

    public TestStatisticsSuiteDRStats(String name) {
        super(name);
    }

    public void testDRNodeStatistics() throws Exception {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            System.out.println("SKIPPING DRNODE STATS TESTS FOR COMMUNITY VERSION");
            return;
        }
        System.out.println("\n\nTESTING DRNODE STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema2 = new ColumnInfo[8];
        assertEquals("Expected DRNodeStatistics schema length is 8", 8, expectedSchema2.length);
        expectedSchema2[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema2[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema2[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema2[3] = new ColumnInfo("STATE", VoltType.STRING);
        expectedSchema2[4] = new ColumnInfo("SYNCSNAPSHOTSTATE", VoltType.STRING);
        expectedSchema2[5] = new ColumnInfo("ROWSINSYNCSNAPSHOT", VoltType.BIGINT);
        expectedSchema2[6] = new ColumnInfo("ROWSACKEDFORSYNCSNAPSHOT", VoltType.BIGINT);
        expectedSchema2[7] = new ColumnInfo("QUEUEDEPTH", VoltType.BIGINT);
        VoltTable expectedTable2 = new VoltTable(expectedSchema2);

        VoltTable[] results = null;
        //
        // DRNODE
        //
        results = client.callProcedure("@Statistics", "DRPRODUCERNODE", 0).getResults();
        // one aggregate tables returned
        assertEquals(1, results.length);
        System.out.println("Test DRPRODUCERNODE table: " + results[0].toString());
        validateSchema(results[0], expectedTable2);
        // One row per host for DRNODE stats
        results[0].advanceRow();
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, true);
    }

    public void testDRPartitionStatistics() throws Exception {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            System.out.println("SKIPPING DRPRODUCERPARTITION STATS TESTS FOR COMMUNITY VERSION");
            return;
        }
        System.out.println("\n\nTESTING DRPRODUCERPARTITION STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema1 = new ColumnInfo[14];
        assertEquals("Expected DRPartitionStatistics schema length is 14", 14, expectedSchema1.length);
        expectedSchema1[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema1[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema1[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema1[3] = new ColumnInfo("PARTITION_ID", VoltType.INTEGER);
        expectedSchema1[4] = new ColumnInfo("STREAMTYPE", VoltType.STRING);
        expectedSchema1[5] = new ColumnInfo("TOTALBYTES", VoltType.BIGINT);
        expectedSchema1[6] = new ColumnInfo("TOTALBYTESINMEMORY", VoltType.BIGINT);
        expectedSchema1[7] = new ColumnInfo("TOTALBUFFERS", VoltType.BIGINT);
        expectedSchema1[8] = new ColumnInfo("LASTQUEUEDDRID", VoltType.BIGINT);
        expectedSchema1[9] = new ColumnInfo("LASTACKDRID", VoltType.BIGINT);
        expectedSchema1[10] = new ColumnInfo("LASTQUEUEDTIMESTAMP", VoltType.TIMESTAMP);
        expectedSchema1[11] = new ColumnInfo("LASTACKTIMESTAMP", VoltType.TIMESTAMP);
        expectedSchema1[12] = new ColumnInfo("ISSYNCED", VoltType.STRING);
        expectedSchema1[13] = new ColumnInfo("MODE", VoltType.STRING);
        VoltTable expectedTable1 = new VoltTable(expectedSchema1);

        VoltTable[] results = null;
        //
        // DRPARTITION
        //
        results = client.callProcedure("@Statistics", "DRPRODUCERPARTITION", 0).getResults();
        // one aggregate tables returned
        assertEquals(1, results.length);
        System.out.println("Test DR table: " + results[0].toString());
        validateSchema(results[0], expectedTable1);
        // One row per site (including the MPI on each host),
        // don't have HSID for ease of check, just check a bunch of stuff
        assertEquals(HOSTS * SITES + HOSTS, results[0].getRowCount());
        results[0].advanceRow();
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, false);
        results[0].advanceRow();
        validateRowSeenAtAllPartitions(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), false);
    }

    public void testDRStatistics() throws Exception {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            System.out.println("SKIPPING DR STATS TESTS FOR COMMUNITY VERSION");
            return;
        }
        System.out.println("\n\nTESTING DR STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema1 = new ColumnInfo[14];
        assertEquals("Expected DRPartitionStatistics schema length is 14", 14, expectedSchema1.length);
        expectedSchema1[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema1[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema1[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema1[3] = new ColumnInfo("PARTITION_ID", VoltType.INTEGER);
        expectedSchema1[4] = new ColumnInfo("STREAMTYPE", VoltType.STRING);
        expectedSchema1[5] = new ColumnInfo("TOTALBYTES", VoltType.BIGINT);
        expectedSchema1[6] = new ColumnInfo("TOTALBYTESINMEMORY", VoltType.BIGINT);
        expectedSchema1[7] = new ColumnInfo("TOTALBUFFERS", VoltType.BIGINT);
        expectedSchema1[8] = new ColumnInfo("LASTQUEUEDDRID", VoltType.BIGINT);
        expectedSchema1[9] = new ColumnInfo("LASTACKDRID", VoltType.BIGINT);
        expectedSchema1[10] = new ColumnInfo("LASTQUEUEDTIMESTAMP", VoltType.TIMESTAMP);
        expectedSchema1[11] = new ColumnInfo("LASTACKTIMESTAMP", VoltType.TIMESTAMP);
        expectedSchema1[12] = new ColumnInfo("ISSYNCED", VoltType.STRING);
        expectedSchema1[13] = new ColumnInfo("MODE", VoltType.STRING);
        VoltTable expectedTable1 = new VoltTable(expectedSchema1);

        ColumnInfo[] expectedSchema2 = new ColumnInfo[8];
        assertEquals("Expected DRNodeStatistics schema length is 8", 8, expectedSchema2.length);
        expectedSchema2[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema2[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema2[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema2[3] = new ColumnInfo("STATE", VoltType.STRING);
        expectedSchema2[4] = new ColumnInfo("SYNCSNAPSHOTSTATE", VoltType.STRING);
        expectedSchema2[5] = new ColumnInfo("ROWSINSYNCSNAPSHOT", VoltType.BIGINT);
        expectedSchema2[6] = new ColumnInfo("ROWSACKEDFORSYNCSNAPSHOT", VoltType.BIGINT);
        expectedSchema2[7] = new ColumnInfo("QUEUEDEPTH", VoltType.BIGINT);
        VoltTable expectedTable2 = new VoltTable(expectedSchema2);

        VoltTable[] results = null;
        //
        // DR
        //
        results = client.callProcedure("@Statistics", "DR", 0).getResults();
        // two aggregate tables returned
        assertEquals(2, results.length);
        System.out.println("Test DR table: " + results[0].toString());
        System.out.println("Test DR table: " + results[1].toString());
        validateSchema(results[0], expectedTable1);
        validateSchema(results[1], expectedTable2);
        // One row per site (including the MPI on each host),
        // don't have HSID for ease of check, just check a bunch of stuff
        assertEquals(HOSTS * SITES + HOSTS, results[0].getRowCount());
        results[0].advanceRow();
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, false);
        results[0].advanceRow();
        validateRowSeenAtAllPartitions(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), false);
        // One row per host for DRNODE stats
        results[1].advanceRow();
        columnTargets.put("HOSTNAME", results[1].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[1], columnTargets, true);
    }
    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        return StatisticsTestSuiteBase.suite(TestStatisticsSuiteDRStats.class, false);
    }
}
