/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.Test;

public class TestEmptySchema extends RegressionSuite
{
    public TestEmptySchema(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("");
        return builder;
    }

    public void testEmptySchema() throws Exception {
        final Client client = getClient();
        // sleep a little so that we have time for the IPC backend to actually be running
        // so it can screw us on empty results
        Thread.sleep(1000);

        // Even running should be an improvement (ENG-4645), but do something just to be sure
        // Also, check to be sure we get a full schema for the table and index stats
        ColumnInfo[] expectedSchema = new ColumnInfo[13];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("PARTITION_ID", VoltType.BIGINT);
        expectedSchema[5] = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        expectedSchema[6] = new ColumnInfo("TABLE_TYPE", VoltType.STRING);
        expectedSchema[7] = new ColumnInfo("TUPLE_COUNT", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("TUPLE_ALLOCATED_MEMORY", VoltType.BIGINT);
        expectedSchema[9] = new ColumnInfo("TUPLE_DATA_MEMORY", VoltType.BIGINT);
        expectedSchema[10] = new ColumnInfo("STRING_DATA_MEMORY", VoltType.BIGINT);
        expectedSchema[11] = new ColumnInfo("TUPLE_LIMIT", VoltType.INTEGER);
        expectedSchema[12] = new ColumnInfo("PERCENT_FULL", VoltType.INTEGER);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = client.callProcedure("@Statistics", "TABLE", 0).getResults();
        System.out.println("TABLE RESULTS: " + results[0]);
        assertEquals(0, results[0].getRowCount());
        assertEquals(expectedSchema.length, results[0].getColumnCount());
        // Test the log search utility
        assertEquals(true, localClusterAllInitLogsContain("Initialized VoltDB root directory"));
        assertEquals(true, localClusterAllHostLogsContain(".*VoltDB [a-zA-Z]* Edition.*"));
        for (int i : getLocalClusterHostIds()) {
            assertEquals(true, localClusterInitlogContains(i, "Initialized VoltDB .* directory"));
            assertEquals(true, localClusterHostLogContains(i, "VoltDB [a-zA-Z]* Edition"));
        }

        validateSchema(results[0], expectedTable);

        expectedSchema = new ColumnInfo[12];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("PARTITION_ID", VoltType.BIGINT);
        expectedSchema[5] = new ColumnInfo("INDEX_NAME", VoltType.STRING);
        expectedSchema[6] = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        expectedSchema[7] = new ColumnInfo("INDEX_TYPE", VoltType.STRING);
        expectedSchema[8] = new ColumnInfo("IS_UNIQUE", VoltType.TINYINT);
        expectedSchema[9] = new ColumnInfo("IS_COUNTABLE", VoltType.TINYINT);
        expectedSchema[10] = new ColumnInfo("ENTRY_COUNT", VoltType.BIGINT);
        expectedSchema[11] = new ColumnInfo("MEMORY_ESTIMATE", VoltType.BIGINT);
        expectedTable = new VoltTable(expectedSchema);

        results = client.callProcedure("@Statistics", "INDEX", 0).getResults();
        System.out.println("INDEX RESULTS: " + results[0]);
        assertEquals(0, results[0].getRowCount());
        assertEquals(expectedSchema.length, results[0].getColumnCount());
        validateSchema(results[0], expectedTable);

        // Shutdown a single host and restart
        LocalCluster localCluster = (LocalCluster) m_config;
        localCluster.killSingleHost(1);

        localCluster.allHostLogsContain("Host 1 failed");

        localCluster.setNewCli(false);  // This is needed to perform rejoin
        localCluster.recoverOne(1, 1, "");

        // In community edition this should fail ? Since rejoin is only supported in enterprise
        // edition
        boolean rejoinMsg = localClusterHostLogContains(1, "VoltDB Community Edition only supports .*") // failure
                            || localClusterHostLogContains(1, "Initializing VoltDB .*");    // Success
        assertEquals(true, rejoinMsg);

        // Shutdown and startup the whole cluster
        localCluster.shutDown();    // After shutdown the in-memory logs are cleared

        // Check the on-disk files instead
        // Note that host 1 is not successfully joined, therefore its log cannot be trusted
        for (int i : getLocalClusterHostIds()) {
            if (i != 1) {
                String hostLogPath = localCluster.getHostLogPath(i);
                assertTrue(localCluster.fileContains(hostLogPath,
                        "VoltDB has encountered an unrecoverable error and is exiting."));
            }
        }
        localCluster.startUp();

        // Test the log search utility
        assertEquals(true, localClusterAllInitLogsContain("Initialized VoltDB root directory"));
        assertEquals(true, localClusterAllHostLogsContain(".*VoltDB [a-zA-Z]* Edition.*"));
        for (int i : getLocalClusterHostIds()) {
            // No init log after restart (newCli == false)
            assertEquals(true, localClusterHostLogContains(i, "VoltDB [a-zA-Z]* Edition"));
        }
    }

    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestEmptySchema.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        boolean success;
        LocalCluster config = new LocalCluster(true, "decimal-default.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);

        success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
