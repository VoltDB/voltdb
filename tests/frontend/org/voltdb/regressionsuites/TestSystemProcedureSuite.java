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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.Random;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.regressionsuites.malicious.GoSleep;

public class TestSystemProcedureSuite extends RegressionSuite {

    private static int SITES = 3;
    private static int HOSTS = MiscUtils.isPro() ? 2 : 1;
    private static int KFACTOR = MiscUtils.isPro() ? 1 : 0;
    private static boolean hasLocalServer = false;

    static final Class<?>[] PROCEDURES =
    {
     GoSleep.class
    };

    public TestSystemProcedureSuite(String name) {
        super(name);
    }

    public void testPing() throws IOException, ProcCallException {
        Client client = getClient();
        ClientResponse cr = client.callProcedure("@Ping");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void checkProSysprocError(Client client, String name, int paramCount)
            throws NoConnectionsException, IOException
    {
        // make some dummy params... real ones aren't needed for this test
        Object[] params = new Object[paramCount];
        for (int i = 0; i < paramCount; i++) {
            params[i] = i;
        }

        try {
            client.callProcedure(name, params);
            fail();
        }
        catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            if (!e.getClientResponse().getStatusString().contains("Enterprise Edition")) {
                System.out.println("sup");
                System.out.println("MESSAGE: " + e.getClientResponse().getStatusString());
            }
            assertTrue(e.getClientResponse().getStatusString().contains("Enterprise"));
        }
    }

    public void testProSysprocErrorOnCommunity() throws Exception {
        // this test only applies to community edition
        if (MiscUtils.isPro()) {
            return;
        }

        Client client = getClient();

        checkProSysprocError(client, "@SnapshotSave", 3);
        checkProSysprocError(client, "@SnapshotRestore", 2);
        checkProSysprocError(client, "@SnapshotStatus", 0);
        checkProSysprocError(client, "@SnapshotScan", 2);
        checkProSysprocError(client, "@SnapshotDelete", 2);
        // Turns out we don't flag @Promote as enterprise.  Not touching that right now. --izzy
        //checkProSysprocError(client, "@Promote", 0);
    }

    public void testInvalidProcedureName() throws IOException {
        Client client = getClient();
        try {
            client.callProcedure("@SomeInvalidSysProcName", "1", "2");
        }
        catch (Exception e2) {
            assertEquals("Procedure @SomeInvalidSysProcName was not found", e2.getMessage());
            return;
        }
        fail("Expected exception.");
    }

    private final String m_loggingConfig =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" +
        "<!DOCTYPE log4j:configuration SYSTEM \"log4j.dtd\">" +
        "<log4j:configuration xmlns:log4j=\"http://jakarta.apache.org/log4j/\">" +
            "<appender name=\"Console\" class=\"org.apache.log4j.ConsoleAppender\">" +
                "<param name=\"Target\" value=\"System.out\" />" +
                "<layout class=\"org.apache.log4j.TTCCLayout\">" +
                "</layout>" +
            "</appender>" +
            "<appender name=\"Async\" class=\"org.apache.log4j.AsyncAppender\">" +
                "<param name=\"Blocking\" value=\"true\" />" +
                "<appender-ref ref=\"Console\" /> " +
            "</appender>" +
            "<root>" +
               "<priority value=\"info\" />" +
               "<appender-ref ref=\"Async\" />" +
            "</root>" +
        "</log4j:configuration>";

    public void testUpdateLogging() throws Exception {
        Client client = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@UpdateLogging", m_loggingConfig).getResults();
        for (VoltTable result : results) {
            assertEquals( 0, result.asScalarLong());
        }
    }

    public void testPromoteMaster() throws Exception {
        Client client = getClient();
        try {
            client.callProcedure("@Promote");
            fail();
        }
        catch (ProcCallException pce) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, pce.getClientResponse().getStatus());
        }
    }

    // Pretty lame test but at least invoke the procedure.
    // "@Quiesce" is used more meaningfully in TestExportSuite.
    public void testQuiesce() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable results[] = client.callProcedure("@Quiesce").getResults();
        assertEquals(1, results.length);
        results[0].advanceRow();
        assertEquals(results[0].get(0, VoltType.BIGINT), new Long(0));
    }

    public void testLoadMultipartitionTableAndIndexStatsAndValidatePartitioning() throws Exception {
        Client client = getClient();

        /*
         * Load a little partitioned data for the mispartitioned check
         */
        Random r = new Random(0);
        for (int ii = 0; ii < 50; ii++) {
            client.callProcedure(new NullCallback(), "@AdHoc",
                    "INSERT INTO new_order values (" + (short)(r.nextDouble() * Short.MAX_VALUE) + ");");
        }

        // try the failure case first
        try {
            client.callProcedure("@LoadMultipartitionTable", "DOES_NOT_EXIST", null, 1);
            fail();
        } catch (ProcCallException ex) {}

        // make a TPCC warehouse table
        VoltTable partitioned_table = new VoltTable(
                new VoltTable.ColumnInfo("W_ID", org.voltdb.VoltType.SMALLINT),
                new VoltTable.ColumnInfo("W_NAME", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STREET_1", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STREET_2", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_CITY", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STATE", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_ZIP", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_TAX",org.voltdb.VoltType.get((byte)8)),
                new VoltTable.ColumnInfo("W_YTD", org.voltdb.VoltType.get((byte)8))
        );

        for (int i = 1; i < 21; i++) {
            Object[] row = new Object[] {new Short((short) i),
                                         "name_" + i,
                                         "street1_" + i,
                                         "street2_" + i,
                                         "city_" + i,
                                         "ma",
                                         "zip_"  + i,
                                         new Double(i),
                                         new Double(i)};
            partitioned_table.addRow(row);
        }

        // make a TPCC item table
        VoltTable replicated_table =
            new VoltTable(new VoltTable.ColumnInfo("I_ID", VoltType.INTEGER),
                          new VoltTable.ColumnInfo("I_IM_ID", VoltType.INTEGER),
                          new VoltTable.ColumnInfo("I_NAME", VoltType.STRING),
                          new VoltTable.ColumnInfo("I_PRICE", VoltType.FLOAT),
                          new VoltTable.ColumnInfo("I_DATA", VoltType.STRING));

        for (int i = 1; i < 21; i++) {
            Object[] row = new Object[] {i,
                                         i,
                                         "name_" + i,
                                         new Double(i),
                                         "data_"  + i};

            replicated_table.addRow(row);
        }

        try {
            try {
                client.callProcedure("@LoadMultipartitionTable", "WAREHOUSE",
                                 partitioned_table);
                fail();
            } catch (ProcCallException e) {}
            client.callProcedure("@LoadMultipartitionTable", "ITEM",
                                 replicated_table);

            // 20 rows per site for the replicated table.  Wait for it...
            int rowcount = 0;
            VoltTable results[] = client.callProcedure("@Statistics", "table", 0).getResults();
            while (rowcount != (20 * SITES * HOSTS)) {
                rowcount = 0;
                results = client.callProcedure("@Statistics", "table", 0).getResults();
                // Check that tables loaded correctly
                while(results[0].advanceRow()) {
                    if (results[0].getString("TABLE_NAME").equals("ITEM"))
                    {
                        rowcount += results[0].getLong("TUPLE_COUNT");
                    }
                }
            }

            System.out.println(results[0]);

            // Check that tables loaded correctly
            int foundItem = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while(results[0].advanceRow()) {
                if (results[0].getString("TABLE_NAME").equals("ITEM"))
                {
                    ++foundItem;
                    //Different values depending on local cluster vs. single process hence ||
                    assertEquals(20, results[0].getLong("TUPLE_COUNT"));
                }
            }
            assertEquals(MiscUtils.isPro() ? 6 : 3, foundItem);

            // Table finally loaded fully should mean that index is okay on first read.
            VoltTable indexStats =
                    client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
            System.out.println(indexStats);
            long memorySum = 0;
            while (indexStats.advanceRow()) {
                memorySum += indexStats.getLong("MEMORY_ESTIMATE");
            }

            /*
             * It takes about a minute to spin through this 1000 times.
             * Should definitely give a 1 second tick time to fire
             */
            long indexMemorySum = 0;
            for (int ii = 0; ii < 1000; ii++) {
                indexMemorySum = 0;
                indexStats = client.callProcedure("@Statistics", "MEMORY", 0).getResults()[0];
                System.out.println(indexStats);
                while (indexStats.advanceRow()) {
                    indexMemorySum += indexStats.getLong("INDEXMEMORY");
                }
                boolean success = indexMemorySum != 120;//That is a row count, not memory usage
                if (success) {
                    success = memorySum == indexMemorySum;
                    if (success) {
                        break;
                    }
                }
                Thread.sleep(1);
            }
            assertTrue(indexMemorySum != 120);//That is a row count, not memory usage
            assertEquals(memorySum, indexMemorySum);

            /*
             * Test once using the current correct hash function,
             * expect no mispartitioned rows
             */
            ClientResponse cr = client.callProcedure("@ValidatePartitioning", 0, null);

            VoltTable hashinatorMatches = cr.getResults()[1];
            while (hashinatorMatches.advanceRow()) {
                assertEquals(1L, hashinatorMatches.getLong("HASHINATOR_MATCHES"));
            }

            VoltTable validateResult = cr.getResults()[0];
            System.out.println(validateResult);
            while (validateResult.advanceRow()) {
                assertEquals(0L, validateResult.getLong("MISPARTITIONED_ROWS"));
            }

            /*
             * Test again with a bad hash function, expect mispartitioned rows
             */
            cr = client.callProcedure("@ValidatePartitioning", 0, new byte[] { 0, 0, 0, 9 });

            hashinatorMatches = cr.getResults()[1];
            while (hashinatorMatches.advanceRow()) {
                assertEquals(0L, hashinatorMatches.getLong("HASHINATOR_MATCHES"));
            }

            validateResult = cr.getResults()[0];
            System.out.println(validateResult);
            while (validateResult.advanceRow()) {
                if (validateResult.getString("TABLE").equals("NEW_ORDER")) {
                    assertTrue(validateResult.getLong("MISPARTITIONED_ROWS") > 0);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    // verify that these commands don't blow up
    public void testProfCtl() throws Exception {
        Client client = getClient();

        //
        // SAMPLER_START
        //
        ClientResponse resp = client.callProcedure("@ProfCtl", "SAMPLER_START");
        VoltTable vt = resp.getResults()[0];
        boolean foundResponse = false;
        while (vt.advanceRow()) {
            if (!vt.getString("Result").equalsIgnoreCase("sampler_start")) {
                fail();
            }
            foundResponse = true;
        }
        assertTrue(foundResponse);

        //
        // GPERF_ENABLE
        //
        resp = client.callProcedure("@ProfCtl", "GPERF_ENABLE");
        vt = resp.getResults()[0];
        foundResponse = false;
        while (vt.advanceRow()) {
            if (vt.getString("Result").equalsIgnoreCase("GPERF_ENABLE")) {
                foundResponse = true;
            }
            else {
                assertTrue(vt.getString("Result").equalsIgnoreCase("GPERF_NOOP"));
            }
        }
        assertTrue(foundResponse);

        //
        // GPERF_DISABLE
        //
        resp = client.callProcedure("@ProfCtl", "GPERF_DISABLE");
        vt = resp.getResults()[0];
        foundResponse = false;
        while (vt.advanceRow()) {
            if (vt.getString("Result").equalsIgnoreCase("gperf_disable")) {
                foundResponse = true;
            }
            else {
                assertTrue(vt.getString("Result").equalsIgnoreCase("GPERF_NOOP"));
            }
        }
        assertTrue(foundResponse);

        //
        // garbage
        //
        resp = client.callProcedure("@ProfCtl", "MakeAPony");
        vt = resp.getResults()[0];
        assertTrue(true);
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSystemProcedureSuite.class);

        // Not really using TPCC functionality but need a database.
        // The testLoadMultipartitionTable procedure assumes partitioning
        // on warehouse id.
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(
                        "CREATE TABLE WAREHOUSE (\n" +
                        "  W_ID SMALLINT DEFAULT '0' NOT NULL,\n" +
                        "  W_NAME VARCHAR(16) DEFAULT NULL,\n" +
                        "  W_STREET_1 VARCHAR(32) DEFAULT NULL,\n" +
                        "  W_STREET_2 VARCHAR(32) DEFAULT NULL,\n" +
                        "  W_CITY VARCHAR(32) DEFAULT NULL,\n" +
                        "  W_STATE VARCHAR(2) DEFAULT NULL,\n" +
                        "  W_ZIP VARCHAR(9) DEFAULT NULL,\n" +
                        "  W_TAX FLOAT DEFAULT NULL,\n" +
                        "  W_YTD FLOAT DEFAULT NULL,\n" +
                        "  CONSTRAINT W_PK_TREE PRIMARY KEY (W_ID)\n" +
                        ");\n" +
                        "CREATE TABLE ITEM (\n" +
                        "  I_ID INTEGER DEFAULT '0' NOT NULL,\n" +
                        "  I_IM_ID INTEGER DEFAULT NULL,\n" +
                        "  I_NAME VARCHAR(32) DEFAULT NULL,\n" +
                        "  I_PRICE FLOAT DEFAULT NULL,\n" +
                        "  I_DATA VARCHAR(64) DEFAULT NULL,\n" +
                        "  CONSTRAINT I_PK_TREE PRIMARY KEY (I_ID)\n" +
                        ");\n" +
                        "CREATE TABLE NEW_ORDER (\n" +
                        "  NO_W_ID SMALLINT DEFAULT '0' NOT NULL\n" +
                        ");\n");

        project.addPartitionInfo("WAREHOUSE", "W_ID");
        project.addPartitionInfo("NEW_ORDER", "NO_W_ID");
        project.addProcedures(PROCEDURES);

        /*config = new LocalCluster("sysproc-twosites.jar", 2, 1, 0,
                                  BackendTarget.NATIVE_EE_JNI);
        ((LocalCluster) config).setHasLocalServer(false);
        config.compile(project);
        builder.addServerConfig(config);*/

        /*
         * Add a cluster configuration for sysprocs too
         */
        config = new LocalCluster("sysproc-cluster.jar", TestSystemProcedureSuite.SITES, TestSystemProcedureSuite.HOSTS, TestSystemProcedureSuite.KFACTOR,
                                  BackendTarget.NATIVE_EE_JNI);
        ((LocalCluster) config).setHasLocalServer(hasLocalServer);
        boolean success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}


