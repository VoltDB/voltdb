/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.malicious.GoSleep;

public class TestSystemProcedureSuite extends RegressionSuite {

    private static int sites = 3;
    private static int hosts = 2;
    private static int kfactor = 1;
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

    public void testStatistics() throws Exception {
        Client client  = getClient();

        //
        // initiator selector
        //
        VoltTable results[] = null;
        results = client.callProcedure("@Statistics", "INITIATOR", 0).getResults();
        results = client.callProcedure("@Statistics", "INITIATOR", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Test initiators table: " + results[0].toString());
        assertEquals(1, results[0].getRowCount());
        VoltTableRow resultRow = results[0].fetchRow(0);
        assertNotNull(resultRow);
        assertEquals("@Statistics", resultRow.getString("PROCEDURE_NAME"));
        assertEquals( 1, resultRow.getLong("INVOCATIONS"));

        //
        // invalid selector
        //
        try {
            // No selector at all.
            client.callProcedure("@Statistics");
            fail();
        }
        catch (ProcCallException ex) {
            // All badness gets turned into ProcCallExceptions, so we need
            // to check specifically for this error, otherwise things that
            // crash the cluster also turn into ProcCallExceptions and don't
            // trigger failure (ENG-2347)
            assertEquals("VOLTDB ERROR: PROCEDURE Statistics EXPECTS 3 PARAMS, BUT RECEIVED 1",
                         ex.getMessage());
        }
        try {
            // Invalid selector
            client.callProcedure("@Statistics", "garbage", 0);
            fail();
        }
        catch (ProcCallException ex) {}

        //
        // Partition count
        //
        results = client.callProcedure("@Statistics", SysProcSelector.PARTITIONCOUNT.name(), 0).getResults();
        assertEquals( 1, results.length);
        assertTrue( results[0] != null);
        assertEquals( 1, results[0].getRowCount());
        assertEquals( 1, results[0].getColumnCount());
        assertEquals( VoltType.INTEGER, results[0].getColumnType(0));
        assertTrue( results[0].advanceRow());
        final int siteCount = (int)results[0].getLong(0);
        assertTrue (siteCount == TestSystemProcedureSuite.sites);

        //
        // table
        //
        results = client.callProcedure("@Statistics", "table", 0).getResults();
        // one aggregate table returned
        assertTrue(results.length == 1);
        // with 10 rows per site. Can be two values depending on the test scenario of cluster vs. local.
        assertEquals(TestSystemProcedureSuite.hosts * TestSystemProcedureSuite.sites * 3, results[0].getRowCount());

        System.out.println("Test statistics table: " + results[0].toString());

        results = client.callProcedure("@Statistics", "index", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);

        //
        // memory
        //
        // give time to seed the stats cache?
        Thread.sleep(1000);
        results = client.callProcedure("@Statistics", "memory", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Node memory statistics table: " + results[0].toString());
        // alternate form
        results = client.callProcedure("@Statistics", "nodememory", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Node memory statistics table: " + results[0].toString());

        //
        // procedure
        //
        // 3 seconds translates to 3 billion nanos, which overflows internal
        // values (ENG-1039)
        results = client.callProcedure("GoSleep", 3000, 0, null).getResults();
        results = client.callProcedure("@Statistics", "procedure", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Test procedures table: " + results[0].toString());

        VoltTable stats = results[0];
        stats.advanceRow();

        // Retrieve all statistics
        long min_time = (Long)stats.get("MIN_EXECUTION_TIME", VoltType.BIGINT);
        long max_time = (Long)stats.get("MAX_EXECUTION_TIME", VoltType.BIGINT);
        long avg_time = (Long)stats.get("AVG_EXECUTION_TIME", VoltType.BIGINT);
        long min_result_size = (Long)stats.get("MIN_RESULT_SIZE", VoltType.BIGINT);
        long max_result_size = (Long)stats.get("MAX_RESULT_SIZE", VoltType.BIGINT);
        long avg_result_size = (Long)stats.get("AVG_RESULT_SIZE", VoltType.BIGINT);
        long min_parameter_set_size = (Long)stats.get("MIN_PARAMETER_SET_SIZE", VoltType.BIGINT);
        long max_parameter_set_size = (Long)stats.get("MAX_PARAMETER_SET_SIZE", VoltType.BIGINT);
        long avg_parameter_set_size = (Long)stats.get("AVG_PARAMETER_SET_SIZE", VoltType.BIGINT);

        // Check for overflow
        assertTrue("Failed MIN_EXECUTION_TIME > 0, value was: " + min_time,
                   min_time > 0);
        assertTrue("Failed MAX_EXECUTION_TIME > 0, value was: " + max_time,
                   max_time > 0);
        assertTrue("Failed AVG_EXECUTION_TIME > 0, value was: " + avg_time,
                   avg_time > 0);
        assertTrue("Failed MIN_RESULT_SIZE > 0, value was: " + min_result_size,
                   min_result_size >= 0);
        assertTrue("Failed MAX_RESULT_SIZE > 0, value was: " + max_result_size,
                   max_result_size >= 0);
        assertTrue("Failed AVG_RESULT_SIZE > 0, value was: " + avg_result_size,
                   avg_result_size >= 0);
        assertTrue("Failed MIN_PARAMETER_SET_SIZE > 0, value was: " + min_parameter_set_size,
                   min_parameter_set_size >= 0);
        assertTrue("Failed MAX_PARAMETER_SET_SIZE > 0, value was: " + max_parameter_set_size,
                   max_parameter_set_size >= 0);
        assertTrue("Failed AVG_PARAMETER_SET_SIZE > 0, value was: " + avg_parameter_set_size,
                   avg_parameter_set_size >= 0);

        // check for reasonable values
        assertTrue("Failed MIN_EXECUTION_TIME > 2,400,000,000ns, value was: " +
                   min_time,
                   min_time > 2400000000L);
        assertTrue("Failed MAX_EXECUTION_TIME > 2,400,000,000ns, value was: " +
                   max_time,
                   max_time > 2400000000L);
        assertTrue("Failed AVG_EXECUTION_TIME > 2,400,000,000ns, value was: " +
                   avg_time,
                   avg_time > 2400000000L);
        assertTrue("Failed MIN_RESULT_SIZE < 1,000,000, value was: " +
                   min_result_size,
                   min_result_size < 1000000L);
        assertTrue("Failed MAX_RESULT_SIZE < 1,000,000, value was: " +
                   max_result_size,
                   max_result_size < 1000000L);
        assertTrue("Failed AVG_RESULT_SIZE < 1,000,000, value was: " +
                   avg_result_size,
                   avg_result_size < 1000000L);
        assertTrue("Failed MIN_PARAMETER_SET_SIZE < 1,000,000, value was: " +
                   min_parameter_set_size,
                   min_parameter_set_size < 1000000L);
        assertTrue("Failed MAX_PARAMETER_SET_SIZE < 1,000,000, value was: " +
                   max_parameter_set_size,
                   max_parameter_set_size < 1000000L);
        assertTrue("Failed AVG_PARAMETER_SET_SIZE < 1,000,000, value was: " +
                   avg_parameter_set_size,
                   avg_parameter_set_size < 1000000L);

        //
        // iostats
        //
        results = client.callProcedure("@Statistics", "iostats", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Test iostats table: " + results[0].toString());
    }

    //
    // planner statistics
    //
    public void testPlannerStatistics() throws Exception {
        Client client  = getClient();
        // Clear the interval statistics
        VoltTable[] results = client.callProcedure("@Statistics", "planner", 1).getResults();
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

    //public void testShutdown() {
    //    running @shutdown kills the JVM.
    //    not sure how to test this.
    // }

    // covered by TestSystemInformationSuite
    /*public void testSystemInformation() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable results[] = client.callProcedure("@SystemInformation").getResults();
        assertEquals(1, results.length);
        System.out.println(results[0]);
    }*/

    // Pretty lame test but at least invoke the procedure.
    // "@Quiesce" is used more meaningfully in TestExportSuite.
    public void testQuiesce() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable results[] = client.callProcedure("@Quiesce").getResults();
        assertEquals(1, results.length);
        results[0].advanceRow();
        assertEquals(results[0].get(0, VoltType.BIGINT), new Long(0));
    }

    public void testLoadMultipartitionTable() throws IOException {
        Client client = getClient();

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
            VoltTable results[] = client.callProcedure("@Statistics", "table", 0).getResults();

            int foundItem = 0;

            System.out.println(results[0]);

            // Check that tables loaded correctly
            while(results[0].advanceRow()) {
                if (results[0].getString("TABLE_NAME").equals("ITEM"))
                {
                    ++foundItem;
                    //Different values depending on local cluster vs. single process hence ||
                    assertEquals(20, results[0].getLong("TUPLE_COUNT"));
                }
            }
            assertEquals(6, foundItem);
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
        config = new LocalCluster("sysproc-cluster.jar", TestSystemProcedureSuite.sites, TestSystemProcedureSuite.hosts, TestSystemProcedureSuite.kfactor,
                                  BackendTarget.NATIVE_EE_JNI);
        ((LocalCluster) config).setHasLocalServer(hasLocalServer);
        boolean success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}


