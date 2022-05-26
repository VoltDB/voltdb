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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.hsqldb_voltpatches.HSQLInterface;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;

import junit.framework.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStatisticsSuiteDatabaseElementStats extends StatisticsTestSuiteBase {

    public TestStatisticsSuiteDatabaseElementStats(String name) {
        super(name);
    }

    public void testInvalidCalls() throws Exception {
        System.out.println("\n\nTESTING INVALID CALLS\n\n\n");
        Client client = getFullyConnectedClient();
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
            assertEquals("Incorrect number of arguments to @Statistics (expects 2, received 0)",
                         ex.getMessage());
        }
        try {
            // extra stuff
            client.callProcedure("@Statistics", "table", 0, "OHHAI");
            fail();
        }
        catch (ProcCallException ex) {
            assertEquals("Incorrect number of arguments to @Statistics (expects 2, received 3)",
                         ex.getMessage());
        }
        try {
            // Invalid selector
            client.callProcedure("@Statistics", "garbage", 0);
            fail();
        }
        catch (ProcCallException ex) {}

    }

    public void testTableStatistics() throws Exception {
        System.out.println("\n\nTESTING TABLE STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[15];
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
        expectedSchema[13] = new ColumnInfo("DR", VoltType.STRING);
        expectedSchema[14] = new ColumnInfo("EXPORT", VoltType.STRING);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        Awaitility.await("for rows seen at all sites")
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                    () -> {
                        VoltTable[] results = client.callProcedure("@Statistics", "table", 0).getResults();
                        assertThat(results).hasSize(1);
                        VoltTable voltTable = results[0];

                        // with 10 rows per site. Can be two values depending on the test scenario of cluster vs. local.
                        assertThat(voltTable.getRowCount()).isEqualTo(HOSTS * SITES * 3);
                        validateSchema(voltTable, expectedTable);

                        assertThat(validateRowSeenAtAllSites(voltTable, "TABLE_NAME", "WAREHOUSE", true)).isTrue();
                        assertThat(validateRowSeenAtAllSites(voltTable, "TABLE_NAME", "NEW_ORDER", true)).isTrue();
                        assertThat(validateRowSeenAtAllSites(voltTable, "TABLE_NAME", "ITEM", true)).isTrue();
                    }
                );
    }

    public void testIndexStatistics() throws Exception {
        System.out.println("\n\nTESTING INDEX STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[12];
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
        VoltTable expectedTable = new VoltTable(expectedSchema);

        Awaitility.await("for rows seen at all sites")
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            VoltTable[] results = client.callProcedure("@Statistics", "index", 0).getResults();
                            assertThat(results).hasSize(1);
                            VoltTable voltTable = results[0];

                            validateSchema(voltTable, expectedTable);

                            assertThat(validateRowSeenAtAllSites(voltTable, "INDEX_NAME", HSQLInterface.AUTO_GEN_NAMED_CONSTRAINT_IDX + "W_PK_TREE", true)).isTrue();
                            assertThat(validateRowSeenAtAllSites(voltTable, "INDEX_NAME", HSQLInterface.AUTO_GEN_NAMED_CONSTRAINT_IDX + "I_PK_TREE", true)).isTrue();
                        }
                );
    }

    public void testProcedureStatistics() throws Exception {
        System.out.println("\n\nTESTING PROCEDURE STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[21];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("PARTITION_ID", VoltType.INTEGER);
        expectedSchema[5] = new ColumnInfo("PROCEDURE", VoltType.STRING);
        expectedSchema[6] = new ColumnInfo("INVOCATIONS", VoltType.BIGINT);
        expectedSchema[7] = new ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT);
        expectedSchema[9] = new ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT);
        expectedSchema[10] = new ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT);
        expectedSchema[11] = new ColumnInfo("MIN_RESULT_SIZE", VoltType.INTEGER);
        expectedSchema[12] = new ColumnInfo("MAX_RESULT_SIZE", VoltType.INTEGER);
        expectedSchema[13] = new ColumnInfo("AVG_RESULT_SIZE", VoltType.INTEGER);
        expectedSchema[14] = new ColumnInfo("MIN_PARAMETER_SET_SIZE", VoltType.INTEGER);
        expectedSchema[15] = new ColumnInfo("MAX_PARAMETER_SET_SIZE", VoltType.INTEGER);
        expectedSchema[16] = new ColumnInfo("AVG_PARAMETER_SET_SIZE", VoltType.INTEGER);
        expectedSchema[17] = new ColumnInfo("ABORTS", VoltType.BIGINT);
        expectedSchema[18] = new ColumnInfo("FAILURES", VoltType.BIGINT);
        expectedSchema[19] = new ColumnInfo("TRANSACTIONAL", VoltType.TINYINT);
        expectedSchema[20] = new ColumnInfo("COMPOUND", VoltType.TINYINT);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;

        //
        // procedure
        //
        // Induce procedure invocations on all partitions.  May fail in non-legacy hashing case
        // this plus R/W replication should ensure that every site on every node runs this transaction
        // at least once

        client.callProcedure("@Statistics", "proceduredetail", 1);
        results = client.callProcedure("@GetPartitionKeys", "INTEGER").getResults();
        VoltTable keys = results[0];
        for (int k = 0;k < keys.getRowCount(); k++) {
            long key = keys.fetchRow(k).getLong(1);
            client.callProcedure("NEW_ORDER.insert", key);
        }

        for (int i = 0; i < HOSTS * SITES; i++) {
            client.callProcedure("NEW_ORDER.insert", i);
        }
        // 3 seconds translates to 3 billion nanos, which overflows internal
        // values (ENG-1039)
        //It's possible that the nanosecond count goes backwards... so run this a couple
        //of times to make sure the min value gets set
        for (int ii = 0; ii < 3; ii++) {
            results = client.callProcedure("GoSleep", 3000, 0, null).getResults();
        }
        results = client.callProcedure("@Statistics", "procedure", 0).getResults();
        System.out.println("Test procedures table: " + results[0].toString());
        // one aggregate table returned
        assertEquals(1, results.length);
        validateSchema(results[0], expectedTable);
        // For this table, where unique HSID isn't written to SITE_ID, these
        // two checks should ensure we get all the rows we expect?
        Map<String, String> columnTargets = new HashMap<>();
        columnTargets.put("PROCEDURE", "NEW_ORDER.insert");
        validateRowSeenAtAllHosts(results[0], columnTargets, false);
        validateRowSeenAtAllPartitions(results[0], "PROCEDURE", "NEW_ORDER.insert", false);
        results[0].resetRowPosition();

        VoltTable stats = results[0];
        String procname = "blerg";
        while (!procname.equals("org.voltdb_testprocs.regressionsuites.malicious.GoSleep")) {
            stats.advanceRow();
            procname = (String)stats.get("PROCEDURE", VoltType.STRING);
        }

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

        // Validate the schema of PROCEDUREDETAIL
        results = client.callProcedure("@Statistics", "proceduredetail", 0).getResults();
        assertEquals(1, results.length);
        expectedSchema = new ColumnInfo[20];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("PARTITION_ID", VoltType.INTEGER);
        expectedSchema[5] = new ColumnInfo("PROCEDURE", VoltType.STRING);
        expectedSchema[6] = new ColumnInfo("STATEMENT", VoltType.STRING);
        expectedSchema[7] = new ColumnInfo("INVOCATIONS", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT);
        expectedSchema[9] = new ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT);
        expectedSchema[10] = new ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT);
        expectedSchema[11] = new ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT);
        expectedSchema[12] = new ColumnInfo("MIN_RESULT_SIZE", VoltType.INTEGER);
        expectedSchema[13] = new ColumnInfo("MAX_RESULT_SIZE", VoltType.INTEGER);
        expectedSchema[14] = new ColumnInfo("AVG_RESULT_SIZE", VoltType.INTEGER);
        expectedSchema[15] = new ColumnInfo("MIN_PARAMETER_SET_SIZE", VoltType.INTEGER);
        expectedSchema[16] = new ColumnInfo("MAX_PARAMETER_SET_SIZE", VoltType.INTEGER);
        expectedSchema[17] = new ColumnInfo("AVG_PARAMETER_SET_SIZE", VoltType.INTEGER);
        expectedSchema[18] = new ColumnInfo("ABORTS", VoltType.BIGINT);
        expectedSchema[19] = new ColumnInfo("FAILURES", VoltType.BIGINT);
        expectedTable = new VoltTable(expectedSchema);
        validateSchema(results[0], expectedTable);

        // Validate the PROCEDUREPROFILE aggregation.
        results = client.callProcedure("@Statistics", "procedureprofile", 1).getResults();
        System.out.println("\n\n\n" + results[0].toString() + "\n\n\n");

        // expect NEW_ORDER.insert, GoSleep
        // see TestStatsProcProfile.java for tests of the aggregation itself.
        List<String> possibleProcs = new ArrayList<>();
        possibleProcs.add("org.voltdb_testprocs.regressionsuites.malicious.GoSleep");
        possibleProcs.add("NEW_ORDER.insert");
        possibleProcs.add("org.voltdb.sysprocs.SnapshotSave");

        while (results[0].advanceRow()) {
            assertTrue("Unexpected stored procedure executed: " +
                        results[0].getString("PROCEDURE"),
                        possibleProcs.contains(results[0].getString("PROCEDURE")));
        }
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        return StatisticsTestSuiteBase.suite(TestStatisticsSuiteDatabaseElementStats.class, false);
    }
}
