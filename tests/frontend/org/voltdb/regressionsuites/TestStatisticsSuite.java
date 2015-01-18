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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.HdrHistogram_voltpatches.Histogram;
import org.hsqldb_voltpatches.HSQLInterface;
import org.voltcore.utils.CompressionStrategySnappy;
import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.join.BalancePartitionsStatistics;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.regressionsuites.malicious.GoSleep;

public class TestStatisticsSuite extends SaveRestoreBase {

    private final static int SITES = 2;
    private final static int HOSTS = 3;
    private final static int KFACTOR = MiscUtils.isPro() ? 1 : 0;
    private final static int PARTITIONS = (SITES * HOSTS) / (KFACTOR + 1);
    private final static boolean hasLocalServer = false;
    private static StringBuilder m_recentAnalysis = null;

    private static final Class<?>[] PROCEDURES =
    {
        GoSleep.class
    };

    public TestStatisticsSuite(String name) {
        super(name);
    }

    // For the provided table, verify that there is a row for each host in the cluster where
    // the column designated by 'columnName' has the value 'targetValue'.  For example, for
    // Initiator stats, if columnName is 'PROCEDURE_NAME' and targetValue is 'foo', this
    // will verify that the initiator at each node has seen a procedure invocation for 'foo'
    private void validateRowSeenAtAllHosts(VoltTable result, String columnName, String targetValue,
            boolean enforceUnique)
    {
        int hostCount = countHostsProvidingRows(result, columnName, targetValue, enforceUnique);
        assertEquals(claimRecentAnalysis(), HOSTS, hostCount);
    }

    private String claimRecentAnalysis() {
        String result = "No root cause analysis is available for this failure.";
        if (m_recentAnalysis != null) {
            result = m_recentAnalysis.toString();
            m_recentAnalysis = null;
        }
        return result;
    }

    private int countHostsProvidingRows(VoltTable result, String columnName, String targetValue,
            boolean enforceUnique)
    {
        result.resetRowPosition();
        Set<Long> hostsSeen = new HashSet<Long>();
        while (result.advanceRow()) {
            String colValFromRow = result.getString(columnName);
            if (targetValue.equalsIgnoreCase(colValFromRow)) {
                Long thisHostId = result.getLong("HOST_ID");
                if (enforceUnique) {
                    assertFalse("HOST_ID: " + thisHostId + " seen twice in table looking for " + targetValue +
                            " in column " + columnName, hostsSeen.contains(thisHostId));
                }
                hostsSeen.add(thisHostId);
            }
        }

        //* Enable this to force a failure with diagnostics */ hostsSeen.add(123456789L);

        // Before possibly failing an assert, prepare to report details of the non-conforming result.
        m_recentAnalysis = null;
        if (HOSTS != hostsSeen.size()) {
            m_recentAnalysis = new StringBuilder();
            m_recentAnalysis.append("Failure follows from these results:\n");
            Set<Long> seenAgain = new HashSet<Long>();
            result.resetRowPosition();
            while (result.advanceRow()) {
                String colValFromRow = result.getString(columnName);
                Long thisHostId = result.getLong("HOST_ID");
                String rowStatus = "Found a non-match";
                if (targetValue.equalsIgnoreCase(colValFromRow)) {
                    if (seenAgain.add(thisHostId)) {
                        rowStatus = "Added a match";
                    } else {
                        rowStatus = "Duplicated a match";
                    }
                }
                m_recentAnalysis.append(rowStatus +
                        " at host " + thisHostId + " for " + columnName + " " + colValFromRow + "\n");
            }
        }
        return hostsSeen.size();
    }

    // For the provided table, verify that there is a row for each site in the cluster where
    // the column designated by 'columnName' has the value 'targetValue'.  For example, for
    // Table stats, if columnName is 'TABLE_NAME' and targetValue is 'foo', this
    // will verify that each site has returned results for table 'foo'
    private boolean validateRowSeenAtAllSites(VoltTable result, String columnName, String targetValue,
            boolean enforceUnique)
    {
        result.resetRowPosition();
        Set<Long> sitesSeen = new HashSet<Long>();
        while (result.advanceRow()) {
            String colValFromRow = result.getString(columnName);
            if (targetValue.equalsIgnoreCase(colValFromRow)) {
                long hostId = result.getLong("HOST_ID");
                long thisSiteId = result.getLong("SITE_ID");
                thisSiteId |= hostId << 32;
                if (enforceUnique) {
                    assertFalse("SITE_ID: " + thisSiteId + " seen twice in table looking for " + targetValue +
                            " in column " + columnName, sitesSeen.contains(thisSiteId));
                }
                sitesSeen.add(thisSiteId);
            }
        }
        return (HOSTS * SITES) == sitesSeen.size();
    }

    // For the provided table, verify that there is a row for each partition in the cluster where
    // the column designated by 'columnName' has the value 'targetValue'.
    private void validateRowSeenAtAllPartitions(VoltTable result, String columnName, String targetValue,
            boolean enforceUnique)
    {
        result.resetRowPosition();
        Set<Long> partsSeen = new HashSet<Long>();
        while (result.advanceRow()) {
            String colValFromRow = result.getString(columnName);
            if (targetValue.equalsIgnoreCase(colValFromRow)) {
                long thisPartId = result.getLong("PARTITION_ID");
                if (enforceUnique) {
                    assertFalse("PARTITION_ID: " + thisPartId + " seen twice in table looking for " + targetValue +
                            " in column " + columnName, partsSeen.contains(thisPartId));
                }
                partsSeen.add(thisPartId);
            }
        }
        assertEquals(PARTITIONS, partsSeen.size());
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

    public void testLatencyStatistics() throws Exception {
        System.out.println("\n\nTESTING LATENCY STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[5];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("HISTOGRAM", VoltType.VARBINARY);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;

        // Do some stuff to generate some latency stats
        for (int i = 0; i < SITES * HOSTS; i++) {
            results = client.callProcedure("NEW_ORDER.insert", i).getResults();
        }
        results = client.callProcedure("@Statistics", "LATENCY", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Test latency table: " + results[0].toString());

        validateSchema(results[0], expectedTable);
        // should have at least one row from each host
        results[0].advanceRow();
        validateRowSeenAtAllHosts(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), false);
        // actually, there are 26 rows per host so:
        assertEquals(HOSTS, results[0].getRowCount());
        // Check for non-zero invocations (ENG-4668)
        long invocations = 0;
        results[0].resetRowPosition();
        while (results[0].advanceRow()) {
            byte histogramBytes[] = results[0].getVarbinary("HISTOGRAM");
            Histogram h = AbstractHistogram.fromCompressedBytes(histogramBytes, CompressionStrategySnappy.INSTANCE);
            invocations += h.getHistogramData().getTotalCount();
        }
        assertTrue(invocations > 0);
    }

    public void testInitiatorStatistics() throws Exception {
        System.out.println("\n\nTESTING INITIATOR STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[13];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("CONNECTION_ID", VoltType.BIGINT);
        expectedSchema[5] = new ColumnInfo("CONNECTION_HOSTNAME", VoltType.STRING);
        expectedSchema[6] = new ColumnInfo("PROCEDURE_NAME", VoltType.STRING);
        expectedSchema[7] = new ColumnInfo("INVOCATIONS", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("AVG_EXECUTION_TIME", VoltType.INTEGER);
        expectedSchema[9] = new ColumnInfo("MIN_EXECUTION_TIME", VoltType.INTEGER);
        expectedSchema[10] = new ColumnInfo("MAX_EXECUTION_TIME", VoltType.INTEGER);
        expectedSchema[11] = new ColumnInfo("ABORTS", VoltType.BIGINT);
        expectedSchema[12] = new ColumnInfo("FAILURES", VoltType.BIGINT);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        //
        // initiator selector
        //
        VoltTable results[] = null;
        // This should get us an invocation at each host
        for (int i = 0; i < 1000; i++) {
            results = client.callProcedure("NEW_ORDER.insert", i).getResults();
        }
        results = client.callProcedure("@Statistics", "INITIATOR", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Test initiators table: " + results[0].toString());
        // Check the schema
        validateSchema(results[0], expectedTable);
        // One WAREHOUSE.select row per host
        assertEquals(HOSTS, results[0].getRowCount());

        // Verify the invocation counts
        int counts = 0;
        while (results[0].advanceRow()) {
            String procName = results[0].getString("PROCEDURE_NAME");
            if (procName.equals("@SystemCatalog")) {
                // One for each connection from the client
                assertEquals(HOSTS, results[0].getLong("INVOCATIONS"));
            } else if (procName.equals("NEW_ORDER.insert")) {
                counts += results[0].getLong("INVOCATIONS");
            }
        }
        assertEquals(1000, counts);
        // verify that each node saw a NEW_ORDER.insert initiation
        validateRowSeenAtAllHosts(results[0], "PROCEDURE_NAME", "NEW_ORDER.insert", true);
    }

    public void testPartitionCount() throws Exception {
        System.out.println("\n\nTESTING PARTITION COUNT\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[4];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("PARTITION_COUNT", VoltType.INTEGER);
        VoltTable expectedTable = new VoltTable(expectedSchema);
        VoltTable[] results = null;

        results = client.callProcedure("@Statistics", "PARTITIONCOUNT", 0).getResults();
        // Only one table returned
        assertEquals(1, results.length);
        validateSchema(results[0], expectedTable);
        // Should only get one row, total
        assertEquals(1, results[0].getRowCount());
        results[0].advanceRow();
        int partCount = (int)results[0].getLong("PARTITION_COUNT");
        assertEquals(PARTITIONS, partCount);
    }

    public void testTableStatistics() throws Exception {
        System.out.println("\n\nTESTING TABLE STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[13];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("PARTITION_ID", VoltType.BIGINT);
        expectedSchema[5] = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        expectedSchema[6] = new ColumnInfo("TABLE_TYPE", VoltType.STRING);
        expectedSchema[7] = new ColumnInfo("TUPLE_COUNT", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("TUPLE_ALLOCATED_MEMORY", VoltType.INTEGER);
        expectedSchema[9] = new ColumnInfo("TUPLE_DATA_MEMORY", VoltType.INTEGER);
        expectedSchema[10] = new ColumnInfo("STRING_DATA_MEMORY", VoltType.INTEGER);
        expectedSchema[11] = new ColumnInfo("TUPLE_LIMIT", VoltType.INTEGER);
        expectedSchema[12] = new ColumnInfo("PERCENT_FULL", VoltType.INTEGER);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;
        boolean success = false;
        long start = System.currentTimeMillis();
        while (!success) {
            if (System.currentTimeMillis() - start > 60000) fail("Took too long");
            success = true;
            // table
            //
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            System.out.println("Test statistics table: " + results[0].toString());
            // one aggregate table returned
            assertEquals(1, results.length);
            validateSchema(results[0], expectedTable);
            // with 10 rows per site. Can be two values depending on the test scenario of cluster vs. local.
            if (HOSTS * SITES * 3 != results[0].getRowCount()) {
                success = false;
            }
            // Validate that each site returns a result for each table
            if (success) {
                success = validateRowSeenAtAllSites(results[0], "TABLE_NAME", "WAREHOUSE", true);
            }
            if (success) {
                success = validateRowSeenAtAllSites(results[0], "TABLE_NAME", "NEW_ORDER", true);
            }
            if (success) {
                validateRowSeenAtAllSites(results[0], "TABLE_NAME", "ITEM", true);
            }
            if (success) break;
        }
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
        expectedSchema[11] = new ColumnInfo("MEMORY_ESTIMATE", VoltType.INTEGER);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;

        boolean success = false;
        long start = System.currentTimeMillis();
        while (!success) {
            if (System.currentTimeMillis() - start > 60000) fail("Took too long");
            success = true;
            results = client.callProcedure("@Statistics", "index", 0).getResults();
            System.out.println("Index results: " + results[0].toString());
            assertEquals(1, results.length);
            validateSchema(results[0], expectedTable);
            if (success) {
                success = validateRowSeenAtAllSites(results[0], "INDEX_NAME",
                        HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX + "W_PK_TREE", true);
            }
            if (success) {
                success = validateRowSeenAtAllSites(results[0], "INDEX_NAME",
                        HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX + "I_PK_TREE", true);
            }
            if (success) break;
        }
    }

    public void testMemoryStatistics() throws Exception {
        System.out.println("\n\nTESTING MEMORY STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[14];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("RSS", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("JAVAUSED", VoltType.INTEGER);
        expectedSchema[5] = new ColumnInfo("JAVAUNUSED", VoltType.INTEGER);
        expectedSchema[6] = new ColumnInfo("TUPLEDATA", VoltType.INTEGER);
        expectedSchema[7] = new ColumnInfo("TUPLEALLOCATED", VoltType.INTEGER);
        expectedSchema[8] = new ColumnInfo("INDEXMEMORY", VoltType.INTEGER);
        expectedSchema[9] = new ColumnInfo("STRINGMEMORY", VoltType.INTEGER);
        expectedSchema[10] = new ColumnInfo("TUPLECOUNT", VoltType.BIGINT);
        expectedSchema[11] = new ColumnInfo("POOLEDMEMORY", VoltType.BIGINT);
        expectedSchema[12] = new ColumnInfo("PHYSICALMEMORY", VoltType.BIGINT);
        expectedSchema[13] = new ColumnInfo("JAVAMAXHEAP", VoltType.INTEGER);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;

        //
        // memory
        //
        // give time to seed the stats cache?
        Thread.sleep(1000);
        results = client.callProcedure("@Statistics", "memory", 0).getResults();
        System.out.println("Node memory statistics table: " + results[0].toString());
        // one aggregate table returned
        assertEquals(1, results.length);
        validateSchema(results[0], expectedTable);
        results[0].advanceRow();
        // Hacky, on a single local cluster make sure that all 'nodes' are present.
        // MEMORY stats lacks a common string across nodes, but we can hijack the hostname in this case.
        validateRowSeenAtAllHosts(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), true);
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
        Thread.sleep(1000);
        results = client.callProcedure("@Statistics", "cpu", 0).getResults();
        System.out.println("Node cpu statistics table: " + results[0].toString());
        // one aggregate table returned
        assertEquals(1, results.length);
        validateSchema(results[0], expectedTable);
        results[0].advanceRow();
        // Hacky, on a single local cluster make sure that all 'nodes' are present.
        // CPU stats lacks a common string across nodes, but we can hijack the hostname in this case.
        validateRowSeenAtAllHosts(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), true);
    }

    public void testProcedureStatistics() throws Exception {
        System.out.println("\n\nTESTING PROCEDURE STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[19];
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
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;

        //
        // procedure
        //
        // Induce procedure invocations on all partitions.  May fail in non-legacy hashing case
        // this plus R/W replication should ensure that every site on every node runs this transaction
        // at least once

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
        validateRowSeenAtAllHosts(results[0], "PROCEDURE", "NEW_ORDER.insert", false);
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

        // Validate the PROCEDUREPROFILE aggregation.
        results = client.callProcedure("@Statistics", "procedureprofile", 0).getResults();
        System.out.println("\n\n\n" + results[0].toString() + "\n\n\n");

        // expect NEW_ORDER.insert, GoSleep
        // see TestStatsProcProfile.java for tests of the aggregation itself.
        assertEquals("Validate site filtering for PROCEDUREPROFILE",
                2, results[0].getRowCount());
    }

    public void testIOStatistics() throws Exception {
        System.out.println("\n\nTESTING IO STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        // Based on doc, not code
        // HOST_ID, SITE_ID, and PARTITION_ID all differ.  Fixed to match
        // reality so tests would pass, but, ugh.
        ColumnInfo[] expectedSchema = new ColumnInfo[9];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("CONNECTION_ID", VoltType.BIGINT);
        expectedSchema[4] = new ColumnInfo("CONNECTION_HOSTNAME", VoltType.STRING);
        expectedSchema[5] = new ColumnInfo("BYTES_READ", VoltType.BIGINT);
        expectedSchema[6] = new ColumnInfo("MESSAGES_READ", VoltType.BIGINT);
        expectedSchema[7] = new ColumnInfo("BYTES_WRITTEN", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("MESSAGES_WRITTEN", VoltType.BIGINT);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;
        //
        // iostats
        //
        results = client.callProcedure("@Statistics", "iostats", 0).getResults();
        System.out.println("Test iostats table: " + results[0].toString());
        // one aggregate table returned
        assertEquals(1, results.length);
        validateSchema(results[0], expectedTable);
    }

    public void testTopoStatistics() throws Exception {
        System.out.println("\n\nTESTING TOPO STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema1 = new ColumnInfo[3];
        expectedSchema1[0] = new ColumnInfo("Partition", VoltType.INTEGER);
        expectedSchema1[1] = new ColumnInfo("Sites", VoltType.STRING);
        expectedSchema1[2] = new ColumnInfo("Leader", VoltType.STRING);
        VoltTable expectedTable1 = new VoltTable(expectedSchema1);

        ColumnInfo[] expectedSchema2 = new ColumnInfo[2];
        expectedSchema2[0] = new ColumnInfo("HASHTYPE", VoltType.STRING);
        expectedSchema2[1] = new ColumnInfo("HASHCONFIG", VoltType.VARBINARY);
        VoltTable expectedTable2 = new VoltTable(expectedSchema2);

        VoltTable[] results = null;

        //
        // TOPO
        //
        results = client.callProcedure("@Statistics", "TOPO", 0).getResults();
        // two aggregate tables returned
        assertEquals(2, results.length);
        System.out.println("Test TOPO table: " + results[0].toString());
        System.out.println("Test TOPO table: " + results[1].toString());
        validateSchema(results[0], expectedTable1);
        validateSchema(results[1], expectedTable2);
        VoltTable topo = results[0];
        // Should have partitions + 1 rows in the first table
        assertEquals(PARTITIONS + 1, results[0].getRowCount());
        // Make sure we can find the MPI, at least
        boolean found = false;
        while (topo.advanceRow()) {
            if ((int)topo.getLong("Partition") == MpInitiator.MP_INIT_PID) {
                found = true;
            }
        }
        assertTrue(found);
        // and only one row in the second table
        assertEquals(1, results[1].getRowCount());
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
        expectedSchema[7] = new ColumnInfo("CACHE1_HITS", VoltType.INTEGER);
        expectedSchema[8] = new ColumnInfo("CACHE2_HITS", VoltType.INTEGER);
        expectedSchema[9] = new ColumnInfo("CACHE_MISSES", VoltType.INTEGER);
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

    public void testDRNodeStatistics() throws Exception {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            System.out.println("SKIPPING DRNODE STATS TESTS FOR COMMUNITY VERSION");
            return;
        }
        System.out.println("\n\nTESTING DRNODE STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema2 = new ColumnInfo[7];
        expectedSchema2[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema2[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema2[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema2[3] = new ColumnInfo("ENABLED", VoltType.STRING);
        expectedSchema2[4] = new ColumnInfo("SYNCSNAPSHOTSTATE", VoltType.STRING);
        expectedSchema2[5] = new ColumnInfo("ROWSINSYNCSNAPSHOT", VoltType.BIGINT);
        expectedSchema2[6] = new ColumnInfo("ROWSACKEDFORSYNCSNAPSHOT", VoltType.BIGINT);
        VoltTable expectedTable2 = new VoltTable(expectedSchema2);

        VoltTable[] results = null;
        //
        // DRNODE
        //
        results = client.callProcedure("@Statistics", "DRNODE", 0).getResults();
        // one aggregate tables returned
        assertEquals(1, results.length);
        System.out.println("Test DRNODE table: " + results[0].toString());
        validateSchema(results[0], expectedTable2);
        // One row per host for DRNODE stats
        results[0].advanceRow();
        validateRowSeenAtAllHosts(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), true);
    }

    public void testDRPartitionStatistics() throws Exception {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            System.out.println("SKIPPING DRPARTITION STATS TESTS FOR COMMUNITY VERSION");
            return;
        }
        System.out.println("\n\nTESTING DRPARTITION STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema1 = new ColumnInfo[11];
        expectedSchema1[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema1[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema1[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema1[3] = new ColumnInfo("PARTITION_ID", VoltType.INTEGER);
        expectedSchema1[4] = new ColumnInfo("STREAMTYPE", VoltType.STRING);
        expectedSchema1[5] = new ColumnInfo("TOTALBYTES", VoltType.BIGINT);
        expectedSchema1[6] = new ColumnInfo("TOTALBYTESINMEMORY", VoltType.BIGINT);
        expectedSchema1[7] = new ColumnInfo("TOTALBUFFERS", VoltType.BIGINT);
        expectedSchema1[8] = new ColumnInfo("LASTACKTIMESTAMP", VoltType.BIGINT);
        expectedSchema1[9] = new ColumnInfo("ISSYNCED", VoltType.STRING);
        expectedSchema1[10] = new ColumnInfo("MODE", VoltType.STRING);
        VoltTable expectedTable1 = new VoltTable(expectedSchema1);

        VoltTable[] results = null;
        //
        // DRPARTITION
        //
        results = client.callProcedure("@Statistics", "DRPARTITION", 0).getResults();
        // one aggregate tables returned
        assertEquals(1, results.length);
        System.out.println("Test DR table: " + results[0].toString());
        validateSchema(results[0], expectedTable1);
        // One row per site, don't have HSID for ease of check, just check a bunch of stuff
        assertEquals(HOSTS * SITES, results[0].getRowCount());
        results[0].advanceRow();
        validateRowSeenAtAllHosts(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), false);
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

        ColumnInfo[] expectedSchema1 = new ColumnInfo[11];
        expectedSchema1[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema1[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema1[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema1[3] = new ColumnInfo("PARTITION_ID", VoltType.INTEGER);
        expectedSchema1[4] = new ColumnInfo("STREAMTYPE", VoltType.STRING);
        expectedSchema1[5] = new ColumnInfo("TOTALBYTES", VoltType.BIGINT);
        expectedSchema1[6] = new ColumnInfo("TOTALBYTESINMEMORY", VoltType.BIGINT);
        expectedSchema1[7] = new ColumnInfo("TOTALBUFFERS", VoltType.BIGINT);
        expectedSchema1[8] = new ColumnInfo("LASTACKTIMESTAMP", VoltType.BIGINT);
        expectedSchema1[9] = new ColumnInfo("ISSYNCED", VoltType.STRING);
        expectedSchema1[10] = new ColumnInfo("MODE", VoltType.STRING);
        VoltTable expectedTable1 = new VoltTable(expectedSchema1);

        ColumnInfo[] expectedSchema2 = new ColumnInfo[7];
        expectedSchema2[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema2[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema2[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema2[3] = new ColumnInfo("ENABLED", VoltType.STRING);
        expectedSchema2[4] = new ColumnInfo("SYNCSNAPSHOTSTATE", VoltType.STRING);
        expectedSchema2[5] = new ColumnInfo("ROWSINSYNCSNAPSHOT", VoltType.BIGINT);
        expectedSchema2[6] = new ColumnInfo("ROWSACKEDFORSYNCSNAPSHOT", VoltType.BIGINT);
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
        // One row per site, don't have HSID for ease of check, just check a bunch of stuff
        assertEquals(HOSTS * SITES, results[0].getRowCount());
        results[0].advanceRow();
        validateRowSeenAtAllHosts(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), false);
        results[0].advanceRow();
        validateRowSeenAtAllPartitions(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), false);
        // One row per host for DRNODE stats
        results[1].advanceRow();
        validateRowSeenAtAllHosts(results[1], "HOSTNAME", results[1].getString("HOSTNAME"), true);
    }

    public void testLiveClientsStatistics() throws Exception {
        System.out.println("\n\nTESTING LIVECLIENTS STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[9];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("CONNECTION_ID", VoltType.BIGINT);
        expectedSchema[4] = new ColumnInfo("CLIENT_HOSTNAME", VoltType.STRING);
        expectedSchema[5] = new ColumnInfo("ADMIN", VoltType.TINYINT);
        expectedSchema[6] = new ColumnInfo("OUTSTANDING_REQUEST_BYTES", VoltType.BIGINT);
        expectedSchema[7] = new ColumnInfo("OUTSTANDING_RESPONSE_MESSAGES", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("OUTSTANDING_TRANSACTIONS", VoltType.BIGINT);
        VoltTable expectedTable = new VoltTable(expectedSchema);
        int patientRetries = 2;
        int hostsHeardFrom = 0;
        //
        // LIVECLIENTS
        //
        do { // loop until we get the desired answer or lose patience waiting out possible races.
            VoltTable[] results = client.callProcedure("@Statistics", "LIVECLIENTS", 0).getResults();
            // one aggregate table returned
            assertEquals(1, results.length);
            System.out.println("Test LIVECLIENTS table: " + results[0].toString());
            validateSchema(results[0], expectedTable);
            // Hacky, on a single local cluster make sure that all 'nodes' are present.
            // LiveClients stats lacks a common string across nodes, but we can hijack the hostname in this case.
            results[0].advanceRow();
            hostsHeardFrom =
                    countHostsProvidingRows(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), true);
        } while ((hostsHeardFrom < HOSTS) && (--patientRetries) > 0);
        assertEquals(claimRecentAnalysis(), HOSTS, hostsHeardFrom);
    }

    public void testStarvationStatistics() throws Exception {
        System.out.println("\n\nTESTING STARVATION STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[10];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("COUNT", VoltType.BIGINT);
        expectedSchema[5] = new ColumnInfo("PERCENT", VoltType.FLOAT);
        expectedSchema[6] = new ColumnInfo("AVG", VoltType.BIGINT);
        expectedSchema[7] = new ColumnInfo("MIN", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("MAX", VoltType.BIGINT);
        expectedSchema[9] = new ColumnInfo("STDDEV", VoltType.BIGINT);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;
        //
        // STARVATION
        //
        results = client.callProcedure("@Statistics", "STARVATION", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Test STARVATION table: " + results[0].toString());
        validateSchema(results[0], expectedTable);
        // One row per site, we don't use HSID though, so hard to do straightforward
        // per-site unique check.  Finesse it.
        // We also get starvation stats for the MPI, so we need to add a site per host.
        assertEquals(HOSTS * (SITES + 1), results[0].getRowCount());
        results[0].advanceRow();
        validateRowSeenAtAllHosts(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), false);
    }

    public void testSnapshotStatus() throws Exception {
        System.out.println("\n\nTESTING SNAPSHOTSTATUS\n\n\n");
        if (KFACTOR == 0) {
            // SnapshotSave is a PRO feature starting from 4.0
            return;
        }

        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[14];
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
        // One row per table per node
        validateRowSeenAtAllHosts(results[0], "TABLE", "WAREHOUSE", true);
        validateRowSeenAtAllHosts(results[0], "TABLE", "NEW_ORDER", true);
        validateRowSeenAtAllHosts(results[0], "TABLE", "ITEM", true);
    }

    public void testManagementStats() throws Exception {
        System.out.println("\n\nTESTING MANAGEMENT STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        VoltTable[] results = null;
        //
        // LIVECLIENTS
        //
        results = client.callProcedure("@Statistics", "MANAGEMENT", 0).getResults();
        // eight aggregate tables returned.  Assume that we have selected the right
        // subset of stats internally, just check that we get stuff.
        assertEquals(8, results.length);
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
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestStatisticsSuite.class);

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

        /*
         * Create a cluster configuration.
         * Some of the sysproc results come back a little strange when applied to a cluster that is being
         * simulated through LocalCluster -- all the hosts have the same HOSTNAME, just different host ids.
         * So, these tests shouldn't rely on the usual uniqueness of host names in a cluster.
         */
        config = new LocalCluster("statistics-cluster.jar", TestStatisticsSuite.SITES,
                TestStatisticsSuite.HOSTS, TestStatisticsSuite.KFACTOR,
                BackendTarget.NATIVE_EE_JNI);
        ((LocalCluster) config).setHasLocalServer(hasLocalServer);
        boolean success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
