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
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.HdrHistogram_voltpatches.Histogram;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.voltcore.utils.CompressionStrategySnappy;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.dtxn.LatencyStats;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;

import junit.framework.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class TestStatisticsSuite extends StatisticsTestSuiteBase {

    public TestStatisticsSuite(String name) {
        super(name);
    }

    private String claimRecentAnalysis() {
        String result = "No root cause analysis is available for this failure.";
        if (m_recentAnalysis != null) {
            result = m_recentAnalysis.toString();
            m_recentAnalysis = null;
        }
        return result;
    }

    // validation functions supporting multiple columns
    private boolean checkRowForMultipleTargets(VoltTable result, Map<String, String> columnTargets) {
        for (Entry<String, String> entry : columnTargets.entrySet()) {
            if (!result.getString(entry.getKey()).equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private int countHostsProvidingRows(VoltTable result, Map<String, String> columnTargets,
                                        boolean enforceUnique) {
        result.resetRowPosition();
        Set<Long> hostsSeen = new HashSet<>();
        while (result.advanceRow()) {
            if (checkRowForMultipleTargets(result, columnTargets)) {
                Long thisHostId = result.getLong("HOST_ID");
                if (enforceUnique) {
                    StringBuilder message = new StringBuilder();
                    message.append("HOST_ID: " + thisHostId + " seen twice in table looking for ");
                    for (Entry<String, String> entry : columnTargets.entrySet()) {
                        message.append(entry.getValue() + " in column " + entry.getKey() + ";");
                    }
                    assertFalse(message.toString(), hostsSeen.contains(thisHostId));
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
                Long thisHostId = result.getLong("HOST_ID");
                String rowStatus = "Found a non-match";
                if (checkRowForMultipleTargets(result, columnTargets)) {
                    if (seenAgain.add(thisHostId)) {
                        rowStatus = "Added a match";
                    } else {
                        rowStatus = "Duplicated a match";
                    }
                }
                m_recentAnalysis.append(rowStatus +
                        " at host " + thisHostId + " for ");
                for (String key : columnTargets.keySet()) {
                    m_recentAnalysis.append(key + " " + result.getString(key) + ";");
                }
                m_recentAnalysis.append("\n");
            }
        }
        return hostsSeen.size();
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
        } catch (ProcCallException ex) {
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
        } catch (ProcCallException ex) {
            assertEquals("Incorrect number of arguments to @Statistics (expects 2, received 3)",
                    ex.getMessage());
        }
        try {
            // Invalid selector
            client.callProcedure("@Statistics", "garbage", 0);
            fail();
        } catch (ProcCallException ex) {
        }

    }

    // Make sure @Statistics LATENCY returns sane data in the expected formats.
    //
    public void testLatencyValidity() throws Exception {
        System.out.println("\n\nTESTING LATENCY STATS VALIDITY\n\n\n");
        Client client = getFullyConnectedClient();

        // Validate column names, types and ordering.
        ColumnInfo[] expectedSchema = new ColumnInfo[13];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);  // milliseconds
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("INTERVAL", VoltType.INTEGER); // milliseconds
        expectedSchema[4] = new ColumnInfo("COUNT", VoltType.INTEGER); // samples
        expectedSchema[5] = new ColumnInfo("TPS", VoltType.INTEGER); // samples per second
        expectedSchema[6] = new ColumnInfo("P50", VoltType.BIGINT);  // microseconds
        expectedSchema[7] = new ColumnInfo("P95", VoltType.BIGINT);  // microseconds
        expectedSchema[8] = new ColumnInfo("P99", VoltType.BIGINT);  // microseconds
        expectedSchema[9] = new ColumnInfo("P99.9", VoltType.BIGINT);  // microseconds
        expectedSchema[10] = new ColumnInfo("P99.99", VoltType.BIGINT);  // microseconds
        expectedSchema[11] = new ColumnInfo("P99.999", VoltType.BIGINT);  // microseconds
        expectedSchema[12] = new ColumnInfo("MAX", VoltType.BIGINT);  // microseconds
        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;

        // Do some work that will generate latency stats
        for (int i = 0; i < SITES * HOSTS; i++) {
            results = client.callProcedure("NEW_ORDER.insert", i).getResults();
        }

        // Statistics roll over every 5 seconds.
        // Retry until we've seen the window with our data.
        // We retry more often than that so that we don't miss the window;
        // this also speeds up tests when the data comes in towards the end of a window.
        long samplesFound;
        int numInvocations = 0;
        final int delayMsec = (int) TimeUnit.SECONDS.toMillis(1);
        final int invocationLimit = 2 * LatencyStats.INTERVAL_MS / delayMsec;
        assertTrue(delayMsec < LatencyStats.INTERVAL_MS); // prevent misleading test failures if INTERVAL_MS changes
        do {
            Thread.sleep(delayMsec);

            results = client.callProcedure("@Statistics", "LATENCY", 0).getResults();
            // one aggregate table returned
            assertEquals(1, results.length);
            System.out.println("Test latency table: " + results[0].toString());

            validateSchema(results[0], expectedTable);
            // should have at least one row from each host
            results[0].advanceRow();
            Map<String, String> columnTargets = new HashMap<String, String>();
            columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
            validateRowSeenAtAllHosts(results[0], columnTargets, false);
            assertEquals(HOSTS, results[0].getRowCount());

            // Search all nodes for the one that has our data
            results[0].resetRowPosition();
            samplesFound = 0;
            while (results[0].advanceRow()) {
                final long interval = results[0].getLong("INTERVAL"); // milliseconds
                final long count = results[0].getLong("COUNT");    // samples
                final long tps = results[0].getLong("TPS");      // samples per second
                final long p50 = results[0].getLong("P50");      // microseconds
                final long p95 = results[0].getLong("P95");      // microseconds
                final long p99 = results[0].getLong("P99");      // microseconds
                final long p39 = results[0].getLong("P99.9");    // microseconds
                final long p49 = results[0].getLong("P99.99");   // microseconds
                final long p59 = results[0].getLong("P99.999");  // microseconds
                final long max = results[0].getLong("MAX");      // microseconds

                // Run sanity checks on the data
                assertEquals(interval, LatencyStats.INTERVAL_MS);
                // Test needs to do this calculation the exact same way as the code due to fixed point rounding
                assertEquals(tps, (int) (TimeUnit.SECONDS.toMillis(count) / interval));
                assertTrue(count == 0 || p50 > 0);
                assertTrue(p50 <= p95);
                assertTrue(p95 <= p99);
                assertTrue(p99 <= p39);
                assertTrue(p39 <= p49);
                assertTrue(p49 <= p59);
                assertTrue(p59 <= max);
                samplesFound += count;
            }
            numInvocations++;
        } while (samplesFound == 0 && numInvocations < invocationLimit);

        assertTrue(samplesFound > 0);
    }

    // Helper method for testLatencyTiming()
    private static VoltTable doLatencyStatsCall(Client client) throws Exception {
        VoltTable[] results = client.callProcedure("@Statistics", "LATENCY", 0).getResults();
        assertEquals(1, results.length);
        // Guarantee the same host is used every time - they are not always returned in a consistent order.
        // Expect about 20% failure if this loop is commented out.
        do {
            results[0].advanceRow();
        } while (results[0].getLong("HOST_ID") != 0);
        return results[0];
    }

    /**
     * Verify that the timestamp from "@Statistics LATENCY"
     * is associated with the data window, not the procedure call.
     * <p>
     * We do this by checking:
     * 1. Consecutive calls return the same timestamp (since they represent the same bin).
     * 2. Calls separated by more than a full time window return different timestamps.
     * <p>
     * There are a lot of reasons two consecutive calls would not return the same timestamp.
     * Notably, histogram bin rollovers and sluggishness that cause the test to not execute.
     * Keep trying until identical timestamps are observed -
     * an implementation that gives each call a unique timestamp will still fail the test.
     */
    public void testLatencyTiming() throws Exception {
        System.out.println("\n\nTESTING LATENCY STATS TIMING\n\n\n");
        Client client = getFullyConnectedClient();

        // Don't make so many attempts where a broken implementation will 'get lucky' and pass,
        // but also try enough times to avoid the benign situations mentioned above.
        // This may require some tweaking.
        final int maxAttempts = 12;
        int numAttempts = 0;
        VoltTable previous;
        VoltTable current = doLatencyStatsCall(client);
        do {
            previous = current;
            current = doLatencyStatsCall(client);
        } while (numAttempts++ < maxAttempts && previous.getLong("TIMESTAMP") != current.getLong("TIMESTAMP"));

        System.out.println(numAttempts + " attempts made at obtaining timestamps from the same time window.");
        assertTrue("Unable to use timestamps to verify that data comes from the same window.", numAttempts < maxAttempts);

        // Verify that matching timestamps implies matching data
        assertEquals(previous.getLong("TIMESTAMP"), current.getLong("TIMESTAMP"));
        assertEquals(previous.getLong("HOST_ID"), current.getLong("HOST_ID"));
        assertEquals(previous.getString("HOSTNAME"), current.getString("HOSTNAME"));
        assertEquals(previous.getLong("INTERVAL"), current.getLong("INTERVAL"));
        assertEquals(previous.getLong("COUNT"), current.getLong("COUNT"));
        assertEquals(previous.getLong("TPS"), current.getLong("TPS"));
        assertEquals(previous.getLong("P50"), current.getLong("P50"));
        assertEquals(previous.getLong("P95"), current.getLong("P95"));
        assertEquals(previous.getLong("P99"), current.getLong("P99"));
        assertEquals(previous.getLong("P99.9"), current.getLong("P99.9"));
        assertEquals(previous.getLong("P99.99"), current.getLong("P99.99"));
        assertEquals(previous.getLong("P99.999"), current.getLong("P99.999"));
        assertEquals(previous.getLong("MAX"), current.getLong("MAX"));

        // Verify that >5 seconds later the timestamps are not the same.
        final int extraDelayMsec = 1000;
        final long beforeTimestamp = current.getLong("TIMESTAMP");
        Thread.sleep(LatencyStats.INTERVAL_MS + extraDelayMsec);
        final long afterTimestamp = doLatencyStatsCall(client).getLong("TIMESTAMP");
        System.out.println("Non-consecutive timestamps: " + beforeTimestamp + ", " + afterTimestamp);
        assertTrue(afterTimestamp > beforeTimestamp);
    }

    public void testLatencyCompressed() throws Exception {
        System.out.println("\n\nTESTING LATENCY_COMPRESSED STATS\n\n\n");
        Client client = getFullyConnectedClient();

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

        results = client.callProcedure("@Statistics", "LATENCY_COMPRESSED", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Test latency_compressed table: " + results[0].toString());

        validateSchema(results[0], expectedTable);
        // should have at least one row from each host
        results[0].advanceRow();
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, false);
        // actually, there are 26 rows per host so:
        assertEquals(HOSTS, results[0].getRowCount());
        // Check for non-zero invocations (ENG-4668)
        long invocations = 0;
        results[0].resetRowPosition();
        while (results[0].advanceRow()) {
            byte histogramBytes[] = results[0].getVarbinary("HISTOGRAM");
            Histogram h = AbstractHistogram.fromCompressedBytes(histogramBytes, CompressionStrategySnappy.INSTANCE);
            invocations += h.getTotalCount();
        }
        assertTrue(invocations > 0);
    }

    public void testInitiatorStatistics() throws Exception {
        System.out.println("\n\nTESTING INITIATOR STATS\n\n\n");
        Client client = getFullyConnectedClient();

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

        System.out.println(results[0].toFormattedString());

        assertTrue(results[0].getRowCount() >= HOSTS);

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
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("PROCEDURE_NAME", "NEW_ORDER.insert");
        validateRowSeenAtAllHosts(results[0], columnTargets, true);
    }

    public void testPartitionCount() throws Exception {
        System.out.println("\n\nTESTING PARTITION COUNT\n\n\n");
        Client client = getFullyConnectedClient();

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
        int partCount = (int) results[0].getLong("PARTITION_COUNT");
        assertEquals(PARTITIONS, partCount);
    }

    public void testMemoryStatistics() throws Exception {
        System.out.println("\n\nTESTING MEMORY STATS\n\n\n");
        Client client = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[14];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("RSS", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("JAVAUSED", VoltType.INTEGER);
        expectedSchema[5] = new ColumnInfo("JAVAUNUSED", VoltType.INTEGER);
        expectedSchema[6] = new ColumnInfo("TUPLEDATA", VoltType.BIGINT);
        expectedSchema[7] = new ColumnInfo("TUPLEALLOCATED", VoltType.BIGINT);
        expectedSchema[8] = new ColumnInfo("INDEXMEMORY", VoltType.BIGINT);
        expectedSchema[9] = new ColumnInfo("STRINGMEMORY", VoltType.BIGINT);
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
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, true);
    }

    public void testIOStatistics() throws Exception {
        System.out.println("\n\nTESTING IO STATS\n\n\n");
        Client client = getFullyConnectedClient();

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

    public void testLatencyUncompressed() throws Exception {
        System.out.println("\n\nTESTING LATENCY_UNCOMPRESSED STATS\n\n\n");
        Client client = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[6];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("HISTOGRAM", VoltType.VARBINARY);
        expectedSchema[5] = new ColumnInfo("UNCOMPRESSED_HISTOGRAM", VoltType.VARBINARY);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        // Do some stuff to generate some latency stats
        for (int i = 0; i < SITES * HOSTS; i++) {
            client.callProcedure("NEW_ORDER.insert", i).getResults();
        }

        VoltTable[] results = client.callProcedure("@Statistics", "LATENCY_HISTOGRAM", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        validateSchema(results[0], expectedTable);
    }

    public void testTopoStatistics() throws Exception {
        System.out.println("\n\nTESTING TOPO STATS\n\n\n");
        Client client = getFullyConnectedClient();

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
        // TOPO with interval set to 0, retrieving binary hash config
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
            if ((int) topo.getLong("Partition") == MpInitiator.MP_INIT_PID) {
                found = true;
            }
        }
        assertTrue(found);
        // and only one row in the second table
        assertEquals(1, results[1].getRowCount());

        //
        // TOPO with interval set to 1, for retrieving compressed json hash config
        //
        results = client.callProcedure("@Statistics", "TOPO", 1).getResults();
        // two aggregate tables returned
        assertEquals(2, results.length);
        System.out.println("Test TOPO table: " + results[0].toString());
        System.out.println("Test TOPO table: " + results[1].toString());
        validateSchema(results[0], expectedTable1);
        validateSchema(results[1], expectedTable2);
        topo = results[0];
        // Should have partitions + 1 rows in the first table
        assertEquals(PARTITIONS + 1, results[0].getRowCount());
        // Make sure we can find the MPI, at least
        found = false;
        while (topo.advanceRow()) {
            if ((int) topo.getLong("Partition") == MpInitiator.MP_INIT_PID) {
                found = true;
            }
        }
        assertTrue(found);
        // and only one row in the second table
        assertEquals(1, results[1].getRowCount());
    }

    public void testLiveClientsStatistics() throws Exception {
        System.out.println("\n\nTESTING LIVECLIENTS STATS\n\n\n");
        Client client = getFullyConnectedClient();

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

        ColumnInfo[] expectedLimitsSchema = new ColumnInfo[9];
        expectedLimitsSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedLimitsSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedLimitsSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedLimitsSchema[3] = new ColumnInfo("FILE_DESCRIPTORS_LIMIT", VoltType.INTEGER);
        expectedLimitsSchema[4] = new ColumnInfo("FILE_DESCRIPTORS_OPEN", VoltType.INTEGER);
        expectedLimitsSchema[5] = new ColumnInfo("CLIENT_CONNECTIONS_LIMIT", VoltType.INTEGER);
        expectedLimitsSchema[6] = new ColumnInfo("CLIENT_CONNECTIONS_OPEN", VoltType.INTEGER);
        expectedLimitsSchema[7] = new ColumnInfo("ACCEPTED_CONNECTIONS", VoltType.INTEGER);
        expectedLimitsSchema[8] = new ColumnInfo("DROPPED_CONNECTIONS", VoltType.INTEGER);
        VoltTable expectedLimitsTable = new VoltTable(expectedLimitsSchema);

        await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> { // loop until we get the desired answer or lose patience waiting out possible races.
                    VoltTable[] results = client.callProcedure("@Statistics", "LIVECLIENTS", 0).getResults();
                    // one aggregate table returned for live clients and one for limits
                    assertEquals(2, results.length);
                    validateSchema(results[0], expectedTable);
                    validateSchema(results[1], expectedLimitsTable);

                    // Hacky, on a single local cluster make sure that all 'nodes' are present.
                    // LiveClients stats lacks a common string across nodes, but we can hijack the hostname in this case.
                    results[0].advanceRow();

                    Map<String, String> columnTargets = new HashMap<String, String>();
                    columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
                    int hostsHeardFrom = countHostsProvidingRows(results[0], columnTargets, true);

                    assertEquals(claimRecentAnalysis(), HOSTS, hostsHeardFrom);
                });
    }

    public void testStarvationStatistics() throws Exception {
        System.out.println("\n\nTESTING STARVATION STATS\n\n\n");
        Client client = getFullyConnectedClient();

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
        // We also get starvation stats for the MPI, so we need to add KFACTOR +1 sites.
        assertEquals(HOSTS * SITES + (KFACTOR + 1), results[0].getRowCount());
        results[0].advanceRow();
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, false);
    }

    public void testQueueDepthStatistics() throws Exception {
        System.out.println("\n\nTESTING QUEUEDEPTH STATS\n\n\n");
        Client client = getFullyConnectedClient();
        ColumnInfo[] expectedSchema = new ColumnInfo[8];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("CURRENT_DEPTH", VoltType.INTEGER);
        expectedSchema[5] = new ColumnInfo("POLL_COUNT", VoltType.BIGINT);
        expectedSchema[6] = new ColumnInfo("AVG_WAIT", VoltType.BIGINT);
        expectedSchema[7] = new ColumnInfo("MAX_WAIT", VoltType.BIGINT);

        VoltTable expectedTable = new VoltTable(expectedSchema);

        VoltTable[] results = null;
        //
        // QUEUEDEPTH
        //
        results = client.callProcedure("@Statistics", "QUEUE", 0).getResults();
        // one aggregate table returned
        assertEquals(1, results.length);
        System.out.println("Test QueueDepth table: " + results[0].toString());
        validateSchema(results[0], expectedTable);
        // One row per site, we don't use HSID though, so hard to do straightforward
        // per-site unique check.  Finesse it.
        // We also get starvation stats for the MPI, so we need to add kfactor+1 sites
        assertEquals(HOSTS * SITES + (KFACTOR + 1), results[0].getRowCount());
        results[0].advanceRow();
        Map<String, String> columnTargets = new HashMap<String, String>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, false);
    }

    public void testManagementStats() throws Exception {
        System.out.println("\n\nTESTING MANAGEMENT STATS\n\n\n");
        Client client = getFullyConnectedClient();

        VoltTable[] results = null;
        //
        // LIVECLIENTS
        //
        results = client.callProcedure("@Statistics", "MANAGEMENT", 0).getResults();
        // nine aggregate tables returned.  Assume that we have selected the right
        // subset of stats internally, just check that we get stuff.
        assertEquals(9, results.length);
    }

    public void testDRConflictsStats() throws Exception {
        System.out.println("\n\nTESTING DRCONFLICTS STATS\n\n\n");
        Client client = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[13];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo("CLUSTER_ID", VoltType.INTEGER);
        expectedSchema[4] = new ColumnInfo("REMOTE_CLUSTER_ID", VoltType.INTEGER);
        expectedSchema[5] = new ColumnInfo("PARTITION_ID", VoltType.INTEGER);
        expectedSchema[6] = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        expectedSchema[7] = new ColumnInfo("LAST_CONFLICT_TIMESTAMP", VoltType.TIMESTAMP);
        expectedSchema[8] = new ColumnInfo("TOTAL_CONFLICT_COUNT", VoltType.BIGINT);
        expectedSchema[9] = new ColumnInfo("DIVERGENCE_COUNT", VoltType.BIGINT);
        expectedSchema[10] = new ColumnInfo("MISSING_ROW_COUNT", VoltType.BIGINT);
        expectedSchema[11] = new ColumnInfo("TIMESTAMP_MISMATCH_COUNT", VoltType.BIGINT);
        expectedSchema[12] = new ColumnInfo("CONSTRAINT_VIOLATION_COUNT", VoltType.BIGINT);
        VoltTable expectedTable = new VoltTable(expectedSchema);
        VoltTable[] results = null;

        results = client.callProcedure("@Statistics", "DRCONFLICTS", 0).getResults();
        // Only one table returned
        assertEquals(1, results.length);
        validateSchema(results[0], expectedTable);

        // Should get zero rows - it is only incremented in case of DR conflicts
        assertEquals(0, results[0].getRowCount());
    }

    /*
     * Test that stats can be performed without error while a host is rejoining
     */
    public void testStatsDoesNotTimeoutDuringRejoin() throws Throwable {
        System.out.println("\n\nTESTING " + getName() + "\n\n\n");

        // Kill one host so it can rejoin
        LocalCluster cluster = (LocalCluster) m_config;
        cluster.killSingleHost(2);

        // Give the cluster time to react
        Thread.sleep(200);

        Client client = getFullyConnectedClient();

        class StatThread extends Thread {
            volatile boolean m_run = true;
            Throwable m_error = null;

            @Override
            public void run() {
                try {
                    while (m_run) {
                        client.callProcedure("@Statistics", "memory", 0);
                    }
                } catch (Throwable t) {
                    m_error = t;
                }
            }
        }

        StatThread st = new StatThread();
        st.start();

        assertTrue(cluster.recoverOne(2, 0));

        st.m_run = false;
        st.join();

        if (st.m_error != null) {
            throw st.m_error;
        }
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        return StatisticsTestSuiteBase.suite(TestStatisticsSuite.class, false);
    }
}
