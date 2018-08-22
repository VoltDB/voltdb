/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.dr2.DRProtocol;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;

import junit.framework.Test;

public class TestStatisticsSuiteDRStats extends StatisticsTestSuiteBase {

    private static int REPLICATION_PORT = 11000;

    private static final ColumnInfo[] expectedDRNodeStatsSchema;
    private static final ColumnInfo[] expectedDRPartitionStatsSchema;

    static {
        expectedDRNodeStatsSchema = new ColumnInfo[] {
            new ColumnInfo("TIMESTAMP", VoltType.BIGINT),
            new ColumnInfo("HOST_ID", VoltType.INTEGER),
            new ColumnInfo("HOSTNAME", VoltType.STRING),
            new ColumnInfo("CLUSTER_ID", VoltType.SMALLINT),
            new ColumnInfo("REMOTE_CLUSTER_ID", VoltType.SMALLINT),
            new ColumnInfo("STATE", VoltType.STRING),
            new ColumnInfo("SYNCSNAPSHOTSTATE", VoltType.STRING),
            new ColumnInfo("ROWSINSYNCSNAPSHOT", VoltType.BIGINT),
            new ColumnInfo("ROWSACKEDFORSYNCSNAPSHOT", VoltType.BIGINT),
            new ColumnInfo("QUEUEDEPTH", VoltType.BIGINT)
        };

        expectedDRPartitionStatsSchema = new ColumnInfo[] {
            new ColumnInfo("TIMESTAMP", VoltType.BIGINT),
            new ColumnInfo("HOST_ID", VoltType.INTEGER),
            new ColumnInfo("HOSTNAME", VoltType.STRING),
            new ColumnInfo("CLUSTER_ID", VoltType.SMALLINT),
            new ColumnInfo("REMOTE_CLUSTER_ID", VoltType.SMALLINT),
            new ColumnInfo("PARTITION_ID", VoltType.INTEGER),
            new ColumnInfo("STREAMTYPE", VoltType.STRING),
            new ColumnInfo("TOTALBYTES", VoltType.BIGINT),
            new ColumnInfo("TOTALBYTESINMEMORY", VoltType.BIGINT),
            new ColumnInfo("TOTALBUFFERS", VoltType.BIGINT),
            new ColumnInfo("LASTQUEUEDDRID", VoltType.BIGINT),
            new ColumnInfo("LASTACKDRID", VoltType.BIGINT),
            new ColumnInfo("LASTQUEUEDTIMESTAMP", VoltType.TIMESTAMP),
            new ColumnInfo("LASTACKTIMESTAMP", VoltType.TIMESTAMP),
            new ColumnInfo("ISSYNCED", VoltType.STRING),
            new ColumnInfo("MODE", VoltType.STRING),
            new ColumnInfo("QUEUE_GAP", VoltType.BIGINT),
        };
    }

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

        VoltTable expectedTable2 = new VoltTable(expectedDRNodeStatsSchema);

        //
        // DRNODE
        //
        VoltTable[] results = client.callProcedure("@Statistics", "DRPRODUCERNODE", 0).getResults();
        // one aggregate tables returned
        assertEquals(1, results.length);
        System.out.println("Test DRPRODUCERNODE table: " + results[0].toString());
        validateSchema(results[0], expectedTable2);
        // One row per host for DRNODE stats
        results[0].advanceRow();
        Map<String, String> columnTargets = new HashMap<>();
        columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[0], columnTargets, true);
    }

    public void testDRPartitionStatisticsWhenIdle() throws Exception {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            System.out.println("SKIPPING DRPRODUCERPARTITION STATS TESTS FOR COMMUNITY VERSION");
            return;
        }
        System.out.println("\n\nTESTING DRPRODUCERPARTITION STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        VoltTable expectedTable1 = new VoltTable(expectedDRPartitionStatsSchema);

        //
        // DRPARTITION
        //
        VoltTable[] results = client.callProcedure("@Statistics", "DRPRODUCERPARTITION", 0).getResults();
        // one aggregate tables returned
        assertEquals(1, results.length);
        System.out.println("Test DR table: " + results[0].toString());
        validateSchema(results[0], expectedTable1);
        assertEquals(0, results[0].getRowCount());
    }

    public void testDRPartitionStatisticsWithConsumers() throws Exception {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            System.out.println("SKIPPING DRPRODUCERPARTITION STATS TESTS FOR COMMUNITY VERSION");
            return;
        }
        System.out.println("\n\nTESTING DRPRODUCERPARTITION STATS\n\n\n");
        Client primaryClient  = getFullyConnectedClient();
        List<Client> consumerClients = new ArrayList<>();
        List<LocalCluster> consumerClusters = new ArrayList<>();

        VoltTable expectedTable1 = new VoltTable(expectedDRPartitionStatsSchema);

        ClientResponse cr = primaryClient.callProcedure("@AdHoc", "insert into employee values(1, 25);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // create a consumer cluster and connect to producer cluster so that there will be rows for partition stats
        String secondaryRoot = "/tmp/" + System.getProperty("user.name") + "-dr-stats-secondary";

        try {
            int CONSUMER_CLUSTER_COUNT = 2;
            for (int n = 1; n <= CONSUMER_CLUSTER_COUNT; n++) {
                LocalCluster consumerCluster = LocalCluster.createLocalCluster(drSchema, SITES, HOSTS, KFACTOR, n,
                        REPLICATION_PORT + 100 * n, REPLICATION_PORT, secondaryRoot, jarName, DrRoleType.REPLICA, false);
                ClientConfig clientConfig = new ClientConfig();
                clientConfig.setProcedureCallTimeout(10 * 60 * 1000);
                Client consumerClient = createClient(clientConfig, consumerCluster);
                consumerClusters.add(consumerCluster);
                consumerClients.add(consumerClient);
                boolean hasSnapshotData = false;
                for (int i = 0; i < 60; i++) {
                    cr = consumerClient.callProcedure("@AdHoc", "select count(e_id) from employee;");
                    assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                    VoltTable result = cr.getResults()[0];
                    assertTrue(result.advanceRow());
                    if (result.getLong(0) == 1) { // get all snapshot records
                        hasSnapshotData = true;
                        break;
                    }
                    Thread.sleep(1000);
                }
                assertTrue(hasSnapshotData);
            }

            //
            // DRPARTITION
            //
            VoltTable[] results = primaryClient.callProcedure("@Statistics", "DRPRODUCERPARTITION", 0).getResults();
            // one aggregate tables returned
            assertEquals(1, results.length);
            System.out.println("Test DR table: " + results[0].toString());
            validateSchema(results[0], expectedTable1);
            // One row per site, including the MPI on each host if there is DR replicated stream
            // don't have HSID for ease of check, just check a bunch of stuff
            boolean hasReplicatedStream = DRProtocol.PROTOCOL_VERSION < DRProtocol.NO_REPLICATED_STREAM_PROTOCOL_VERSION;
            assertEquals(CONSUMER_CLUSTER_COUNT * (HOSTS * (SITES + (hasReplicatedStream ? 1 : 0))), results[0].getRowCount());
            results[0].advanceRow();
            Map<String, String> columnTargets = new HashMap<>();
            columnTargets.put("HOSTNAME", results[0].getString("HOSTNAME"));
            validateRowSeenAtAllHosts(results[0], columnTargets, false);
            validateRowSeenAtAllPartitions(results[0], "HOSTNAME", results[0].getString("HOSTNAME"), false);
        }
        finally {
            primaryClient.close();
            for (Client consumerClient : consumerClients) {
                if (consumerClient != null) {
                    consumerClient.close();
                }
            }
            for (LocalCluster consumerCluster : consumerClusters) {
                if (consumerCluster != null) {
                    consumerCluster.shutDown();
                }
            }
        }
    }

    public void testDRStatistics() throws Exception {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            System.out.println("SKIPPING DR STATS TESTS FOR COMMUNITY VERSION");
            return;
        }
        System.out.println("\n\nTESTING DR STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        VoltTable expectedTable1 = new VoltTable(expectedDRPartitionStatsSchema);
        VoltTable expectedTable2 = new VoltTable(expectedDRNodeStatsSchema);

        //
        // DR
        //
        VoltTable[] results = client.callProcedure("@Statistics", "DR", 0).getResults();
        // two aggregate tables returned
        assertEquals(2, results.length);
        System.out.println("Test DR table: " + results[0].toString());
        System.out.println("Test DR table: " + results[1].toString());
        validateSchema(results[0], expectedTable1);
        validateSchema(results[1], expectedTable2);

        // One row per host for DRNODE stats
        results[1].advanceRow();
        Map<String, String> columnTargets = new HashMap<>();
        columnTargets.put("HOSTNAME", results[1].getString("HOSTNAME"));
        validateRowSeenAtAllHosts(results[1], columnTargets, true);
    }

    public void testDRRole() throws IOException, ProcCallException {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            System.out.println("SKIPPING DRNODE STATS TESTS FOR COMMUNITY VERSION");
            return;
        }
        System.out.println("\n\nTESTING DRROLE STATS\n\n\n");
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema2 = new ColumnInfo[3];
        assertEquals("Expected DRRoleStatistics schema length is 3", 3, expectedSchema2.length);
        expectedSchema2[0] = new ColumnInfo("ROLE", VoltType.STRING);
        expectedSchema2[1] = new ColumnInfo("STATE", VoltType.STRING);
        expectedSchema2[2] = new ColumnInfo("REMOTE_CLUSTER_ID", VoltType.INTEGER);
        VoltTable expectedTable2 = new VoltTable(expectedSchema2);

        VoltTable[] results = null;
        //
        // DRNODE
        //
        results = client.callProcedure("@Statistics", "DRROLE", 0).getResults();
        // one aggregate tables returned
        assertEquals(1, results.length);
        System.out.println("Test DRROLE table: " + results[0].toString());
        validateSchema(results[0], expectedTable2);
        // Only one row for DRROLE stats
        assertEquals(1, results[0].getRowCount());
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        return StatisticsTestSuiteBase.suite(TestStatisticsSuiteDRStats.class, false, REPLICATION_PORT, false);
    }

    private Client createClient(ClientConfig config, LocalCluster cluster) throws IOException {
        Client client = ClientFactory.createClient(config);
        for (String address : cluster.getListenerAddresses()) {
            client.createConnection(address);
        }
        return client;
    }
}
