package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;

import org.junit.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

public class TestZombieHosts extends JUnit4LocalClusterTest {

    LocalCluster cluster = null;
    Client c = null;
    @Test
    public void testZombieHosts() throws Exception {
        assertEquals(TheHashinator.getConfiguredHashinatorType(), TheHashinator.HashinatorType.ELASTIC);
        try{
            //4 sph, 5 host count, k = 1, and 2 zombie nodes,
            cluster = createLocalCluster("zomebie-nodes.jar", 4, 5, 1, 2);
            c = ClientFactory.createClient();
            c.createConnection("", cluster.port(0));
            verifyPartitionCount(c, 10);
            for (int ii = 0; ii < 10; ii++) {
                c.callProcedure("P1.insert", ii, ii);
            }
        } finally {
            cleanup();
        }
    }
    private LocalCluster createLocalCluster(String jarName, int sph, int hostCount, int kfactor, int inactiveCount) throws IOException {
        final String schema = "CREATE TABLE P1 (ID BIGINT DEFAULT '0' NOT NULL," +
                        " VIOLATION BIGINT DEFAULT '0' NOT NULL," +
                        " CONSTRAINT VIOC ASSUMEUNIQUE ( VIOLATION )," +
                        " PRIMARY KEY (ID)); PARTITION TABLE P1 ON COLUMN ID;";
        LocalCluster cluster = new LocalCluster(jarName, sph, hostCount, kfactor, BackendTarget.NATIVE_EE_JNI, inactiveCount);
        cluster.setNewCli(false);
        cluster.overrideAnyRequestForValgrind();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(schema);
        builder.configureLogging(null, null, false, true, 200, Integer.MAX_VALUE, 300);
        cluster.setHasLocalServer(false);
        cluster.setJavaProperty("ELASTIC_TOTAL_TOKENS", "128");
        cluster.setJavaProperty("ELASTIC_TARGET_THROUGHPUT", "10485760");
        cluster.setJavaProperty("ELASTIC_TARGET_TRANSFER_TIME_MS", "1000");
        boolean success = cluster.compile(builder);
        assertTrue(success);
        cluster.startUp();
        return cluster;
    }

    private void verifyPartitionCount(Client client, int expectedCount) throws NoConnectionsException, IOException, ProcCallException {
        long sleep = 100;
        //allow time to get the stats
        final long maxSleep = 1800000;
        int partitionKeyCount;
        while (true) {
            try {
                VoltTable vt = client.callProcedure("@GetPartitionKeys", "INTEGER").getResults()[0];
                partitionKeyCount = vt.getRowCount();
                if (expectedCount == partitionKeyCount) {
                    break;
                }
                try { Thread.sleep(sleep); } catch (Exception ignored) { }
                if (sleep < maxSleep) {
                    sleep = Math.min(sleep + sleep, maxSleep);
                } else {
                    break;
                }
            } catch (Exception e) {
            }
        }
        assertEquals(expectedCount, partitionKeyCount);
        VoltTable vt = client.callProcedure("@GetHashinatorConfig").getResults()[0];
        assertTrue(vt.advanceRow());
        HashSet<Integer> partitionIds = new HashSet<Integer>();
        ByteBuffer buf = ByteBuffer.wrap(vt.getVarbinary(1));
        int tokens = buf.getInt();
        for (int ii = 0; ii < tokens; ii++) {
            buf.getInt();
            partitionIds.add(buf.getInt());
        }
        assertEquals(expectedCount, partitionIds.size());
    }

    private void cleanup() throws InterruptedException {
        if ( c != null) {
            c.close();
        }
        if (cluster != null) {
            cluster.shutDown();
        }
    }
}
