/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
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

package org.voltdb.iv2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.RealVoltDB;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.VoltFile;


public class TestVoltTableSerialization extends JUnit4LocalClusterTest {

    private static final String SCHEMA = "CREATE TABLE FOO (" +
            "PKEY INTEGER NOT NULL," +
            " VAL VARCHAR(63) NOT NULL, PRIMARY KEY(PKEY)," +
            ");" +
            " \nPARTITION TABLE FOO ON COLUMN PKEY;";

    private static  final String PROCS =
            "CREATE PROCEDURE FROM CLASS org.voltdb.iv2.TestVoltTableSerialization$PoisonProc;" +
            "PARTITION PROCEDURE TestVoltTableSerialization$PoisonProc ON TABLE FOO COLUMN PKEY;";


    private static final int HOST_COUNT = 3;
    private static final int SPH = 4;
    private static final int K_FACTOR = 1;

    // Poison stored procedure to simulate IndexOutofBoundsException
    public static class PoisonProc extends VoltProcedure {
        public final SQLStmt stmt = new SQLStmt("INSERT INTO FOO VALUES(?, ?);");

        public VoltTable run(int partitionKey) {
            VoltTable t = new VoltTable(new ColumnInfo[] { new ColumnInfo("C1", VoltType.BIGINT) });
            for (int ii = 0; ii < 10; ++ii) {
                t.addRow(ii);
            }

            ByteBuffer buf = ByteBuffer.allocate(t.getSerializedSize());
            t.flattenToBuffer(buf);

            RealVoltDB db = (RealVoltDB)VoltDB.instance();
            SpInitiator init = (SpInitiator)db.getInitiator(getPartitionId());

            // Set m_rowStart out of bounds in VoltTable buffer on replica if partitionKey = 0 or 2.
            // or on leader if partitionKey = 1 or 2
            // Partition leader will catch an IndexOutOfBoundsException, shutdown its PicoNetwork to the
            // replica. Arbiter will reach a consensus to evict one node. The surviving cluster should be
            // fully functional
            if ((!init.isLeader() && partitionKey == 0) || (init.isLeader() && partitionKey == 1) || partitionKey == 2) {
                // Make m_rowStart out of bounds
                buf.putInt(0, buf.capacity() + 1);
                return VoltTable.getInstanceForTest(buf);
            }

            return t;
        }
    }

    LocalCluster cluster = null;
    Client client = null;

    @Test
    public void testDeserializationErrorFromLeader() throws Exception {
        System.out.println("Test IndexOutOfBoundsException from partition leader.");
        setUp("OnLeader");
        // Partition leader sends itself a bad message. No message deserialization and no PicoNetwork shutdown.
        // However if client is not connected to the host with partition leader, final response will be sent
        // to the ClientInterface on replica host from the partition leader host. Then the replica host will
        // get IndexOutOfBoundsException and one node will be evicted.
        //
        int liveNode = executePoisonProc(1);
        assertTrue(liveNode == HOST_COUNT || liveNode == (HOST_COUNT -1));
    }

    @Test
    public void testDeserializationErrorFromReplica() throws Exception {
        System.out.println("Test IndexOutOfBoundsException from replica.");
        setUp("OnReplica");
        // Partition leader gets a bad message from its replica.
        // Lost one node with PicoNetwork shutdown
        assertEquals(executePoisonProc(0), HOST_COUNT -1);
    }

    @Test
    public void testDeserializationErrorFromLeaderReplica() throws Exception {
        System.out.println("Test IndexOutOfBoundsException from both replica and leader.");
        setUp("OnLeaderReplica");
        // Partition leader gets a bad message from its replica and a bad message from itself
        // Lost one node with PicoNetwork shutdown
        assertEquals(executePoisonProc(2), HOST_COUNT -1);
    }

    @SuppressWarnings("deprecation")
    private void setUp(String test) throws Exception {
        VoltFile.resetSubrootForThisProcess();
        cluster = new LocalCluster("TEST_FOO.jar", SPH, HOST_COUNT, K_FACTOR, BackendTarget.NATIVE_EE_JNI);
        cluster.overrideAnyRequestForValgrind();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(SCHEMA);
        builder.addLiteralSchema(PROCS);
        cluster.setCallingMethodName(test);
        cluster.setHasLocalServer(false);
        boolean success = cluster.compile(builder);
        assertTrue(success);

        cluster.startUp();
        ClientConfig config = new ClientConfig();
        config.setTopologyChangeAware(true);
        client = ClientFactory.createClient(config);
        client.createConnection("", cluster.port(0));
        for (int i = 0; i < 10; i++) {
            client.callProcedure("@AdHoc", "INSERT INTO FOO VALUES (" + i + ", 'x')");
        }
    }

    private int executePoisonProc(int key) throws Exception {
        int liveNodes = 0;
        try {
            // This call will trigger PicoNectwork shutdown.
            try {
                client.callProcedure("TestVoltTableSerialization$PoisonProc", key);
            } catch(ProcCallException e) {
            }

            Thread.sleep(10000);
            liveNodes = cluster.getLiveNodeCount();

            // More inserts to the surviving cluster to test cluster's vitality.
            for (int i = 10; i < 20; i++) {
                client.callProcedure("@AdHoc", "INSERT INTO FOO VALUES (" + i + ", 'x')");
            }
            // Check out inserted tuples
            VoltTable t =  client.callProcedure("@AdHoc", "select count(*) from foo").getResults()[0];
            assertEquals(20, t.asScalarLong());
       } finally {
           if ( client != null) {
               try {
                   client.close();
                   client = null;
               } catch (InterruptedException e) {
               }
           }
           if (cluster != null) {
               try {
                   cluster.shutDown();
                   cluster = null;
               } catch (InterruptedException e) {
               }
           }
        }
        return liveNodes;
    }
}
