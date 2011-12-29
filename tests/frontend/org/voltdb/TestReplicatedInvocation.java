/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.network.WriteStream;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.VoltFile;

public class TestReplicatedInvocation {
    ServerThread server;
    File root;
    ReplicationRole role = ReplicationRole.REPLICA;

    static class MockWriteStream implements WriteStream {
        @Override
        public boolean hadBackPressure() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean enqueue(BBContainer c) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean enqueue(FastSerializable f) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean enqueue(FastSerializable f, int expectedSize) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean enqueue(DeferredSerialization ds) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean enqueue(ByteBuffer b) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public int calculatePendingWriteDelta(long now) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean isEmpty() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public int getOutstandingMessageCount() {
            // TODO Auto-generated method stub
            return 0;
        }
    }

    @Before
    public void setUp() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = " create table A (i integer not null, primary key (i));";
        builder.addLiteralSchema(schema);
        builder.addPartitionInfo("A", "i");
        builder.addProcedures(ReplicatedProcedure.class);

        root = File.createTempFile("temp", "replicatedinvocation");
        root.delete();
        assertTrue(root.mkdir());
        File cat = File.createTempFile("temp", "replicatedinvocationcat");
        cat.deleteOnExit();
        assertTrue(builder.compile(cat.getAbsolutePath(), 2, 1, 0, root.getAbsolutePath()));
        String deployment = builder.getPathToDeployment();

        // start server
        server = new ServerThread(cat.getAbsolutePath(), deployment,
                                  BackendTarget.NATIVE_EE_JNI);
        server.m_config.m_replicationRole = role;
        server.start();
        server.waitForInitialization();
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        server.shutdown();
        VoltFile.recursivelyDelete(root);
    }

    /**
     * Send a replicated procedure invocation and checks if the procedure sees
     * the specified txn ID.
     */
    @Test
    public void testReplicatedInvocation() throws Exception {
        Client client = ClientFactory.createClient();
        client.createConnection("localhost");
        ClientResponse response = client.callProcedure(3, "ReplicatedProcedure", 1, "haha");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        VoltTable result = response.getResults()[0];
        result.advanceRow();
        assertEquals(3, result.getLong("txnId"));
        client.close();
    }

    @Test
    public void testAcceptanceOnPrimary() throws Exception {
        Client client = ClientFactory.createClient();
        client.createConnection("localhost");
        try {
            client.callProcedure("A.insert", 1);
        } catch (ProcCallException e) {
            if (role == ReplicationRole.REPLICA) {
                client.close();
                return;
            } else {
                throw e;
            }
        }
        if (role == ReplicationRole.REPLICA) {
            fail("Should not succeed on replica cluster");
        }
    }

    @Test
    public void testSysprocAcceptanceOnReplica() {
        ReplicaInvocationAcceptancePolicy policy = new ReplicaInvocationAcceptancePolicy(true);
        for (Entry<String, Config> e : SystemProcedureCatalog.listing.entrySet()) {
            StoredProcedureInvocation invocation = new StoredProcedureInvocation();
            invocation.procName = e.getKey();
            if (e.getKey().equalsIgnoreCase("@UpdateApplicationCatalog") ||
                e.getKey().equalsIgnoreCase("@SnapshotRestore") ||
                e.getKey().equalsIgnoreCase("@BalancePartitions") ||
                e.getKey().equalsIgnoreCase("@LoadMultipartitionTable") ||
                e.getKey().equalsIgnoreCase("@LoadSinglePartitionTable")) {
                assertFalse(policy.shouldAccept(null, invocation, e.getValue(), new MockWriteStream()));
            } else if (e.getKey().equalsIgnoreCase("@AdHoc")) {
                invocation.setParams("select * from A");
                assertTrue(policy.shouldAccept(null, invocation, e.getValue(), new MockWriteStream()));

                invocation.setParams("insert into A values (1, 2, 3)");
                assertFalse(policy.shouldAccept(null, invocation, e.getValue(), new MockWriteStream()));
            } else {
                if (!policy.shouldAccept(null, invocation, e.getValue(), new MockWriteStream())) {
                    System.out.println(e.getKey());
                }
                assertTrue(policy.shouldAccept(null, invocation, e.getValue(), new MockWriteStream()));
            }
        }
    }

    /**
     * Test promoting a replica to master
     * @throws Exception
     */
    @Test
    public void testPromote() throws Exception {
        if (role != ReplicationRole.REPLICA) {
            return;
        }

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");
        ClientResponse resp = client.callProcedure("@SystemInformation", "overview");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable result = resp.getResults()[0];
        while (result.advanceRow()) {
            if (result.getString("KEY").equalsIgnoreCase("replicationrole")) {
                assertTrue(result.getString("VALUE").equalsIgnoreCase("replica"));
            }
        }

        resp = client.callProcedure("@Promote");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        resp = client.callProcedure("@SystemInformation", "overview");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        result = resp.getResults()[0];
        while (result.advanceRow()) {
            if (result.getString("KEY").equalsIgnoreCase("replicationrole")) {
                assertTrue(result.getString("VALUE").equalsIgnoreCase("master"));
            }
        }
    }
}
