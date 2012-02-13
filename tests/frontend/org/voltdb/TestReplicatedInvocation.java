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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ReplicaProcCaller;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.utils.VoltFile;

public class TestReplicatedInvocation {
    ServerThread server;
    File root;
    ReplicationRole role = ReplicationRole.REPLICA;

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
        ClientImpl client = (ClientImpl) ClientFactory.createClient();
        client.createConnection("localhost");
        ReplicaProcCaller pc = client;
        SyncCallback callback = new SyncCallback();
        pc.callProcedure(3, callback, "ReplicatedProcedure", 1, "haha");
        callback.waitForResponse();
        ClientResponse response = callback.getResponse();
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
                // Rejected
                assertTrue(policy.shouldAccept(null, invocation, e.getValue()) != null);
            } else if (e.getKey().equalsIgnoreCase("@AdHoc")) {
                // Accepted
                invocation.setParams("select * from A");
                assertTrue(policy.shouldAccept(null, invocation, e.getValue()) == null);

                // Rejected
                invocation.setParams("insert into A values (1, 2, 3)");
                assertTrue(policy.shouldAccept(null, invocation, e.getValue()) != null);
            } else {
                // Accepted
                assertTrue(policy.shouldAccept(null, invocation, e.getValue()) == null);
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
