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

package org.voltdb.catalog;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltProcedure;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.DeploymentBuilder;

/**
 * Check that changing the partitioning of a procedure using multi-txn DDL is safe even if
 * you're calling the proc during the change.
 *
 * The issue to be careful about is the CI/TransactionInitialtor routing the invocation
 * with one partitioning scheme, but the scheme changes before the procedure can be run.
 *
 * In this case the execution site needs to return the call to the ClientInterface for
 * re-routing.
 *
 */
public class TestLiveProcedurePartitioningChanges extends TestCase {

    //LocalCluster cluster;
    ServerThread server;
    Random rand = new Random(0);

    final static String PROC_NAME = TestLiveProcedurePartitioningChanges.class.getSimpleName() + "$FooProc";
    final static String LONG_PROC_NAME = TestLiveProcedurePartitioningChanges.class.getCanonicalName() + "$FooProc";

    /**
     * Trivial Java procedure that can be partitioned or not.
     */
    public static class FooProc extends VoltProcedure {
        public long run(String param) {
            return 0;
        }
    }

    Client getRandomClient() throws UnknownHostException, IOException {
        String address = "localhost";

        Client client = ClientFactory.createClient();
        client.createConnection(address);

        return client;
    }

    void deleteProcedure() throws NoConnectionsException, IOException, ProcCallException, InterruptedException {
        Client client = getRandomClient();
        ClientResponse response = client.callProcedure("@AdHoc", "DROP PROCEDURE " + PROC_NAME + ";");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();
    }

    void addProcedure() throws UnknownHostException, IOException, ProcCallException, InterruptedException {
        Client client = getRandomClient();
        ClientResponse response = client.callProcedure("@AdHoc", "CREATE PROCEDURE FROM CLASS " + LONG_PROC_NAME + ";");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();
    }

    void partitionProcedure() throws NoConnectionsException, IOException, ProcCallException, InterruptedException {
        Client client = getRandomClient();
        ClientResponse response = client.callProcedure("@AdHoc", "PARTITION PROCEDURE " + PROC_NAME + " ON TABLE dummy COLUMN sval1;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();
    }

    void unpartitionProcedure() throws NoConnectionsException, IOException, ProcCallException, InterruptedException {
        Client client = getRandomClient();
        ClientResponse response = client.callProcedure("@AdHoc", "DROP PROCEDURE " + PROC_NAME + "; "
                + "CREATE PROCEDURE FROM CLASS " + LONG_PROC_NAME + ";");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();
    }

    public void testSlamming() throws IOException, ProcCallException, InterruptedException {
        String simpleSchema =
                "create table dummy (" +
                "sval1 varchar(100) not null, " +
                "sval2 varchar(100) default 'foo', " +
                "sval3 varchar(100) default 'bar', " +
                "PRIMARY KEY(sval1));\n" +
                "PARTITION TABLE dummy ON COLUMN sval1;";

        DeploymentBuilder deploymentBuilder = new DeploymentBuilder(3, 1, 0);
        deploymentBuilder.setUseDDLSchema(true);
        deploymentBuilder.setEnableCommandLogging(false);
        deploymentBuilder.writeXML(Configuration.getPathToCatalogForTest("slamcatalog.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("slamcatalog.xml");
        server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        Client client = getRandomClient();

        ClientResponse response;

        response = client.callProcedure("@AdHoc", simpleSchema);
        assert(response.getStatus() == ClientResponse.SUCCESS);

        final AtomicBoolean shouldContinue = new AtomicBoolean(true);

        // create a thread to call the proc over and over with different pkeys
        Thread clientThread = new Thread() {
            @Override
            public void run() {
                for (long i = 0; shouldContinue.get(); i++) {
                    try {
                        client.callProcedure(PROC_NAME, String.valueOf(i));
                    } catch (NoConnectionsException e) {
                        fail();
                    } catch (IOException e) {
                        fail();
                    } catch (ProcCallException e) {
                        String msg = e.getMessage();
                        e.printStackTrace();
                        assertTrue(msg.contains("is not present") || msg.contains("was not found"));
                    }
                }
            }
        };
        clientThread.start();

        // mess up the presence and partitioning of the procedure
        for (int i = 0; i < 50; i++) {
            addProcedure();
            partitionProcedure();
            unpartitionProcedure();
            deleteProcedure();
        }

        shouldContinue.set(false);
        clientThread.join();

        client.close();
        server.shutdown();
    }
}
