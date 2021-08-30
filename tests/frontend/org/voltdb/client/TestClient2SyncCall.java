/* This file is part of VoltDB.
 * Copyright (C) 2021 VoltDB Inc.
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

package org.voltdb.client;

import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.voltdb.ServerThread;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;

import junit.framework.TestCase;

/**
 * These sync tests implictly test async calls, since
 * the sync implementation in Client2 merely issues the
 * async call and waits for it to complete.
 *
 * Specifically-async features are tested elsewhere.
 */
public class TestClient2SyncCall extends TestCase {

    ServerThread localServer;
    DeploymentBuilder depBuilder;

    @Override
    public void setUp() {
        try {
            System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", getName());

            CatalogBuilder catBuilder = new CatalogBuilder();
            catBuilder.addProcedures(ArbitraryDurationProc.class);

            boolean success = catBuilder.compile(Configuration.getPathToCatalogForTest("timeouts.jar"));
            if (!success) throw new RuntimeException("bad catalog");

            depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.writeXML(Configuration.getPathToCatalogForTest("timeouts.xml"));

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("timeouts.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("timeouts.xml");
            localServer = new ServerThread(config);
            localServer.start();
            localServer.waitForInitialization();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Override
    public void tearDown() throws Exception {
        localServer.shutdown();
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", getName());
    }

    volatile boolean gotLateResp = false;

    private void lateResponse(ClientResponse resp, String host, int port) {
        gotLateResp = true;
    }

    private boolean waitForLateResp(long timeout) {
        long start = System.currentTimeMillis();
        while (!gotLateResp && System.currentTimeMillis() - start < timeout) {
            Thread.yield();
        }
        boolean ret =  gotLateResp;
        System.out.printf("Late response notification %s received\n", ret ? "was" : "was not");
        return ret;
    }

    /**
     * Simple call testing with timeout
     */
    public void testSimpleTimeout() throws Exception {
        Client2Config config = new Client2Config()
            .procedureCallTimeout(1100, TimeUnit.MILLISECONDS)
            .lateResponseHandler(this::lateResponse);

        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost");

        // proc runs for 0 sec, timeout 1.1 sec, expect no timeout
        ClientResponse response = client.callProcedureSync("ArbitraryDurationProc", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        response = null;
        try {
            // proc runs for 2.2 sec, timeout after 1.1 sec
            response = client.callProcedureSync("ArbitraryDurationProc", 2200);
            fail();
        }
        catch (ProcCallException ex) {
            response = ex.getClientResponse();
            System.out.println("testPerCallTimeout got expected ProcCallException with status " + response.getStatus());
            assertEquals(ClientResponse.CLIENT_RESPONSE_TIMEOUT, response.getStatus());
        }

        // response had better be here after ~4 sec (3.2 more sec after expected 1.1 sec timeout)
        assertTrue(waitForLateResp(3200));
    }

    /**
     * Test special exception for slow snapshots or catalogs updates
     * Both features are pro only
     */
    public void testLongCallNoTimeout() throws Exception {
        Client2Config config = new Client2Config()
            .procedureCallTimeout(500, TimeUnit.MILLISECONDS)
            .lateResponseHandler(this::lateResponse);

        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost");

        // build a catalog with a ton of indexes so catalog update will be slow
        CatalogBuilder builder = new CatalogBuilder();
        builder.addSchema(getClass().getResource("clientfeatures-wellindexed.sql"));
        builder.addProcedures(ArbitraryDurationProc.class);
        byte[] catalogToUpdate = builder.compileToBytes();
        assert(catalogToUpdate != null);

        // make a copy of the table from ddl for loading
        // (shouldn't have to do this, but for now, the table loader requires
        //  a VoltTable, and can't read schema. Could fix by using this VoltTable
        //  to generate schema or by teaching to loader how to discover tables)
        TableHelper.Configuration helperConfig = new TableHelper.Configuration();
        helperConfig.rand = new Random();
        TableHelper helper = new TableHelper(helperConfig);
        VoltTable t = TableHelper.quickTable("indexme (pkey:bigint, " +
                                             "c01:varchar63, " +
                                             "c02:varchar63, " +
                                             "c03:varchar63, " +
                                             "c04:varchar63, " +
                                             "c05:varchar63, " +
                                             "c06:varchar63, " +
                                             "c07:varchar63, " +
                                             "c08:varchar63, " +
                                             "c09:varchar63, " +
                                             "c10:varchar63) " +
                                             "PKEY(pkey)");

        // get a client with a normal timeout
        // uses old client for now; this is not the test
        Client clientX = ClientFactory.createClient();
        clientX.createConnection("localhost");
        helper.fillTableWithBigintPkey(t, 400, 0, clientX, 0, 1);

        // run a catalog update that *might* normally timeout
        long start = System.nanoTime();
        ClientResponse response = client.callProcedureSync("@UpdateApplicationCatalog", catalogToUpdate, depBuilder.getXML());
        double duration = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("Catalog update duration: %.2f sec\n", duration);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        // run a blocking snapshot that *might* normally timeout
        start = System.nanoTime();
        response = client.callProcedureSync("@SnapshotSave", Configuration.getPathToCatalogForTest(""), "slow", 1);
        duration = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("Snapshot save duration: %.2f sec\n", duration);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }

    /**
     * Zero timeout = a very long timeout.
     */
    public void testMaxTimeout() throws Exception {
        Client2Config config = new Client2Config()
            .procedureCallTimeout(0, TimeUnit.MILLISECONDS);

        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost");

        ClientResponse response = client.callProcedureSync("ArbitraryDurationProc", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        response = client.callProcedureSync("ArbitraryDurationProc", 2000);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }

    /**
     * Per-call override of timeout
     */
    public void testClientTimeoutOverride() throws Exception {
        Client2Config config = new Client2Config()
            .procedureCallTimeout(0, TimeUnit.MILLISECONDS);

        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost");

        // proc runs 0 sec, timeout infinite, so no timeout expected
        ClientResponse response = client.callProcedureSync("ArbitraryDurationProc", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        response = null;
        boolean exceptionCalled = false;
        try {
            // proc runs for 2 sec, timeout after 1.2 sec
            Client2CallOptions opts = new Client2CallOptions().clientTimeout(1200, TimeUnit.MILLISECONDS);
            response = client.callProcedureSync(opts, "ArbitraryDurationProc", 2000);
        }
        catch (ProcCallException ex) {
            response = ex.getClientResponse();
            System.out.println("testClientTimeoutOverride got expected ProcCallException with status " + response.getStatus());
            assertEquals(ClientResponse.CLIENT_RESPONSE_TIMEOUT, response.getStatus());
            exceptionCalled = true;
        }
        assertTrue(exceptionCalled);

        response = null;
        exceptionCalled = false;
        try {
            // proc runs for 1.5 sec, short timeout after 0.5 sec (sub-second timeouta handled separately)
            Client2CallOptions opts = new Client2CallOptions().clientTimeout(500, TimeUnit.MILLISECONDS);
            response = client.callProcedureSync(opts, "ArbitraryDurationProc", 1500);
        }
        catch (ProcCallException ex) {
            response = ex.getClientResponse();
            System.out.println("testClientTimeoutOverride got expected ProcCallException with status " + response.getStatus());
            assertEquals(ClientResponse.CLIENT_RESPONSE_TIMEOUT, response.getStatus());
            exceptionCalled = true;
        }
        assertTrue(exceptionCalled);

        response = null;
        exceptionCalled = false;
        try {
            // proc runs for 2 sec, timeout after 10 sec, so no timeout
            Client2CallOptions opts = new Client2CallOptions().clientTimeout(10000, TimeUnit.MILLISECONDS);
            response = client.callProcedureSync(opts, "ArbitraryDurationProc", 2000);
        }
        catch (ProcCallException ex) {
            response = ex.getClientResponse();
            System.out.println("testClientTimeoutOverride got unexpected ProcCallException with status " + response.getStatus());
            exceptionCalled = true;
        }
        assertFalse(exceptionCalled);

        response = null;
        try {
            // proc runs for 2 sec, infinite timeout, so no timeout
            Client2CallOptions opts = new Client2CallOptions().clientTimeout(0, TimeUnit.MILLISECONDS);
            response = client.callProcedureSync(opts, "ArbitraryDurationProc", 2000);
        }
        catch (ProcCallException ex) {
            exceptionCalled = true;
            response = ex.getClientResponse();
            System.out.println("testClientTimeoutOverride got unexpected ProcCallException with status " + response.getStatus());
        }
        assertFalse(exceptionCalled);
    }
}
