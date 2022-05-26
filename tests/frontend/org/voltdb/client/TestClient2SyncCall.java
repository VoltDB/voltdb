/* This file is part of VoltDB.
 * Copyright (C) 2021-2022 Volt Active Data Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;


/**
 * These sync tests implictly test async calls, since
 * the sync implementation in Client2 merely issues the
 * async call and waits for it to complete.
 *
 * Specifically-async features are tested elsewhere.
 */
public class TestClient2SyncCall {

    static VoltDB.Configuration serverConfig;

    @Rule
    public final TestName testname = new TestName();

    @BeforeClass
    public static void prologue() {
        try {
            System.out.println("=-=-=-= Prologue =-=-=-=");

            CatalogBuilder catBuilder = new CatalogBuilder();
            catBuilder.addProcedures(ArbitraryDurationProc.class);

            boolean success = catBuilder.compile(Configuration.getPathToCatalogForTest("timeouts.jar"));
            assertTrue("bad catalog", success);

            DeploymentBuilder depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.writeXML(Configuration.getPathToCatalogForTest("timeouts.xml"));

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("timeouts.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("timeouts.xml");
            serverConfig = config;
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        // Note: we have a new server for each test because otherwise
        // timeout testing causes cross-talk between tests, because
        // tests remain queued in the server after test completion.
    }

    @AfterClass
    public static void epilogue() {
        System.out.println("=-=-=-= Epilogue =-=-=-=");
    }

    ServerThread localServer;

    @Before
    public void setup() {
        System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", testname.getMethodName());
        ServerThread.resetUserTempDir();
        localServer = new ServerThread(serverConfig);
        localServer.start();
        localServer.waitForInitialization();
    }

    @After
    public void teardown() throws Exception {
        localServer.shutdown();
        localServer.join();
        localServer = null;
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", testname.getMethodName());
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
    @Test
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
     * Zero timeout = a very long timeout.
     */
    @Test
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
    @Test
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
            // proc runs for 1.5 sec, short timeout after 0.5 sec (sub-second timeouts handled separately)
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
