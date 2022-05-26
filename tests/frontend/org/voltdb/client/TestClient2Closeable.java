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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.TimeUnit;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;

/**
 * Tests closing via the 'Closeable' interface.
 *
 * This is separate from TestClient2Close because the
 * setup is somewhat different.
 */
public class TestClient2Closeable {

    ServerThread localServer;

    @Rule
    public final TestName testname = new TestName();

    @Before
    public void setup() {
        try {
            System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", testname.getMethodName());

            CatalogBuilder catBuilder = new CatalogBuilder();
            catBuilder.addProcedures(ArbitraryDurationProc.class);
            boolean success = catBuilder.compile(VoltDB.Configuration.getPathToCatalogForTest("closetest.jar"));
            assertTrue("bad catalog", success);

            DeploymentBuilder depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.writeXML(VoltDB.Configuration.getPathToCatalogForTest("closetest.xml"));

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = VoltDB.Configuration.getPathToCatalogForTest("closetest.jar");
            config.m_pathToDeployment = VoltDB.Configuration.getPathToCatalogForTest("closetest.xml");

            ServerThread.resetUserTempDir();
            localServer = new ServerThread(config);
            localServer.start();
            localServer.waitForInitialization();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @After
    public void teardown() throws Exception {
        localServer.shutdown();
        localServer.join();
        localServer = null;
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", testname.getMethodName());
    }

    /*
     * Test handling of 'Closeable' interface, which just calls the
     * close method on the client. But we try to do that when the
     * client is not quiescent.
     */
    @Test
    public void testBailout() throws Exception {
        final int PROCTIME = 1000; // execution time for one proc call
        final int TIMEOUT = 60_000; // relatively long so does not time out
        final int TOTALREQ = 20; // count of requests to send

        Client2Config config = new Client2Config()
            .procedureCallTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
            .outstandingTransactionLimit(5) // high enough to get past initial flurry
            .clientRequestBackpressureLevel(15, 5); // backpressure on at 15, off at 5
        long quitStart = 0;
        Client2 savedClient = null;

        // Drive into backpressure so that client queues are occupied,
        // then force closure by leaving the try-block.
        try (Client2 client = ClientFactory.createClient(config)) {
            client.connectSync("localhost");
            for (int i=0; i<TOTALREQ; i++) {
                client.callProcedureAsync("ArbitraryDurationProc", PROCTIME);
            }
            Thread.sleep(10); // help ensure a few txns get out to the server
            System.out.printf("Autoclosing with %d requests, %d transactions outstanding ...\n",
                              client.currentRequestCount(), client.outstandingTxnCount());
            savedClient = client;
            quitStart = System.currentTimeMillis();
        }
        long quitTime = System.currentTimeMillis() - quitStart;

        // Check we really did execute a close (call should fail)
        boolean ok = false;
        try {
            savedClient.callProcedureSync("FictitiousProcedureName");
        }
        catch (IllegalStateException ex) {
            System.out.println("Expected exception: " + ex);
            ok = ex.getMessage().contains("shutting down");
        }
        catch (Exception ex) {
            System.out.println("Not the exception we're looking for: " + ex);
        }
        savedClient = null;
        assertTrue("Did not close", ok);

        // Check time taken. At present there's no protection in this test
        // against hanging forever. That's not supposed to be possible.
        System.out.printf("Time taken for autoclose: %d msec\n", quitTime);
        assertTrue("Excessive time taken to close", quitTime < 10_000);
    }
}
