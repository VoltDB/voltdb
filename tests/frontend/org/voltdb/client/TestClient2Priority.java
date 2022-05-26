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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;

/**
 * Test for priority queue handling in Client2 API.
 */
public class TestClient2Priority {

    ServerThread localServer;

    @Rule
    public final TestName testname = new TestName();

    @Before
    public void setup() {
        try {
            System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", testname.getMethodName());

            CatalogBuilder catBuilder = new CatalogBuilder();
            catBuilder.addProcedures(ArbitraryDurationProc.class);
            boolean success = catBuilder.compile(Configuration.getPathToCatalogForTest("priority.jar"));
            assertTrue("bad catalog", success);

            DeploymentBuilder depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.writeXML(Configuration.getPathToCatalogForTest("priority.xml"));

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("priority.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("priority.xml");

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

    volatile boolean threw = false;

    private void checkSuccess(ClientResponse resp) {
        assertNotNull("Null response", resp);
        assertEquals("Expected success", ClientResponse.SUCCESS, resp.getStatus());
    }

    private void saveFailure(List<Integer> respList, ClientResponse resp, int maxnum) {
        assertNotNull("Null response", resp);
        assertEquals("Expected unexpected failure", ClientResponse.UNEXPECTED_FAILURE, resp.getStatus());
        String text = resp.getStatusString();
        assertNotNull("No status string", text);
        Matcher m = Pattern.compile(".*BogoProc(\\d\\d\\d) .*").matcher(text);
        assertTrue("Unexpected status string: " + text, m.matches());
        String numStr = m.group(1);
        assertNotNull("No number", numStr);
        int num = Integer.parseInt(numStr);
        assertTrue("Bad number " + num, num > 0 && num <= maxnum);
        respList.add(num);
    }

    private Void screamAndShout(Throwable th) {
        assertNotNull(th);
        System.out.printf("Call completed exceptionally: %s\n", th);
        threw = true;
        return null;
    }

    /**
     * This test case ensures that we send requests in correct
     * priority order, and that within equal priorities, we
     * operate FIFO.
     */
    @Test
    public void testPriority() throws Exception {
        final int OUTLIMIT = 4; // high enough to get past initial flurry
        final int QUEUEDCNT = 8; // number of queued requests to make
        final int TOTALREQ = OUTLIMIT + 1 + QUEUEDCNT; // how many requests we will send

        Client2Config config = new Client2Config()
            .outstandingTransactionLimit(OUTLIMIT)
            .responseExecutorService(Executors.newSingleThreadExecutor(), true);

        System.out.println("Connecting");
        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost");
        threw = false;

        // Hacky code to wait until traffic caused by topo code
        // has settled down
        Thread.sleep(1000);
        client.drain();

        CountDownLatch allDone = new CountDownLatch(TOTALREQ);
        List<Integer> respList = new ArrayList<>(TOTALREQ);

        // This batch of calls will get sent to the network immediately
        // and therefore client priority will have no effect. They
        // also stick around for a while, preventing us from sending
        // anything else. We need one more than the outstanding txn
        // limit, because it will be dequeued before waiting for a
        // send permit.
        System.out.printf("Priming with %d requests\n", OUTLIMIT+1);
        for (int n=0; n<=OUTLIMIT; n++) {
            client.callProcedureAsync("ArbitraryDurationProc", 1500)
                .thenAccept(this::checkSuccess)
                .exceptionally(this::screamAndShout)
                .whenComplete((v,t) -> allDone.countDown());
        }

        // The rest get queued up. We queue them in reverse priority order
        // (lowest first) and expect them to get sent in priority order.
        // As a terrible hack, we make up non-existent proc names, which
        // will be reflected in the error messages from the server and
        // thus allow us to find out the send order (on the assumption
        // that response order is send order, which is true in the
        // limited case of one server, one response thread, and
        // immediate failure)
        assertTrue("test bug - QUEUEDCNT must be even", (QUEUEDCNT & 1) == 0);
        System.out.printf("Queueing %d further requests\n", QUEUEDCNT);
        for (int n=0; n<QUEUEDCNT; n+=2) {
            int evenseq = QUEUEDCNT - n; // thus range [1, QUEUEDCNT]
            int oddseq = evenseq - 1;
            int prio = oddseq; // send 2 of each prio 1,3,..,21
            Client2CallOptions opts = new Client2CallOptions().requestPriority(prio);
            client.callProcedureAsync(opts, String.format("BogoProc%03d", oddseq))
                .thenAccept((resp) -> { saveFailure(respList, resp, QUEUEDCNT); })
                .exceptionally(this::screamAndShout)
                .whenComplete((v,t) -> allDone.countDown());
            client.callProcedureAsync(opts, String.format("BogoProc%03d", evenseq))
                .thenAccept((resp) -> { saveFailure(respList, resp, QUEUEDCNT); })
                .exceptionally(this::screamAndShout)
                .whenComplete((v,t) -> allDone.countDown());
        }

        // Wait on completion. Timeout is just to avoid hanging forever.
        boolean finished = allDone.await(30, TimeUnit.SECONDS);
        assertTrue("Timed out awaiting finished", finished);
        client.drain();
        client.close();

        // No exceptions expected
        assertFalse("Exceptions were thrown", threw);

        // Now check that the responses were received in priority order.
        // This is pretty hacky; it is not guaranteed, but in this limited
        // case, it should work ok. See prior comments.
        StringBuilder sb = new StringBuilder(QUEUEDCNT*4 + 32);
        sb.append("Response order:");
        for (Integer r : respList) {
            sb.append(String.format(" %03d", r));
        }
        System.out.println(sb);
        assertEquals("Response count is wrong", QUEUEDCNT, respList.size());
        for (int n=0; n<respList.size(); n++) {
            assertEquals("Unexpected response order", n+1, respList.get(n).intValue());
        }
        System.out.println("All in order");
    }
}
