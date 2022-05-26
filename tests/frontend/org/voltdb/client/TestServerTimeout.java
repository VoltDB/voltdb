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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

/**
 * These tests are for server-side timeouts, using
 * the Client2 API.
 */
public class TestServerTimeout {

    private LocalCluster cluster;
    private int port;

    @Rule
    public final TestName testname = new TestName();

    @Before
    public void setup() {
        Client2 client = null;
        try {
            System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", testname.getMethodName());

            cluster = new LocalCluster("server-timeout.jar", 1/*sph*/, 1/*hosts*/, 0/*k-factor*/, BackendTarget.NATIVE_EE_JNI);
            cluster.overrideAnyRequestForValgrind();
            cluster.setHasLocalServer(true);

            VoltProjectBuilder project = new VoltProjectBuilder();
            project.setUseDDLSchema(true);
            project.addSchema(TestServerTimeout.class.getResource("server-timeout.sql"));

            boolean success = cluster.compile(project);
            assertTrue("compile", success);
            cluster.startUp();
            port = cluster.port(0);

            client = ClientFactory.createClient(new Client2Config());
            client.connectSync("localhost", port);
            load(client, "SERVER_TMO_TEST");
            client.close();
            client = null;
        }
        catch (Exception e) {
            e.printStackTrace();
            safeClose(client);
            fail("!!! failed in setUp !!!");
        }
    }

    private void safeClose(Client2 client) {
        if (client != null) {
            try {
                client.close();
            }
            catch (Exception e2) {
            }
        }
    }

    private void load(Client2 client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        final int ROWS = 9;
        final String ins = "insert into %s values ('row_%s')";
        for (int i=1; i<=ROWS; i++) {
            String cmd = String.format(ins, tableName, i);
            System.out.println("  command: " + cmd);
            client.callProcedureSync("@AdHoc", cmd);
        }
        final String sel = "select count(*) from %s";
        String cmd = String.format(sel, tableName);
        System.out.println("  command: " + cmd);
        VoltTable vt = client.callProcedureSync("@AdHoc", cmd).getResults()[0];
        assertEquals("wrong row count", ROWS, vt.fetchRow(0).getLong(0));
    }

    @After
    public void teardown() throws Exception {
        if (cluster != null) {
            cluster.shutDown();
        }
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", testname.getMethodName());
    }

    volatile boolean goodResponse = false;
    volatile boolean threw = false;

    private void checkTimedOut(ClientResponse resp) {
        assertNotNull(resp);
        byte sts = resp.getStatus();
        System.out.printf("checkTimedOut: status %d %s\n", sts, resp.getStatusString());
        switch (sts) {
        case ClientResponse.CLIENT_REQUEST_TIMEOUT:
        case ClientResponse.CLIENT_RESPONSE_TIMEOUT:
            clientTmoCount.incrementAndGet();
            break;
        case ClientResponse.GRACEFUL_FAILURE:
            String msg = resp.getStatusString();
            assertTrue("Did not get expected timeout message", msg != null && msg.contains("timed out"));
            serverTmoCount.incrementAndGet();
            break;
        default:
            fail("Did not get expected timeout status");
            break;
        }
        goodResponse = true; // 'good' as in 'expected'
    }

    private Void screamAndShout(Throwable th) {
        assertNotNull(th);
        System.out.printf("Call completed exceptionally: %s\n", th);
        threw = true;
        return null;
    }

    AtomicInteger lateRespCount = new AtomicInteger();
    AtomicInteger lateRespFail = new AtomicInteger();
    AtomicInteger clientTmoCount = new AtomicInteger();
    AtomicInteger serverTmoCount = new AtomicInteger();

    private void lateResponse(ClientResponse resp, String host, int port) {
        byte sts = resp.getStatus();
        String msg = resp.getStatusString();
        int rtt = resp.getClusterRoundtrip(); // as measured by server; does not include client+network time
        System.out.printf("lateResponse: rtt %dms status %d %s\n", rtt, sts, msg);
        if (sts != ClientResponse.SUCCESS) {
            lateRespFail.incrementAndGet();
            assertEquals("Did not get expected late response status", ClientResponse.GRACEFUL_FAILURE, sts);
            assertTrue("Did not get expected late response error message", msg != null && msg.contains("timed out"));
        }
        lateRespCount.incrementAndGet();
    }

    private void waitForLateResp(int count, long timeout) {
        long start = System.currentTimeMillis();
        int lateCount;
        long delta;
        System.out.printf("Waiting for remaining %d late responses\n", count - lateRespCount.get());
        while ((lateCount = lateRespCount.get()) < count & (delta = System.currentTimeMillis() - start) < timeout) {
            Thread.yield();
        }
        System.out.printf("Late response count: %d (after %d ms)\n", lateCount, delta);
    }

    /**
     * Test operation of server-side timeout.
     * This is intrinsically tricky since calls are independently
     * timed out by client and server. The client will normally report
     * a timeout as soon as its periodic check detects timer runout.
     * The server detects timeout when it dequeues an invocation that
     * has been queued for too long.
     */
    @Test
    public void testSpProcTimeout() throws Exception {
        Client2Config config = new Client2Config()
            .procedureCallTimeout(1000, TimeUnit.MILLISECONDS)
            .lateResponseHandler(this::lateResponse);

        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost", port);

        // SPH=1 so all requests end up on the same site.
        // All requests should get client timeouts after 1 sec.
        // System dynamics may sometimes let us see the server
        // timeout before the client timeout, which we allow..
        final int CALLCOUNT = 4;
        final int EXECTIME = 4000;
        goodResponse = threw = false;
        CountDownLatch allUndone = new CountDownLatch(CALLCOUNT);
        for (int i=1; i<=CALLCOUNT; i++) {
            client.callProcedureAsync("ServerTimeoutTestProc", "row_"+i, EXECTIME)
                .thenAccept(this::checkTimedOut)
                .exceptionally(this::screamAndShout)
                .whenComplete((v,t) -> allUndone.countDown());
            Thread.sleep(10); // insert a little spacing
        }

        // All should complete 'soon' after the client times them out
        // after about a second, so the '5' here is arbitrary
        boolean finished = allUndone.await(5, TimeUnit.SECONDS);
        assertFalse("Threw exception", threw);
        assertTrue("Unfinished", finished);
        assertTrue("Ungood response", goodResponse);

        // First call should have executed for 4 secs, rest should
        // then have got server timeouts, seen as late responses.
        // Wait for up to the execution time plus a couple of
        // seconds leeway.
        int srvTmoNotLate = serverTmoCount.get();
        int expectedLate = CALLCOUNT - srvTmoNotLate;
        waitForLateResp(expectedLate, EXECTIME+2000);
        assertEquals("Wrong server-response count", CALLCOUNT, lateRespCount.get() + srvTmoNotLate);
        assertEquals("Wrong server-fail count", CALLCOUNT-1, lateRespFail.get() + srvTmoNotLate);

        client.close();
    }

}
