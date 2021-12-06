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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.VoltFile;

import junit.framework.TestCase;

/**
 * These tests are for server-side timeouts, using
 * the Client2 API.
 */
public class TestServerTimeout extends TestCase {

    private LocalCluster cluster;
    private int port;

    @Override
    public void setUp() {
        Client2 client = null;
        try {
            System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", getName());

            VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
            File f = new File("/tmp/" + System.getProperty("user.name"));
            f.mkdirs();

            cluster = new LocalCluster("server-timeout.jar", 1/*sph*/, 1/*hosts*/, 0/*k-factor*/, BackendTarget.NATIVE_EE_JNI);
            cluster.overrideAnyRequestForValgrind();
            cluster.setHasLocalServer(true);

            VoltProjectBuilder project = new VoltProjectBuilder();
            project.setUseDDLSchema(true);
            project.addSchema(getClass().getResource("server-timeout.sql"));

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

    @Override
    public void tearDown() throws Exception {
        if (cluster != null) {
            cluster.shutDown();
        }
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", getName());
    }

    volatile boolean goodResponse = false;
    volatile boolean threw = false;
    volatile long endTime = 0;
    volatile long roundTrip = 0;
    volatile Throwable lastThrown = null;
    volatile int respStatus = 0;

    private void checkResponse(ClientResponse resp) {
        endTime = System.nanoTime();
        assertNotNull(resp);
        System.out.printf("checkResponse: status %d %s\n", resp.getStatus(), resp.getStatusString());
        assertEquals("Did not get expected success status", ClientResponse.SUCCESS, resp.getStatus());
        roundTrip = resp.getClientRoundtripNanos();
        respStatus = resp.getStatus();
        goodResponse = true;
    }

    private void checkTimedOut(ClientResponse resp) {
        endTime = System.nanoTime();
        assertNotNull(resp);
        byte sts = resp.getStatus();
        System.out.printf("checkTimedOut: status %d %s\n", sts, resp.getStatusString());
        assertTrue("Did not get expected timeout status", sts == ClientResponse.CLIENT_REQUEST_TIMEOUT || sts == ClientResponse.CLIENT_RESPONSE_TIMEOUT);
        roundTrip = resp.getClientRoundtripNanos();
        respStatus = sts;
        goodResponse = true; // 'good' as in 'expected'
    }

    private Void screamAndShout(Throwable th) {
        endTime = System.nanoTime();
        assertNotNull(th);
        System.out.printf("Call completed exceptionally: %s\n", th);
        threw = true;
        lastThrown = th;
        respStatus = -999; // unknowable
        roundTrip = -999; // unknowable
        return null;
    }

    AtomicInteger lateRespCount = new AtomicInteger();
    AtomicInteger lateRespFail = new AtomicInteger();

    private void lateResponse(ClientResponse resp, String host, int port) {
        lateRespCount.incrementAndGet();
        byte sts = resp.getStatus();
        String msg = resp.getStatusString();
        System.out.printf("lateResponse: status %d %s\n", sts, msg);
        if (sts != ClientResponse.SUCCESS) {
            lateRespFail.incrementAndGet();
            assertEquals("Did not get expected late response status", ClientResponse.GRACEFUL_FAILURE, sts);
            assertTrue("Did not get expected late response error message", msg != null && msg.contains("timed out"));
        }
    }

    private void waitForLateResp(int count, long timeout) {
        long start = System.currentTimeMillis();
        int lateCount;
        while ((lateCount = lateRespCount.get()) < count && System.currentTimeMillis() - start < timeout) {
            Thread.yield();
        }
        System.out.printf("Late response count: %d\n", lateCount);
    }

    /**
     * Test operation of server-side timeout
     */
    public void testSpProcTimeout() throws Exception {
        Client2Config config = new Client2Config()
            .procedureCallTimeout(1000, TimeUnit.MILLISECONDS)
            .lateResponseHandler(this::lateResponse);

        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost", port);

        // SPH=1 so all requests end up on the same site.
        // All requests should get client timeouts after 1 sec.
        final int CALLCOUNT = 4;
        goodResponse = threw = false;
        CountDownLatch allUndone = new CountDownLatch(CALLCOUNT);
        for (int i=1; i<=CALLCOUNT; i++) {
            client.callProcedureAsync("ServerTimeoutTestProc", "row_"+i, 2000)
                .thenAccept(this::checkTimedOut)
                .exceptionally(this::screamAndShout)
                .whenComplete((v,t) -> allUndone.countDown());
            Thread.sleep(10); // insert a little spacing
        }

        boolean finished = allUndone.await(5, TimeUnit.SECONDS);
        assertTrue("Unfinished", finished);
        assertTrue("Ungood response", goodResponse);
        assertFalse("Threw exception", threw);

        // First should have executed for 2 secs, rest should
        // have got server timeouts, seen as late responses.
        waitForLateResp(CALLCOUNT, 4000);
        assertEquals("Wrong late-response total count", CALLCOUNT, lateRespCount.get());
        assertEquals("Wrong late-response fail count", CALLCOUNT-1, lateRespFail.get());

        client.close();
    }

}
