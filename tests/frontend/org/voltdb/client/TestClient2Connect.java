/* This file is part of VoltDB.
 * Copyright (C) 2021-2022 VoltDB Inc.
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

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;

/**
 * Tests a few variations on the connect call
 */
public class TestClient2Connect {

    static ServerThread localServer;

    @Rule
    public final TestName testname = new TestName();

    @BeforeClass
    public static void prologue() {
        try {
            System.out.println("=-=-=-=-=-=-= Prologue =-=-=-=-=-=-=");
            localServer = new ServerThread(new VoltDB.Configuration());
            localServer.start();
            localServer.waitForInitialization();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @AfterClass
    public static void epilogue() throws Exception {
        System.out.println("=-=-=-=-=-=-= Epilogue =-=-=-=-=-=-=");
        localServer.shutdown();
        localServer.join();
        localServer = null;
    }

    @Before
    public void setup() {
        System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", testname.getMethodName());
    }

    @After
    public void teardown() throws Exception {
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", testname.getMethodName());
    }

    private volatile long startTime, elapsedTime;
    private volatile int errorCount;

    private void connectionUp(String host, int port) {
        long relTime = System.currentTimeMillis() - startTime;
        System.out.printf("%05d | Notification: connection up: %s port %d%n", relTime, host, port);
    }

    private void connectionDown(String host, int port) {
        long relTime = System.currentTimeMillis() - startTime;
        System.out.printf("%05d | Notification: connection down: %s port %d%n", relTime, host, port);
    }

    private void connectFailed(String host, int port) {
        errorCount++;
        long relTime = System.currentTimeMillis() - startTime;
        System.out.printf("%05d | Notification: connect failed: %s port %d%n", relTime, host, port);
    }

    private boolean connect(String servers, int timeout) {
        errorCount = 0;
        Client2Config config = new Client2Config()
            .connectionUpHandler(this::connectionUp)
            .connectionDownHandler(this::connectionDown)
            .connectFailureHandler(this::connectFailed);
        try (Client2 client = ClientFactory.createClient(config)) {
            int delay = 200;
            System.out.printf("Test case: servers %s, timeout %d, retry delay %d\n", servers, timeout, delay);
            startTime = System.currentTimeMillis();
            client.connectSync(servers, timeout, delay, TimeUnit.MILLISECONDS);
            elapsedTime = System.currentTimeMillis() - startTime;
            System.out.printf("%05d | Finally: connected\n", elapsedTime);
            return true;
        }
        catch (Exception ex) {
            elapsedTime = System.currentTimeMillis() - startTime;
            System.out.printf("%05d | Finally: failed: %s\n", elapsedTime, ex);
            return false;
        }
    }

    @Test
    public void testConnectSuccess() throws Exception {
        boolean b = connect("nxnode1:12345, localhost, nxnode2", 0);
        assertEquals("expected success", true, b);
        assertEquals("error count wrong", 1, errorCount);
    }

    @Test
    public void testConnectFail() throws Exception {
        boolean b = connect("nxnode1:12345, nxnode2", 0);
        assertEquals("expected failure", false, b);
        assertEquals("error count wrong", 2, errorCount);
    }

    @Test
    public void testConnectTimeout() throws Exception {
        // huge timeout values, junits run in the lab
        // fail with anything reasonable. i suppose the
        // machines are just stupidly overloaded.
        final int tmo = 6000, slop = 6000;
        boolean b = connect("nxnode1:12345, nxnode2", tmo);
        assertEquals("expected failure", false, b);
        assertTrue("error count low (no retries)", errorCount > 2);
        assertTrue("too short", elapsedTime > tmo);
        assertTrue("too long", elapsedTime < tmo+slop);
    }
}
