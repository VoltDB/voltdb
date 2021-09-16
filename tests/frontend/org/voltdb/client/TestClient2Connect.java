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

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;

import junit.framework.TestCase;

/**
 * Tests a few variations on the connect call
 */
public class TestClient2Connect extends TestCase {

    ServerThread localServer;

    @Override
    public void setUp() {
        try {
            System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", getName());
            localServer = new ServerThread(new VoltDB.Configuration());
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

    public void testConnectSuccess() throws Exception {
        boolean b = connect("nxnode1:12345, localhost, nxnode2", 0);
        assertEquals("expected success", true, b);
        assertEquals("error count wrong", 1, errorCount);
    }

    public void testConnectFail() throws Exception {
        boolean b = connect("nxnode1:12345, nxnode2", 0);
        assertEquals("expected failure", false, b);
        assertEquals("error count wrong", 2, errorCount);
    }

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
