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
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;

/**
 * Test handling of no-connections condition
 */
public class TestClient2NoConnections {

    ServerThread localServer;

    @Rule
    public final TestName testname = new TestName();

    @Before
    public void setup() {
        System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", testname.getMethodName());
        ServerThread.resetUserTempDir();
    }

    @After
    public void teardown() throws Exception {
        stopServer();
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", testname.getMethodName());
    }

    private void startServer() {
        try {
            localServer = new ServerThread(new VoltDB.Configuration());
            localServer.start();
            localServer.waitForInitialization();
        }
        catch (Exception e) {
            e.printStackTrace();
            localServer = null;
            fail();
        }
    }

    private void stopServer() {
        try {
            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
                localServer = null;
            }
        }
        catch (InterruptedException x) {
        }
    }

    /*
     * Track connection state.
     */

    final static int DOWN = 0, UP = 1, FAILED = 2;
    final static String states[] = { "down", "up", "failed" };

    AtomicInteger state = new AtomicInteger(DOWN);

    void connectionUp(String host, int port) {
        String prev = states[state.getAndSet(UP)];
        System.out.printf("Notification: connection up: %s port %d  (client was %s)%n", host, port, prev);
    }

    void connectionDown(String host, int port) {
        String prev = states[state.getAndSet(DOWN)];
        System.out.printf("Notification: connection down: %s port %d (client was %s)%n", host, port, prev);
    }

    void connectFailed(String host, int port) {
        String prev = states[state.getAndSet(FAILED)];
        System.out.printf("Notification: connect failed: %s port %d (client was %s)%n", host, port, prev);
    }

    void waitFor(int want, int tmo) {
        System.out.printf("Waiting for connection %s%n", states[want]);
        long t0 = System.currentTimeMillis();
        try {
            while (state.get() != want && System.currentTimeMillis() - t0 < tmo)
                Thread.sleep(250);
        }
        catch (InterruptedException x) {
            System.out.println("Interrupted in waitFor");
        }
        assertEquals("Timed out waiting", want, state.get());
    }

    /*
     * Issue procedure call, return coded result
     */
    int ping(Client2 client) {
        try {
            ClientResponse resp = client.callProcedureSync("@Ping");
            System.out.println("Got response from procedure call");
            return 0;
        }
        catch (NoConnectionsException ex) {
            System.out.println("Got NoConnectionsException");
            return 1;
        }
        catch (Exception ex) {
            System.out.println("Got unexpected exception: " + ex);
            return 2;
        }
    }

    /**
     * Verify 'no connection' exception when we do not
     * initially create any connections
     */
    @Test
    public void testNoConnect() throws Exception {
        state.set(DOWN);

        Client2Config config = new Client2Config()
            .connectionUpHandler(this::connectionUp)
            .connectionDownHandler(this::connectionDown)
            .connectFailureHandler(this::connectFailed);

        Client2 client = ClientFactory.createClient(config);

        System.out.println("Expecting NoConnectionsException");
        assertEquals("Did not fail in the expected way", 1, ping(client));
    }

    /**
     * Verify 'no connection' exception after all
     * connections gone.
     */
    @Test
    public void testDisconnected() throws Exception {
        System.out.println("Starting server");
        startServer();
        state.set(DOWN);

        Client2Config config = new Client2Config()
            .connectionUpHandler(this::connectionUp)
            .connectionDownHandler(this::connectionDown)
            .connectFailureHandler(this::connectFailed);

        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost");
        waitFor(UP, 10); // should be up already

        System.out.println("Expecting successful procedure call");
        assertEquals("Did not succeed", 0, ping(client));

        System.out.println("Shutting down server");
        stopServer();
        waitFor(DOWN, 5_000);

        System.out.println("Expecting NoConnectionsException");
        assertEquals("Did not fail in the expected way", 1, ping(client));
    }
}
