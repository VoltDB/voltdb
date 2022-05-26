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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

/**
 * This whole thing is riddled with arbitrary timeouts
 * and wild guesses as to how long it takes for some
 * unobservable server change to occur. Sorry.
 */
public class TestClient2AutoConnect {

    ServerThread localServer;
    LocalCluster cluster;

    @Rule
    public final TestName testname = new TestName();

    @Before
    public void setup() {
        System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", testname.getMethodName());
        ServerThread.resetUserTempDir();
    }

    @After
    public void teardown() {
        stopServer();
        stopCluster();
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

    private int nsites = 4, nhosts = 3, kfactor = 0;

    private void startCluster() {
        try {
            cluster = new LocalCluster("client-all-partitions.jar", nsites, nhosts, kfactor, BackendTarget.NATIVE_EE_JNI);
            cluster.overrideAnyRequestForValgrind();
            cluster.setHasLocalServer(false);

            VoltProjectBuilder project = new VoltProjectBuilder();
            project.setUseDDLSchema(true);
            project.addSchema(getClass().getResource("allpartitioncall.sql"));

            boolean success = cluster.compile(project);
            assertTrue(success);
            cluster.startUp();
        }
        catch (Exception e) {
            e.printStackTrace();
            cluster = null;
            fail();
        }
    }

    private void stopCluster() {
        try {
            if (cluster != null) {
                cluster.shutDown();
                cluster = null;
            }
        }
        catch (InterruptedException x) {
        }
    }

    /*
     * Track connection state (one per client, not
     * one per host, so it's basically wrong most of
     * the time in a multi-host test).
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
        System.out.printf("Took %d msec to get %s%n", System.currentTimeMillis() - t0, states[want]);
    }

    void printConn(Client2 client) {
        System.out.println("Connected hosts:");
        List<InetSocketAddress> list = client.connectedHosts();
        for (InetSocketAddress sa : list) {
            System.out.printf("  %s%n", sa);
        }
        if (list.isEmpty()) {
            System.out.println("  None");
        }
    }

    /**
     * Verify a client can reconnect automatically to a cluster that has
     * been restarted; this tests both the "reconnect with no existing
     * connections" case and the cluster id-change case.
     */
    @Test
    public void testAutoReconnect() throws Exception {
        startServer();
        state.set(DOWN);

        Client2Config config = new Client2Config()
            .connectionUpHandler(this::connectionUp)
            .connectionDownHandler(this::connectionDown)
            .connectFailureHandler(this::connectFailed);

        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost");
        waitFor(UP, 10); // should be up already
        printConn(client);

        // give client time to finish initial topo queries
        Thread.sleep(2_000);

        System.out.println("Shutting down server");
        stopServer();
        waitFor(DOWN, 5_000);

        // give it chance to fail to connect before restarting
        // current defaults are 5 secs delay to first reconnect attempt.
        // 20 secs between retries.
        System.out.println("Biding our time");
        Thread.sleep(5_000);

        System.out.println("Restarting server");
        startServer();

        // note, no explicit connect call here.

        waitFor(UP, 60_000);
        printConn(client);
    }

    /**
     * Verify a client can discover cluster topology.
     */
    @Test
    public void testTopoDiscovery() throws Exception {
        startCluster();
        state.set(DOWN);

        Client2Config config = new Client2Config()
            .connectionUpHandler(this::connectionUp)
            .connectionDownHandler(this::connectionDown)
            .connectFailureHandler(this::connectFailed);

        int port0 = 21312;
        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost", port0);
        waitFor(UP, 10); // should be up already
        printConn(client);

        // give client time to finish initial topo queries
        Thread.sleep(2_000);
        printConn(client);
        assertEquals(nhosts, client.connectedHosts().size());

        System.out.println("Shutting down server");
        stopCluster();
        waitFor(DOWN, 5_000); // DOWN is (wrongly) set when first connection goes down
        printConn(client);
        assertEquals(0, client.connectedHosts().size());
    }
}
