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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.voltcore.network.ReverseDNSCache;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;

/**
 * Validates that threads are terminated when Client2
 * instances are closed.
 */
public class TestClient2Close {

    static ServerThread localServer;

    static final String[] standardThreads = {
        // created by Client2Impl
        "Client2-Worker",
        "Client2-Exec",
        "Client2-Response",
        "Volt Client2 Network",
        // created by ClientFactory
        "Reverse DNS lookups",
        "Estimated Time Updater"
    };

    @Rule
    public final TestName testname = new TestName();

    @BeforeClass
    public static void prologue() {
        try {
            System.out.println("=-=-=-=-=-=-= Prologue =-=-=-=-=-=-=");
            ServerThread.resetUserTempDir();
            localServer = new ServerThread(new VoltDB.Configuration());
            localServer.start();
            localServer.waitForInitialization();
            ClientFactory.m_preserveResources = false;
            resetActive();
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

    private static void resetActive() {
        while (ClientFactory.m_activeClientCount > 0) {
            try {
                ClientFactory.decreaseClientNum();
            }
            catch (InterruptedException ex) {
            }
        }
        // The DNS cache is always initialized in the started state
        ReverseDNSCache.start();
    }

    @Before
    public void setup() {
        System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", testname.getMethodName());
    }

    @After
    public void teardown() {
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", testname.getMethodName());
    }

    /*
     * Creates two clients, closes one client, then executes a call
     * on the second client to make sure it is still viable.
     */
    @Test
    public void testClientIndependence() throws Exception {
        Client2Config config = new Client2Config();
        Client2 client1 = ClientFactory.createClient(config);
        Client2 client2 = ClientFactory.createClient(config);

        client1.connectSync("localhost");
        client2.connectSync("localhost");
        VoltTable configData1 = client1.callProcedureSync("@Ping").getResults()[0];
        client1.close();

        try {
            VoltTable configData2 = client2.callProcedureSync("@Ping").getResults()[0];
        } catch (Exception ex) {
            System.out.println("*** Exception: " + ex);
            fail("Something failed in callProcedure for client2 after closing client1.");
        }

        client2.close();
    }

    /*
     * Creates and closes one client, then checks all threads gone
     */
    @Test
    public void testThreadsKilledOnClose() throws Exception {
        Client2 client = ClientFactory.createClient(new Client2Config());
        client.connectSync("localhost");
        client.callProcedureSync("@Ping");
        assertTrue("expected threads missing", checkThreadsAllExist());

        client.close();
        assertTrue("some threads still running", checkThreadsAllDead(2000));
    }

    /*
     * Creates many clients, sequentially and in parallel, closes them
     * all, and ensures all threads are cleaned up.
     */
    @Test
    public void testAllClosed() throws Exception{
        createAndCloseClients(50, 3);
        assertTrue("some threads still running", checkThreadsAllDead(2000));
    }

    /*
     * Creates many clients, sequentially and in parallel, closes them
     * all except one, and ensures all necessary threads remain
     */
    @Test
    public void testAllButOneClosed() throws Exception {
        Client2 remainingClient = ClientFactory.createClient(new Client2Config());
        remainingClient.connectSync("localhost");
        remainingClient.callProcedureSync("@Ping");
        assertTrue("expected threads missing", checkThreadsAllExist());

        createAndCloseClients(50, 3);
        assertTrue("expected threads missing", checkThreadsAllExist());
        remainingClient.close();
    }

    /*
     * Creates many clients, sequentially and in parallel, closes them
     * all, then starts a new client, and ensures threads are created.
     */
    @Test
    public void testAllClosedThenStartOne() throws Exception {
        createAndCloseClients(50, 3);

        Client2 client = ClientFactory.createClient(new Client2Config());
        client.connectSync("localhost");
        client.callProcedureSync("@Ping");

        assertTrue("expected threads missing", checkThreadsAllExist());
        client.close();
    }

    /*
     * Check all expected threads are running.
     */
    private boolean checkThreadsAllExist() {
        boolean[] seen = new boolean[standardThreads.length];

        Map<Thread, StackTraceElement[]> stMap = Thread.getAllStackTraces();
        for (Thread t : stMap.keySet()) {
            int n = knownThread(t.getName());
            if (n >= 0) seen[n] = true;
        }

        System.out.println("checkThreadsAllExist:");
        boolean all = true ;
        for (int i=0; i<seen.length; i++) {
            all &= seen[i];
            System.out.printf("  %s  %s%n", standardThreads[i], seen[i] ? "exists" : "not found");
        }
        return all;
    }

    /*
     * Check running threads to ensure that there are no client
     * threads running.
     */
    private boolean checkThreadsAllDead(int totalTimeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        boolean[] alive = new boolean[standardThreads.length];

        Map<Thread, StackTraceElement[]> stMap = Thread.getAllStackTraces();
        for (Thread t : stMap.keySet()) {
            String name = t.getName();
            int n = knownThread(name);
            if (n >= 0) {
                long timeLeft = Math.max(10, totalTimeout - (System.currentTimeMillis() - start));
                System.out.printf("Joining %s (%dms timeout)%n", name, timeLeft);
                t.join(timeLeft);
                alive[n] |= t.isAlive();
            }
        }

        System.out.println("checkThreadsAllDead:");
        boolean any = false;
        for (int i=0; i<alive.length; i++) {
            System.out.printf("  %s  %s%n", standardThreads[i], alive[i] ? "still alive" : "not found");
            any |= alive[i];
        }
        return !any;
    }

    /*
     * Checks for a thread we're interested in.
     */
    private int knownThread(String name) {
        for (int i=0; i<standardThreads.length; i++) {
            if (name.startsWith(standardThreads[i])) {
                return i;
            }
        }
        return -1;
    }

    /*
     * Sequentially creates, connects, and immediately closes a specified
     * number of clients. And does that a specified number of times in parallel.
     */
    private void createAndCloseClients(int clientCount, int loops) throws Exception {
        CountDownLatch latch = new CountDownLatch(loops);
        for (int i=0; i<loops; i++) {
            new CreateAndCloseThread(clientCount, latch).start();
        }
        latch.await();
    }

    @Ignore
    private static class CreateAndCloseThread extends Thread {
        private int clientCount;
        private CountDownLatch latch;

        public CreateAndCloseThread(int clientCount, CountDownLatch latch) {
            this.clientCount = clientCount;
            this.latch = latch;
        }

        @Override
        public void run () {
            try {
                Client2Config config = new Client2Config();
                for (int i=0; i<clientCount; i++) {
                    Client2 client = ClientFactory.createClient(config);
                    client.connectSync("localhost");
                    client.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }
}
