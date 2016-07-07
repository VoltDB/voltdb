/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;

import junit.framework.TestCase;

public class TestClientClose extends TestCase {

    ServerThread localServer;
    DeploymentBuilder depBuilder;

    @Override
    public void setUp()
    {
        try {
            CatalogBuilder catBuilder = new CatalogBuilder();
            catBuilder.addSchema(getClass().getResource("clientfeatures.sql"));
            catBuilder.addProcedures(ArbitraryDurationProc.class);

            boolean success = catBuilder.compile(Configuration.getPathToCatalogForTest("timeouts.jar"));
            assert(success);

            depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.writeXML(Configuration.getPathToCatalogForTest("timeouts.xml"));

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("timeouts.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("timeouts.xml");
            localServer = new ServerThread(config);
            localServer.start();
            localServer.waitForInitialization();
            ClientFactory.ACTIVE_CLIENT_COUNT.set(0);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Override
    public void tearDown() throws Exception {
        localServer.shutdown();
    }

    public void testThreadsKilledClientClose() throws Exception {
        int preNumClientReaper = 0;
        Map<Thread, StackTraceElement[]> preStMap = Thread.getAllStackTraces();
        for (Thread t : preStMap.keySet()) {
            if (t.getName().contains("VoltDB Client Reaper Thread")) {
                preNumClientReaper++;
            }
        }
        ClientConfig config = new ClientConfig();
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");
        client.close();
        ClientFactory.decreaseClientNum();
        Thread.sleep(2000);
        Map<Thread, StackTraceElement[]> stMap = Thread.getAllStackTraces();
        int postNumClientReaper = 0;
        for (Entry<Thread, StackTraceElement[]> e : stMap.entrySet()) {
            // skip the current thread
            Thread t = e.getKey();
            if (t == Thread.currentThread()) {
                continue;
            }
            // check thread name and whether the thread should be close.
            String threadName = t.getName();
            if (threadName.contains("VoltDB Client Reaper Thread")) {
                postNumClientReaper++;
            }
            assertFalse(threadName.contains("Reverse DNS lookups") || threadName.contains("Async Logger")
                    || threadName.contains("Estimated Time Updater"));
        }
        assertEquals(preNumClientReaper, postNumClientReaper);
    }

    public void testThreadsKilledOneOfClientClose() throws Exception {
        ClientConfig config = new ClientConfig();
        Client client1 = ClientFactory.createClient(config);
        Client client2 = ClientFactory.createClient(config);

        try {
            client1.createConnection("localhost");
            client2.createConnection("localhost");
            VoltTable configData1 = client1.callProcedure("@SystemCatalog", "CONFIG").getResults()[0];
            client1.close();
            VoltTable configData2 = client2.callProcedure("@SystemCatalog", "CONFIG").getResults()[0];
        } catch (IOException | ProcCallException e) {
            fail("Something failed in call procedure for a client after close another one.");
        } finally {
            client2.close();
        }
    }

    public void testCreateCloseAllClientInParallel() throws Exception{
        clientCreateCloseAll(50, 3);
        Thread.sleep(500);
        Map<Thread, StackTraceElement[]> stMap = Thread.getAllStackTraces();
        for (Entry<Thread, StackTraceElement[]> e : stMap.entrySet()) {
            // skip the current thread
            Thread t = e.getKey();
            if (t == Thread.currentThread()) {
                continue;
            }
            // check thread name and whether the thread should be close.
            String threadName = t.getName();
            assertFalse(threadName.contains("Reverse DNS lookups") || threadName.contains("Async Logger")
                    || threadName.contains("Estimated Time Updater"));
        }
    }

    public void testCreateCloseInParallelRemainOne() throws Exception {
        Client remainClient = ClientFactory.createClient();
        remainClient.createConnection("localhost");
        clientCreateCloseAll(50, 3);
        Thread.sleep(500);
        assertTrue(checkThreadsAllExist());
        remainClient.close();
    }

    public void testCreateCloseInParallelStartOne() throws Exception {
        clientCreateCloseAll(50, 3);
        Client client = ClientFactory.createClient();
        client.createConnection("localhost");
        Thread.sleep(500);
        assertTrue(checkThreadsAllExist());
        client.close();
    }

    private void clientCreateCloseAll(int clientNum, int loops) throws Exception {
        CountDownLatch latch = new CountDownLatch(loops);
        for (int i = 0; i < loops; i++) {
            (new clientCreateCloseAllLauncher(clientNum, latch, i)).start();
        }
        latch.await();
    }

    private boolean checkThreadsAllExist() {
        boolean haveReverseDNSLookups = false;
        boolean haveAsyncLogger = false;
        boolean haveEstTimeUpdater = false;
        boolean haveClientReaper = false;
        Map<Thread, StackTraceElement[]> stMap = Thread.getAllStackTraces();
        for (Entry<Thread, StackTraceElement[]> e : stMap.entrySet()) {
            // skip the current thread
            Thread t = e.getKey();
            if (t == Thread.currentThread()) {
                continue;
            }
            // check thread name and whether the thread should be close.
            String threadName = t.getName();
            if (threadName.contains("Reverse DNS lookups")) {
                haveReverseDNSLookups = true;
            } else if (threadName.contains("Async Logger")) {
                haveAsyncLogger = true;
            } else if (threadName.contains("Estimated Time Updater")) {
                haveEstTimeUpdater = true;
            } else if (threadName.contains("VoltDB Client Reaper Thread")) {
                haveClientReaper = true;
            }
        }
        return haveReverseDNSLookups && haveAsyncLogger && haveEstTimeUpdater && haveClientReaper;
    }

    class clientCreateCloseAllLauncher extends Thread{
        private final int m_clientNum;
        private final CountDownLatch m_latch;

        public clientCreateCloseAllLauncher (int clientNum, CountDownLatch latch, int loopNum) {
            m_clientNum = clientNum;
            m_latch = latch;
        }

        @Override
        public void run () {
            try {
                for (int i = 0; i < m_clientNum; i++) {
                    Client client = ClientFactory.createClient();
                    client.createConnection("localhost");
                    client.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                m_latch.countDown();
            }
        }
    }
}
