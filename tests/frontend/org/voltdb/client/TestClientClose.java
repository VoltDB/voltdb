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
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;

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
        ClientConfig config = new ClientConfig();
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");
        client.close();
        Thread.sleep(2000);
        Map<Thread, StackTraceElement[]> stMap = Thread.getAllStackTraces();
        for (Entry<Thread, StackTraceElement[]> e : stMap.entrySet()) {
            // skip the current thread
            Thread t = e.getKey();
            StackTraceElement[] st = e.getValue();
            if (t == Thread.currentThread()) {
                continue;
            }
            // check thread name and whether the thread should be close.
            String threadName = t.getName();
            if (threadName.contains("Reverse DNS lookups")
                    || threadName.contains("Estimated Time Updater")
                    || threadName.contains("Async Logger")
                    || threadName.contains("VoltDB Client Reaper Thread")) {
                System.out.println("threadName: " + threadName);
                for (StackTraceElement element : st) {
                    System.out.println("stack trace element: " + element);
                }
                fail("Something failed to clean up.");
            }
        }
    }

    public void testThreadsKilledOneOfClientClose() {
        ClientConfig config = new ClientConfig();
        Client client1 = ClientFactory.createClient(config);
        Client client2 = ClientFactory.createClient(config);
        try {
            client1.createConnection("localhost");
            client2.createConnection("localhost");
        } catch (UnknownHostException ue) {
            fail("Something failed in connecting to localhost, io exception");
        } catch (IOException ioe) {
            fail("Something failed in connecting to localhost, unknow exception");
        }
        //String m_procName = "@LoadSinglepartitionTable";
        try {
            VoltTable configData1 = client1.callProcedure("@SystemCatalog", "CONFIG").getResults()[0];
        } catch (IOException | ProcCallException e) {
            fail("Something failed in call procedure for client1.");
        }
        try {
            client1.close();
        } catch (InterruptedException e1) {
            fail("Something failed in closing client.");
        }
        Map<Thread, StackTraceElement[]> preStMap = Thread.getAllStackTraces();
        for (Thread t : preStMap.keySet()) {
            System.out.println("Thread After Close client1: " + t.getName());
        }
        try {
            VoltTable configData2 = client2.callProcedure("@SystemCatalog", "CONFIG").getResults()[0];
        } catch (IOException | ProcCallException e) {
            fail("Something failed in call procedure for client1.");
        }
    }
}
