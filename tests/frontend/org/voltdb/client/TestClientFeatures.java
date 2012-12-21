/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestClientFeatures extends TestCase {

    static final String SCHEMA =
            "create table kv (" +
            "key bigint default 0 not null, " +
            "PRIMARY KEY(key));";

    ServerThread localServer;

    @Override
    public void setUp()
    {
        try {
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(SCHEMA);
            builder.addPartitionInfo("kv", "key");
            builder.addProcedures(ArbitraryDurationProc.class);

            boolean success = builder.compile(Configuration.getPathToCatalogForTest("timeouts.jar"), 1, 1, 0);
            assert(success);
            MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("timeouts.xml"));

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

    class CSL extends ClientStatusListenerExt {

        AtomicBoolean m_gotCall = new AtomicBoolean(false);

        @Override
        public synchronized void lateProcedureResponse(ClientResponse r, String hostname, int port) {
            m_gotCall.set(true);
        }

        public boolean waitForCall(long timeout) {
            long start = System.currentTimeMillis();
            long now = start;
            while ((m_gotCall.get() == false) && ((now - start) < timeout)) {
                Thread.yield();
                now = System.currentTimeMillis();
            }
            return m_gotCall.get();
        }
    }

    public void testPerCallTimeout() throws NoConnectionsException, IOException, ProcCallException {
        CSL csl = new CSL();

        ClientConfig config = new ClientConfig(null, null, csl);
        config.setProcedureCallTimeout(500);
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");

        ClientResponse response = client.callProcedure("ArbitraryDurationProc", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        try {
            client.callProcedure("ArbitraryDurationProc", 3000);
            fail();
        }
        catch (ProcCallException e) {
            assertTrue(e.getMessage().startsWith("No response received in the allotted time"));
        }
        // make sure the callback gets called
        assertTrue(csl.waitForCall(6000));
    }

    public void testMaxTimeout() throws NoConnectionsException, IOException, ProcCallException {
        CSL csl = new CSL();

        ClientConfig config = new ClientConfig(null, null, csl);
        config.setProcedureCallTimeout(0);
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");

        ClientResponse response = client.callProcedure("ArbitraryDurationProc", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        client.callProcedure("ArbitraryDurationProc", 3000);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }

    public void testGetAddressList() throws UnknownHostException, IOException, InterruptedException {
        CSL csl = new CSL();

        ClientConfig config = new ClientConfig(null, null, csl);
        config.setProcedureCallTimeout(0);
        Client client = ClientFactory.createClient(config);

        List<InetSocketAddress> addrs = client.getConnectedHostList();
        assertEquals(0, addrs.size());
        client.createConnection("localhost");
        addrs = client.getConnectedHostList();
        assertEquals(1, addrs.size());
        assertEquals(VoltDB.DEFAULT_PORT, addrs.get(0).getPort());
        client.close();
        addrs = client.getConnectedHostList();
        assertEquals(0, addrs.size());
    }
}
