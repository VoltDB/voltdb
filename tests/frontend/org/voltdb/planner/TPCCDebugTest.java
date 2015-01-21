/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.planner;

import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;

public class TPCCDebugTest extends TestCase {
    private Client client;
    private ServerThread server;

    @Override
    public void setUp() throws IOException {
        CatalogBuilder cb = TPCCProjectBuilder.catalogBuilderNoProcs()
        .addProcedures(
                /*debugTPCCostat.class, debugTPCCpayment.class,*/
                /*debugTPCCdelivery.class, debugTPCCslev.class,*/
                debugUpdateProc.class)
        ;
        Configuration config = Configuration.compile(getClass().getSimpleName(), cb,
                new DeploymentBuilder());
        assertNotNull("Configuration failed to compile", config);
        // start VoltDB server
        server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        ClientConfig clientConfig = new ClientConfig("program", "none");
        client = ClientFactory.createClient(clientConfig);
        // connect
        client.createConnection("localhost");
    }

    public void waitUntilDone() throws InterruptedException {
        server.join();
    }

    @Override
    public void tearDown() throws InterruptedException {
        server.shutdown();
    }

    /*public void testOStatDebug() {
        try {
            VoltTable[] retvals = client.callProcedure("debugTPCCostat", 0L);
            assertTrue(retvals.length == 0);
        } catch (ProcCallException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    /*public void testPaymentDebug() {
        try {
            VoltTable[] retvals = client.callProcedure("debugTPCCpayment", 0L);
            assertTrue(retvals.length == 0);
        } catch (ProcCallException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    public void testUpdateDebug() throws IOException, ProcCallException {
        VoltTable[] retvals = client.callProcedure("debugUpdateProc", 0L).getResults();
        assertTrue(retvals.length == 0);
    }

    /*public void testDeliveryDebug() {
        try {
            VoltTable[] retvals = client.callProcedure("debugTPCCdelivery", 0L);
            assertTrue(retvals.length == 0);
        } catch (ProcCallException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    /*public void testSlevDebug() {
        try {
            VoltTable[] retvals = client.callProcedure("debugTPCCslev", 0L);
            assertTrue(retvals.length == 0);
        } catch (ProcCallException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
}
