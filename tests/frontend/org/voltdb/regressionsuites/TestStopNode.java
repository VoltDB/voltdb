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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;


import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestStopNode extends RegressionSuite
{

    static LocalCluster m_config;
    static int kfactor = 3;

    public TestStopNode(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("");
        return builder;
    }

    class StopCallBack implements ProcedureCallback {
        final byte m_status;
        final int m_hid;
        final CountDownLatch m_cdl;

        public StopCallBack(CountDownLatch cdl, byte status, int hid) {
            m_status = status;
            m_hid = hid;
            m_cdl = cdl;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            m_cdl.countDown();
            System.out.println("Host " + m_hid
                    + " Result : " + clientResponse.getStatusString() + " Status: " + clientResponse.getStatus());

            assertEquals(m_status, clientResponse.getStatus());
        }

    }

    public void waitForHostToBeGone(int hid) {
        while (true) {
            Client client = null;
            VoltTable table = null;
            try {
                Thread.sleep(1000);
                client = getFullyConnectedClient();
                table = client.callProcedure("@SystemInformation", "overview").getResults()[0];
                client.close();
                client = null;
            } catch (Exception ex) {
                System.out.println("Failed to get SystemInformation overview: " + ex.getMessage());
                continue;
            }
            boolean done = true;
            while (table.advanceRow()) {
                long hc = table.getLong("HOST_ID");
                if (hc == hid) {
                    done = false;
                    break;
                }
            }
            System.out.println("Host " + hid + " Still there");
            if (done) {
                if (client != null) {
                    try {
                        client.close();
                    } catch (InterruptedException ex) {
                    }
                }
                return;
            }
        }
    }

    public void testStopNode() throws Exception {
        Client client = ClientFactory.createClient();

        client.createConnection("localhost", m_config.port(0));

        try {
            CountDownLatch cdl = new CountDownLatch(1);
            byte expectedResponse = (kfactor > 0) ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE;
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 4), "@StopNode", 4);
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                waitForHostToBeGone(4);
            }
            cdl = new CountDownLatch(1);
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 3), "@StopNode", 3);
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                waitForHostToBeGone(3);
            }
            cdl = new CountDownLatch(1);
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 2), "@StopNode", 2);
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                waitForHostToBeGone(2);
            }
            client.callProcedure("@SystemInformation", "overview");
        } catch (Exception ex) {
            //We should not get here
            fail();
            ex.printStackTrace();
        }
        boolean lostConnect = false;
        try {
            CountDownLatch cdl = new CountDownLatch(3);
            //Stop a node that should stay up
            client.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 1), "@StopNode", 1);
            //Stop already stopped node.
            client.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 4), "@StopNode", 4);
            //Stop a node that should stay up
            client.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 0), "@StopNode", 0);
            client.callProcedure("@SystemInformation", "overview");
            client.drain();
            cdl.await();
        } catch (Exception pce) {
            pce.printStackTrace();
            lostConnect = pce.getMessage().contains("was lost before a response was received");
        }
        //We should never lose contact.
        assertFalse(lostConnect);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestStopNode.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        boolean success;
        //Lets tolerate 3 node failures.
        if (!MiscUtils.isPro()) {
            kfactor = 0;
        }
        m_config = new LocalCluster("decimal-default.jar", 4, 5, kfactor, BackendTarget.NATIVE_EE_JNI);
        m_config.setHasLocalServer(true);
        success = m_config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(m_config);
        return builder;
    }
}

