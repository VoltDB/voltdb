/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

public class TestStopNode extends RegressionSuite
{

    static LocalCluster m_config;

    public TestStopNode(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("");
        return builder;
    }

    class StopCallBack implements ProcedureCallback {
        final String m_expected;
        final long m_hid;
        final CountDownLatch m_cdl;

        public StopCallBack(CountDownLatch cdl, String expected, long hid) {
            m_expected = expected;
            m_hid = hid;
            m_cdl = cdl;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            m_cdl.countDown();
            boolean foundExpected = false;
            String foundResult = "UNKNOWN";
            String emsg = "";
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                VoltTable tab = clientResponse.getResults()[0];
                while (tab.advanceRow()) {
                    String status = tab.getString("RESULT");
                    emsg = tab.getString("ERR_MSG");
                    long hid = tab.getLong("HOST_ID");
                    if (hid == m_hid) {
                        foundExpected = true;
                        foundResult = status;
                        System.out.println("Host " + m_hid
                                + " Matched Expected @StopNode Reslt of: " + m_expected + " ERR_MSG: " + emsg);
                    }
                }
            }
            if (!foundExpected) {
                System.out.println("Host " + m_hid
                        + " Failed Expected @StopNode Result of: " + foundResult + " ERR_MSG: " + emsg);
            }
            assertTrue(foundExpected);
        }

    }
    public void testStopNode() throws Exception {
        Client client = ClientFactory.createClient();

        client.createConnection("localhost", m_config.port(4));
        Thread.sleep(1000);

        try {
            CountDownLatch cdl = new CountDownLatch(3);
            client.callProcedure(new StopCallBack(cdl, "SUCCESS", 2), "@StopNode", 2);
            client.callProcedure("@SystemInformation", "overview");
            client.callProcedure(new StopCallBack(cdl, "SUCCESS", 1), "@StopNode", 1);
            client.callProcedure("@SystemInformation", "overview");
            client.callProcedure(new StopCallBack(cdl, "SUCCESS", 0), "@StopNode", 0);
            client.callProcedure("@SystemInformation", "overview");
            cdl.await();
        } catch (Exception ex) {
        }
        boolean lostConnect = false;
        try {
            CountDownLatch cdl = new CountDownLatch(3);
            //Stop a node that should stay up
            client.callProcedure(new StopCallBack(cdl, "FAILED", 3), "@StopNode", 3);
            //Stop already stopped node.
            client.callProcedure(new StopCallBack(cdl, "FAILED", 1), "@StopNode", 1);
            //Stop a node that should stay up
            client.callProcedure(new StopCallBack(cdl, "FAILED", 4), "@StopNode", 4);
            VoltTable tab = client.callProcedure("@SystemInformation", "overview").getResults()[0];
            cdl.await();
        } catch (Exception pce) {
            pce.printStackTrace();
            lostConnect = pce.getMessage().contains("was lost before a response was received");
        }
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
        m_config = new LocalCluster("decimal-default.jar", 4, 5, 3, BackendTarget.NATIVE_EE_JNI);
        m_config.setHasLocalServer(false);
        m_config.setExpectedToCrash(true);
        success = m_config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(m_config);
        return builder;
    }
}

