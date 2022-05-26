/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.FlakyTestRule;
import org.voltdb.FlakyTestRule.Flaky;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.iv2.MpInitiator;

import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.collect.Sets.SetView;

public class TestStopNode extends RegressionSuite
{
    @Rule
    public FlakyTestRule ftRule = new FlakyTestRule();

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

    public void waitForHostToBeGone(int hid, int[] survivorHostIds) {
        Set<Integer> hids = new HashSet<Integer>();
        hids.add(hid);
        waitForHostsToBeGone(hids, survivorHostIds);
    }

    public void waitForHostsToBeGone(Set<Integer> hids, int[] survivorHostIds) {
        while (true) {
            Client client = null;
            VoltTable table = null;
            try {
                Thread.sleep(1000);
                client = getClientToSubsetHosts(survivorHostIds);
                table = client.callProcedure("@SystemInformation", "overview").getResults()[0];
                client.close();
                client = null;
            } catch (Exception ex) {
                System.out.println("Failed to get SystemInformation overview: " + ex.getMessage());
                continue;
            }
            Set<Integer> liveHids = new HashSet<Integer>();
            while (table.advanceRow()) {
                liveHids.add((int)table.getLong("HOST_ID"));
            }
            SetView<Integer> intersect = Sets.intersection(liveHids, hids);
            if (!intersect.isEmpty()) {
                intersect.forEach((hid) -> {
                    if (hids.contains(hid)) System.out.println("Host " + hid + " Still there");
                });
            } else {
                return;
            }
        }
    }

    @Test
    @Flaky(description="TestStopNode.testStopNode")
    public void testStopNode() throws Exception {
        Client client = ClientFactory.createClient();

        client.createConnection("localhost", m_config.port(0));

        try {
            CountDownLatch cdl = new CountDownLatch(1);
            byte expectedResponse = (kfactor > 0) ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE;
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 4), "@StopNode", 4);
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                waitForHostToBeGone(4, new int[] {0, 1, 2, 3 });
            }
            cdl = new CountDownLatch(1);
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 3), "@StopNode", 3);
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                waitForHostToBeGone(3, new int[] {0, 1, 2 });
            }
            cdl = new CountDownLatch(1);
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 2), "@StopNode", 2);
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                waitForHostToBeGone(2, new int[] {0, 1 });
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

    @Test
    public void testStopThreeNodesSimultaneously() throws Exception {
        if (kfactor < 1) return;

        Client client = ClientFactory.createClient();

        client.createConnection("localhost", m_config.port(0));

        try {
            CountDownLatch cdl = new CountDownLatch(3);
            byte expectedResponse = (kfactor > 0) ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE;
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 4), "@StopNode", 4);
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 3), "@StopNode", 3);
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 2), "@StopNode", 2);
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                Set<Integer> hids = new HashSet<Integer>();
                hids.add(4);
                hids.add(3);
                hids.add(2);
                waitForHostsToBeGone(hids, new int[] {0, 1 });
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

    @Test
    public void testConcurrentStopNode() throws Exception {
        if (kfactor < 1) return;

        Client client1 = ClientFactory.createClient();
        Client client2 = ClientFactory.createClient();
        Client client3 = ClientFactory.createClient();

        client1.createConnection("localhost", m_config.port(0));
        client2.createConnection("localhost", m_config.port(1));
        client3.createConnection("localhost", m_config.port(2));

        try {
            CountDownLatch cdl = new CountDownLatch(3);
            byte expectedResponse = (kfactor > 0) ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE;
            client1.callProcedure(new StopCallBack(cdl, expectedResponse, 4), "@StopNode", 4);
            client2.callProcedure(new StopCallBack(cdl, expectedResponse, 3), "@StopNode", 3);
            client3.callProcedure(new StopCallBack(cdl, expectedResponse, 2), "@StopNode", 2);
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                Set<Integer> hids = new HashSet<Integer>();
                hids.add(4);
                hids.add(3);
                hids.add(2);
                waitForHostsToBeGone(hids, new int[] {0, 1 });
            }
            client1.callProcedure("@SystemInformation", "overview");
        } catch (Exception ex) {
            //We should not get here
            fail();
            ex.printStackTrace();
        }
        boolean lostConnect = false;
        try {
            CountDownLatch cdl = new CountDownLatch(3);
            //Stop a node that should stay up
            client1.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 1), "@StopNode", 1);
            //Stop already stopped node.
            client1.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 4), "@StopNode", 4);
            //Stop a node that should stay up
            client1.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 0), "@StopNode", 0);
            client1.callProcedure("@SystemInformation", "overview");
            client1.drain();
            client2.drain();
            client3.drain();
            cdl.await();
        } catch (Exception pce) {
            pce.printStackTrace();
            lostConnect = pce.getMessage().contains("was lost before a response was received");
        }
        //We should never lose contact.
        assertFalse(lostConnect);
    }

    @Test
    @Flaky(description="TestStopNode.testStopNodesMoreThanAllowed")
    public void testStopNodesMoreThanAllowed() throws Exception {
        if (kfactor < 1) return;

        Client client = ClientFactory.createClient();

        client.createConnection("localhost", m_config.port(3));

        try {
            CountDownLatch cdl = new CountDownLatch(4);
            byte expectedResponse = (kfactor > 0) ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE;
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 0), "@StopNode", 0);
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 1), "@StopNode", 1);
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 2), "@StopNode", 2);
            client.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 3), "@StopNode", 3);  /*should stay up*/
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                Set<Integer> hids = new HashSet<Integer>();
                hids.add(0);
                hids.add(1);
                hids.add(2);
                waitForHostsToBeGone(hids, new int[] {3, 4 });
            }
            client.callProcedure("@SystemInformation", "overview");
        } catch (Exception ex) {
            //We should not get here
            fail();
            ex.printStackTrace();
        }
        boolean lostConnect = false;
        try {
            CountDownLatch cdl = new CountDownLatch(2);
            //Stop a node that should stay up
            client.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 4), "@StopNode", 4);
            //Stop already stopped node.
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

    @Test
    public void testMixStopNodeWithNodeFailure() throws Exception {
        if (kfactor < 1) return;

        Client client = ClientFactory.createClient();

        client.createConnection("localhost", m_config.port(3));

        try {
            CountDownLatch cdl = new CountDownLatch(2);
            byte expectedResponse = (kfactor > 0) ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE;
            ((LocalCluster)getServerConfig()).killSingleHost(0);  // Create a node failure
            client.callProcedure(new StopCallBack(cdl, expectedResponse, 1), "@StopNode", 1);
            ((LocalCluster)getServerConfig()).killSingleHost(2);  // Create a node failure
            client.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 3), "@StopNode", 3);  /*should stay up*/
            cdl.await();
            if (expectedResponse == ClientResponse.SUCCESS) {
                Set<Integer> hids = new HashSet<Integer>();
                hids.add(0);
                hids.add(1);
                hids.add(2);
                waitForHostsToBeGone(hids, new int[] {3, 4 });
            }
            client.callProcedure("@SystemInformation", "overview");
        } catch (Exception ex) {
            //We should not get here
            fail();
            ex.printStackTrace();
        }
        boolean lostConnect = false;
        try {
            CountDownLatch cdl = new CountDownLatch(2);
            //Stop a node that should stay up
            client.callProcedure(new StopCallBack(cdl, ClientResponse.GRACEFUL_FAILURE, 4), "@StopNode", 4);
            //Stop already stopped node.
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

    @Test
    public void testPrepareStopNode() throws Exception {
        Client client = ClientFactory.createClient();
        client.createConnection("localhost", m_config.port(0));

        try {
            client.callProcedure("@PrepareStopNode", 1);
            final long maxSleep = TimeUnit.MINUTES.toMillis(5);
            int leaderCount = Integer.MAX_VALUE;
            long start = System.currentTimeMillis();
            while (leaderCount > 0) {
                leaderCount = 0;
                VoltTable vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
                while (vt.advanceRow()) {
                    long partition = vt.getLong("Partition");
                    if (MpInitiator.MP_INIT_PID == partition) { continue; }
                    String leader = vt.getString("Leader").split(":")[0];
                    if (leader.equals("1")) {
                        leaderCount++;
                    }
                }
                if (leaderCount > 0) {
                    if (maxSleep < (System.currentTimeMillis() - start)) {
                        break;
                    }
                    try { Thread.sleep(1000); } catch (Exception ignored) { }
                }
            }
            assert(leaderCount == 0);
        } catch (Exception ex) {
            //We should not get here
            ex.printStackTrace();
            fail();
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    static public junit.framework.Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestStopNode.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        boolean success;
        //Lets tolerate 3 node failures.
        m_config = new LocalCluster("teststopnode.jar", 4, 5, kfactor, BackendTarget.NATIVE_EE_JNI);
        m_config.setHasLocalServer(false);
        m_config.setDelayBetweenNodeStartup(1000);
        project.setPartitionDetectionEnabled(true);
        success = m_config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(m_config, MultiConfigSuiteBuilder.ReuseServer.NEVER);
        return builder;
    }
}

