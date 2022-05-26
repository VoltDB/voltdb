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

package org.voltdb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Ignore;
import org.junit.Test;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestRejoinFuzz extends RejoinTestBase {
    //
    // Load some stuff, kill some stuff, rejoin some stuff, update some stuff, rejoin some stuff, drain,
    // verify the updates occurred. Lather, rinse, and repeat.
    //
    @Test
    @Ignore("test for debugging")
    public void testRejoinFuzz() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);
        int processors = CoreUtils.availableProcessors();
        final int numHosts = processors >= 8 ? 10 : 5;
        final int kfactor = 4;
        final LocalCluster cluster =
            new LocalCluster(
                    "rejoin.jar",
                    4,
                    numHosts,
                    kfactor,
                    BackendTarget.NATIVE_EE_JNI,
                    LocalCluster.FailureState.ALL_RUNNING,
                    true);
        cluster.setMaxHeap(256);
        cluster.overrideAnyRequestForValgrind();

        final int numTuples = cluster.isValgrind() ? 1000 : 60000;
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        final ArrayList<Integer> serverValues = new ArrayList<Integer>();
        cluster.startUp();

        Client client = ClientFactory.createClient(m_cconfig);

        client.createConnection("localhost", cluster.port(0));

        final Random r = new Random();
        final Semaphore rateLimit = new Semaphore(25);
        for (int ii = 0; ii < numTuples; ii++) {
            rateLimit.acquire();
            int value = r.nextInt(numTuples);
            serverValues.add(value);
            client.callProcedure( new ProcedureCallback() {

                @Override
                public void clientCallback(ClientResponse clientResponse)
                        throws Exception {
                    if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                        System.err.println(clientResponse.getStatusString());
                        return;
                    }
                    if (clientResponse.getResults()[0].asScalarLong() != 1) {
                        System.err.println("Update didn't happen");
                        return;
                    }
                    rateLimit.release();
                }

            }, "InsertPartitioned", ii, value);
        }
        ArrayList<Integer> lastServerValues = new ArrayList<Integer>(serverValues);
        client.drain();
        client.close();
        Random forWhomTheBellTolls = new Random();
        int toConnectToTemp = 0;
        Thread lastRejoinThread = null;
        final java.util.concurrent.atomic.AtomicBoolean haveFailed = new AtomicBoolean(false);
        for (int zz = 0; zz < 5; zz++) {
            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost", cluster.port(toConnectToTemp));
            VoltTable results = client.callProcedure( "SelectPartitioned").getResults()[0];
            while (results.advanceRow()) {
                int key = (int)results.getLong(0);
                int value = (int)results.getLong(1);
                if (serverValues.get(key).intValue() != value) {
                    System.out.println(
                            "zz is " + zz + " and server value is " +
                            value + " and expected was " + serverValues.get(key).intValue() +
                            " and last time it was " + lastServerValues.get(key).intValue());
                }
                assertTrue(serverValues.get(key).intValue() == value);
            }
            client.close();
            if (lastRejoinThread != null) {
                lastRejoinThread.join();
            }

            final ArrayList<Integer> toKillFirst = new ArrayList<Integer>();
            final ArrayList<Integer> toKillDuringRecovery = new ArrayList<Integer>();
            while (toKillFirst.size() < kfactor / 2) {
                int candidate = forWhomTheBellTolls.nextInt(numHosts);
                if (!toKillFirst.contains(candidate)) {
                    toKillFirst.add(candidate);
                }
            }
            while (toKillDuringRecovery.size() < kfactor / 2) {
                int candidate = forWhomTheBellTolls.nextInt(numHosts);
                if (!toKillFirst.contains(candidate) && !toKillDuringRecovery.contains(candidate)) {
                    toKillDuringRecovery.add(candidate);
                }
            }
            System.out.println("Killing " + toKillFirst.toString() + toKillDuringRecovery.toString());

            toConnectToTemp = forWhomTheBellTolls.nextInt(numHosts);
            while (toKillFirst.contains(toConnectToTemp) || toKillDuringRecovery.contains(toConnectToTemp)) {
                toConnectToTemp = forWhomTheBellTolls.nextInt(numHosts);
            }
            final int toConnectTo = toConnectToTemp;

            Random sr = new Random();
            for (Integer uhoh : toKillFirst) {
                boolean shouldStop = (sr.nextInt(3) == 0);
                if (!shouldStop) {
                    cluster.killSingleHost(uhoh);
                } else {
                    System.out.println("Stopping with @StopNode: " + uhoh + " Connecting to: "
                            + toConnectTo + " Port is: " + cluster.port(toConnectTo));
                    Client kclient = ClientFactory.createClient(m_cconfig);
                    kclient.createConnection("localhost", cluster.port(toConnectTo));
                    kclient.callProcedure("@StopNode", uhoh);
                    kclient.close();
                    waitForHostToBeGone(uhoh, cluster);
                }
            }

            Thread recoveryThread = new Thread("Recovery thread") {
                @Override
                public void run() {
                    for (Integer dead : toKillFirst) {
                        int attempts = 0;
                        while (true) {
                            if (attempts == 6) {
                                haveFailed.set(true);
                                break;
                            }
                            if (cluster.recoverOne( dead, toConnectTo)) {
                                break;
                            }
                            attempts++;
                        }
                    }
                }
            };

            final java.util.concurrent.atomic.AtomicBoolean killerFail =
                new java.util.concurrent.atomic.AtomicBoolean(false);
            Thread killerThread = new Thread("Killer thread") {
                @Override
                public void run() {
                    try {
                        Random r = new Random();
                        Random sr = new Random();
                        for (Integer toKill : toKillDuringRecovery) {
                            Thread.sleep(r.nextInt(2000));
                            boolean shouldStop = (sr.nextInt(3) == 0);
                            if (!shouldStop) {
                                cluster.killSingleHost(toKill);
                            } else {
                                System.out.println("Stopping with @StopNode During recovery: "
                                        + toKill + " Connecting to: " + toConnectTo);
                                Client kclient = ClientFactory.createClient(m_cconfig);
                                kclient.createConnection("localhost", cluster.port(toConnectTo));
                                kclient.callProcedure("@StopNode", toKill);
                                waitForHostToBeGone(toKill, cluster);
                            }
                        }
                    } catch (Exception e) {
                        killerFail.set(true);
                        e.printStackTrace();
                    }
                }
            };

            client = ClientFactory.createClient(m_cconfig);
            final Client clientRef = client;
            System.out.println("Connecting to " + toConnectTo);
            client.createConnection("localhost", cluster.port(toConnectTo));
            lastServerValues = new ArrayList<Integer>(serverValues);
            final AtomicBoolean shouldContinue = new AtomicBoolean(true);
            final Thread loadThread = new Thread("Load thread") {
                @Override
                public void run() {
                    try {
                        while (shouldContinue.get()) {
                            for (int ii = 0; ii < numTuples && shouldContinue.get(); ii++) {
                                    rateLimit.acquire();
                                    final int updateKey = r.nextInt(numTuples);
                                    final int updateValue = r.nextInt(numTuples);
                                    serverValues.set(updateKey, updateValue);
                                    clientRef.callProcedure( new ProcedureCallback() {

                                        @Override
                                        public void clientCallback(ClientResponse clientResponse)
                                                throws Exception {
                                            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                                                System.err.println("Update failed for values ("
                                                    + updateKey + "," + updateValue +
                                                    ") " + clientResponse.getStatusString());
                                                return;
                                            }
                                            if (clientResponse.getResults()[0].asScalarLong() != 1) {
                                                System.err.println("Update row count error for values ("
                                                    + updateKey + "," + updateValue +
                                                    ") ");
                                            }
                                            rateLimit.release();
                                        }

                                    }, "UpdatePartitioned", updateValue, updateKey);

                            }
                        }
                        clientRef.drain();
                        clientRef.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            //
            // This version doesn't work. It causes concurrent failures during the rejoin sysproc
            // that aren't handled correctly
            //
            loadThread.start();
            killerThread.start();
            recoveryThread.start();

            recoveryThread.join();
            killerThread.join();

//            loadThread.start();
//            recoveryThread.start();
//            recoveryThread.join();
//            killerThread.start();
//            killerThread.join();

            if (killerFail.get()) {
                fail("Exception in killer thread");
            }

            shouldContinue.set(false);
            rateLimit.release();
            loadThread.join();

            lastRejoinThread = new Thread("Last rejoin thread") {
                @Override
                public void run() {
                    try {
                        for (Integer recover : toKillDuringRecovery) {
                            int attempts = 0;
                            while (true) {
                                if (attempts == 6) {
                                    haveFailed.set(true);
                                    break;
                                }
                                if (cluster.recoverOne( recover, toConnectTo)) {
                                    break;
                                }
                                attempts++;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            assertFalse(haveFailed.get());
            lastRejoinThread.start();
            System.out.println("Finished iteration " + zz);
        }
        lastRejoinThread.join();
        cluster.shutDown();
        assertFalse(haveFailed.get());
    }

    private Client getFullyConnectedClient(LocalCluster cluster) throws IOException {
        final List<String> listeners = cluster.getListenerAddresses();
        final Random r = new Random();
        final Client client = ClientFactory.createClient(m_cconfig);
        for (String listener : listeners) {
            // Use the port generated by LocalCluster if applicable
            try {
                client.createConnection(listener);
            } // retry once
            catch (ConnectException e) {
                listener = listeners.get(r.nextInt(listeners.size()));
                client.createConnection(listener);
            }
        }
        return client;
    }

    private void waitForHostToBeGone(int hid, LocalCluster cluster) {
        while (true) {
            Client client = null;
            VoltTable table = null;
            try {
                Thread.sleep(1000);
                client = getFullyConnectedClient(cluster);
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
                System.out.println("Host " + hid + " Gone");
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

}
