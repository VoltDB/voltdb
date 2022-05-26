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

import java.util.ArrayList;
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

public class TestRejoinFuzz2 extends RejoinTestBase {

    //
    // Load a whole lot of stuff, kill some stuff, rejoin some stuff, and then kill some more stuff during the rejoin
    // This test doesn't validate data because it would be too slow. It has uncovered some bugs that occur
    // when a failure is concurrent with the recovery processors work. It is quite slow due to the loading,
    // but it is worth having.
    //
    @Test
    @Ignore("test for debugging")
    public void testRejoinFuzz2ElectricBoogaloo() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);
        int processors = CoreUtils.availableProcessors();
        final int numHosts = processors >= 8 ? 6 : 3;
        final int numTuples = 204800 * (processors >= 8 ? 6 : 1);//about 100 megs per
        //final int numTuples = 0;
        final int kfactor = 2;
        final LocalCluster cluster =
            new LocalCluster(
                    "rejoin.jar",
                    1,
                    numHosts,
                    kfactor,
                    BackendTarget.NATIVE_EE_JNI,
                    LocalCluster.FailureState.ALL_RUNNING,
                    false, null); // doesnt run with IV2 yet -- note from jh: what?
        cluster.setMaxHeap(256);
        if (LocalCluster.isMemcheckDefined()) {
            //Way to much data in this test. Using less data makes it redundant
            return;
        }
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        Client client = ClientFactory.createClient(m_cconfig);

        client.createConnection("localhost", cluster.port(0));

        Random r = new Random();
        StringBuilder sb = new StringBuilder(512);
        for (int ii = 0; ii < 512; ii++) {
            sb.append((char)(34 + r.nextInt(90)));
        }
        String theString = sb.toString();

        final Semaphore rateLimit = new Semaphore(1000);
        for (int ii = 0; ii < numTuples; ii++) {
            rateLimit.acquire();
            int value = r.nextInt(numTuples);
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

            }, "InsertPartitionedLarge", ii, value, theString);
        }
        client.drain();
        client.close();
        final java.util.concurrent.atomic.AtomicBoolean haveFailed = new AtomicBoolean(false);
        Random forWhomTheBellTolls = new Random();
        for (int zz = 0; zz < 5; zz++) {
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

            int toConnectToTemp = forWhomTheBellTolls.nextInt(numHosts);
            while (toKillFirst.contains(toConnectToTemp) || toKillDuringRecovery.contains(toConnectToTemp)) {
                toConnectToTemp = forWhomTheBellTolls.nextInt(numHosts);
            }
            final int toConnectTo = toConnectToTemp;

            for (Integer uhoh : toKillFirst) {
                cluster.killSingleHost(uhoh);
            }

            Thread recoveryThread = new Thread() {
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
            Thread killerThread = new Thread() {
                @Override
                public void run() {
                    try {
                        Random r = new Random();
                        for (Integer toKill : toKillDuringRecovery) {
                                Thread.sleep(r.nextInt(5000));
                            cluster.killSingleHost(toKill);
                        }
                    } catch (Exception e) {
                        killerFail.set(true);
                        e.printStackTrace();
                    }
                }
            };

            recoveryThread.start();
            killerThread.start();
            recoveryThread.join();
            killerThread.join();

            if (killerFail.get()) {
                fail("Exception in killer thread");
            }

            for (Integer recoverNow : toKillDuringRecovery) {
                int attempts = 0;
                while (true) {
                    if (attempts == 6) {
                        haveFailed.set(true);
                        break;
                    }
                    if (cluster.recoverOne( recoverNow, toConnectTo)) {
                        break;
                    }
                    attempts++;
                }
            }
            System.out.println("Finished iteration " + zz);
        }
        cluster.shutDown();
        assertFalse(haveFailed.get());
    }
}
