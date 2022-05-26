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

package org.voltdb.rejoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.RejoinTestBase;
import org.voltdb.ServerThread;
import org.voltdb.StartAction;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.iv2.KSafetyStats.KSafety;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

/**
 * Currently, all the tests in this test suite only include single partition
 * workload.
 */
public class TestPauselessRejoinEndToEnd extends RejoinTestBase {
    final int FAIL_NO_OPEN_SOCKET = 0;
    final int FAIL_TIMEOUT_ON_SOCKET = 1;
    final int FAIL_SKEW = 2;
    final int DONT_FAIL = 3;

    private LocalCluster cluster = null;
    private ServerThread localServer = null;

    @Before
    public void setUp() throws Exception {
        cluster = null;
        localServer = null;
    }

    @After
    public void tearDown() throws Exception {
        if (localServer != null) {
            localServer.shutdown();
        }
        if (cluster != null) {
            cluster.shutDown();
        }
    }

    @Test
    public void testRejoin() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 5, 2, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.setMaxHeap(1300);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        response = client.callProcedure("InsertSinglePartition", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        cluster.killSingleHost(0);
        Thread.sleep(1000);

        VoltTable ksafetyProbe = client.callProcedure("@Statistics", "KSAFETY", 0).getResults()[0];
        assertNotNull(ksafetyProbe);
        long missingReplicaSum = 0;
        while (ksafetyProbe.advanceRow()) {
            missingReplicaSum += ksafetyProbe.getLong(KSafety.MISSING_REPLICA.name());
        }
        assertTrue(missingReplicaSum > 0);

        client.close();

        VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
        config.m_startAction = StartAction.PROBE;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_leader = ":" + cluster.internalPort(1);
        config.m_coordinators = cluster.coordinators(1);
        config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
        config.m_forceVoltdbCreate = false;
        config.m_hostCount = 2;
        cluster.setPortsFromConfig(0, config);
        localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForRejoin();

        Thread.sleep(5000);

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        ksafetyProbe = client.callProcedure("@Statistics", "KSAFETY", 0).getResults()[0];
        assertNotNull(ksafetyProbe);
        missingReplicaSum = 0;
        while (ksafetyProbe.advanceRow()) {
            missingReplicaSum += ksafetyProbe.getLong(KSafety.MISSING_REPLICA.name());
        }
        assertEquals(0L,missingReplicaSum);

        response = client.callProcedure("InsertSinglePartition", 5);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));
        response = client.callProcedure("InsertSinglePartition", 8);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();
    }

    @Test
    public void testRejoinDataTransfer() throws Exception {
        System.out.println("testRejoinDataTransfer");
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);

        cluster = new LocalCluster("rejoin.jar", 4, 4, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.setMaxHeap(1300);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        response = client.callProcedure("InsertSinglePartition", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        cluster.killSingleHost(0);
        Thread.sleep(1000);

        VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
        config.m_startAction = StartAction.PROBE;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_leader = ":" + cluster.internalPort(1);
        config.m_coordinators = cluster.coordinators(1);
        config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
        config.m_forceVoltdbCreate = false;
        config.m_hostCount = 4;

        cluster.setPortsFromConfig(0, config);
        localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForRejoin();

        Thread.sleep(2000);

        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));

        //
        // Check that the recovery data transferred
        //
        response = client.callProcedure("SelectBlahSinglePartition", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 0);

        //
        //  Try to insert new data
        //
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        //
        // See that it was inserted
        //
        response = client.callProcedure("SelectBlahSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 2);

        //
        // Kill one of the old ones (not the recovered partition)
        //
        cluster.killSingleHost(1);
        Thread.sleep(1000);

        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        //
        // See that the cluster is available and the data is still there.
        //
        response = client.callProcedure("SelectBlahSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 2);

        client.close();
    }

    @Test
    public void testRejoinWithMultipartLoad() throws Exception {

        System.out.println("testRejoinWithMultipartLoad");
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);


        cluster = new LocalCluster("rejoin.jar", 2, 2, 1,
                BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING,
                false, null);
        cluster.setMaxHeap(1300);
        cluster.overrideAnyRequestForValgrind();
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));

        response = client.callProcedure("InsertSinglePartition", 33);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 34);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        cluster.killSingleHost(0);
        Thread.sleep(1000);

        final Client clientForLoadThread = client;
        final java.util.concurrent.atomic.AtomicBoolean shouldContinue =
            new java.util.concurrent.atomic.AtomicBoolean(true);
        Thread loadThread = new Thread("Load Thread") {
            @Override
            public void run() {
                try {
                    final long startTime = System.currentTimeMillis();
                    while (shouldContinue.get()) {
                        try {
                            clientForLoadThread.callProcedure(new org.voltdb.client.ProcedureCallback(){

                                @Override
                                public void clientCallback(
                                        ClientResponse clientResponse)
                                throws Exception {
                                    if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                                        //                       System.err.println(clientResponse.getStatusString());
                                    }
                                }

                            }, "@Statistics", "MANAGEMENT", 1);
                            //clientForLoadThread.callProcedure("@Statistics", );
                            Thread.sleep(1);
                            final long now = System.currentTimeMillis();
                            if (now - startTime > 1000 * 10) {
                                break;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                } finally {
                    try {
                        clientForLoadThread.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        loadThread.start();

        Thread.sleep(2000);

        localServer = null;
        try {
            VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
            config.m_startAction = StartAction.PROBE;
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
            config.m_leader = ":" + cluster.internalPort(1);
            config.m_coordinators = cluster.coordinators(1);
            config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
            config.m_forceVoltdbCreate = false;
            config.m_hostCount = 2;
            cluster.setPortsFromConfig(0, config);
            localServer = new ServerThread(config);

            localServer.start();
            localServer.waitForRejoin();

            Thread.sleep(2000);

            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost", cluster.port(1));

            //
            // Check that the recovery data transferred
            //
            response = client.callProcedure("SelectBlahSinglePartition", 33);
            assertEquals(ClientResponse.SUCCESS, response.getStatus());
            assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 33);

        } finally {
            shouldContinue.set(false);
        }

        response = client.callProcedure("SelectBlah", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 1);

        response = client.callProcedure("SelectBlahReplicated", 34);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 34);

        //
        //  Try to insert new data
        //
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        //
        // See that it was inserted
        //
        response = client.callProcedure("SelectBlahSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 2
        );
        response = client.callProcedure("SelectBlah", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 3);

        response = client.callProcedure("SelectBlahReplicated", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 1);

        //
        // Kill one of the old ones (not the recovered partition)
        //
        cluster.killSingleHost(1);
        Thread.sleep(1000);

        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        //
        // See that the cluster is available and the data is still there.
        //
        response = client.callProcedure("SelectBlahSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 2);

        response = client.callProcedure("SelectBlah", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 3);

        response = client.callProcedure("SelectBlahReplicated", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 1);

        client.close();
    }

    public void testRejoinWithMultipartUpdateFirehoseWorkload() throws Exception {

        final AtomicLong mpTxnsRun = new AtomicLong(0);
        final AtomicLong spTxnsRun = new AtomicLong(0);
        final AtomicLong adhocTxnsRun = new AtomicLong(0);

        Client client = null;
        Client clientForLoadThread2 = null;
        Thread loadThread = null;
        Thread adHocThread = null;

        final AtomicBoolean shouldContinue = new AtomicBoolean(true);
        final AtomicBoolean loadThreadHasFailed = new AtomicBoolean(false);
        final AtomicBoolean adhocThreadHasFailed = new AtomicBoolean(false);

        final int PKEYS = 10;

        try {
            System.out.println("testRejoinWithMultipartUpdateFirehoseWorkload");
            VoltProjectBuilder builder = getBuilderForTest();
            builder.addProcedures(new ProcedureInfo(IncrementProc.class, null, new String[] { "foo" }));
            builder.addProcedures(new ProcedureInfo(IncrementProcSP.class,
                    new ProcedurePartitionData("PARTITIONED", "PKEY"), new String[] { "foo" }));
            builder.setSecurityEnabled(false, true);

            cluster = new LocalCluster("rejoin.jar", 4, 3, 1,
                    BackendTarget.NATIVE_EE_JNI,
                    LocalCluster.FailureState.ALL_RUNNING,
                    false, null);
            cluster.setMaxHeap(1300);
            cluster.overrideAnyRequestForValgrind();
            boolean success = cluster.compile(builder);
            assertTrue(success);
            MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
            cluster.setHasLocalServer(false);

            cluster.startUp();

            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost", cluster.port(0));

            // prime the checking thing
            client.callProcedure("BLAH_REPLICATED.insert", 0);
            for (int i = 0; i < PKEYS; i++) {
                client.callProcedure("PARTITIONED.insert", i, 0);
            }

            // load a modicum of data
            String data = "x";
            for (int i = 0; i < 9; i++) {
                data = data + data;
            }
            assert(data.length() == 512);
            for (int i = 0; i < 100000; i++) {
                client.callProcedure(new ProcedureCallback() {
                    @Override
                    public void clientCallback(ClientResponse clientResponse) throws Exception {
                        if (clientResponse.getStatus() != ClientResponse.SUCCESS &&
                            clientResponse.getStatus() != ClientResponse.RESPONSE_UNKNOWN) {
                            ClientResponseImpl cri = (ClientResponseImpl) clientResponse;
                            System.err.println(cri.toJSONString());
                            fail();
                        }
                    }
                }, "PARTITIONED_LARGE.insert", i, i, data);
            }

            client.drain();

            cluster.killSingleHost(1);
            Thread.sleep(1000);

            m_cconfig.setMaxOutstandingTxns(100);
            clientForLoadThread2 = ClientFactory.createClient(m_cconfig);
            clientForLoadThread2.createConnection("localhost", cluster.port(0));
            final Client clientForLoadThread = clientForLoadThread2;
            loadThread = new Thread("Load Thread") {
                @Override
                public void run() {
                    try {
                        Random r = new Random();

                        while (shouldContinue.get()) {
                            Thread.sleep(10);
                            clientForLoadThread.callProcedure(new org.voltdb.client.ProcedureCallback() {
                                @Override
                                public void clientCallback(ClientResponse clientResponse) throws Exception {
                                    if (clientResponse.getStatus() != ClientResponse.SUCCESS &&
                                        clientResponse.getStatus() != ClientResponse.RESPONSE_UNKNOWN) {
                                        if (clientResponse.getAppStatus() != IncrementProc.USER_ABORT) {
                                            ClientResponseImpl cri = (ClientResponseImpl) clientResponse;
                                            System.err.println(cri.toJSONString());
                                            loadThreadHasFailed.set(true);
                                            shouldContinue.set(false);
                                        }
                                    }
                                    mpTxnsRun.incrementAndGet();
                                }
                            }, IncrementProc.class.getSimpleName());

                            for (int i = 0; i < 5; i++) {
                                clientForLoadThread.callProcedure(new org.voltdb.client.ProcedureCallback() {
                                    @Override
                                    public void clientCallback(ClientResponse clientResponse) throws Exception {
                                        if (clientResponse.getStatus() != ClientResponse.SUCCESS &&
                                            clientResponse.getStatus() != ClientResponse.RESPONSE_UNKNOWN) {
                                            if (clientResponse.getAppStatus() != IncrementProc.USER_ABORT) {
                                                ClientResponseImpl cri = (ClientResponseImpl) clientResponse;
                                                System.err.println(cri.toJSONString());
                                                loadThreadHasFailed.set(true);
                                                shouldContinue.set(false);
                                            }
                                        }
                                        spTxnsRun.incrementAndGet();
                                    }
                                }, IncrementProcSP.class.getSimpleName(), r.nextInt(PKEYS));
                            }
                        }
                        clientForLoadThread.drain();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        loadThreadHasFailed.set(true);
                    }
                }
            };
            loadThread.start();

            adHocThread = new Thread("AdHoc Load Thread") {
                @Override
                public void run() {
                    try {
                        while (shouldContinue.get()) {
                            Thread.sleep(10);
                            try {
                                clientForLoadThread.callProcedure("@AdHoc",
                                                                  "update blah_replicated set ival = ival + 1; " +
                                                                  "select ival from blah_replicated order by ival limit 1");
                            } catch (ProcCallException e) {
                                if (e.getClientResponse().getStatus() == ClientResponse.RESPONSE_UNKNOWN) {
                                    // RESPONSE_UNKNOWN is used when an in-flight txn is
                                    // dropped due to change of mastership. This is expected
                                    // during rejoin.
                                } else {
                                    throw e;
                                }
                            }
                            adhocTxnsRun.incrementAndGet();
                        }
                        clientForLoadThread.drain();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        adhocThreadHasFailed.set(true);
                    }
                }
            };
            adHocThread.start();

            assertFalse(loadThreadHasFailed.get());
            assertFalse(adhocThreadHasFailed.get());

            Thread.sleep(2000);

            assertFalse(loadThreadHasFailed.get());
            assertFalse(adhocThreadHasFailed.get());

            VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
            config.m_startAction = StartAction.PROBE;
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
            config.m_leader = ":" + cluster.internalPort(1);
            config.m_coordinators = cluster.coordinators(1);
            config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
            config.m_forceVoltdbCreate = false;
            config.m_hostCount = 2;
            cluster.setPortsFromConfig(0, config);
            localServer = new ServerThread(config);

            System.out.printf("\n\nBefore rejoin MP: %d, SP: %d, AH: %d\n\n",
                    mpTxnsRun.get(), spTxnsRun.get(), adhocTxnsRun.get());
            System.out.flush();

            localServer.start();
            localServer.waitForRejoin();

            System.out.printf("\n\nAfter rejoin  MP: %d, SP: %d, AH: %d\n\n",
                    mpTxnsRun.get(), spTxnsRun.get(), adhocTxnsRun.get());
            System.out.flush();

            assertFalse(loadThreadHasFailed.get());
            assertFalse(adhocThreadHasFailed.get());

            Thread.sleep(2000);

            assertFalse(loadThreadHasFailed.get());
            assertFalse(adhocThreadHasFailed.get());

            shouldContinue.set(false);
            if (loadThread != null) {
                loadThread.join();
            }
            loadThread = null;
            if (adHocThread != null) {
                adHocThread.join();
            }
            adHocThread = null;

            for (int i = 0; i < 10; i++) {
                try {
                    client.callProcedure(IncrementProc.class.getSimpleName());
                    mpTxnsRun.incrementAndGet();
                }
                catch (ProcCallException pce) {
                    if (pce.getClientResponse().getAppStatus() != IncrementProc.USER_ABORT) {
                        fail();
                    }
                }
            }

            client.drain();

            // let it finish rejoining
            Thread.sleep(2000);

            System.out.printf("\n\nAt the end   MP: %d, SP: %d, AH: %d\n\n",
                    mpTxnsRun.get(), spTxnsRun.get(), adhocTxnsRun.get());
            System.out.flush();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        finally {
            shouldContinue.set(false);
            if (loadThread != null) {
                loadThread.join();
            }
            if (adHocThread != null) {
                adHocThread.join();
            }

            try {
                if (clientForLoadThread2 != null) {
                    clientForLoadThread2.close();
                }
                if (client != null) {
                    client.close();
                }
            }
            catch (Exception e) {}
        }
    }
}
