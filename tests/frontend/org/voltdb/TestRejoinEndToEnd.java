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

package org.voltdb;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportClientException;
import org.voltdb.exportclient.ExportConnection;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

@RunWith(value = Parameterized.class)
public class TestRejoinEndToEnd extends RejoinTestBase {

    @Parameters
    public static Collection<Object[]> useIv2() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    protected final boolean m_useIv2;
    public TestRejoinEndToEnd(boolean useIv2)
    {
        m_useIv2 = useIv2 || VoltDB.checkTestEnvForIv2();
    }

    final int FAIL_NO_OPEN_SOCKET = 0;
    final int FAIL_TIMEOUT_ON_SOCKET = 1;
    final int FAIL_SKEW = 2;
    final int DONT_FAIL = 3;

    @Test
    public void testRejoinWithMultipartLoad() throws Exception {
        ExecutionSite.m_recoveryPermit.drainPermits();
        ExecutionSite.m_recoveryPermit.release();
        try {
            System.out.println("testRejoinWithMultipartLoad");
            VoltProjectBuilder builder = getBuilderForTest();
            builder.setSecurityEnabled(true);

            LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                    BackendTarget.NATIVE_EE_JNI,
                    LocalCluster.FailureState.ALL_RUNNING,
                    false, true, m_useIv2);
            cluster.setMaxHeap(256);
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

            cluster.shutDownSingleHost(0);
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
                                }, "SelectCountPartitioned");
                                //clientForLoadThread.callProcedure("SelectCountPartitioned");
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

            ServerThread localServer = null;
            try {
                VoltDB.Configuration config = new VoltDB.Configuration(cluster.portGenerator);
                config.m_enableIV2 = m_useIv2;
                config.m_startAction = START_ACTION.REJOIN;
                config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
                config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
                config.m_leader = ":" + cluster.internalPort(1);
                config.m_isRejoinTest = true;
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
            cluster.shutDownSingleHost(1);
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

            localServer.shutdown();
            cluster.shutDown();
        } finally {
            ExecutionSite.m_recoveryPermit.drainPermits();
            ExecutionSite.m_recoveryPermit.release(Integer.MAX_VALUE);
        }
    }

    // These tests have moved to TestRejoinWithCatalogUpdate.java pending IV2 implementation of update catalog.
    // public void testRestoreThenRejoinPropagatesRestore() throws Exception;
    // public void testCatalogUpdateAfterRejoin() throws Exception;

    @Test
    public void testLocalClusterRecoveringMode() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 2, 1,
                BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ONE_FAILURE,
                false, false, m_useIv2);
        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(256);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        Thread.sleep(100);

        cluster.shutDown();

        cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ONE_RECOVERING,
                false, true, m_useIv2);
        cluster.setMaxHeap(256);
        cluster.overrideAnyRequestForValgrind();
        success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        Thread.sleep(100);

        cluster.shutDown();
    }

    @Test
    public void testRejoinInlineStringBug() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        LocalCluster cluster = new LocalCluster("rejoin.jar", 1, 2, 1,
                BackendTarget.NATIVE_EE_JNI, false, m_useIv2);
        cluster.setMaxHeap(256);
        cluster.overrideAnyRequestForValgrind();
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        ProcedureCallback callback = new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
            throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    System.out.println(clientResponse.getStatusString());
                }
            }
        };

        StringBuffer shortBuffer = new StringBuffer();
        for (int ii = 0; ii < 33; ii++) {
            shortBuffer.append('a');
        }
        String shortString = shortBuffer.toString();

        StringBuffer longBuffer = new StringBuffer();
        for (int ii = 0; ii < 17700; ii++) {
            longBuffer.append('a');
        }
        String longString = longBuffer.toString();

        for (int ii = 0; ii < 119; ii++) {
            client.callProcedure( callback, "InsertInlinedString", ii, shortString, longString);
        }

        shortBuffer.append("aaa");
        client.callProcedure( callback, "InsertInlinedString", 120, shortBuffer.toString(), longString);

        client.drain();
        client.close();

        cluster.shutDownSingleHost(0);
        cluster.recoverOne( 0, 1, "");

        cluster.shutDown();
    }

    @Test
    public void testRejoin() throws Exception {
        //Reset the VoltFile prefix that may have been set by previous tests in this suite
        org.voltdb.utils.VoltFile.resetSubrootForThisProcess();
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, false, m_useIv2);
        cluster.setMaxHeap(256);
        cluster.overrideAnyRequestForValgrind();
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
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        cluster.shutDownSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration(cluster.portGenerator);
        config.m_enableIV2 = m_useIv2;
        config.m_startAction = START_ACTION.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_leader = ":" + cluster.internalPort(1);

        config.m_isRejoinTest = true;
        cluster.setPortsFromConfig(0, config);
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForRejoin();

        Thread.sleep(5000);

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        response = client.callProcedure("InsertSinglePartition", 5);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 6);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 7);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));
        response = client.callProcedure("InsertSinglePartition", 8);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 9);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        localServer.shutdown();
        cluster.shutDown();
    }

    @Test
    public void testRejoinPropogateAdminMode() throws Exception {
        //Reset the VoltFile prefix that may have been set by previous tests in this suite
        org.voltdb.utils.VoltFile.resetSubrootForThisProcess();
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, false, m_useIv2);
        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(256);
        boolean success = cluster.compileWithAdminMode(builder, 21211, false); // note this admin port is ignored
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.adminPort(1));

        response = client.callProcedure("@Pause");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        cluster.shutDownSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration(cluster.portGenerator);
        config.m_enableIV2 = m_useIv2;
        config.m_startAction = START_ACTION.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_leader = ":" + cluster.internalPort(1);

        config.m_isRejoinTest = true;
        cluster.setPortsFromConfig(0, config);
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForRejoin();

        Thread.sleep(1000);

        assertTrue(VoltDB.instance().getMode() == OperationMode.PAUSED);

        localServer.shutdown();
        cluster.shutDown();
    }

    class TrivialExportClient extends ExportClientBase {

        public class TrivialDecoder extends ExportDecoderBase {

            public TrivialDecoder(AdvertisedDataSource source) {
                super(source);
            }

            @Override
            public boolean processRow(int rowSize, byte[] rowData) {
                return true;
            }

            @Override
            public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            }

        }

        public TrivialExportClient(int port) throws ExportClientException {
            super.addServerInfo(new InetSocketAddress("localhost", port));
            super.addCredentials(null, null);
            super.connect();
        }

        @Override
        public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
            return new TrivialDecoder(source);
        }

        @Override
        public int work() throws ExportClientException {
            super.work();
            for (ExportConnection ec : m_exportConnections.values()) {
                System.out.printf("Export Conn Offset: %d\n", ec.getLastAckOffset());
            }
            return 1;
        }

    }

    @Test
    public void testRejoinWithExportWithActuallyExportedTables() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        builder.setTableAsExportOnly("export_ok_blah_with_no_pk");
        builder.addExport("org.voltdb.export.processors.RawProcessor",
                true,  // enabled
                null);  // authGroups (off)

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, false, m_useIv2);
        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(256);
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
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        TrivialExportClient exportClient = new TrivialExportClient(cluster.port(0));
        exportClient.work();
        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        cluster.shutDownSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration(cluster.portGenerator);
        config.m_enableIV2 = m_useIv2;
        config.m_startAction = START_ACTION.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_leader = ":" + cluster.internalPort(1);

        config.m_isRejoinTest = true;
        cluster.setPortsFromConfig(0, config);
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForRejoin();

        Thread.sleep(1000);
        while (VoltDB.instance().rejoining()) {
            Thread.sleep(100);
        }

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        response = client.callProcedure("InsertSinglePartition", 5);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 6);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 7);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));
        response = client.callProcedure("InsertSinglePartition", 8);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 9);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        exportClient = new TrivialExportClient(cluster.port(0));
        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        localServer.shutdown();
        cluster.shutDown();
    }

    @Test
    public void testRejoinWithExport() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        //builder.setTableAsExportOnly("blah",false);
        //builder.setTableAsExportOnly("blah_replicated", false);
        //builder.setTableAsExportOnly("PARTITIONED", false);
        //builder.setTableAsExportOnly("PARTITIONED_LARGE", false);
        builder.addExport("org.voltdb.export.processors.RawProcessor",
                true,  // enabled
                null);  // authGroups (off)

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, false, m_useIv2);
        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(256);
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
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        TrivialExportClient exportClient = new TrivialExportClient(cluster.port(0));
        exportClient.work();
        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        cluster.shutDownSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration(cluster.portGenerator);
        config.m_enableIV2 = m_useIv2;
        config.m_startAction = START_ACTION.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_leader = ":" + cluster.internalPort(1);

        config.m_isRejoinTest = true;
        cluster.setPortsFromConfig(0, config);
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForRejoin();

        Thread.sleep(1000);
        while (VoltDB.instance().rejoining()) {
            Thread.sleep(100);
        }

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        response = client.callProcedure("InsertSinglePartition", 5);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 6);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 7);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));
        response = client.callProcedure("InsertSinglePartition", 8);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 9);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        exportClient = new TrivialExportClient(cluster.port(0));
        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        localServer.shutdown();
        cluster.shutDown();
    }

    @Test
    public void testRejoinDataTransfer() throws Exception {
        System.out.println("testRejoinDataTransfer");
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, false, m_useIv2);
        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(256);
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
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        cluster.shutDownSingleHost(0);
        Thread.sleep(1000);

        VoltDB.Configuration config = new VoltDB.Configuration(cluster.portGenerator);
        config.m_enableIV2 = m_useIv2;
        config.m_startAction = START_ACTION.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_leader = ":" + cluster.internalPort(1);

        config.m_isRejoinTest = true;
        cluster.setPortsFromConfig(0, config);
        ServerThread localServer = new ServerThread(config);

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

        response = client.callProcedure("SelectBlah", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 1);

        response = client.callProcedure("SelectBlahReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 0);

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
        cluster.shutDownSingleHost(1);
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

        localServer.shutdown();
        cluster.shutDown();
    }

}
