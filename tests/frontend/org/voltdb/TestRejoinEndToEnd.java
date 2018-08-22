/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientAuthScheme;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.utils.MiscUtils;

public class TestRejoinEndToEnd extends RejoinTestBase {

    @After
    public void tearDown() throws Exception {
        System.gc();
        System.runFinalization();
    }

    @Test
    public void testRejoinWithMultipartLoad() throws Exception {
        System.out.println("testRejoinWithMultipartLoad");
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING,
                false, true, null);
        cluster.setMaxHeap(768);
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
            VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
            config.m_startAction = cluster.isNewCli() ? StartAction.PROBE : StartAction.REJOIN;
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
            if (cluster.isNewCli()) {
                config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
                config.m_forceVoltdbCreate = false;
                config.m_hostCount = 3;
            } else {
                config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
            }
            config.m_leader = ":" + cluster.internalPort(1);
            config.m_coordinators = cluster.coordinators(1);
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

        localServer.shutdown();
        cluster.shutDown();
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
                false, false, null);
        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(768);
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
                false, true, null);
        cluster.setMaxHeap(768);
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
                BackendTarget.NATIVE_EE_JNI, false);
        cluster.setMaxHeap(768);
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

        cluster.killSingleHost(0);
        cluster.recoverOne( 0, 1, "");

        cluster.shutDown();
    }

    @Test
    public void testRejoin() throws Exception {
        //Reset the VoltFile prefix that may have been set by previous tests in this suite
        org.voltdb.utils.VoltFile.resetSubrootForThisProcess();
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);
        builder.setHTTPDPort(0);
        int sitesPerHost = 4;
        int hostCount = 3;
        int kFactor = 2;

        LocalCluster cluster = new LocalCluster("rejoin.jar", sitesPerHost, hostCount, kFactor,
                BackendTarget.NATIVE_EE_JNI, false);
        cluster.setMaxHeap(1400);
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
        response = client.callProcedure("InsertPartitioned", 10, 20);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(1));
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 100);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertPartitioned", 45, 72);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        cluster.killSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
        config.m_startAction = cluster.isNewCli() ? StartAction.PROBE : StartAction.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        if (cluster.isNewCli()) {
            config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
            config.m_forceVoltdbCreate = false;
            config.m_hostCount = hostCount;
        } else {
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        }
        config.m_leader = ":" + cluster.internalPort(1);
        config.m_coordinators = cluster.coordinators(1);

        config.m_isRejoinTest = true;
        cluster.setPortsFromConfig(0, config);
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForRejoin();

        Thread.sleep(5000);

        // Run a transaction through the HTTP interface to make sure the
        // internal adapter is correctly setup after rejoin
        final TestJSONInterface.Response httpResp =
            TestJSONInterface.responseFromJSON(TestJSONInterface.callProcOverJSON("Insert",
                                                                                  ParameterSet.fromArrayNoCopy(200),
                                                                                  config.m_httpPort,
                                                                                  "ry@nlikesthe", "y@nkees",
                                                                                  false, false, 200, ClientAuthScheme.HASH_SHA256, 1));
        assertEquals(ClientResponse.SUCCESS, httpResp.status);

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
        response = client.callProcedure("InsertReplicated", 68);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertPartitioned", 34, 68);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        // Verify the view content after the rejoin.
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM vblah_replicated ORDER BY ival;").getResults()[0];
        RegressionSuite.assertContentOfTable(new Object[][] { {0, 1}, {7, 1}, {68, 1}, {100, 1} }, result);
        result = client.callProcedure("@AdHoc", "SELECT * FROM vpartitioned ORDER BY pkey;").getResults()[0];
        RegressionSuite.assertContentOfTable(new Object[][] { {10, 1}, {34, 1}, {45, 1} }, result);
        // This view is randomly partitioned.
        result = client.callProcedure("@AdHoc", "SELECT * FROM vrpartitioned ORDER BY value;").getResults()[0];
        RegressionSuite.assertContentOfTable(new Object[][] { {20, 1}, {68, 1}, {72, 1} }, result);
        result = client.callProcedure("@AdHoc", "SELECT * FROM vjoin ORDER BY pkey;").getResults()[0];
        RegressionSuite.assertContentOfTable(new Object[][] { {34, 1} }, result);
        client.close();

        localServer.shutdown();
        cluster.shutDown();
    }

    @Test
    public void testRejoinPropogateAdminMode() throws Exception {
        //Reset the VoltFile prefix that may have been set by previous tests in this suite
        org.voltdb.utils.VoltFile.resetSubrootForThisProcess();
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, false);
        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(768);
        boolean success = cluster.compileWithAdminMode(builder, -1, false); // note this admin port is ignored
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

        cluster.killSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
        config.m_startAction = cluster.isNewCli() ? StartAction.PROBE : StartAction.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        if (cluster.isNewCli()) {
            config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
            config.m_forceVoltdbCreate = false;
            config.m_hostCount = 3;
        } else {
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        }
        config.m_leader = ":" + cluster.internalPort(1);
        config.m_coordinators = cluster.coordinators(1);

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

    // Export tests for rejoin. Use ExportTestClient which is onserver test client.
    //The tests does not verify any data but just rejoin functionality.
    @Test
    public void testRejoinDataTransfer() throws Exception {
        System.out.println("testRejoinDataTransfer");
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);

        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, false, false, additionalEnv);

        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(768);
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
        GeographyValue poly1 = GeographyValue.fromWKT("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        GeographyPointValue point1 = GeographyPointValue.fromWKT("POINT(100 87)");
        response = client.callProcedure("InsertSinglePartitionPolygon", 1, poly1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertMultiPartitionPolygon", 2, poly1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertSinglePartitionPoint", 3, point1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertMultiPartitionPoint", 4, point1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        cluster.killSingleHost(0);
        Thread.sleep(1000);

        VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
        config.m_startAction = cluster.isNewCli() ? StartAction.PROBE : StartAction.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        if (cluster.isNewCli()) {
            config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
            config.m_forceVoltdbCreate = false;
            config.m_hostCount = 3;
        } else {
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        }
        config.m_leader = ":" + cluster.internalPort(1);
        config.m_coordinators = cluster.coordinators(1);

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

        response = client.callProcedure("SelectSinglePartitionPolygon", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(1, response.getResults()[0].fetchRow(0).getLong(0));
        assertEquals(poly1.toString(), response.getResults()[0].fetchRow(0).getGeographyValue(1).toString());

        response = client.callProcedure("SelectMultiPartitionPolygon", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(2, response.getResults()[0].fetchRow(0).getLong(0));
        assertEquals(poly1.toString(), response.getResults()[0].fetchRow(0).getGeographyValue(1).toString());

        response = client.callProcedure("SelectSinglePartitionPoint", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(3, response.getResults()[0].fetchRow(0).getLong(0));
        assertEquals(point1.toString(), response.getResults()[0].fetchRow(0).getGeographyPointValue(1).toString());

        response = client.callProcedure("SelectMultiPartitionPoint", 4);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(4, response.getResults()[0].fetchRow(0).getLong(0));
        assertEquals(point1.toString(), response.getResults()[0].fetchRow(0).getGeographyPointValue(1).toString());
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

        localServer.shutdown();
        cluster.shutDown();
    }

    /**
     * ENG-5276: Rejoin a node while the cluster is idle, the snapshot after the rejoin should contain correct
     * per-partition txnIds.
     */
    @Test
    public void testIdleRejoinThenSnapshot() throws Exception
    {
        final String snapshotDir = "/tmp/" + System.getProperty("user.name");

        //Reset the VoltFile prefix that may have been set by previous tests in this suite
        org.voltdb.utils.VoltFile.resetSubrootForThisProcess();
        VoltProjectBuilder builder = getBuilderForTest();

        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, false, false, additionalEnv);

        cluster.setMaxHeap(768);
        cluster.overrideAnyRequestForValgrind();
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);
        //Test that use Snapshot needs VoltFile magic.
        cluster.setNewCli(false);
        // start a 2 node k=1 cluster
        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        // run some RW transactions to increase the per-partition txnIds
        response = client.callProcedure("InsertSinglePartition", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        // shutdown and rejoin a node
        cluster.killSingleHost(1);
        assertTrue(cluster.recoverOne(1, 0, ""));

        // reuse the previous client to do a snapshot save
        response = client.callProcedure("@SnapshotSave", snapshotDir, "testnonce", (byte) 1);
        /*
         * Bend over backwards to get a snapshot done and know when it completes
         * Queuing mechanism ruins everything everywhere all the time
         */
        if (SnapshotUtil.isSnapshotQueued(response.getResults())) {
            Thread.sleep(60000);
        } else {
            while (SnapshotUtil.isSnapshotInProgress(response.getResults())) {
                assertEquals(ClientResponse.SUCCESS, response.getStatus());
                response = client.callProcedure("@SnapshotSave", snapshotDir, "testnonce", (byte) 1);
                if (SnapshotUtil.isSnapshotQueued(response.getResults())) {
                    Thread.sleep(60000);
                    break;
                }
            }
        }

        client.close();

        // restart the cluster
        cluster.shutDown();
        cluster.startUp(false);

        // try to restore from the snapshot
        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));
        try {
            response = client.callProcedure("@SnapshotRestore", snapshotDir, "testnonce");
            System.out.println(response.getResults()[0].toJSONString());
        } catch (ProcCallException ex) {
            System.out.println(ex.getClientResponse().getResults()[0].toFormattedString(true));
            ex.printStackTrace();
        }
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        cluster.shutDown();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRejoinWithCatalogUpdate() throws Exception {
        //Reset the VoltFile prefix that may have been set by previous tests in this suite
        org.voltdb.utils.VoltFile.resetSubrootForThisProcess();
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setHTTPDPort(0);
        int sitesPerHost = 2;
        int hostCount = 3;
        int kFactor = 2;

        LocalCluster cluster = new LocalCluster("rejoin.jar", sitesPerHost, hostCount, kFactor,
                BackendTarget.NATIVE_EE_JNI, false);
        cluster.setMaxHeap(1400);
        cluster.overrideAnyRequestForValgrind();
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        // prepare the other catalog
        String pathToOtherCatalog = Configuration.getPathToCatalogForTest("newddl.jar");
        String pathToOtherDeployment = Configuration.getPathToCatalogForTest("newddl.xml");

        VoltProjectBuilder otherBuilder = getBuilderForTest();
        otherBuilder.addLiteralSchema("create table foo (a int not null, b int); ");
        otherBuilder.addPartitionInfo("foo", "a");
        otherBuilder.setHTTPDPort(0);
        success = otherBuilder.compile(pathToOtherCatalog, 2, 3, 2);
        assertTrue(success);
        MiscUtils.copyFile(otherBuilder.getPathToDeployment(), pathToOtherDeployment);

        // start cluster
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


        cluster.killSingleHost(0);
        Thread.sleep(100);

        // catalog update to increase the catalog version
        response = client.updateApplicationCatalog(
                new File(Configuration.getPathToCatalogForTest("newddl.jar")),
                new File(pathToOtherDeployment));
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        client.close();

        VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
        config.m_startAction = cluster.isNewCli() ? StartAction.PROBE : StartAction.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        if (cluster.isNewCli()) {
            config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
            config.m_forceVoltdbCreate = false;
            config.m_hostCount = hostCount;
        } else {
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        }
        config.m_leader = ":" + cluster.internalPort(1);
        config.m_coordinators = cluster.coordinators(1);

        config.m_isRejoinTest = true;
        cluster.setPortsFromConfig(0, config);
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForRejoin();

        Thread.sleep(10000);

        // Run a transaction through the HTTP interface to make sure the
        // internal adapter is correctly setup after rejoin
        final TestJSONInterface.Response httpResp =
            TestJSONInterface.responseFromJSON(TestJSONInterface.callProcOverJSON("Insert",
                                                                                  ParameterSet.fromArrayNoCopy(200),
                                                                                  config.m_httpPort,
                                                                                  "ry@nlikesthe", "y@nkees",
                                                                                  false, false, 200, ClientAuthScheme.HASH_SHA256, 1));
        assertEquals(ClientResponse.SUCCESS, httpResp.status);

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        for (int i = 0; i < 3; i++) {
            response = client.callProcedure("@AdHoc", "insert into Foo values(" + i + "," +  i+ ")");
            assertEquals(ClientResponse.SUCCESS, response.getStatus());
        }
        VoltTable vt = client.callProcedure("@AdHoc", "select count(*) from foo;").getResults()[0];
        RegressionSuite.validateTableOfScalarLongs(vt, new long[]{3});

        response = client.callProcedure("FOO.insert", 4, 4);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        client.close();

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
        builder.addExport(true, "file", null);  // authGroups (off)

        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, false, false, additionalEnv);

        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(768);
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

        cluster.killSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
        config.m_startAction = cluster.isNewCli() ? StartAction.PROBE : StartAction.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        if (cluster.isNewCli()) {
            config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
            config.m_forceVoltdbCreate = false;
            config.m_hostCount = 3;
        } else {
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        }
        config.m_leader = ":" + cluster.internalPort(1);
        config.m_coordinators = cluster.coordinators(1);

        config.m_isRejoinTest = true;
        cluster.setPortsFromConfig(0, config);
        cluster.recoverOne(0, 1, "");

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

        cluster.shutDown();
    }

    @Test
    public void testRejoinWithExportWithActuallyExportedTables() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        builder.addExport(true, "file", null);  // authGroups (off)

        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, false, false, additionalEnv);

        cluster.overrideAnyRequestForValgrind();
        cluster.setMaxHeap(768);
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

        cluster.killSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
        config.m_startAction = cluster.isNewCli() ? StartAction.PROBE : StartAction.REJOIN;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        if (cluster.isNewCli()) {
            config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
            config.m_forceVoltdbCreate = false;
            config.m_hostCount = 3;
        } else {
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        }
        config.m_leader = ":" + cluster.internalPort(1);
        config.m_coordinators = cluster.coordinators(1);

        config.m_isRejoinTest = true;
        cluster.setPortsFromConfig(0, config);

        cluster.recoverOne(0, 1, "");

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

        cluster.shutDown();
    }
}
