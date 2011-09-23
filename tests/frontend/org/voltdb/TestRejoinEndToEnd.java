/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.voltdb.BackendTarget;
import org.voltdb.ExecutionSite;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportClientException;
import org.voltdb.exportclient.ExportConnection;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestRejoinEndToEnd extends RejoinTestBase {

    final int FAIL_NO_OPEN_SOCKET = 0;
    final int FAIL_TIMEOUT_ON_SOCKET = 1;
    final int FAIL_SKEW = 2;
    final int DONT_FAIL = 3;

    private static final String TMPDIR = "/tmp";
    private static final String TESTNONCE = "testnonce";

    private void deleteTestFiles()
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE) ||
                file.endsWith(".vpt") ||
                file.endsWith(".digest") ||
                file.endsWith(".tsv") ||
                file.endsWith(".csv");
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files)
        {
            tmp_file.delete();
        }
    }

    boolean failNext(int failType) throws Exception {
        Context context = getServerReadyToReceiveNewNode();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ServerSocketChannel listener = null;
        if (failType != FAIL_NO_OPEN_SOCKET) {
            try {
                listener = ServerSocketChannel.open();
                listener.socket().bind(new InetSocketAddress(VoltDB.DEFAULT_INTERNAL_PORT + 1));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(-1);
            }
        }

        SyncCallback scb = new SyncCallback();
        boolean success = false;
        while (!success) {
            success = client.callProcedure(scb, "@Rejoin", "localhost", VoltDB.DEFAULT_INTERNAL_PORT + 1);
            if (!success) Thread.sleep(100);
        }

        SocketChannel socket = null;
        if (failType != FAIL_NO_OPEN_SOCKET) {
            socket = listener.accept();
            listener.close();
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.socket().getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.socket().getInputStream()));

            int hostId = in.readInt();
            assertEquals(hostId, 1);

            if (failType != FAIL_TIMEOUT_ON_SOCKET) {
                //COMMAND_SENDTIME_AND_CRC
                out.writeInt(4);
                out.flush();
                // ignore what the other host says the time is
                in.readLong();
                // fake a clock skew of 1ms
                if (failType == FAIL_SKEW) {
                    out.writeLong(100000);
                    // COMMAND_NTPFAIL
                    out.writeInt(5);
                }
                else {
                    out.writeLong(1);
                    // COMMAND_COMPLETE
                    out.writeInt(3);
                }
                out.flush();
            }
        }

        scb.waitForResponse();
        ClientResponse response = scb.getResponse();

        switch (failType) {
            case FAIL_NO_OPEN_SOCKET:
                assertTrue(response.getStatus() != ClientResponse.SUCCESS);
                break;
            case FAIL_TIMEOUT_ON_SOCKET:
                assertTrue(response.getStatus() != ClientResponse.SUCCESS);
                break;
            case FAIL_SKEW:
                assertTrue(response.getStatus() != ClientResponse.SUCCESS);
                break;
            case DONT_FAIL:
                assertTrue(response.getStatus() == ClientResponse.SUCCESS);
                break;
        }

        if (failType != FAIL_NO_OPEN_SOCKET)
            socket.close();
        context.localServer.shutdown();
        context.localServer.join();

        client.close();

        Thread.sleep(250);
        // this means there is nothing else to try
        return failType != DONT_FAIL;
    }

    public void testRejoinWithMultipartLoad() throws Exception {
        ExecutionSite.m_recoveryPermit.drainPermits();
        ExecutionSite.m_recoveryPermit.release();
        try {
        System.out.println("testRejoinWithMultipartLoad");
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 2, 1,
                                                BackendTarget.NATIVE_EE_JNI,
                                                LocalCluster.FailureState.ALL_RUNNING,
                                                true, true);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", 21213);

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

        ServerThread localServer = null;
        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
            config.m_rejoinToHostAndPort = m_username + ":" + m_password + "@localhost:21213";
            config.m_isRejoinTest = true;
            localServer = new ServerThread(config);

            localServer.start();
            localServer.waitForInitialization();

            Thread.sleep(2000);

            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost", 21213);

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
        client.createConnection("localhost", 21212);

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

    public void testRestoreThenRejoinPropagatesRestore() throws Exception {
        System.out.println("testRestoreThenRejoinThenRestore");
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster(
                "rejoin.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI, true);
        ServerThread localServer = null;
        try {
            boolean success = cluster.compileWithAdminMode(builder, 9998, false);
            assertTrue(success);
            MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
            cluster.setHasLocalServer(false);

            cluster.startUp();

            Client client;

            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost");

            deleteTestFiles();

            client.callProcedure("@SnapshotSave", TMPDIR,
                    TESTNONCE, (byte)1).getResults();

            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);

            cluster.shutDownSingleHost(0);
            Thread.sleep(1000);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
            config.m_rejoinToHostAndPort = m_username + ":" + m_password + "@localhost:21213";
            config.m_isRejoinTest = true;
            localServer = new ServerThread(config);

            localServer.start();
            localServer.waitForInitialization();

            Thread.sleep(2000);

            client.close();

            assertTrue(org.voltdb.sysprocs.SnapshotRestore.m_haveDoneRestore);

            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost");

            // Also make sure a catalog update doesn't reset m_haveDoneRestore
            File newCatalog = new File(Configuration.getPathToCatalogForTest("rejoin.jar"));
            File deployment = new File(Configuration.getPathToCatalogForTest("rejoin.xml"));

            VoltTable[] results =
                client.updateApplicationCatalog(newCatalog, deployment).getResults();
            assertTrue(results.length == 1);

            client.close();

            assertTrue(org.voltdb.sysprocs.SnapshotRestore.m_haveDoneRestore);
        } finally {
            cluster.shutDown();
            if (localServer != null) {
                localServer.shutdown();
            }
        }
    }

    public void testCatalogUpdateAfterRejoin() throws Exception {
        System.out.println("testCatalogUpdateAfterRejoin");
        VoltProjectBuilder builder = getBuilderForTest();

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 2, 1,
                                                BackendTarget.NATIVE_EE_JNI);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

        try {
            cluster.startUp();

            for (int ii = 0; ii < 3; ii++) {
                cluster.shutDownSingleHost(1);
                Thread.sleep(1000);
                cluster.recoverOne( 1, 0, "localhost");

                File newCatalog = new File(Configuration.getPathToCatalogForTest("rejoin.jar"));
                File deployment = new File(Configuration.getPathToCatalogForTest("rejoin.xml"));

                Client client = ClientFactory.createClient();
                client.createConnection("localhost");

                VoltTable[] results =
                    client.updateApplicationCatalog(newCatalog, deployment).getResults();
                assertTrue(results.length == 1);
                client.close();
            }
        } finally {
            cluster.shutDown();
        }
    }

    public void testRejoinSysprocButFail() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("rejoin.jar"), 1, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_isRejoinTest = true;
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        SyncCallback scb = new SyncCallback();
        success = false;
        while (!success) {
            success = client.callProcedure(scb, "@Rejoin", "localhost", config.m_internalPort + 1);
            if (!success) Thread.sleep(100);
        }

        scb.waitForResponse();
        ClientResponse response = scb.getResponse();
        assertTrue(response.getStatusString().contains("Unable to find down node"));

        client.close();
        localServer.shutdown();
        localServer.join();
    }


    public void testWithFakeSecondHostMessengerNoSocket() throws Exception {
        failNext(FAIL_NO_OPEN_SOCKET);
    }

    public void testWithFakeSecondHostMessengerTimeout() throws Exception {
        failNext(FAIL_TIMEOUT_ON_SOCKET);
    }

    public void testWithFakeSecondHostMessengerSkew() throws Exception {
        failNext(FAIL_SKEW);
    }

    public void testWithFakeSecondHostMessengerSuccess() throws Exception {
        failNext(FAIL_NO_OPEN_SOCKET);
    }

    public void testLocalClusterRecoveringMode() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 2, 1,
                                                BackendTarget.NATIVE_EE_JNI,
                                                LocalCluster.FailureState.ONE_FAILURE,
                                                false, true);
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
                                   false, true);
        success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        Thread.sleep(100);

        cluster.shutDown();
    }

    public void testRejoinInlineStringBug() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        LocalCluster cluster = new LocalCluster("rejoin.jar", 1, 2, 1,
                                                BackendTarget.NATIVE_EE_JNI, true);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost");

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
        cluster.recoverOne( 0, 1, "localhost");

        cluster.shutDown();
    }

    public void testRejoin() throws Exception {
        //Reset the VoltFile prefix that may have been set by previous tests in this suite
        org.voltdb.utils.VoltFile.resetSubrootForThisProcess();
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                                                BackendTarget.NATIVE_EE_JNI, true);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost");

        response = client.callProcedure("InsertSinglePartition", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", 21213);
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        cluster.shutDownSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_rejoinToHostAndPort = m_username + ":" + m_password + "@localhost:21213";
        config.m_isRejoinTest = true;
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Thread.sleep(5000);

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost");

        response = client.callProcedure("InsertSinglePartition", 5);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 6);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 7);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", 21213);
        response = client.callProcedure("InsertSinglePartition", 8);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 9);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        localServer.shutdown();
        cluster.shutDown();
    }

    public void testRejoinPropogateAdminMode() throws Exception {
        //Reset the VoltFile prefix that may have been set by previous tests in this suite
        org.voltdb.utils.VoltFile.resetSubrootForThisProcess();
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                                                BackendTarget.NATIVE_EE_JNI, true);
        boolean success = cluster.compileWithAdminMode(builder, 9998, false);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", 9997);

        response = client.callProcedure("@Pause");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        cluster.shutDownSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_rejoinToHostAndPort = m_username + ":" + m_password + "@localhost:9996";
        config.m_isRejoinTest = true;
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

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
                // TODO Auto-generated method stub

            }

        }

        public TrivialExportClient() throws ExportClientException {
            super.addServerInfo(new InetSocketAddress("localhost", VoltDB.DEFAULT_PORT));
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

    public void testRejoinWithExport() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        //builder.setTableAsExportOnly("blah", false);
        //builder.setTableAsExportOnly("blah_replicated", false);
        //builder.setTableAsExportOnly("PARTITIONED", false);
        //builder.setTableAsExportOnly("PARTITIONED_LARGE", false);
        builder.addExport("org.voltdb.export.processors.RawProcessor",
                true,  // enabled
                null);  // authGroups (off)

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1,
                                                BackendTarget.NATIVE_EE_JNI, true);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost");

        response = client.callProcedure("InsertSinglePartition", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", 21213);
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        TrivialExportClient exportClient = new TrivialExportClient();
        exportClient.work();
        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        cluster.shutDownSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_rejoinToHostAndPort = m_username + ":" + m_password + "@localhost:21213";
        config.m_isRejoinTest = true;
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Thread.sleep(1000);
        while (VoltDB.instance().recovering()) {
            Thread.sleep(100);
        }

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost");

        response = client.callProcedure("InsertSinglePartition", 5);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 6);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 7);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", 21213);
        response = client.callProcedure("InsertSinglePartition", 8);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 9);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        exportClient = new TrivialExportClient();
        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        Thread.sleep(4000);

        exportClient.work();

        localServer.shutdown();
        cluster.shutDown();
    }

    public void testRejoinDataTransfer() throws Exception {
        System.out.println("testRejoinDataTransfer");
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 2, 1,
                                                BackendTarget.NATIVE_EE_JNI, true);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost");

        response = client.callProcedure("InsertSinglePartition", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        cluster.shutDownSingleHost(0);
        Thread.sleep(1000);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_rejoinToHostAndPort = m_username + ":" + m_password + "@localhost:21213";
        config.m_isRejoinTest = true;
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Thread.sleep(2000);

        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", 21213);

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
        client.createConnection("localhost", 21212);

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
