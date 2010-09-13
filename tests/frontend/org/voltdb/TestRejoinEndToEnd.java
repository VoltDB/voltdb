/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Random;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.SyncCallback;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.network.VoltNetwork;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.InMemoryJarfile;

public class TestRejoinEndToEnd extends TestCase {
    private static final String m_username;
    private static final String m_password;
    private static final ClientConfig m_cconfig = new ClientConfig("ry@nlikesthe", "y@nkees");

    static {
        String username = null;
        String password = null;
        try {
        username = URLEncoder.encode( "ry@nlikesthe", "UTF-8");
        password = URLEncoder.encode( "y@nkees", "UTF-8");
        } catch (Exception e) {}
        m_username = username;
        m_password = password;
    }

    VoltProjectBuilder getBuilderForTest() throws UnsupportedEncodingException {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));\n" +
            "create table blah_replicated (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));" +
            "create table PARTITIONED (" +
            "pkey bigint default 0 not null, " +
            "value bigint default 0 not null, " +
            "PRIMARY KEY(pkey));" +
            "create table PARTITIONED_LARGE (" +
            "pkey bigint default 0 not null, " +
            "value bigint default 0 not null, " +
            "data VARCHAR(512) default null," +
            "PRIMARY KEY(pkey));";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("blah", "ival");
        builder.addPartitionInfo("PARTITIONED", "pkey");
        builder.addPartitionInfo("PARTITIONED_LARGE", "pkey");

        GroupInfo gi = new GroupInfo("foo", true, true);
        builder.addGroups(new GroupInfo[] { gi } );
        UserInfo ui = new UserInfo( "ry@nlikesthe", "y@nkees", new String[] { "foo" } );
        builder.addUsers(new UserInfo[] { ui } );

        ProcedureInfo[] pi = new ProcedureInfo[] {
            new ProcedureInfo(new String[] { "foo" }, "Insert", "insert into blah values (?);", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertSinglePartition", "insert into blah values (?);", "blah.ival:0"),
            new ProcedureInfo(new String[] { "foo" }, "InsertReplicated", "insert into blah_replicated values (?);", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectBlahSinglePartition", "select * from blah where ival = ?;", "blah.ival:0"),
            new ProcedureInfo(new String[] { "foo" }, "SelectBlah", "select * from blah where ival = ?;", null),
            new ProcedureInfo(new String[] { "foo" }, "SelectBlahReplicated", "select * from blah_replicated where ival = ?;", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertPartitioned", "insert into PARTITIONED values (?, ?);", "PARTITIONED.pkey:0"),
            new ProcedureInfo(new String[] { "foo" }, "UpdatePartitioned", "update PARTITIONED set value = ? where pkey = ?;", "PARTITIONED.pkey:1"),
            new ProcedureInfo(new String[] { "foo" }, "SelectPartitioned", "select * from PARTITIONED;", null),
            new ProcedureInfo(new String[] { "foo" }, "InsertPartitionedLarge", "insert into PARTITIONED_LARGE values (?, ?, ?);", "PARTITIONED_LARGE.pkey:0")
        };

        builder.addProcedures(pi);
        return builder;
    }

    static class Context {
        long catalogCRC;
        ServerThread localServer;
    }

    /**
     * Simple code to copy a file from one place to another...
     * Java should have this built in... stupid java...
     */
    static void copyFile(String fromPath, String toPath) {
        File inputFile = new File(fromPath);
        File outputFile = new File(toPath);

        try {
            FileReader in = new FileReader(inputFile);
            FileWriter out = new FileWriter(outputFile);
            int c;

            while ((c = in.read()) != -1)
              out.write(c);

            in.close();
            out.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    Context getServerReadyToReceiveNewNode() throws Exception {
        Context retval = new Context();

        VoltProjectBuilder builder = getBuilderForTest();
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("rejoin.jar"), 1, 2, 1, "localhost");
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        retval.localServer = new ServerThread(config);

        // start the fake HostMessenger
        retval.catalogCRC = new InMemoryJarfile(Configuration.getPathToCatalogForTest("rejoin.jar")).getCRC();
        VoltNetwork network2 = new VoltNetwork();
        InetAddress leader = InetAddress.getByName("localhost");
        HostMessenger host2 = new HostMessenger(network2, leader, 2, retval.catalogCRC, null);

        retval.localServer.start();
        host2.waitForGroupJoin();
        network2.start();
        host2.sendReadyMessage();

        retval.localServer.waitForInitialization();
        HostMessenger host1 = VoltDB.instance().getHostMessenger();

        //int host2id = host2.getHostId();
        host2.closeForeignHostSocket(host1.getHostId());
        // this is just to wait for the fault manager to kick in
        Thread.sleep(50);
        host2.shutdown();
        // this is just to wait for the fault manager to kick in
        Thread.sleep(50);

        // wait until the fault manager has kicked in

        for (int i = 0; host1.countForeignHosts() > 0; i++) {
            if (i > 10) fail();
            Thread.sleep(50);
        }
        assertEquals(0, host1.countForeignHosts());

        return retval;
    }

    final int FAIL_NO_OPEN_SOCKET = 0;
    final int FAIL_TIMEOUT_ON_SOCKET = 1;
    final int FAIL_SKEW = 2;
    final int DONT_FAIL = 3;

    boolean failNext(int failType) throws Exception {
        Context context = getServerReadyToReceiveNewNode();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost", null, null);

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
        // this means there is nothing else to try
        return failType != DONT_FAIL;
    }

    public void testRejoinSysprocButFail() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("rejoin.jar"), 1, 1, 0, "localhost");
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost", null, null);

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

    public void testWithFakeSecondHostMessenger() throws Exception {
        // failNext(i) runs the test
        for (int i = 0; failNext(i); i++) {
            // makes this less likely to fail for dumbness
            Thread.sleep(100);
        }
    }

    public void testLocalClusterRecoveringMode() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ONE_FAILURE, false);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        Thread.sleep(100);

        cluster.shutDown();

        cluster = new LocalCluster("rejoin.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ONE_RECOVERING, false);
        success = cluster.compile(builder);
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        Thread.sleep(100);

        cluster.shutDown();
    }

    public void testRejoin() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        cluster.shutDownSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_rejoinToHostAndPort = m_username + ":" + m_password + "@localhost:21213";
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Thread.sleep(5000);

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

        localServer.shutdown();
        cluster.shutDown();
    }

    public void testRejoinDataTransfer() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
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
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Thread.sleep(2000);

        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", 21213);

        /*
         * Check that the recovery data transferred
         */
        response = client.callProcedure("SelectBlahSinglePartition", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 0);

        response = client.callProcedure("SelectBlah", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 1);

        response = client.callProcedure("SelectBlahReplicated", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].fetchRow(0).getLong(0), 0);

        /*
         * Try to insert new data
         */
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("InsertReplicated", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        /*
         * See that it was inserted
         */
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

        /*
         * Kill one of the old ones (not the recovered partition)
         */
        cluster.shutDownSingleHost(1);
        Thread.sleep(1000);

        client.close();

        client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", 21212);

        /*
         * See that the cluster is available and the data is still there.
         */
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

    /*
     * Load some stuff, kill some stuff, rejoin some stuff, update some stuff, rejoin some stuff, drain,
     * verify the updates occurred. Lather, rinse, and repeat.
     */
    public void testRejoinFuzz() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        final int numHosts = 10;
        final int kfactor = 4;
        final LocalCluster cluster =
            new LocalCluster(
                    "rejoin.jar",
                    2,
                    numHosts,
                    kfactor,
                    BackendTarget.NATIVE_EE_JNI,
                    LocalCluster.FailureState.ALL_RUNNING,
                    true);
        cluster.setMaxHeap(256);
        final int numTuples = cluster.isValgrind() ? 10000 : 60000;
        boolean success = cluster.compile(builder);
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        final ArrayList<Integer> serverValues = new ArrayList<Integer>();
        cluster.startUp();

        Client client = ClientFactory.createClient(m_cconfig);

        client.createConnection("localhost");

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
        for (int zz = 0; zz < 5; zz++) {
            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost", Client.VOLTDB_SERVER_PORT + toConnectToTemp);
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

            for (Integer uhoh : toKillFirst) {
                cluster.shutDownSingleHost(uhoh);
            }

            Thread recoveryThread = new Thread("Recovery thread") {
                @Override
                public void run() {
                    for (Integer dead : toKillFirst) {
                        while (!cluster.recoverOne( dead, toConnectTo, m_username + ":" + m_password + "@localhost")) {};
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
                        for (Integer toKill : toKillDuringRecovery) {
                            Thread.sleep(r.nextInt(2000));
                            cluster.shutDownSingleHost(toKill);
                        }
                    } catch (Exception e) {
                        killerFail.set(true);
                        e.printStackTrace();
                    }
                }
            };

            client = ClientFactory.createClient(m_cconfig);
            final Client clientRef = client;
            client.createConnection("localhost", Client.VOLTDB_SERVER_PORT + toConnectTo);
            lastServerValues = new ArrayList<Integer>(serverValues);
            final AtomicBoolean shouldContinue = new AtomicBoolean(true);
            final Thread loadThread = new Thread("Load thread") {
                @Override
                public void run() {
                    try {
                        while (shouldContinue.get()) {
                            for (int ii = 0; ii < numTuples && shouldContinue.get(); ii++) {
                                    rateLimit.acquire();
                                    int updateKey = r.nextInt(numTuples);
                                    int updateValue = r.nextInt(numTuples);
                                    serverValues.set(updateKey, updateValue);
                                    clientRef.callProcedure( new ProcedureCallback() {

                                        @Override
                                        public void clientCallback(ClientResponse clientResponse)
                                                throws Exception {
                                            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                                                System.err.println(clientResponse.getStatusString());
                                                return;
                                            }
                                            if (clientResponse.getResults()[0].asScalarLong() != 1) {
                                                System.err.println("Update didn't happen");
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

            /*
             * This version doesn't work. It causes concurrent failures during the rejoin sysproc
             * that aren't handled correctly
             */
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
            loadThread.join();

            lastRejoinThread = new Thread("Last rejoin thread") {
                @Override
                public void run() {
                    try {
                        for (Integer recover : toKillDuringRecovery) {
                            while (!cluster.recoverOne( recover, toConnectTo, m_username + ":" + m_password + "@localhost")) {}
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            lastRejoinThread.start();
            System.out.println("Finished iteration " + zz);
        }
        lastRejoinThread.join();
        cluster.shutDown();
    }

    /*
     * Load a whole lot of stuff, kill some stuff, rejoin some stuff, and then kill some more stuff during the rejoin
     * This test doesn't validate data because it would be too slow. It has uncovered some bugs that occur
     * when a failure is concurrent with the recovery processors work. It is quite slow due to the loading,
     * but it is worth having.
     */
    public void testRejoinFuzz2ElectricBoogaloo() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true);

        final int numHosts = 6;
        final int numTuples = 204800 * 6;//about 100 megs per
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
                    false);
        cluster.setMaxHeap(256);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();

        Client client = ClientFactory.createClient(m_cconfig);

        client.createConnection("localhost");

        Random r = new Random();
        StringBuilder sb = new StringBuilder(512);
        for (int ii = 0; ii < 512; ii++) {
            sb.append((char)(34 + r.nextInt(90)));
        }
        String theString = sb.toString();

        final Semaphore rateLimit = new Semaphore(3000);
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
                cluster.shutDownSingleHost(uhoh);
            }

            Thread recoveryThread = new Thread() {
                @Override
                public void run() {
                    for (Integer dead : toKillFirst) {
                        while (!cluster.recoverOne( dead, toConnectTo, m_username + ":" + m_password + "@localhost")){}
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
                            cluster.shutDownSingleHost(toKill);
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
                cluster.recoverOne( recoverNow, toConnectTo, m_username + ":" + m_password + "@localhost");
            }
            System.out.println("Finished iteration " + zz);
        }
        cluster.shutDown();
    }
}
