/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.network.VoltNetwork;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.JarReader;

public class TestRejoinEndToEnd extends TestCase {

    VoltProjectBuilder getBuilderForTest() throws UnsupportedEncodingException {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("blah", "ival");

        GroupInfo gi = new GroupInfo("foo", true, true);
        builder.addGroups(new GroupInfo[] { gi } );
        UserInfo ui = new UserInfo("ry@nlikesthe", "y@nkees", new String[] { "foo" } );
        builder.addUsers(new UserInfo[] { ui } );

        ProcedureInfo[] pi = new ProcedureInfo[2];
        pi[0] = new ProcedureInfo(new String[] { "foo" }, "Insert", "insert into blah values (?);", null);
        pi[1] = new ProcedureInfo(new String[] { "foo" }, "InsertSinglePartition", "insert into blah values (?);", "blah.ival:0");
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
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("rejoin.jar"), 1, 2, 1, "localhost", false);
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        retval.localServer = new ServerThread(config);

        // start the fake HostMessenger
        retval.catalogCRC = JarReader.crcForJar(Configuration.getPathToCatalogForTest("rejoin.jar"));
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
        host2.closeForeignHostScoket(host1.getHostId());
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

        // this means there is nothing else to try
        return failType != DONT_FAIL;
    }

    public void testRejoinSysprocButFail() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("rejoin.jar"), 1, 1, 0, "localhost", false);
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

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ONE_FAILURE);
        boolean success = cluster.compile(builder, false);
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        Thread.sleep(100);

        cluster.shutDown();

        cluster = new LocalCluster("rejoin.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ONE_RECOVERING);
        success = cluster.compile(builder, false);
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
        boolean success = cluster.compile(builder, false);
        assertTrue(success);
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);

        cluster.startUp();
        cluster.shutDownSingleHost(0);
        Thread.sleep(100);

        String username = URLEncoder.encode("ry@nlikesthe", "UTF-8");
        String password = URLEncoder.encode("y@nkees", "UTF-8");

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        config.m_rejoinToHostAndPort = username + ":" + password + "@localhost:21213";
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Thread.sleep(100);

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient();
        client.createConnection("localhost", "ry@nlikesthe", "y@nkees");
        response = client.callProcedure("InsertSinglePartition", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        client = ClientFactory.createClient();
        client.createConnection("localhost", 21213, "ry@nlikesthe", "y@nkees");
        response = client.callProcedure("InsertSinglePartition", 2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("Insert", 3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.close();

        localServer.shutdown();
        cluster.shutDown();
    }
}
