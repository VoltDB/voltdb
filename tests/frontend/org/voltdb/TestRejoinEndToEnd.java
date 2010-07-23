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
import org.voltdb.messaging.HostMessenger;
import org.voltdb.network.VoltNetwork;
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
        builder.addStmtProcedure("Insert", "insert into blah values (?);");
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
        copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        assertTrue(success);

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

        host2.shutdown();

        return retval;
    }

    boolean failNext(int i) throws Exception {
        Context context = getServerReadyToReceiveNewNode();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost", null, null);

        ServerSocketChannel listener = null;
        try {
            listener = ServerSocketChannel.open();
            listener.socket().bind(new InetSocketAddress(VoltDB.DEFAULT_INTERNAL_PORT + 1));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(-1);
        }

        SyncCallback scb = new SyncCallback();
        boolean success = false;
        while (!success) {
            success = client.callProcedure(scb, "@Rejoin", "localhost", VoltDB.DEFAULT_INTERNAL_PORT + 1);
            if (!success) Thread.sleep(100);
        }

        SocketChannel socket = listener.accept();
        listener.close();
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.socket().getOutputStream()));
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.socket().getInputStream()));

        int hostId = in.readInt();
        assertEquals(hostId, 1);

        //COMMAND_SENDTIME_AND_CRC
        out.writeInt(4);
        out.flush();
        // ignore what the other host says the time is
        in.readLong();
        // fake a clock skew of 1ms
        out.writeLong(1);
        // COMMAND_COMPLETE
        out.writeInt(3);
        out.flush();

        //Thread.sleep(100000);

        scb.waitForResponse();
        @SuppressWarnings("unused")
        ClientResponse response = scb.getResponse();

        socket.close();
        context.localServer.shutdown();
        context.localServer.join();

        // this means there is nothing else to try
        return false;
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
        for (int i = 0; failNext(i); i++);
    }

    /*public void testRejoin() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        LocalCluster cluster = new LocalCluster("rejoin.jar", 1, 2, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.compile(builder);
        cluster.setHasLocalServer(false);

        cluster.startUp();
        cluster.shutDownSingleHost(0);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = "rejoin.jar";
        config.m_rejoinToHostAndPort = "localhost:21212";
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Thread.sleep(100);

        localServer.shutdown();
        cluster.shutDown();
    }*/
}
