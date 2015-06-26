/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import org.voltdb.client.ClientResponse;
import org.voltdb.regressionsuites.RegressionSuite;

/**
 * End to end Import tests using the injected socket importer.
 *
 */

public class TestImportSuite extends RegressionSuite {

    @Override
    public void setUp() throws Exception
    {
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    class DataPusher extends Thread {
        private final String m_server;
        private final int m_port;
        private final int m_count;
        private final CountDownLatch m_latch;

        public DataPusher(String server, int port, int count, CountDownLatch latch) {
            m_server = server;
            m_port = port;
            m_count = count;
            m_latch = latch;
        }

        @Override
        public void run() {
            OutputStream sout = connectToOneServerWithRetry(m_server, m_port);

            System.out.printf("Connected to VoltDB socket importer at: %s.\n", m_server + ":" + m_port);
            try {
                for (int icnt = 0; icnt < m_count; icnt++) {
                    String s = String.valueOf(System.nanoTime() + icnt) + "," + System.currentTimeMillis() + "\n";
                    sout.write(s.getBytes());
                    Thread.sleep(0, 1);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                try {
                    sout.flush();
                } catch (IOException ex) {
                }
                m_latch.countDown();
            }
        }

    }

    public void testImportSimpleData() throws Exception {
        System.out.println("testImportSimpleData");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        CountDownLatch latch = new CountDownLatch(1);
        (new DataPusher("localhost", 7001, 100, latch)).start();
        latch.await();
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from importTable");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(100, response.getResults()[0].asScalarLong());

        client.close();
    }

    public void testImportMultipleTimes() throws Exception {
        System.out.println("testImportUpdateApplicationCatalog");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        CountDownLatch latch = new CountDownLatch(1);
        (new DataPusher("localhost", 7001, 100, latch)).start();
        latch.await();
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from importTable");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(100, response.getResults()[0].asScalarLong());

        Thread.sleep(0, 1);
        latch = new CountDownLatch(1);
        (new DataPusher("localhost", 7001, 100, latch)).start();
        latch.await();
        response = client.callProcedure("@AdHoc", "select count(*) from importTable");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(200, response.getResults()[0].asScalarLong());

        client.close();
    }

    public void testImportMultipleClientsInParallel() throws Exception {
        System.out.println("testImportMultipleClientsInParallel");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        CountDownLatch latch = new CountDownLatch(2);
        (new DataPusher("localhost", 7001, 100, latch)).start();
        (new DataPusher("localhost", 7001, 100, latch)).start();
        latch.await();
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from importTable");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(200, response.getResults()[0].asScalarLong());

        client.close();
    }

    public void testImportMultipleClientsUpdateApplicationCatalogWhenNotPushing() throws Exception {
        System.out.println("testImportMultipleClientsUpdateApplicationCatalogWhenNotPushing");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        CountDownLatch latch = new CountDownLatch(3);
        (new DataPusher("localhost", 7001, 1000, latch)).start();
        (new DataPusher("localhost", 7001, 1000, latch)).start();
        (new DataPusher("localhost", 7001, 1000, latch)).start();
        latch.await();
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from importTable");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(3000, response.getResults()[0].asScalarLong());

        response = client.callProcedure("@AdHoc", "create table nudge(id integer);");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        latch = new CountDownLatch(4);
        (new DataPusher("localhost", 7001, 1000, latch)).start();
        (new DataPusher("localhost", 7001, 1000, latch)).start();
        (new DataPusher("localhost", 7001, 1000, latch)).start();
        (new DataPusher("localhost", 7001, 1000, latch)).start();
        latch.await();
        response = client.callProcedure("@AdHoc", "select count(*) from importTable");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(7000, response.getResults()[0].asScalarLong());

        client.close();
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    static OutputStream connectToOneServerWithRetry(String server, int port) {
        int sleep = 1000;
        while (true) {
            try {
                Socket pushSocket = new Socket(server, port);
                OutputStream out = pushSocket.getOutputStream();
                System.out.printf("Connected to VoltDB node at: %s.\n", server);
                return out;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
    }

    public TestImportSuite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {

        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<String, String>();

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestImportSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-import-ddl.sql"));

        Properties props = new Properties();
        props.putAll(ImmutableMap.<String, String>of(
                "port", "7001",
                "decode", "true",
                "procedure", "importTable.insert"));
        project.addImport(true, "custom", "csv", "org.voltdb.importclient.SocketStreamImporter", props);
        project.addPartitionInfo("importTable", "PKEY");

        /*
         * compile the catalog all tests start with
         */

        config = new LocalCluster("import-ddl-cluster-rep.jar", 4, 1, 0,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);

        return builder;
    }
}
