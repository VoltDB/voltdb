/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketAppender;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.VoltFile;

/**
 * End to end Import tests using the injected socket importer.
 *
 */

public class TestImportSuite extends RegressionSuite {
    private static final Logger s_testSocketLogger = Logger.getLogger("testSocketLogger");
    private static final Level[] s_levels =
        { Level.DEBUG, Level.ERROR, Level.FATAL, Level.INFO, Level.TRACE, Level.WARN };

    private Boolean m_socketHandlerInitialized = false;

    @Override
    public void setUp() throws Exception
    {
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();

        super.setUp();
    }

    private void setupLog4jSocketHandler() {
        synchronized(m_socketHandlerInitialized) {
            if (m_socketHandlerInitialized) return;

            SocketAppender appender = new SocketAppender("localhost", 6060);
            appender.setReconnectionDelay(50);
            s_testSocketLogger.setAdditivity(false);
            s_testSocketLogger.removeAllAppenders();
            s_testSocketLogger.setLevel(Level.ALL);
            s_testSocketLogger.addAppender(appender);
            m_socketHandlerInitialized = true;
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    abstract class DataPusher extends Thread {
        private final int m_count;
        private final CountDownLatch m_latch;
        private final char m_separator;

        public DataPusher(int count, CountDownLatch latch, char separator) {
            m_count = count;
            m_latch = latch;
            m_separator = separator;
        }

        protected abstract void initialize();
        protected abstract void close();
        protected abstract void pushData(String str) throws Exception;

        @Override
        public void run() {
            initialize();

            try {
                for (int icnt = 0; icnt < m_count; icnt++) {
                    String s = String.valueOf(System.nanoTime() + icnt) + m_separator + System.currentTimeMillis() + "\n";
                    pushData(s);
                    Thread.sleep(0, 1);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                close();
                m_latch.countDown();
            }
        }

    }

    class SocketDataPusher extends DataPusher {
        private final String m_server;
        private final int m_port;
        private OutputStream m_sout;

        public SocketDataPusher(String server, int port, int count, CountDownLatch latch, char separator) {
            super(count, latch, separator);
            m_server = server;
            m_port = port;
        }

        @Override
        protected void initialize() {
            m_sout = connectToOneServerWithRetry(m_server, m_port);
            System.out.printf("Connected to VoltDB socket importer at: %s.\n", m_server + ":" + m_port);
        }

        @Override
        protected void pushData(String str) throws Exception {
            m_sout.write(str.getBytes());
        }

        @Override
        protected void close() {
            try {
                m_sout.flush();
                m_sout.close();
            } catch (IOException ex) {
            }
        }
    }

    class Log4jDataPusher extends DataPusher {

        private final Random random = new Random();

        public Log4jDataPusher(int count, CountDownLatch latch, char separator) {
            super(count, latch, separator);
        }

        @Override
        protected void initialize() {
            TestImportSuite.this.setupLog4jSocketHandler();
        }

        @Override
        protected void pushData(String str) throws Exception {
            s_testSocketLogger.log(s_levels[random.nextInt(s_levels.length)], str);
        }

        @Override
        protected void close() {
        }
    }

    private void pushDataToImporters(int count, int loops) throws Exception {
        CountDownLatch latch = new CountDownLatch(2*loops);
        for (int i=0; i<loops; i++) {
            (new SocketDataPusher("localhost", 7001, count, latch, '\t')).start();
            (new Log4jDataPusher(count, latch, ',')).start();
        }
        latch.await();
    }

    private static Map<String, String> expectedStatRows = new HashMap<>();
    static {
        expectedStatRows.put("SocketServerImporter", "importTable.insert");
        expectedStatRows.put("Log4jSocketHandlerImporter", "log_events.insert");
    };
    private static final String CONN_HOST_COL = "CONNECTION_HOSTNAME";
    private static final String PROC_NAME_COL = "PROCEDURE_NAME";
    private static final String INVOCATIONS_COL = "INVOCATIONS";

    private void verifyData(Client client, int count) throws Exception {
        verifyData(client, count, -1);
    }

    private void verifyData(Client client, int count, int min) throws Exception {
        //Wait 20 sec to get out of backpressure.
        long end = System.currentTimeMillis() + 20000;
        boolean success = false;
        String error = "";
        while (System.currentTimeMillis() < end) {
            int scnt = 0;
            ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from importTable");
            assertEquals(ClientResponse.SUCCESS, response.getStatus());

            if (count == response.getResults()[0].asScalarLong()) scnt++;

            response = client.callProcedure("@AdHoc", "select count(*) from log_events");
            assertEquals(ClientResponse.SUCCESS, response.getStatus());
            if (min<0) {
                if (count == response.getResults()[0].asScalarLong()) scnt++;
            } else {
                long result = response.getResults()[0].asScalarLong();
                if (result >= min && result <= count) {
                    scnt++;
                } else {
                    error = result + " not between " + min + " and " + count;
                }
            }
            if (scnt == 2) {
                success = true;
                break;
            }
            Thread.sleep(50);
        }
        assertTrue(error, success);

       ClientResponse response = client.callProcedure("@Statistics", "Initiator", 0);
       VoltTable stats = response.getResults()[0];
       int foundCount = 0;
       for (int i=0; i<stats.getRowCount(); i++) {
           VoltTableRow row = stats.fetchRow(i);
           String name = row.getString(CONN_HOST_COL);
           if (!expectedStatRows.containsKey(name)) {
               continue;
           }
           foundCount++;
           assertEquals(expectedStatRows.get(name), row.getString(PROC_NAME_COL));
           long invocations = row.getLong(INVOCATIONS_COL);
           if (min<0) {
               assertEquals(count, invocations);
           } else {
               assertTrue(invocations>=min && invocations <= count);
           }
       }

       assertEquals(expectedStatRows.size(), foundCount);
    }

    public void testImportSimpleData() throws Exception {
        System.out.println("testImportSimpleData");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        pushDataToImporters(100, 1);
        verifyData(client, 100);
        client.close();
    }

    public void testImportMultipleTimes() throws Exception {
        System.out.println("testImportUpdateApplicationCatalog");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        pushDataToImporters(100, 1);
        verifyData(client, 100);

        Thread.sleep(0, 1);

        pushDataToImporters(100, 1);
        verifyData(client, 200);

        client.close();
    }

    public void testImportMultipleClientsInParallel() throws Exception {
        System.out.println("testImportMultipleClientsInParallel");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        pushDataToImporters(100, 2);
        verifyData(client, 100*2);
        client.close();
    }

    public void testImportMultipleClientsUpdateApplicationCatalogWhenNotPushing() throws Exception {
        System.out.println("testImportMultipleClientsUpdateApplicationCatalogWhenNotPushing");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        pushDataToImporters(1000, 3);
        verifyData(client, 3000);

        ClientResponse response = client.callProcedure("@AdHoc", "create table nudge(id integer);");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        pushDataToImporters(1000, 4);
        // log4j will lose some events because of reconnection delay
        verifyData(client, 7000, 3001);

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
        //Specify bundle location
        String bundleLocation = System.getProperty("user.dir") + "/bundles";
        System.out.println("Bundle location is: " + bundleLocation);
        additionalEnv.put("voltdbbundlelocation", bundleLocation);

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestImportSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-import-ddl.sql"));

        // configure socket importer
        Properties props = buildProperties(
                "port", "7001",
                "decode", "true",
                "procedure", "importTable.insert");
        project.addImport(true, "custom", "tsv", "socketstream.jar", props);
        project.addPartitionInfo("importTable", "PKEY");

        // configure log4j socket handler importer
        props = buildProperties(
                "port", "6060",
                "procedure", "log_events.insert",
                "log-event-table", "log_events");
        project.addImport(true, "custom", null, "log4jsocketimporter.jar", props);

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
