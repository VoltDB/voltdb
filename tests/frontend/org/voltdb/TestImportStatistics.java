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
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketAppender;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.importer.ImporterStatsCollector;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * End to end Import tests using the injected socket importer.
 *
 */

public class TestImportStatistics extends RegressionSuite {
    private static final Logger s_testSocketLogger = Logger.getLogger("testSocketLogger");
    private static final Level[] s_levels =
        { Level.DEBUG, Level.ERROR, Level.FATAL, Level.INFO, Level.TRACE, Level.WARN };

    private Boolean m_socketHandlerInitialized = false;
    private long m_expectedSocketFailures;
    private long m_expectedLog4jFailures;

    private long m_lastSocketSuccesses = -1;
    private long m_lastSocketFailures = -1;
    private long m_lastLog4jSuccesses = -1;
    private long m_lastLog4jFailures = -1;

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
        private final int m_failureDivisor;
        private long m_expectedFailures;

        public DataPusher(int count, CountDownLatch latch, int failureDivisor) {
            m_count = count;
            m_latch = latch;
            m_failureDivisor = failureDivisor;
        }

        protected abstract void initialize();
        protected abstract void close();
        protected abstract void pushData(String str) throws Exception;

        @Override
        public void run() {
            initialize();

            try {
                for (int icnt = 0; icnt < m_count; icnt++) {
                    long millis = System.currentTimeMillis();
                    String s = String.valueOf(System.nanoTime() + icnt) + "," + millis + "\n";
                    if (millis%m_failureDivisor == 0) {
                        m_expectedFailures++;
                    }
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

        public long getExpectedFailures() {
            return m_expectedFailures;
        }

    }

    class SocketDataPusher extends DataPusher {
        private final String m_server;
        private final int m_port;
        private OutputStream m_sout;

        public SocketDataPusher(String server, int port, int count, CountDownLatch latch, int failureDivisor) {
            super(count, latch, failureDivisor);
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

        public Log4jDataPusher(int count, CountDownLatch latch, int failureDivisor) {
            super(count, latch, failureDivisor);
        }

        @Override
        protected void initialize() {
            TestImportStatistics.this.setupLog4jSocketHandler();
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
        List<DataPusher> socketDataPushers = new ArrayList<>();
        List<DataPusher> log4jDataPushers = new ArrayList<>();
        for (int i=0; i<loops; i++) {
            SocketDataPusher socketPusher = new SocketDataPusher("localhost", 7001, count, latch, 7);
            socketDataPushers.add(socketPusher);
            socketPusher.start();
            Log4jDataPusher log4jPusher = (new Log4jDataPusher(count, latch, 11));
            log4jDataPushers.add(log4jPusher);
            log4jPusher.start();
        }
        latch.await();

        //m_expectedSocketFailures = 0;
        for (DataPusher pusher : socketDataPushers) {
            m_expectedSocketFailures += pusher.getExpectedFailures();
        }
    }

    private void verifyTotal(Client client, int count) throws Exception {
       ClientResponse response = client.callProcedure("@Statistics", "Importer", 0);
       VoltTable stats = response.getResults()[0];
       for (int i=0; i<stats.getRowCount(); i++) {
           VoltTableRow row = stats.fetchRow(i);
           String name = row.getString(ImporterStatsCollector.IMPORTER_NAME_COL);
           long expectedFailures = 0;
           String procName = null;
           if (name.equals("SocketImporter")) {
               expectedFailures = m_expectedSocketFailures;
               procName = "TestImportStatistics$TestStatsProcedure7";
           } else if (name.equals("Log4jSocketHandlerImporter")) {
               expectedFailures = m_expectedLog4jFailures;
               procName = "TestImportStatistics$TestStatsProcedure11";
           } else {
               continue;
           }
           assertEquals(procName, row.getString(ImporterStatsCollector.PROC_NAME_COL));
           assertEquals(expectedFailures, row.getLong(ImporterStatsCollector.FAILURE_COUNT_COL));
           assertEquals(count-expectedFailures, row.getLong(ImporterStatsCollector.SUCCESS_COUNT_COL));
           assertEquals(0, row.getLong(ImporterStatsCollector.PENDING_COUNT_COL));
           assertEquals(0, row.getLong(ImporterStatsCollector.RETRY_COUNT_COL));
       }
    }

    private void verifyInterval(Client client, int count) throws Exception {
       ClientResponse response = client.callProcedure("@Statistics", "Importer", 1);
       VoltTable stats = response.getResults()[0];
       for (int i=0; i<stats.getRowCount(); i++) {
           VoltTableRow row = stats.fetchRow(i);
           String name = row.getString(ImporterStatsCollector.IMPORTER_NAME_COL);
           long expectedFailures = 0;
           long lastFailures = 0;
           long lastSuccesses = 0;
           String procName = null;
           if (name.equals("SocketImporter")) {
               expectedFailures = m_expectedSocketFailures;
               procName = "TestImportStatistics$TestStatsProcedure7";
               lastFailures = (m_lastSocketFailures < 0) ? 0 : m_lastSocketFailures;
               lastSuccesses = (m_lastSocketSuccesses < 0) ? 0 : m_lastSocketSuccesses;
           } else if (name.equals("Log4jSocketHandlerImporter")) {
               expectedFailures = m_expectedLog4jFailures;
               procName = "TestImportStatistics$TestStatsProcedure11";
               lastFailures = (m_lastLog4jFailures < 0) ? 0 : m_lastLog4jFailures;
               lastSuccesses = (m_lastLog4jSuccesses < 0) ? 0 : m_lastLog4jSuccesses;
           } else {
               continue;
           }
           assertEquals(procName, row.getString(ImporterStatsCollector.PROC_NAME_COL));
           assertEquals(expectedFailures-lastFailures, row.getLong(ImporterStatsCollector.FAILURE_COUNT_COL));
           assertEquals(count-expectedFailures-lastSuccesses, row.getLong(ImporterStatsCollector.SUCCESS_COUNT_COL));
           if (name.equals("SocketImporter")) {
               m_lastSocketFailures = expectedFailures;
               m_lastSocketSuccesses = count-expectedFailures;
           } else if (name.equals("Log4jSocketHandlerImporter")) {
               m_lastLog4jFailures = expectedFailures;
               m_lastLog4jSuccesses = count-expectedFailures;
           }
           assertEquals(0, row.getLong(ImporterStatsCollector.PENDING_COUNT_COL));
           assertEquals(0, row.getLong(ImporterStatsCollector.RETRY_COUNT_COL));
       }
    }

    public void testImportSingleClient() throws Exception {
        m_expectedLog4jFailures = 0;
        m_expectedSocketFailures = 0;

        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        pushDataToImporters(100, 1);
        verifyTotal(client, 100);
        verifyInterval(client, 100);
        verifyInterval(client, 100);

        Thread.sleep(0, 1);

        pushDataToImporters(100, 1);
        verifyTotal(client, 200);

        pushDataToImporters(100, 1);
        verifyTotal(client, 300);
        verifyInterval(client, 300);
        verifyInterval(client, 300);

        client.close();
    }

    public void testImportMultipleClientsInParallel() throws Exception {
        m_expectedLog4jFailures = 0;
        m_expectedSocketFailures = 0;

        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        pushDataToImporters(100, 2);
        verifyTotal(client, 100*2);
        verifyInterval(client, 100*2);
        verifyInterval(client, 100*2);

        Thread.sleep(0, 1);

        pushDataToImporters(100, 2);
        verifyTotal(client, 200*2);

        pushDataToImporters(100, 2);
        verifyTotal(client, 300*2);
        verifyInterval(client, 300*2);
        verifyInterval(client, 300*2);

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
                @SuppressWarnings("resource")
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

    public TestImportStatistics(final String name) {
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
            new MultiConfigSuiteBuilder(TestImportStatistics.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-import-ddl.sql"));
        project.addProcedures(TestStatsProcedure7.class);

        // configure socket importer
        Properties props = new Properties();
        props.putAll(ImmutableMap.<String, String>of(
                "port", "7001",
                "decode", "true",
                "procedure", "TestImportStatistics$TestStatsProcedure7"));
        project.addImport(true, "custom", "csv", "socketstream.jar", props);
        project.addPartitionInfo("importTable", "PKEY");

        config = new LocalCluster("import-stats-ddl-cluster-rep.jar", 4, 1, 0,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);

        return builder;
    }

    public static class TestStatsProcedure7 extends VoltProcedure {
        private AtomicLong failedCount = new AtomicLong(0);

        public long run(long pkvalue, long colvalue) {
            if (colvalue%7==0) {
                throw new RuntimeException("Sending back failure from test proc");
            }

            return 0;
        }
    }

    public static class TestStatsProcedure11 extends VoltProcedure {

        public long run(long pkvalue, long colvalue) {
            if (colvalue%11==0) {
                throw new RuntimeException("Sending back failure from test proc");
            }

            return 0;
        }
    }
}
