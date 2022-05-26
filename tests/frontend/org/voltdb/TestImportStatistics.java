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

package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketAppender;
import org.voltdb.SiteStatsSource.SiteStats;
import org.voltdb.StatsSource.StatsCommon;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.importer.ImporterStatsCollector.Import;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.TestSQLTypesSuite;

/**
 * Import statistics tests using the socket importer and a test log4j importer.
 */

public class TestImportStatistics extends RegressionSuite {
    private static final String SERVER_SOCKET_IMPORTER_NAME = "SocketServerImporter";
    private static final Logger s_testSocketLogger = Logger.getLogger("testSocketLogger");
    private static final Level[] s_levels =
        { Level.DEBUG, Level.ERROR, Level.FATAL, Level.INFO, Level.TRACE, Level.WARN };

    private Boolean m_socketHandlerInitialized = false;
    // Identify socket data pushers by the procedure they use
    private Map<String, Long> m_expectedSocketFailures = new HashMap<>();;

    private Map<String, Long> m_lastSocketSuccesses = new HashMap<>();;
    private Map<String, Long> m_lastSocketFailures = new HashMap<>();;
    private long m_lastLog4jSuccesses = -1;

    @Override
    public void setUp() throws Exception
    {
        FileUtils.deleteDirectory(new File("/tmp/" + System.getProperty("user.name")));
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
        private final boolean m_goSlow;

        public DataPusher(int count, CountDownLatch latch, int failureDivisor, boolean goSlow) {
            m_count = count;
            m_latch = latch;
            m_failureDivisor = failureDivisor;
            m_goSlow = goSlow;
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
                    String s = String.valueOf(System.nanoTime() + icnt) + "," + millis + "," +
                        (m_goSlow ? 1 : 0) + "\n";
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
        private final String m_procName;
        private OutputStream m_sout;

        public SocketDataPusher(String server, int port, String procName, int count, CountDownLatch latch, int failureDivisor, boolean goSlow) {
            super(count, latch, failureDivisor, goSlow);
            m_server = server;
            m_port = port;
            m_procName = procName;
        }

        public String getProcName() {
            return m_procName;
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
            super(count, latch, failureDivisor, false);
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
        pushDataToImporters(count, loops, false);
    }

    private void pushDataToImporters(int count, int loops, boolean goSlow) throws Exception {
        CountDownLatch latch = new CountDownLatch(3*loops);
        List<SocketDataPusher> socketDataPushers = new ArrayList<>();
        for (int i=0; i<loops; i++) {
            SocketDataPusher socketPusher = new SocketDataPusher("localhost", 7001, "TestImportStatistics$TestStatsProcedure7", count, latch, 7, goSlow);
            socketDataPushers.add(socketPusher);
            socketPusher.start();
            socketPusher = new SocketDataPusher("localhost", 7002, "TestImportStatistics$TestStatsProcedure11", count, latch, 11, goSlow); // second socket importer
            socketDataPushers.add(socketPusher);
            socketPusher.start();

            (new Log4jDataPusher(count, latch, 11)).start();;
        }
        latch.await();

        for (SocketDataPusher pusher : socketDataPushers) {
            String procName = pusher.getProcName();
            Long currValue = m_expectedSocketFailures.get(procName);
            long newValue = ((currValue==null) ? 0 : currValue.longValue()) + pusher.getExpectedFailures();
            m_expectedSocketFailures.put(procName, newValue);
        }
    }

    private static Set<String> expectedImporters = new HashSet<>();
    static {
        expectedImporters.add("SocketServerImporter");
        expectedImporters.add("Log4jSocketHandlerImporter");
    };
    private static final String CONN_HOST_COL = "CONNECTION_HOSTNAME";
    private static final String PROC_NAME_COL = "PROCEDURE_NAME";
    private static final String INVOCATIONS_COL = "INVOCATIONS";

    private void waitForLogEvents(Client client, int count) throws Exception {
        //Wait 60 sec to get out of backpressure.
        long end = System.currentTimeMillis() + 60000;
        int successCount = 0;
        Map<String, Boolean> expectedInovcations = new HashMap<>();
        {
            expectedInovcations.put("TestImportStatistics$TestStatsProcedure7",
                            false);
            expectedInovcations.put("TestImportStatistics$TestStatsProcedure11",
                            false);
            expectedInovcations.put("log_events.insert",
                            false);
        };
        while (System.currentTimeMillis() < end) {
            ClientResponse response = client.callProcedure("@Statistics", "INITIATOR", 0);
            assertEquals(ClientResponse.SUCCESS, response.getStatus());
            VoltTable stats = response.getResults()[0];
            for (int i=0; i<stats.getRowCount(); i++) {
                VoltTableRow row = stats.fetchRow(i);
                String name = row.getString(CONN_HOST_COL);
                if (!expectedImporters.contains(name)) {
                    continue;
                }

                String proc = row.getString(PROC_NAME_COL);
                assert(expectedInovcations.containsKey(proc));
                if (!expectedInovcations.get(proc)) {
                    long invocations = row.getLong(INVOCATIONS_COL);
                    if (count == invocations) {
                        expectedInovcations.put(proc, true);
                        successCount += 1;
                    }

                }
            }
            if (successCount == expectedInovcations.size()) {
                    break;
            }
            Thread.sleep(50);
        }
        if (successCount < expectedInovcations.size()) {
            fail("fail to get out backpressure for 40 Sec");
        }
    }

    private void verifyTotal(Client client, int count) throws Exception {
        waitForLogEvents(client, count);

        ClientResponse response = client.callProcedure("@Statistics", "Importer", 0);
        VoltTable stats = response.getResults()[0];
        int numSocketsFound = 0;
        boolean foundLog4j = false;
        for (int i=0; i<stats.getRowCount(); i++) {
            VoltTableRow row = stats.fetchRow(i);
            String name = row.getString(Import.IMPORTER_NAME.name());
            long expectedFailures = 0;
            String procName = row.getString(Import.PROCEDURE_NAME.name());
            if (SERVER_SOCKET_IMPORTER_NAME.equals(name)) {
                expectedFailures = m_expectedSocketFailures.get(procName);
                numSocketsFound++;
            } else if (name.equals("Log4jSocketHandlerImporter")) {
                expectedFailures = 0;
                procName = "log_events.insert";
                foundLog4j = true;
            } else {
                continue;
            }
            assertEquals(procName, row.getString(Import.PROCEDURE_NAME.name()));
            assertEquals(expectedFailures, row.getLong(Import.FAILURES.name()));
            assertEquals(count-expectedFailures, row.getLong(Import.SUCCESSES.name()));
            assertEquals(0, row.getLong(Import.OUTSTANDING_REQUESTS.name()));
            assertEquals(0, row.getLong(Import.RETRIES.name()));
            assertNotNull(row.getLong(StatsCommon.TIMESTAMP.name()));
            assertNotNull(row.getLong(StatsCommon.HOST_ID.name()));
            assertNotNull(row.getString(StatsCommon.HOSTNAME.name()));
            assertNotNull(row.getLong(SiteStats.SITE_ID.name()));
        }

        assertTrue(numSocketsFound==2 && foundLog4j);
    }

    private void verifyInterval(Client client, int count) throws Exception {
        ClientResponse response = client.callProcedure("@Statistics", "Importer", 1);
        VoltTable stats = response.getResults()[0];
        int numSocketsFound = 0;
        boolean foundLog4j = false;
        for (int i=0; i<stats.getRowCount(); i++) {
            VoltTableRow row = stats.fetchRow(i);
            String name = row.getString(Import.IMPORTER_NAME.name());
            long expectedFailures = 0;
            long lastFailures = 0;
            long lastSuccesses = 0;
            String procName = row.getString(Import.PROCEDURE_NAME.name());
            if (SERVER_SOCKET_IMPORTER_NAME.equals(name)) {
                expectedFailures = m_expectedSocketFailures.get(procName);
                lastFailures = m_lastSocketFailures.containsKey(procName) ? m_lastSocketFailures.get(procName) : 0;
                lastSuccesses = m_lastSocketSuccesses.containsKey(procName) ? m_lastSocketSuccesses.get(procName) : 0;
                numSocketsFound++;
            } else if (name.equals("Log4jSocketHandlerImporter")) {
                expectedFailures = 0;
                procName = "log_events.insert";
                lastFailures = 0;
                lastSuccesses = (m_lastLog4jSuccesses < 0) ? 0 : m_lastLog4jSuccesses;
                foundLog4j = true;
            } else {
                continue;
            }
            assertEquals(procName, row.getString(Import.PROCEDURE_NAME.name()));
            assertEquals(expectedFailures-lastFailures, row.getLong(Import.FAILURES.name()));
            assertEquals(count-expectedFailures-lastSuccesses, row.getLong(Import.SUCCESSES.name()));
            if (SERVER_SOCKET_IMPORTER_NAME.equals(name)) {
                m_lastSocketFailures.put(procName, expectedFailures);
                m_lastSocketSuccesses.put(procName, count-expectedFailures);
            } else if (name.equals("Log4jSocketHandlerImporter")) {
                m_lastLog4jSuccesses = count;
            }
            assertEquals(0, row.getLong(Import.OUTSTANDING_REQUESTS.name()));
            assertEquals(0, row.getLong(Import.RETRIES.name()));
            assertNotNull(row.getLong(StatsCommon.TIMESTAMP.name()));
            assertNotNull(row.getLong(StatsCommon.HOST_ID.name()));
            assertNotNull(row.getString(StatsCommon.HOSTNAME.name()));
            assertNotNull(row.getLong(SiteStats.SITE_ID.name()));
        }

        assertTrue(numSocketsFound==2 && foundLog4j);
    }

    public void testImportSingleClient() throws Exception {
        m_expectedSocketFailures.clear();

        Client client = getClient();
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info");
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
        m_expectedSocketFailures.clear();

        Client client = getClient();
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info");
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

    // leave this disabled because this is not perfectly reliable and is slow. Enable and run manually as needed.
    public void donttestPending() throws Exception {
        Client client = getClient();
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info");
        }

        pushDataToImporters(100, 1, true);
        waitForLogEvents(client, 100);

        long lastPending = 0;
        ClientResponse response = client.callProcedure("@Statistics", "Importer", 0);
        VoltTable stats = response.getResults()[0];
        boolean found = false;
        for (int i=0; i<stats.getRowCount(); i++) {
            VoltTableRow row = stats.fetchRow(i);
            String name = row.getString(Import.IMPORTER_NAME.name());
            String procName = row.getString(Import.PROCEDURE_NAME.name());
            if (!SERVER_SOCKET_IMPORTER_NAME.equals(name)) {
                continue;
            }
            assertEquals(procName, row.getString(Import.PROCEDURE_NAME.name()));
            assertEquals(0, row.getLong(Import.FAILURES.name()));
            long pending = row.getLong(Import.OUTSTANDING_REQUESTS.name());
            if (pending > 0) {
                found = true;
            }
            assertEquals(100-pending, row.getLong(Import.SUCCESSES.name()));
            assertEquals(0, row.getLong(Import.RETRIES.name()));
            lastPending = pending;
        }

        assertTrue(found);

        // try to see if pending goes down for 10 seconds or so
        long endTime = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() <= endTime) {
            response = client.callProcedure("@Statistics", "Importer", 0);
            stats = response.getResults()[0];
            found = false;
            for (int i=0; i<stats.getRowCount(); i++) {
                VoltTableRow row = stats.fetchRow(i);
                String name = row.getString(Import.IMPORTER_NAME.name());
                if (!SERVER_SOCKET_IMPORTER_NAME.equals(name)) {
                    continue;
                }
                assertEquals(0, row.getLong(Import.FAILURES.name()));
                long pending = row.getLong(Import.OUTSTANDING_REQUESTS.name());
                if (pending < lastPending) {
                    lastPending = pending;
                    found = true;
                    assertEquals(100-pending, row.getLong(Import.SUCCESSES.name()));
                    assertEquals(0, row.getLong(Import.RETRIES.name()));
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) { }
            }
            if (found) {
                break;
            }
        }

        assertTrue(found);
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
        Map<String, String> additionalEnv = new HashMap<>();
        //Specify bundle location
        String bundleLocation = System.getProperty("user.dir") + "/bundles";
        System.out.println("Bundle location is: " + bundleLocation);
        additionalEnv.put("voltdbbundlelocation", bundleLocation);

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestImportStatistics.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-import-ddl.sql"));
        project.addProcedure(TestStatsProcedure7.class);
        project.addProcedure(TestStatsProcedure11.class);

        // configure socket importer
        Properties props = buildProperties(
                "port", "7001",
                "decode", "true",
                "procedure", "TestImportStatistics$TestStatsProcedure7");
        project.addImport(true, "custom", "csv", "socketstream.jar", props);
        project.addPartitionInfo("importTable", "PKEY");

        // another socket importer
        props = buildProperties(
                "port", "7002",
                "decode", "true",
                "procedure", "TestImportStatistics$TestStatsProcedure11");
        project.addImport(true, "custom", "csv", "socketstream.jar", props);
        project.addPartitionInfo("importTable", "PKEY");

        // configure log4j socket handler importer
        props = buildProperties(
                "port", "6060",
                "procedure", "log_events.insert",
                "log-event-table", "log_events");
        project.addImport(true, "custom", null, "log4jsocketimporter.jar", props);

        config = new LocalCluster("import-stats-ddl-cluster-rep.jar", 4, 1, 0,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);

        return builder;
    }

    public static class TestStatsProcedure7 extends VoltProcedure {
        public long run(long pkvalue, long colvalue, int goslow) {
            if (goslow > 0) {
                // sleep a bit to slow things down
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) { }
            } else if (colvalue%7==0) {
                throw new RuntimeException("Sending back failure from test proc");
            }

            return 0;
        }
    }

    public static class TestStatsProcedure11 extends VoltProcedure {
        public long run(long pkvalue, long colvalue, int goslow) {
            if (goslow > 0) {
                // sleep a bit to slow things down
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) { }
            } else if (colvalue%11==0) {
                throw new RuntimeException("Sending back failure from test proc");
            }

            return 0;
        }
    }
}
