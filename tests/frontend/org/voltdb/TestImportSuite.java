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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketAppender;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.CatalogUtil;

/**
 * End to end Import tests using the injected socket importer.
 *
 */

public class TestImportSuite extends RegressionSuite {
    private static final Logger s_testSocketLogger = Logger.getLogger("testSocketLogger");
    private static final Level[] s_levels =
        { Level.DEBUG, Level.ERROR, Level.FATAL, Level.INFO, Level.TRACE, Level.WARN };

    private static final String SERVER = "localhost";
    private static final int SOCKET_IMPORTER_PORT = 7001;
    private static final char DELIMITER = '\t';

    private Boolean m_socketHandlerInitialized = false;
    private Client m_client;
    private List<DataPusher> m_dataPushers = new ArrayList<>();
    private CountDownLatch m_dataAwaiter = null;

    @Override
    public void setUp() throws Exception
    {
        FileUtils.deleteDirectory(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();

        super.setUp();

        m_client = getClient();
        if (!m_client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info");
        }
    }

    private void setupLog4jSocketHandler() {
        synchronized(m_socketHandlerInitialized) {
            if (m_socketHandlerInitialized) return;

            SocketAppender appender = new SocketAppender(SERVER, 6060);
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
        m_dataPushers.clear();
        m_client.close();
        super.tearDown();
    }

    /** Allows the test thread to check if the importer is alive by writing data to it. */
    static class ImporterConnector {
        private final String m_server;
        private final int m_port;
        private final char m_separator;
        private int m_counter = 0;

        public ImporterConnector(String server, int port, char separator) {
            m_separator = separator;
            m_server = server;
            m_port = port;
        }

        /** Tries to write data to the importer, but may fail. */
        public void tryPush(int maxAttempts) throws IOException {
            int numConnectAttempts = 0;
            do {
                try {
                    Socket pushSocket = new Socket(m_server, m_port);
                    OutputStream socketStream = pushSocket.getOutputStream();
                    System.out.printf("Connected to VoltDB socket importer at: %s.\n", m_server + ":" + m_port);
                    String s = String.valueOf(m_counter) + m_separator + System.currentTimeMillis() + "\n";
                    socketStream.write(s.getBytes());
                    pushSocket.close();
                    m_counter++;
                    return;
                } catch (IOException e) {
                    numConnectAttempts++;
                    if (numConnectAttempts >= maxAttempts) {
                        throw e;
                    }
                    try {
                        Thread.sleep((int) (Math.random() * 1000) + 500);
                    } catch (InterruptedException ignore) {
                    }
                }
            } while (true);
        }
    }

    abstract class DataPusher extends Thread {
        private final CountDownLatch m_startupLatch;
        private final CountDownLatch m_shutdownLatch;
        private final char m_separator;
        private volatile int m_totalCount;
        private volatile int m_currentCount = 0;
        private volatile Exception m_error = null;

        public DataPusher(int count, CountDownLatch startupLatch, CountDownLatch shutdownLatch, char separator) {
            m_totalCount = count;
            m_startupLatch = startupLatch;
            m_shutdownLatch = shutdownLatch;
            m_separator = separator;
            m_dataPushers.add(this);
        }

        protected abstract void initialize();
        protected abstract void close();
        protected abstract void pushData(String str) throws Exception;

        @Override
        public void run() {
            initialize();
            if (m_totalCount == 0) {
                m_totalCount = Integer.MAX_VALUE; // keep running until explicitly stopped
            }
            m_startupLatch.countDown();
            try {
                while (m_currentCount < m_totalCount) {
                    String s = String.valueOf(System.nanoTime() + m_currentCount) + m_separator + System.currentTimeMillis() + "\n";
                    pushData(s);
                    Thread.sleep(0, 1);
                    m_currentCount++;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                m_error = ex;
            } finally {
                close();
                m_shutdownLatch.countDown();
            }
        }

        public void shutDown() throws Exception {
            if (m_totalCount == Integer.MAX_VALUE) {
                m_totalCount = 0; // this will stop the pusher's forever loop
            }
            if (m_error != null) {
                throw m_error;
            }
        }
    }

    class SocketDataPusher extends DataPusher {
        private final String m_server;
        private final int m_port;
        private OutputStream m_sout;

        public SocketDataPusher(String server, int port, int count, CountDownLatch startupLatch, CountDownLatch shutdownLatch, char separator) {
            super(count, startupLatch, shutdownLatch, separator);
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

        public Log4jDataPusher(int count, CountDownLatch startupLatch, CountDownLatch shutdownLatch, char separator) {
            super(count, startupLatch, shutdownLatch, separator);
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

    private void asyncPushDataToImporters(int count, int loops) throws Exception {
        CountDownLatch startupAwaiter = new CountDownLatch(2*loops);
        m_dataAwaiter = new CountDownLatch(2*loops);
        for (int i=0; i<loops; i++) {
            (new SocketDataPusher(SERVER, SOCKET_IMPORTER_PORT, count, startupAwaiter, m_dataAwaiter, DELIMITER)).start();
            (new Log4jDataPusher(count, startupAwaiter, m_dataAwaiter, ',')).start();
        }
        // ensure pushers are running before test begins
        startupAwaiter.await();
    }

    private void waitForData() throws Exception {
        for (DataPusher pusher : m_dataPushers) {
            pusher.shutDown();
        }
        m_dataAwaiter.await();
    }

    private void pushDataToImporters(int count, int loops) throws Exception {
        asyncPushDataToImporters(count, loops);
        waitForData();
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
        Map<String, Long> statsRollup = new HashMap<>();
        for (int i=0; i<stats.getRowCount(); i++) {
            VoltTableRow row = stats.fetchRow(i);
            String name = row.getString(CONN_HOST_COL);
            if (!expectedStatRows.containsKey(name)) {
                continue;
            }
            assertEquals(expectedStatRows.get(name), row.getString(PROC_NAME_COL));
            long invocations = row.getLong(INVOCATIONS_COL);

            // Importers use more than one internal adapters, they'll show up as multiple rows.
            if (statsRollup.computeIfPresent(name, (k, v) -> v + invocations) == null) {
                statsRollup.put(name, invocations);
            }
        }

        assertEquals(expectedStatRows.size(), statsRollup.size());
        for (long invocations : statsRollup.values()) {
            if (min<0) {
                assertEquals(count, invocations);
            } else {
                assertTrue(invocations>=min && invocations <= count);
            }
        }
    }

    public void testImportSimpleData() throws Exception {
        System.out.println("testImportSimpleData");

        pushDataToImporters(100, 1);
        verifyData(m_client, 100);
    }

    public void testImportMultipleTimes() throws Exception {
        System.out.println("testImportUpdateApplicationCatalog");

        pushDataToImporters(100, 1);
        verifyData(m_client, 100);

        Thread.sleep(0, 1);

        pushDataToImporters(100, 1);
        verifyData(m_client, 200);
    }

    public void testImportMultipleClientsInParallel() throws Exception {
        System.out.println("testImportMultipleClientsInParallel");

        pushDataToImporters(100, 2);
        verifyData(m_client, 100*2);
    }

    public void testImportMultipleClientsUpdateApplicationCatalogWhenNotPushing() throws Exception {
        System.out.println("testImportMultipleClientsUpdateApplicationCatalogWhenNotPushing");

        pushDataToImporters(1000, 3);
        verifyData(m_client, 3000);

        applySchemaChange("create table nudge(id integer);");

        pushDataToImporters(1000, 4);
        // log4j will lose some events because of reconnection delay
        verifyData(m_client, 7000, 3001);
    }

    /** Verify that importer stays running during unrelated schema changes.
     * The data pushers will get IOExceptions if the importers restart.
     */
    public void testImportUnrelatedUACWhilePushing() throws Exception {
        System.out.println("Schema changes that don't affect importers should not result in importer restarts.");

        // Run data pushers until they are explicitly stopped.
        asyncPushDataToImporters(0, 3);

        applySchemaChange("CREATE TABLE nudge(id integer);");
        applySchemaChange("CREATE PROCEDURE NudgeThatDB AS INSERT INTO nudge VALUES (?);");
        applySchemaChange("DROP PROCEDURE NudgeThatDB;");
        applySchemaChange("DROP TABLE nudge;");

        waitForData();
    }

    /** Verify that importer can withstand unrelated UACs without restarting.
     * The data pushers will get IOExceptions if the importers restart.
     */
    public void testImportUnrelatedDeploymentChangesWhilePushing() throws Exception {
        System.out.println("Deployment changes that don't affect importers should not result in them restarting.");

        // Run data pushers until they are explicitly stopped.
        asyncPushDataToImporters(0, 3);

        updateDeploymentFile(true, true);
        updateDeploymentFile(true, false);

        waitForData();
    }

    /** Verify that importer restarts if it is removed from the configuration and then restored.
     */
    public void testImporterDeploymentChanges() throws Exception {
        System.out.println("Removing and re-adding importers should cause them to restart");

        ImporterConnector testConnector = new ImporterConnector(SERVER, SOCKET_IMPORTER_PORT, DELIMITER);
        testConnector.tryPush(5);

        updateDeploymentFile(false, false);
        try {
            testConnector.tryPush(5);
            fail("Importer is still running even though it is no longer configured");
        } catch (IOException expected) {
        }

        updateDeploymentFile(true, false);
        testConnector.tryPush(5);
    }

    public void testImporterWithMissingProcedure() throws Exception {
        System.out.println("test importer with missing procedure.");

        ImporterConnector testConnector = new ImporterConnector(SERVER, SOCKET_IMPORTER_PORT, DELIMITER);
        testConnector.tryPush(5);

        //drop procedure from the importer
        VoltProjectBuilder projectBuilder = generateVoltProject(true, false, true);
        File deploymentFilePath = new File(projectBuilder.compileDeploymentOnly(null, 1, 4, 0, 0));
        deploymentFilePath.deleteOnExit();
        ClientResponse response = UpdateApplicationCatalog.update(m_client, null, deploymentFilePath);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        try {
            testConnector.tryPush(5);
            fail("Importer is still running even though it is no longer configured since the procedure is missing.");
        } catch (IOException expected) {
        }
    }

    /** Verify that importer restarts if the procedure it uses is removed from the configuration and then restored.
     */
    public void testAddRemoveImporterProcedure() throws Exception {
        System.out.println("Removing and adding procedures that the importers rely on should result in a restart");

        ImporterConnector testConnector = new ImporterConnector(SERVER, SOCKET_IMPORTER_PORT, DELIMITER);
        testConnector.tryPush(5);

        // importer uses CRUD procedure associated with this table.
        applySchemaChange("DROP TABLE importTable;");
        try {
            testConnector.tryPush(5);
            fail("Importer is still running even though its procedure is disabled");
        } catch (IOException expected) {
        }

        applySchemaChange("CREATE TABLE IMPORTTABLE (PKEY bigint NOT NULL, A_INTEGER_VALUE bigint); PARTITION TABLE IMPORTTABLE ON COLUMN PKEY;");
        testConnector.tryPush(5);
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

    private void applySchemaChange(String adhocddl) throws Exception {
        ClientResponse response = m_client.callProcedure("@AdHoc", adhocddl);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }

    /** Builds a CatalogContext for the import manager to use.
     * @param includeImporters Whether or not to include the importers associated with this test.
     * @param unrelatedChange Whether or not to make a change that has no impact on the importer.
     * @return New deployment file
     * @throws Exception upon error or test failure
     */
    private void updateDeploymentFile(boolean includeImporters, boolean unrelatedChange) throws Exception {
        VoltProjectBuilder projectBuilder = generateVoltProject(includeImporters, unrelatedChange);
        File deploymentFilePath = new File(projectBuilder.compileDeploymentOnly(null, 1, 4, 0, 0));
        System.out.println("Deployment file " + (includeImporters ? "with" : "without") + " importers, " +
                (unrelatedChange ? "with" : "without") + " command logs written to " + deploymentFilePath.getCanonicalPath());
        deploymentFilePath.deleteOnExit();
        ClientResponse response = UpdateApplicationCatalog.update(m_client, null, deploymentFilePath);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }

    private static VoltProjectBuilder generateVoltProject(boolean includeImporters, boolean unrelatedChange) throws Exception {
        return generateVoltProject(includeImporters, unrelatedChange, false);
    }
    private static VoltProjectBuilder generateVoltProject(boolean includeImporters, boolean unrelatedChange, boolean missingImporterProcedure) throws Exception {
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-import-ddl.sql"));

        // configure socket importer
        Properties props = buildProperties(
                "port", Integer.toString(SOCKET_IMPORTER_PORT),
                "decode", "true");
        if (!missingImporterProcedure) {
            props.put("procedure", "importTable.insert");
        }
        project.addImport(includeImporters, "custom", "tsv", "socketstream.jar", props);
        project.addPartitionInfo("importTable", "PKEY");

        // configure log4j socket handler importer
        props = buildProperties(
                "port", "6060",
                "procedure", "log_events.insert",
                "log-event-table", "log_events");
        project.addImport(includeImporters, "custom", null, "log4jsocketimporter.jar", props);

        project.setHeartbeatTimeoutSeconds(5 + (unrelatedChange ? 1 : 0));
        return project;
    }

    static public junit.framework.Test suite() throws Exception
    {
        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<>();
        //Specify bundle location
        String bundleLocation = System.getProperty("user.dir") + "/bundles";
        System.out.println("Bundle location is: " + bundleLocation);
        additionalEnv.put(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME, bundleLocation);

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestImportSuite.class);

        VoltProjectBuilder project = generateVoltProject(true, false);

        /*
         * compile the catalog all tests start with
         */

        config = new LocalCluster("import-ddl-cluster-rep.jar", 4, 1, 0,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);

        return builder;
    }
}
