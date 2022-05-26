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

package org.voltdb.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.localserver.LocalTestServer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.exportclient.kafka.KafkaExportClient;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

/**
 * Smoke tests for the standard export clients.
 * This test runs the Test annotated method with different export types.
 * The configuration required for different export types are set up as test parameters created using method exportConfigs().
 */
@RunWith(Parameterized.class)
public class TestExportClientBasic extends JUnit4LocalClusterTest {

    /**
     * Struct like class to hold different export configurations
     * required for different export types.
     */
    private static class ExportConfig {
        final TestType m_type;
        final String m_className;
        final Properties m_props;

        @SafeVarargs
        ExportConfig(TestType type, String className, Pair<String, String> ...configs) {
            m_type = type;
            m_className = className;
            m_props = new Properties();
            for (Pair<String, String> pair : configs) {
                m_props.put(pair.getFirst(), pair.getSecond());
            }
        }
    }

    private static final String FILE_EXPORT_DIR = "voltdbroot/file_export";
    private static final String S1_SCHEMA =
            "CREATE STREAM s1 "
            + "PARTITION ON COLUMN col1 "
            + "EXPORT TO TARGET s1_target ("
            + "     col1 INTEGER NOT NULL, "
            + "     col2 INTEGER NOT NULL"
            + ");";

    private static final String JDBC_URL = "jdbc:hsqldb:mem:x1";
    private static final String JDBC_USER = "sa";
    private static final String HSQL_DRIVER = "org.hsqldb_voltpatches.jdbcDriver";
    private enum TestType {
        FILE,
        HTTP,
        JDBC,
        KAFKA;
    }

    @Parameterized.Parameters
    public static Collection<ExportConfig> exportConfigs() {
       return Arrays.asList(
        /*
         * To start kafka server:
         *  - Download and untar kafka release
         *  - go to kafka directory
         *  - start zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
         *  - start kafka server: bin/kafka-server-start.sh config/server.properties
        new ExportConfig(TestType.KAFKA, "org.voltdb.exportclient.kafka.KafkaExportClient",
                         new Pair<String, String>("bootstrap.servers", "localhost:9092")),
         */
        new ExportConfig(TestType.FILE, "org.voltdb.exportclient.ExportToFileClient",
                         new Pair<String, String>("type", "csv"),
                         new Pair<String, String>("nonce", "basic1")),
        new ExportConfig(TestType.HTTP, "org.voltdb.exportclient.HttpExportClient",
                         new Pair<String, String>("method", "post"),
                         new Pair<String, String>("type", "form")),
        new ExportConfig(TestType.JDBC, "org.voltdb.exportclient.JDBCExportClient",
                         new Pair<String, String>("jdbcdriver", HSQL_DRIVER),
                         new Pair<String, String>("jdbcurl", JDBC_URL),
                         new Pair<String, String>("jdbcuser", JDBC_USER))
       );
    }

    private final int m_count = 10;
    private LocalCluster m_cluster;
    private Client m_client;
    private LocalTestServer m_httpServer;
    TestHttpRequestHandler m_httpHandler = new TestHttpRequestHandler();
    private final ExportConfig m_exportConfig;

    @Rule
    public final TestName m_name = new TestName();

    public TestExportClientBasic(ExportConfig exportConfig) {
        m_exportConfig = exportConfig;
    }

    /**
     * Creates local cluster with the correct export configurations and
     * starts up any required test servers, like HttpServer.
     * @throws Exception
     */
    @Before
    public void setup() throws Exception {
        System.out.println("Setup with type: " + m_exportConfig.m_type);
        if (m_exportConfig.m_type == TestType.HTTP) {
            m_httpServer = new LocalTestServer(null, null);
            m_httpServer.start();
            m_httpHandler = new TestHttpRequestHandler();
            m_httpServer.register("*", m_httpHandler);
            m_exportConfig.m_props.setProperty("endpoint", "http:/" + m_httpServer.getServiceAddress().toString() + "/exporttest");
        }

        VoltProjectBuilder builder = null;
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(S1_SCHEMA);
        builder.setUseDDLSchema(true);
        builder.addExport(true,
                ServerExportEnum.CUSTOM, m_exportConfig.m_className,
                m_exportConfig.m_props,
                "s1_target");

        int sph = 4;
        int hostCount = 3;
        int kfactor = 1;
        m_cluster = new LocalCluster("testexport.jar", sph, hostCount, kfactor, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setHasLocalServer(false);
        m_cluster.overrideAnyRequestForValgrind();
        assertTrue(m_cluster.compile(builder));
        m_cluster.startUp(true);
    }

    @After
    public void tearDown() {
        if (m_cluster != null) {
            try {
                m_cluster.shutDown();
            } catch(Exception ex) {}
        }

        if (m_client != null) {
            try {
                m_client.close();
            } catch(Exception ex) {}
        }

        if (m_httpServer != null) {
            try {
                m_httpServer.stop();
            } catch(Exception ex) {}
        }
    }

    @Test(timeout = 30_000)
    public void runTest() throws Exception {
        System.out.println("runTest with type: " + m_exportConfig.m_type);

        m_client = m_cluster.createClient();
        for (int i=0; i<m_count; i++) {
            m_client.callProcedure("@AdHoc", "INSERT INTO s1 VALUES(" + i + ", " +  i + ")");
            m_client.callProcedure("@Quiesce");
        }
        waitForAllExportToDrain(m_client, m_count);
        verifyExport();
    }

    private void verifyExport() throws IOException {
        switch(m_exportConfig.m_type) {
        case FILE : verifyFileExportCount();
            break;
        case HTTP : assertEquals(m_count, m_httpHandler.getNumResponses());
            break;
        case JDBC : // Cannot verify count because we are using embedded db in the server
            break;
        case KAFKA : verifyKafkaExportCount();
            break;
        }
    }

    private void verifyKafkaExportCount() {
        // Read from kafka and verify count
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "exporttest" + System.currentTimeMillis());
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        try {
            consumer.subscribe(Arrays.asList(KafkaExportClient.DEFAULT_EXPORT_PREFIX + "S1"));
            int count = 0;
            while (count < m_count) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    count++;
                }
            }
        } finally {
            consumer.close();
        }
    }

    private void verifyFileExportCount() throws IOException {
        int actualRows = 0;
        for(Map.Entry<String, String> entry : m_cluster.getHostRoots().entrySet()) {
            File voltdbRoot = new File(entry.getValue(), FILE_EXPORT_DIR);
            for (File file : voltdbRoot.listFiles()) {
                if (file.isFile()) {
                    actualRows += countRowsInFile(file);
                }
            }
        }
        assertEquals(m_count, actualRows);
    }

    private int countRowsInFile(File file) throws FileNotFoundException {
        Scanner fileStream = new Scanner(file);
        int numRows = 0;
        try {
            while (fileStream.hasNextLine()) {
                fileStream.nextLine();
                numRows++;
            }
        } finally {
            fileStream.close();
        }

        return numRows;
    }

    private void waitForAllExportToDrain(Client client, int expectedCount) throws Exception {
        boolean drained = false;
        while (!drained) {
            ClientResponse cr = client.callProcedure("@Statistics", "EXPORT", 0);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            VoltTable stats = cr.getResults()[0];
            drained = true;
            int foundCount = 0;
            while (stats.advanceRow()) {
                if (Boolean.parseBoolean(stats.getString("ACTIVE"))) {
                    foundCount += stats.getLong("TUPLE_COUNT");
                }
                if (stats.getLong("TUPLE_PENDING") > 0) {
                    System.out.println("For " +
                            stats.getString("SOURCE") + ":" + stats.getLong("PARTITION_ID") +
                            ": TUPLE_PENDING=" + stats.getLong("TUPLE_PENDING") +
                            ", TUPLE_COUNT=" + stats.getLong("TUPLE_COUNT"));
                    drained = false;
                    break;
                }
            }
            // When making sure it's drained, also make sure that export actually sent all the data (TUPLE_COUNT).
            // Otherwise we may exit drain before records were pushed out from EE.
            drained &= (foundCount == expectedCount);
            if (!drained) {
                Thread.sleep(250);
            }
        }
    }

    private class TestHttpRequestHandler implements HttpRequestHandler {
        private AtomicInteger m_numResponses = new AtomicInteger(0);

        @Override
        public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
        {
            m_numResponses.incrementAndGet();
        }

        public int getNumResponses() {
            return m_numResponses.get();
        }
    }
}
