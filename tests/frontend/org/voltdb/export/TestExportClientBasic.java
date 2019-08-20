/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.localserver.LocalTestServer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

/**
 * Smoke tests for the standard export clients.
 */
public class TestExportClientBasic extends JUnit4LocalClusterTest {
    
    /**
     * Struct like class to hold different export configurations
     * required for different export types.
     */
    private static class ExportConfig {
        final String m_className;
        final Properties m_config;
        
        @SafeVarargs
        ExportConfig(String className, Pair<String, String> ...configs) {
            m_className = className;
            m_config = new Properties();
            for (Pair<String, String> pair : configs) {
                m_config.put(pair.getFirst(), pair.getSecond());
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
    private static Map<String, ExportConfig> s_exportConfigs = new HashMap<>();
    static {
        s_exportConfigs.put("File", new ExportConfig("org.voltdb.exportclient.ExportToFileClient",
                                                     new Pair<String, String>("type", "csv"),
                                                     new Pair<String, String>("nonce", "basic1")));
        s_exportConfigs.put("Http", new ExportConfig("org.voltdb.exportclient.HttpExportClient",
                                                     new Pair<String, String>("method", "post"),
                                                     new Pair<String, String>("type", "form")));
        s_exportConfigs.put("Jdbc", new ExportConfig("org.voltdb.exportclient.JDBCExportClient",
                                                     new Pair<String, String>("jdbcdriver", HSQL_DRIVER),
                                                     new Pair<String, String>("jdbcurl", JDBC_URL),
                                                     new Pair<String, String>("jdbcuser", JDBC_USER)));
    }
    
    private LocalCluster m_cluster;
    private Client m_client;
    private LocalTestServer m_httpServer;
    private int m_count = 10;
    
    @Rule
    public final TestName m_name = new TestName();
    
    /**
     * Creates local cluster with the correct export configurations and 
     * starts up any required test servers, like HttpServer.
     * @throws Exception
     */
    @Before
    public void setup() throws Exception {
        System.out.println(m_name.getMethodName() + " setup...");
        String testType = null;
        for (String type : s_exportConfigs.keySet()) {
            if (m_name.getMethodName().contains(type)) {
                testType = type;
                break;
            }
        }
        assert(testType != null);
        if (testType.equals("Http")) {
            m_httpServer = new LocalTestServer(null, null);
            m_httpServer.start();
            s_exportConfigs.get(testType).m_config.setProperty("endpoint", "http:/" + m_httpServer.getServiceAddress().toString() + "/exporttest");
        }
        VoltProjectBuilder builder = null;
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(S1_SCHEMA);
        builder.setUseDDLSchema(true);
        builder.addExport(true,
                ServerExportEnum.CUSTOM, s_exportConfigs.get(testType).m_className,
                s_exportConfigs.get(testType).m_config,
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
    public void testFileExport() throws Exception {
        runTest();
        verifyFileExportCount(m_cluster, m_count);
    }
    
    @Test(timeout = 30_000)
    public void testHttpExport() throws Exception {
        TestHttpRequestHandler handler = new TestHttpRequestHandler();
        m_httpServer.register("*", handler);
        
        runTest();
        assertEquals(m_count, handler.getNumResponses());
    }
    
    @Test(timeout = 30_000)
    public void testJdbcExport() throws Exception {
        runTest();
        // Cannot verify count because we are using embedded db in the server
    }
    
    private void runTest() throws Exception {
        
        m_client = m_cluster.createClient();
        for (int i=0; i<m_count; i++) {
            m_client.callProcedure("@AdHoc", "INSERT INTO s1 VALUES(" + i + ", " +  i + ")");
            m_client.callProcedure("@Quiesce");
        }
        waitForAllExportToDrain(m_client);
    }
    
    private class TestHttpRequestHandler implements HttpRequestHandler {
        private int m_numResponses = 0;
        @Override
        public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
        {
            m_numResponses++;
        }
        
        public int getNumResponses() {
            return m_numResponses;
        }
    }
    
    private void verifyFileExportCount(LocalCluster cluster, int expectedNumRows) throws IOException {
        int actualRows = 0;
        for(Map.Entry<String, String> entry : cluster.getHostRoots().entrySet()) {
            File voltdbRoot = new File(entry.getValue(), FILE_EXPORT_DIR);
            for (File file : voltdbRoot.listFiles()) {
                if (file.isFile()) {
                    actualRows += countRowsInFile(file);
                }
            }
        }
        assertEquals(expectedNumRows, actualRows);
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
    
    private void waitForAllExportToDrain(Client client) throws Exception {
        boolean drained = false;
        while (!drained) {
            ClientResponse cr = client.callProcedure("@Statistics", "EXPORT", 0);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            VoltTable stats = cr.getResults()[0];
            drained = true;
            while (stats.advanceRow()) {
                if (stats.getLong("TUPLE_PENDING") > 0) {
                    System.out.println("Tuple pending for " +
                            stats.getString("SOURCE") + ":" + stats.getLong("PARTITION_ID") +
                            ": " + stats.getLong("TUPLE_PENDING"));
                    drained = false;
                    break;
                }
            }
            if (!drained) {
                Thread.sleep(250);
            }
        }
    }
}
