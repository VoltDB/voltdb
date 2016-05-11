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
import java.util.concurrent.CountDownLatch;

import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * End to end CSV formatter tests using the injected socket importer.
 *
 */

public class TestCSVStrictQuoteAndBlankErrorSuite extends RegressionSuite {

    @Override
    public void setUp() throws Exception {
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    class SocketDataPusher extends Thread {
        private final String m_server;
        private OutputStream m_sout;
        private final CountDownLatch m_latch;
        private final int m_port;
        private final String[] m_data;

        public SocketDataPusher(String server, int port, CountDownLatch latch, String[] data) {
            m_latch = latch;
            m_server = server;
            m_port = port;
            m_data = data;
        }

        protected void initialize() {
            m_sout = connectToOneServerWithRetry(m_server, m_port);
            System.out.printf("Connected to VoltDB socket importer at: %s.\n", m_server + ":" + m_port);
        }

        @Override
        public void run() {
            initialize();

            try {
                for (int icnt = 0; icnt < m_data.length; icnt++) {
                    m_sout.write(m_data[icnt].getBytes());
                    Thread.sleep(0, 1);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                close();
                m_latch.countDown();
            }
        }

        protected void close() {
            try {
                m_sout.close();
            } catch (IOException ex) {
            }
        }
    }

    public void testStrictQuoteAndBlankError() throws Exception {
        System.out.println("testStrictQuote");

        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        String[] myData = {
                "\"1\",\"1\",\"1\",\"1\",\"a word\",\"1.10\",\"1.11\",\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                "2,2,2,2,a word,1.10,1.11,7777-12-25 14:35:26,POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"\n",
                "3,3,3,3,a word,1.10,1.11,7777-12-25 14:35:26,POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"\n",
                "\"4\",\"1\",\"1\",\"1\",\"a word\",\"1.10\",\"1.11\",\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                "5,\"5\",\"5\",\"5\",,,,,,\n", "\"5\",5,\"5\",\"5\",,,,,,\n", "\"5\",\"5\",,,,,,,,\n", };

        CountDownLatch latch = new CountDownLatch(1);
        (new SocketDataPusher("localhost", 7001, latch, myData)).start();

        VoltTable ts_table = client.callProcedure("@AdHoc", "SELECT * FROM importCSVTable ORDER BY clm_integer;")
                .getResults()[0];
        assertEquals(2, ts_table.getRowCount());
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
            } catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                }
                if (sleep < 8000)
                    sleep += sleep;
            }
        }
    }

    public TestCSVStrictQuoteAndBlankErrorSuite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception {
        Properties formatConfig = new Properties();
        formatConfig.setProperty("custom.null.string", "test");
        formatConfig.setProperty("separator", ",");
        formatConfig.setProperty("blank", "error");
        formatConfig.setProperty("escape", "\\");
        formatConfig.setProperty("quotechar", "\"");
        formatConfig.setProperty("strictquotes", "true");

        return buildEnv(formatConfig);
    }

    static public MultiConfigSuiteBuilder buildEnv(Properties formatConfig) throws Exception {
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCSVStrictQuoteAndBlankErrorSuite.class);
        Map<String, String> additionalEnv = new HashMap<String, String>();
        //Specify bundle location
        String bundleLocation = System.getProperty("user.dir") + "/bundles";
        System.out.println("Bundle location is: " + bundleLocation);
        additionalEnv.put("voltdbbundlelocation", bundleLocation);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-import-ddl.sql"));
        project.addPartitionInfo("importCSVTable", "clm_integer");

        // configure socket importer
        Properties props = new Properties();
        props.putAll(ImmutableMap.<String, String> of("port", "7001", "decode", "true", "procedure",
                "importCSVTable.insert"));

        project.addImport(true, "custom", "csv", "socketstream.jar", props, formatConfig);
        project.addPartitionInfo("importCSVTable", "clm_integer");

        /*
         * compile the catalog all tests start with
         */

        LocalCluster config = new LocalCluster("import-ddl-cluster-rep.jar", 4, 1, 0, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
}
