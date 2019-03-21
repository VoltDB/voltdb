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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.TestExportBaseSocketExport.ServerListener;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.VoltFile;

public class TestExportEndToEnd extends JUnit4LocalClusterTest {

    private ServerListener m_serverSocket;
    private LocalCluster m_cluster = null;

    @Before
    public void setUp() {
        try {
            VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
            File f = new File("/tmp/" + System.getProperty("user.name"));
            f.mkdirs();
            VoltFile.resetSubrootForThisProcess();
        } catch (IOException e) {
            fail();
        }
    }

    @After
    public void tearDown() {
        try {
            m_cluster.shutDown();
            TestExportBaseSocketExport.closeSocketExporterClientAndServer();
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testExportRollingRejoin() throws UnknownHostException, IOException, InterruptedException
    {
        m_serverSocket = new ServerListener(5001);
        m_serverSocket.start();


        Client client = null;

        // Use socket exporter
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        TestExportBaseSocketExport.project = new VoltProjectBuilder();
        TestExportBaseSocketExport.project.addLiteralSchema("CREATE STREAM export_table"
                            + " PARTITION ON COLUMN a ("
                            + "  a integer not null,"
                            + "  b varchar(32)"
                            + ");");
        TestExportBaseSocketExport.wireupExportTableToSocketExport("export_table");
        m_cluster = new LocalCluster("testExportRollingRejoin.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setNewCli(true);
        m_cluster.setHasLocalServer(false);
        m_cluster.overrideAnyRequestForValgrind();
        boolean success = m_cluster.compile(TestExportBaseSocketExport.project);
        assertTrue(success);
        m_cluster.startUp(true);

        ClientConfig config = new ClientConfig();
        config.setClientAffinity(true);
        config.setTopologyChangeAware(true);
        config.setConnectionResponseTimeout(4*60*1000);
        config.setProcedureCallTimeout(4*60*1000);
        client = ClientFactory.createClient(config);
        client.createConnection(m_cluster.getListenerAddress(0));

        // create some data into pdb
        for (int i = 0; i < 1000; i++) {
            try {
                client.callProcedure("export_table.insert", i, "deadbeef");
            } catch (ProcCallException e) {
                fail();
            }
        }

        // kill one node
        m_cluster.killSingleHost(1);
        // Keep inserting data
        for (int i = 1000; i < 2000; i++) {
            try {
                client.callProcedure("export_table.insert", i, "deadbeef");
            } catch (ProcCallException e) {
                fail();
            }
        }
        // Still have problem in rejoin
//        m_cluster.rejoinOne(1);

        // wait for partition migration finishes

        // check masters on host 1 to see if they detect the gap
    }
}
