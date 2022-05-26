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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportLocalClusterBase;
import org.voltdb.export.TestExportBase;
import org.voltdb.export.SocketExportTestServer;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;

public class TestPrepareShutdownWithExport extends TestExportBase
{
    private SocketExportTestServer m_serverSocket;
    public TestPrepareShutdownWithExport(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception {
        m_username = "default";
        m_password = "password";
        ExportLocalClusterBase.resetDir();
        super.setUp();
        m_serverSocket = new SocketExportTestServer(5001);
        m_serverSocket.start();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            m_serverSocket.shutdown();
            m_serverSocket = null;
        } catch (Exception e) {}
    }

    public void testPrepareShutdown() throws Exception {

        final Client client2 = this.getClient();
        //add tuples for export
        client2.callProcedure("@AdHoc", "create stream ex export to target ex (i bigint not null)");
        m_streamNames.add("ex");
        for (int i= 0;i < 10000; i++) {
            client2.callProcedure("@AdHoc", "insert into ex values(" + i + ");");
        }
        client2.drain();
        final Client client = getAdminClient();
        ClientResponse resp = client.callProcedure("@PrepareShutdown");
        assertTrue(resp.getStatus() == ClientResponse.SUCCESS);

        //push out export buffer and verify if there are any export queue.
        waitForExportAllRowsDelivered(client2, m_streamNames);
        m_serverSocket.verifyExportedTuples(10000);

        long sum = Long.MAX_VALUE;
        while (sum > 0) {
            resp = client2.callProcedure("@Statistics", "liveclients", 0);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable t = resp.getResults()[0];
            long trxn=0, bytes=0, msg=0;
            if (t.advanceRow()) {
                trxn = t.getLong(6);
                bytes = t.getLong(7);
                msg = t.getLong(8);
                sum =  trxn + bytes + msg;
            }
            System.out.printf("Outstanding transactions: %d, buffer bytes :%d, response messages:%d\n", trxn, bytes, msg);
            Thread.sleep(2000);
        }
        assertTrue (sum == 0);

        try{
            client.callProcedure("@Shutdown");
            fail("@Shutdown fails via admin mode");
        } catch (ProcCallException e) {
            //if execution reaches here, it indicates the expected exception was thrown.
            System.out.println("@Shutdown: cluster has been shutdown via admin mode ");
        }
    }

    static public junit.framework.Test suite() throws Exception {

        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPrepareShutdownWithExport.class);
        Map<String, String> additionalEnv = new HashMap<String, String>();

        //use socket exporter
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(ArbitraryDurationProc.class.getResource("clientfeatures.sql"));
        project.addProcedure(ArbitraryDurationProc.class);
        project.setUseDDLSchema(true);

        //enable export
        Properties props = new Properties();
        props.put("replicated", "true");
        props.put("skipinternals", "true");
        project.addExport(true, ServerExportEnum.CUSTOM, props, "ex");

        LocalCluster config = new LocalCluster("client-all-partitions.jar", 4, 2, 0, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compileWithAdminMode(project, -1, false);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
}
