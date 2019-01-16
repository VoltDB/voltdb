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

package org.voltdb;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportTestExpectedData;
import org.voltdb.export.TestExportBaseSocketExport;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportElasticRemove extends TestExportBaseSocketExport {
    private static final int m_kfactor = 1;
    private static final int m_hostCount = 4;

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();

        startListener();
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, m_kfactor+1);
        m_verifier.m_verifySequenceNumber = false;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeClientAndServer();
    }

    public void testElasitcRemoveWhileExporting() throws Exception {
        System.out.println("testElasitcRemoveWhileExporting");
        if (!MiscUtils.isPro()) {
            return;
        }

        Client client = getFullyConnectedClient();
        for (int i = 0; i < 100; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client, "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            final Object[] paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);
        }

        System.out.println("Starting remove operation");
        startRemove(client);

        for (int i = 100; i < 200; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client, "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            final Object[] paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);
        }

        waitForRemovalToFinish(client);

        // must still be able to verify the export data.
        while (true) {
            try {
                quiesceAndVerifyTarget(client, m_verifier);
                break;
            } catch (ProcCallException pe) {
                System.err.println("Encountered error while trying to verify export target, client connected hostList: " + client.getConnectedHostList());
                pe.printStackTrace();
            }
        }
    }

    private void startRemove(Client client) throws NoConnectionsException, IOException, ProcCallException {
        assertEquals(ClientResponse.SUCCESS, client.callProcedure("@ElasticRemoveNT", 1, "").getStatus());
    }

    private int waitForRemovalToFinish(Client client) throws IOException, InterruptedException {
        int currentHostCount = -1, expectedHostCount = TestExportElasticRemove.m_hostCount - (TestExportElasticRemove.m_kfactor + 1);
        do {
            Thread.sleep(500);
            try {
                ClientResponse cr = client.callProcedure("@SystemInformation", "DEPLOYMENT");

                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                VoltTable table = cr.getResults()[0];
                while (table.advanceRow()) {
                    if ("hostcount".equals(table.getString(0))) {
                        currentHostCount = Integer.parseInt(table.getString(1));
                        break;
                    }
                }
            } catch (NoConnectionsException | ProcCallException e) {
                System.err.println("Encountered error trying to get hostCount, connected hostList: " + client.getConnectedHostList());
                e.printStackTrace();
                System.out.println("New connected hostList: " + client.getConnectedHostList());
            }
        } while (currentHostCount > expectedHostCount);

        assertEquals(expectedHostCount, currentHostCount);
        return currentHostCount;
    }

    public TestExportElasticRemove(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster  config;
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportElasticRemove.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl-with-target.sql"));

        wireupExportTableToSocketExport("ALLOW_NULLS");
        wireupExportTableToSocketExport("NO_NULLS");
        wireupExportTableToSocketExport("ALLOW_NULLS_GRP");
        wireupExportTableToSocketExport("NO_NULLS_GRP");

        project.addProcedures(PROCEDURES);
        config = new LocalCluster("export-ddl-cluster-rep.jar", 4, m_hostCount, m_kfactor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
}
