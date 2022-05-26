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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportRejoin extends TestExportBaseSocketExport {
    private static final int k_factor = 1;

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        ExportLocalClusterBase.resetDir();
        super.setUp();

        startListener();
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, k_factor+1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeSocketExporterClientAndServer();
    }

    public void testExportAndThenRejoinUpdatesExportFlow() throws Exception {
        System.out.println("testExportAndThenRejoinClearsExportOverflow");
        String targetStream = "S_NO_NULLS";
        Client client = getClient();
        for (int i = 0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, targetStream, i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams(targetStream, i, rowdata);
            client.callProcedure("ExportInsertNoNulls", params);
        }
        client.drain();

        ((LocalCluster) m_config).killSingleHost(1);
        Thread.sleep(500);
        client = getClientToSubsetHosts(new int[] {0, 2});

        for (int i = 10; i < 20; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, targetStream, i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams(targetStream, i, rowdata);
            client.callProcedure("ExportInsertNoNulls", params);
        }
        client.drain();
        // We need to make sure that the buffer has drained on 2 of the three hosts. Otherwise,
        // the buffer could be dropped by both hosts that are being killed.
        client.callProcedure("@Quiesce");

        ((LocalCluster) m_config).recoverOne(1, null);
        Thread.sleep(500);

        ((LocalCluster) m_config).killSingleHost(2);
        Thread.sleep(500);
        client = getClientToSubsetHosts(new int[] {0, 1});

        for (int i = 20; i < 30; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, targetStream, i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams(targetStream, i, rowdata);
            client.callProcedure("ExportInsertNoNulls", params);
        }
        client.drain();

        ((LocalCluster) m_config).recoverOne(2, null);
        Thread.sleep(500);
        client = getClientToSubsetHosts(new int[] {0, 1, 2});

        // must still be able to verify the export data. (wait for 2 minutes max
        m_verifier.waitForTuplesAndVerify(client, Duration.ofMinutes(2));
    }

    public TestExportRejoin(final String name) {
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
            new MultiConfigSuiteBuilder(TestExportRejoin.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-nonulls-ddl-with-target.sql"));

        wireupExportTableToSocketExport("S_NO_NULLS");

        project.addProcedures(NONULLS_PROCEDURES);
        config = new LocalCluster("export-ddl-cluster-rep.jar", 8, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
}
