/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.TheHashinator;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.VoltFile;

public class TestExportSnapshotPreservesSequenceNumber extends TestExportBaseSocketExport {

    private static final int k_factor = 0;

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
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, k_factor+1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeClientAndServer();
    }

    public void testExportSnapshotPreservesSequenceNumber() throws Exception {
        System.out.println("testExportSnapshotPreservesSequenceNumber");
        Client client = getClient();
        for (int i=0; i < 1000; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            // Grp tables added to verifier because they are needed by ExportToFileVerifier
            m_verifier.addRow(client, "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            final Object[] paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);
        }
        client.drain();
        Thread.sleep(2000);
        quiesce(client);
        Thread.sleep(2000);
        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte)1);

        m_config.shutDown();
        m_config.startUp(false);

        client = getClient();

        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");

        for (int i=1000; i < 1025; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            // Grp tables added to verifier because they are needed by ExportToFileVerifier
            m_verifier.addRow(client, "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            final Object[] paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);
        }
        client.drain();

        // must still be able to verify the export data.
        quiesceAndVerify(client, m_verifier);
    }


    public TestExportSnapshotPreservesSequenceNumber(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        LocalCluster config;
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportSnapshotPreservesSequenceNumber.class);

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

        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 1, 1, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(2));
        config.setHasLocalServer(false);
        //TODO: export overflow why is this test expects it to hang around?
        config.setNewCli(false);
        config.setMaxHeap(768);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);

        return builder;
    }
}
