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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.voltdb.FlakyTestRule.Flaky;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportLocalClusterBase;
import org.voltdb.export.SocketExportTestServer;
import org.voltdb.export.TestExportBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.utils.MiscUtils;

/**
 * Listens for connections from socket export and then counts expected rows.
 * @author akhanzode
 */
public class TestExportSuiteReplicatedSocketExportRecover extends TestExportBase {
    @Rule
    public FlakyTestRule ftRule = new FlakyTestRule();

    private static SocketExportTestServer m_serverSocket;
    private static LocalCluster config;
    private static List<String> exStream = new ArrayList<>(Arrays.asList("ex"));

    @Override
    public void setUp() throws Exception
    {
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

    @Test
    @Flaky(description="TestExportSuiteReplicatedSocketExportRecover.testExportReplicatedExportToSocketRecover")
    public void testExportReplicatedExportToSocketRecover() throws Exception {
        if (config.isValgrind()) {
            return;
        }
        System.out.println("testExportReplicatedExportToSocketRecover");
        Client client = getClient();
        Client adminClient = getAdminClient();

        client.callProcedure("@AdHoc", "create stream ex export to target socketconn (i bigint not null)");
        //We need a non export table
        client.callProcedure("@AdHoc", "create table ex2 (i bigint not null)");
        StringBuilder insertSql;
        for (int i=0;i<1000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        m_serverSocket.verifyExportedTuples(1000);
        client.close();
        if (MiscUtils.isPro()) {
            config.shutDown();
        } else {
            config.shutdownSave(adminClient);
            config.waitForNodesToShutdown();
        }
        System.out.println("Recovering the database...........");
        config.startUp(false);
        client = getClient();
        for (int i=1000;i<2000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        m_serverSocket.verifyExportedTuples(2000);
    }

    public TestExportSuiteReplicatedSocketExportRecover(final String name) {
        super(name);
    }

    public static junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportSuiteReplicatedSocketExportRecover.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        Properties props = new Properties();
        props.put("replicated", "true");
        props.put("skipinternals", "true");
        project.addExport(true, ServerExportEnum.CUSTOM, props, "socketconn");
        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 8, 3, 2,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        if (MiscUtils.isPro()) {
            project.configureLogging(null, null, false, true, 200, 999, 300);
        }
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
}
