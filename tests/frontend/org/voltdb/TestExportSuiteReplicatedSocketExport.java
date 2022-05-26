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
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientConfigForTest;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportLocalClusterBase;
import org.voltdb.export.SocketExportTestServer;
import org.voltdb.export.TestExportBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;

/**
 * Listens for connections from socket export and then counts expected rows.
 * @author akhanzode
 */
public class TestExportSuiteReplicatedSocketExport extends TestExportBase {
    @Rule
    public FlakyTestRule ftRule = new FlakyTestRule();

    private static SocketExportTestServer m_serverSocket;
    private static LocalCluster config;
    private static List<String> exStream = new ArrayList<>(Arrays.asList("EX"));

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

    @Test
    @Flaky(description="TestExportSuiteReplicatedSocketExport.testExportReplicatedExportToSocket")
    public void testExportReplicatedExportToSocket() throws Exception {
        System.out.println("testExportReplicatedExportToSocket");
        final Client client = getClient();

        client.callProcedure("@AdHoc", "create stream ex export to target ex (i bigint not null)");
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        m_serverSocket.verifyExportedTuples(5000);

        for (int i=5000;i<10000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        m_serverSocket.verifyExportedTuples(10000);

        for (int i=10000;i<15000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        m_serverSocket.verifyExportedTuples(15000);

        for (int i=15000;i<30000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        m_serverSocket.verifyExportedTuples(30000);

        for (int i=30000;i<45000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        m_serverSocket.verifyExportedTuples(45000);
    }

    @Test
    public void testExportReplicatedExportToSocketRejoin() throws Exception {
        ClientConfig cconfig = new ClientConfigForTest("ry@nlikesthe", "y@nkees");
        Client client = ClientFactory.createClient(cconfig);
        client.createConnection("localhost", config.port(0));

        client.callProcedure("@AdHoc", "create table regular (i bigint not null)");
        client.callProcedure("@AdHoc", "create stream ex export to target ex (i bigint not null)");
        StringBuilder insertSql = new StringBuilder();
        for (int i=0;i<50;i++) {
            insertSql.append("insert into ex values(" + i + ");");
        }
        client.callProcedure("@AdHoc", insertSql.toString());
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        m_serverSocket.verifyExportedTuples(50);

        config.killSingleHost(1);
        config.recoverOne(1, 0);
        Thread.sleep(2000);
        insertSql = new StringBuilder();
        for (int i=50;i<100;i++) {
            insertSql.append("insert into ex values(" + i + ");");
        }
        client.callProcedure("@AdHoc", insertSql.toString());
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        //After recovery make sure we get exact 2 of each.
        m_serverSocket.verifyExportedTuples(100);

        config.killSingleHost(2);
        config.recoverOne(2, 0);
        Thread.sleep(2000);
        insertSql = new StringBuilder();
        for (int i=100;i<150;i++) {
            insertSql.append("insert into ex values(" + i + ");");
        }
        client.callProcedure("@AdHoc", insertSql.toString());
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        //After recovery make sure we get exact 2 of each.
        m_serverSocket.verifyExportedTuples(150);

        //Kill host with all masters now.
        config.killSingleHost(0);
        config.recoverOne(0, 1);
        Thread.sleep(4000);
        client = ClientFactory.createClient(cconfig);
        client.createConnection("localhost", config.port(0));
        insertSql = new StringBuilder();
        for (int i=150;i<2000;i++) {
            insertSql.append("insert into ex values(" + i + ");");
        }
        client.callProcedure("@AdHoc", insertSql.toString());
        client.drain();
        waitForExportAllRowsDelivered(client, exStream);
        m_serverSocket.verifyExportedTuples(2000);
    }

    public TestExportSuiteReplicatedSocketExport(final String name) {
        super(name);
    }

    public static junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportSuiteReplicatedSocketExport.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        Properties props = new Properties();
        props.put("replicated", "true");
        props.put("skipinternals", "true");

        project.addExport(true, ServerExportEnum.CUSTOM, props, "ex");
        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 8, 3, 2,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);
        return builder;
    }
}
