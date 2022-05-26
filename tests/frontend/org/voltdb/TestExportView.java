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

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportLocalClusterBase;
import org.voltdb.export.TestExportBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;

/**
 * Listens for connections from socket export and then counts expected rows.
 * @author akhanzode
 */
public class TestExportView extends TestExportBase {

    private static LocalCluster config;

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        ExportLocalClusterBase.resetDir();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testExportView() throws Exception {
        System.out.println("testExportView");
        final Client client = getClient();

        client.callProcedure("@AdHoc", "create stream ex partition on column i (i bigint not null)");
        m_streamNames.add("EX");
        client.callProcedure("@AdHoc", "create view v_ex (i, counti) AS select i, count(*) from ex group by i");
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, m_streamNames);
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
    }

    public void testStreamView() throws Exception {
        System.out.println("testStreamView");
        final Client client = getClient();

        client.callProcedure("@AdHoc", "create stream ex partition on column i (i bigint not null)");
        m_streamNames.add("EX");
        client.callProcedure("@AdHoc", "create view v_ex (i, counti) AS select i, count(*) from ex group by i");
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, m_streamNames);
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
    }

    public void testExportViewBadTable() throws Exception {
        System.out.println("testExportViewBadTable");
        final Client client = getClient();

        client.callProcedure("@AdHoc", "create stream exx (i bigint not null)");
        try {
            client.callProcedure("@AdHoc", "create view v_exx (i, counti) AS select i, count(*) from exx group by i");
        } catch (ProcCallException ex) {
            ClientResponse response = ex.getClientResponse();
            assertFalse((response.getStatus() == ClientResponse.SUCCESS));
        }
    }

    public void testStreamViewBadTable() throws Exception {
        System.out.println("testStreamViewBadTable");
        final Client client = getClient();

        client.callProcedure("@AdHoc", "create stream exx (i bigint not null)");
        try {
            client.callProcedure("@AdHoc", "create view v_exx (i, counti) AS select i, count(*) from exx group by i");
        } catch (ProcCallException ex) {
            ClientResponse response = ex.getClientResponse();
            assertFalse((response.getStatus() == ClientResponse.SUCCESS));
        }
    }

    public void testExportViewBadView() throws Exception {
        System.out.println("testExportViewBadView");
        final Client client = getClient();

        client.callProcedure("@AdHoc", "create stream exz partition on column i (i bigint not null, j integer)");
        try {
            client.callProcedure("@AdHoc", "create view v_exz (j, counti) AS select j, count(*) from exz group by i");
        } catch (ProcCallException ex) {
            ClientResponse response = ex.getClientResponse();
            assertFalse((response.getStatus() == ClientResponse.SUCCESS));
        }
    }

    public void testStreamViewBadView() throws Exception {
        System.out.println("testStreamViewBadView");
        final Client client = getClient();

        client.callProcedure("@AdHoc", "create stream exz partition on column i (i bigint not null, j integer)");
        try {
            client.callProcedure("@AdHoc", "create view v_exz (j, counti) AS select j, count(*) from exz group by i");
        } catch (ProcCallException ex) {
            ClientResponse response = ex.getClientResponse();
            assertFalse((response.getStatus() == ClientResponse.SUCCESS));
        }
    }

    public TestExportView(final String name) {
        super(name);
    }

    public static junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.NoOpExporter");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.NoOpExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportView.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        Properties props = new Properties();

        project.addExport(true, ServerExportEnum.CUSTOM, props);
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
