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
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportLocalClusterBase;
import org.voltdb.export.ExportTestClient;
import org.voltdb.export.ExportTestVerifier;
import org.voltdb.export.TestExportBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;

/**
 * Export to nowhere and build view. Rejoin nodes and verify view is intact.
 * @author akhanzode
 */
public class TestExportRejoinWithView extends TestExportBase {

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
        ExportTestVerifier.m_closed = true;
        super.tearDown();
        ExportTestClient.clear();
    }

    public void testExportViewWithRejoin() throws Exception {
        System.out.println("testExportViewWithRejoin");
        Client client = getClient();
        client.callProcedure("@AdHoc", "CREATE TABLE foo (PKEY INTEGER NOT NULL, VAL INTEGER)");
        client.callProcedure("@AdHoc", "CREATE STREAM EXPORT_WITH_VIEW PARTITION ON COLUMN PKEY EXPORT TO TARGET CONN (PKEY INTEGER NOT NULL, VAL INTEGER)");
        client.callProcedure("@AdHoc", "CREATE VIEW V_EXPORT_WITH_VIEW (PKEY, KEYCOUNT) AS\n" +
                                        "SELECT PKEY, COUNT(*) FROM EXPORT_WITH_VIEW GROUP BY PKEY");
        client.callProcedure("@AdHoc", "CREATE INDEX V_IDX ON V_EXPORT_WITH_VIEW ( ABS(PKEY) )");
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into EXPORT_WITH_VIEW values(" + i + "," + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from V_EXPORT_WITH_VIEW");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
        client.close();

        //Recycle through all hosts and verify view is uptodate.
        ((LocalCluster) m_config).killSingleHost(1);
        ((LocalCluster) m_config).recoverOne(1, null);
        Thread.sleep(500);
        client = getClient();
        response = client.callProcedure("@AdHoc", "select count(*) from V_EXPORT_WITH_VIEW");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
    }

    public void testDeleteInExportViewWithRejoin() throws Exception {
        System.out.println("testExportViewWithRejoin");
        Client client = getClient();
        client.callProcedure("@AdHoc", "CREATE TABLE foo (PKEY INTEGER NOT NULL, VAL INTEGER)");
        client.callProcedure("@AdHoc", "CREATE STREAM EXPORT_WITH_VIEW PARTITION ON COLUMN PKEY EXPORT TO TARGET CONN (PKEY INTEGER NOT NULL, VAL INTEGER)");
        client.callProcedure("@AdHoc", "CREATE VIEW V_EXPORT_WITH_VIEW (PKEY, KEYCOUNT) AS\n" +
                                        "SELECT PKEY, COUNT(*) FROM EXPORT_WITH_VIEW GROUP BY PKEY");
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into EXPORT_WITH_VIEW values(" + i + "," + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        client.callProcedure("@AdHoc", "delete from V_EXPORT_WITH_VIEW where PKEY = 0");
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from V_EXPORT_WITH_VIEW");
        assertEquals(response.getResults()[0].asScalarLong(), 4999);
        client.close();

        //Recycle through all hosts and verify view is up to date.
        ((LocalCluster) m_config).killSingleHost(1);
        ((LocalCluster) m_config).recoverOne(1, null);
        Thread.sleep(500);
        client = getClient();
        response = client.callProcedure("@AdHoc", "select count(*) from V_EXPORT_WITH_VIEW");
        assertEquals(response.getResults()[0].asScalarLong(), 4999);
    }

    public void testUpdateInExportViewWithRejoin() throws Exception {
        System.out.println("testExportViewWithRejoin");
        Client client = getClient();
        client.callProcedure("@AdHoc", "CREATE TABLE foo (PKEY INTEGER NOT NULL, VAL INTEGER)");
        client.callProcedure("@AdHoc", "CREATE STREAM EXPORT_WITH_VIEW PARTITION ON COLUMN PKEY EXPORT TO TARGET CONN (PKEY INTEGER NOT NULL, VAL INTEGER)");
        client.callProcedure("@AdHoc", "CREATE VIEW V_EXPORT_WITH_VIEW (PKEY, KEYCOUNT) AS\n" +
                                        "SELECT PKEY, COUNT(*) FROM EXPORT_WITH_VIEW GROUP BY PKEY");
        client.callProcedure("@AdHoc", "insert into EXPORT_WITH_VIEW values(0,0)");
        client.drain();
        client.callProcedure("@AdHoc", "update V_EXPORT_WITH_VIEW set KEYCOUNT = 10");
        ClientResponse response = client.callProcedure("@AdHoc", "select KEYCOUNT from V_EXPORT_WITH_VIEW");
        assertEquals(response.getResults()[0].asScalarLong(), 10);
        client.close();

        //Recycle through all hosts and verify view is uptodate.
        ((LocalCluster) m_config).killSingleHost(1);
        ((LocalCluster) m_config).recoverOne(1, null);
        Thread.sleep(500);
        client = getClient();
        response = client.callProcedure("@AdHoc", "select KEYCOUNT from V_EXPORT_WITH_VIEW");
        assertEquals(response.getResults()[0].asScalarLong(), 10);
    }

    public void testStreamViewWithRejoin() throws Exception {
        System.out.println("testStreamViewWithRejoin");
        Client client = getClient();
        client.callProcedure("@AdHoc", "CREATE TABLE foo (PKEY INTEGER NOT NULL, VAL INTEGER)");
        client.callProcedure("@AdHoc", "CREATE STREAM EXPORT_WITH_VIEW PARTITION ON COLUMN PKEY EXPORT TO TARGET CONN (PKEY INTEGER NOT NULL, VAL INTEGER)");
        client.callProcedure("@AdHoc", "CREATE VIEW V_EXPORT_WITH_VIEW (PKEY, KEYCOUNT) AS\n" +
                "SELECT PKEY, COUNT(*) FROM EXPORT_WITH_VIEW GROUP BY PKEY");
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
        insertSql = new StringBuilder();
        insertSql.append("insert into EXPORT_WITH_VIEW values(" + i + "," + i + ");");
        client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from V_EXPORT_WITH_VIEW");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
        client.close();

        //Recycle through all hosts and verify view is uptodate.
        ((LocalCluster) m_config).killSingleHost(1);
        ((LocalCluster) m_config).recoverOne(1, null);
        Thread.sleep(500);
        client = getClient();
        response = client.callProcedure("@AdHoc", "select count(*) from V_EXPORT_WITH_VIEW");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
    }

    public TestExportRejoinWithView(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.NoOpExporter");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.NoOpExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportRejoinWithView.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        Properties props = new Properties();
        project.addExport(true, ServerExportEnum.CUSTOM, props, "CONN");

        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 8, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setMaxHeap(1024);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);

        return builder;
    }
}
