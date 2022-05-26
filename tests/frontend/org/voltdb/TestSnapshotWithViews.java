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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportTestClient;
import org.voltdb.export.ExportTestVerifier;
import org.voltdb.export.TestExportBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.VoltServerConfig;
import org.voltdb.utils.MiscUtils;

import org.apache.commons.io.FileUtils;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestSnapshotWithViews extends TestExportBase {

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        FileUtils.deleteDirectory(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();

    }

    @Override
    public void tearDown() throws Exception {
        ExportTestVerifier.m_closed = true;
        super.tearDown();
        ExportTestClient.clear();
    }

    public void testExportViewSnapshotRestore() throws Exception {
        System.out.println("testExportViewSnapshotRestore");
        Client client = getClient();

        String[] ddls = {
            "create stream ex partition on column i (i bigint not null)",
            "create view v_ex (i, counti) AS select i, count(*) from ex group by i"
        };

        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }
        m_streamNames.add("EX");
        int numValues=5000;
        for (int i=0;i<numValues;i++) {
            client.callProcedure("@AdHoc", "insert into ex values(" + i + ");");
        }
        client.drain();
        waitForExportAllRowsDelivered(client, m_streamNames);
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), numValues);

        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte) 1);

        m_config.shutDown();
        m_config.startUp(false);

        System.out.println("Restart is done...........");
        client = getClient();
        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }

        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");
        System.out.println("Snapshot Restore is done...........");
        response = client.callProcedure("@AdHoc", "select count(*) from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), numValues);
    }

    public void testDeleteInExportViewSnapshotRestore() throws Exception {
        System.out.println("testDeleteInExportViewSnapshotRestore");
        Client client = getClient();

        String[] ddls = {
            "create stream ex partition on column i (i bigint not null)",
            "create view v_ex (i, counti) AS select i, count(*) from ex group by i"
        };

        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }
        m_streamNames.add("EX");
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForExportAllRowsDelivered(client, m_streamNames);
        client.callProcedure("@AdHoc", "delete from v_ex where i = 0");
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), 4999);

        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte) 1);

        m_config.shutDown();
        m_config.startUp(false);

        System.out.println("Restart is done...........");
        client = getClient();
        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }

        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");
        System.out.println("Snapshot Restore is done...........");
        response = client.callProcedure("@AdHoc", "select count(*) from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), 4999);
    }

    public void testUpdateInExportViewSnapshotRestore() throws Exception {
        System.out.println("testUpdateInExportViewSnapshotRestore");
        Client client = getClient();

        String[] ddls = {
            "create stream ex partition on column i (i bigint not null)",
            "create view v_ex (i, counti) AS select i, count(*) from ex group by i",
        };

        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }
        m_streamNames.add("EX");
        client.callProcedure("@AdHoc", "insert into ex values(0)");
        client.drain();
        waitForExportAllRowsDelivered(client, m_streamNames);
        client.callProcedure("@AdHoc", "update v_ex set counti = 10");
        ClientResponse response = client.callProcedure("@AdHoc", "select counti from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), 10);

        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte) 1);

        m_config.shutDown();
        m_config.startUp(false);

        System.out.println("Restart is done...........");
        client = getClient();
        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }

        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");
        System.out.println("Snapshot Restore is done...........");
        response = client.callProcedure("@AdHoc", "select counti from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), 10);
    }

    public void testSingledTableViewMergeAfterSnapshotRestore() throws Exception {
        System.out.println("testSingledTableViewMergeAfterSnapshotRestore");
        Client client = getClient();

        String[] ddls = {
            "create table ex1 (i bigint not null, j bigint not null)",
            "partition table ex1 on column i",
            "create view v_ex_p (i, counti) AS select i, count(*) from ex1 group by i",
            // randomly partitioned view
            "create view v_ex_np (j, countj) AS select j, count(*) from ex1 group by j"
        };

        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex1 values(" + i + ", " + (5000-i) + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from v_ex_p");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
        response = client.callProcedure("@AdHoc", "select count(*) from v_ex_np");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);

        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte) 1);

        m_config.shutDown();
        m_config.startUp(false);

        System.out.println("Restart is done...........");
        client = getClient();
        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }
        for (int i=0;i<1000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex1 values(" + i + ", " + (5000-i) + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }

        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");
        System.out.println("Snapshot Restore is done...........");
        VoltTable resultTable = client.callProcedure("@AdHoc", "select * from v_ex_p order by 1;").getResults()[0];
        assertEquals(resultTable.getRowCount(), 5000);
        // Check that for the partitioned single table view, the view content is correctly merged after snapshot restore.
        for (int i = 0; i < 1000; i++) {
            assertTrue(resultTable.advanceRow());
            assertEquals(2L, resultTable.getLong(1));
        }
        for (int i = 0; i < 4000; i++) {
            assertTrue(resultTable.advanceRow());
            assertEquals(1L, resultTable.getLong(1));
        }
        resultTable = client.callProcedure("@AdHoc", "select count(*) from v_ex_np").getResults()[0];
        assertEquals(resultTable.asScalarLong(), 5000);
    }

    public void testJoinedTableViewAfterSnapshotRestore() throws Exception {
        System.out.println("testJoinedTableViewSnapshotRestore");
        Client client = getClient();

        String[] ddls = {
            "create table ex1 (i bigint not null, j bigint not null)",
            "create table ex2 (i bigint not null)",
            "partition table ex1 on column i",
            "partition table ex2 on column i",
            "create view v_ex (i, counti) AS select ex1.i, count(*) from ex1 join ex2 on ex1.i=ex2.i group by ex1.i",
            // randomly partitioned joined view
            "create view v_ex_np (j, countj) AS select ex1.j, count(*) from ex1 join ex2 on ex1.i=ex2.i group by ex1.j"
        };

        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex1 values(" + i + ", " + (5000-i) + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
            insertSql.append("insert into ex2 values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        ClientResponse response = client.callProcedure("@AdHoc", "select count(*) from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
        response = client.callProcedure("@AdHoc", "select count(*) from v_ex_np");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);

        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte) 1);

        m_config.shutDown();
        m_config.startUp(false);

        System.out.println("Restart is done...........");
        client = getClient();
        for (String ddl : ddls) {
            client.callProcedure("@AdHoc", ddl);
        }

        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");
        System.out.println("Snapshot Restore is done...........");
        response = client.callProcedure("@AdHoc", "select count(*) from v_ex");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
        response = client.callProcedure("@AdHoc", "select count(*) from v_ex_np");
        assertEquals(response.getResults()[0].asScalarLong(), 5000);
        System.out.println("Snapshot Restore for the second time...........");
        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");
    }

    public TestSnapshotWithViews(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.NoOpExporter");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.NoOpExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSnapshotWithViews.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        Properties props = new Properties();
        project.addExport(true, ServerExportEnum.CUSTOM, props);

        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 8, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        //TODO: Snapshot test to use old CLI
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);

        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml"));
        assertTrue(compile);

        return builder;
    }
}
