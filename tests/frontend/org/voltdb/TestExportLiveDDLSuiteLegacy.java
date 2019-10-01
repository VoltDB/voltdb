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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportLocalClusterBase;
import org.voltdb.export.ExportTestExpectedData;
import org.voltdb.export.TestExportBaseSocketExport;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.utils.MiscUtils;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 *
 *  The only difference between this and TestExportLiveDDLSuite is that this
 *  uses org.voltdb.exportclient.SocketExporterLegacy
 */

public class TestExportLiveDDLSuiteLegacy extends TestExportBaseSocketExport {
    private static final int k_factor = 0;

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

    public void testExportDataAfterCatalogUpdateDropAndAdd() throws Exception {
        if (isValgrind()) {
            return;
        }
        int numOfStreams = 2;
        if (MiscUtils.isPro()) {
            numOfStreams += 2;
        }
        System.out.println("testExportDataAfterCatalogUpdateDropAndAdd");
        Client client = getClient();
        ClientResponse response;
        for (int i = 0; i < numOfStreams; i++) {
            String tab = "EX" + i;
            m_streamNames.add(tab);
            response = client.callProcedure("@AdHoc", "create stream " + tab +
                    " partition on column i export to target " + tab + " (i integer not null)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
            response = client.callProcedure("@AdHoc", "insert into " + tab + " values(111)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        //We should consume all.
        waitForExportAllRowsDelivered(client, m_streamNames);

        //create a non stream table
        for (int i = 0; i < numOfStreams; i++) {
            String tab = "reg" + i;
            String etab = "EX" + i;
            response = client.callProcedure("@AdHoc", "create table " + tab + " (i integer)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
            response = client.callProcedure("@AdHoc", "insert into " + etab + " values(222)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        //We should consume all again.
        waitForExportAllRowsDelivered(client, m_streamNames);

        //drop a non stream table
        for (int i = 0; i < numOfStreams; i++) {
            String tab = "reg" + i;
            String etab = "EX" + i;
            response = client.callProcedure("@AdHoc", "drop table " + tab);
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
            response = client.callProcedure("@AdHoc", "insert into " + etab + " values(222)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        //We should consume all again.
        waitForExportAllRowsDelivered(client, m_streamNames);

        //create a stream view table
        for (int i = 0; i < numOfStreams; i++) {
            String view = "v_" + i;
            String etab = "EX" + i;
            response = client.callProcedure("@AdHoc", "create view  " + view + " (i, num_i) AS SELECT i, count(*) from " + etab + " GROUP BY i");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
            response = client.callProcedure("@AdHoc", "insert into " + etab + " values(333)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        //We should consume all again.
        waitForExportAllRowsDelivered(client, m_streamNames);

        if (MiscUtils.isPro() ) {
            // drop a new stream table
            for (int i = 0; i < numOfStreams; i++) {
                String newtab = "NEX" + i;
                m_streamNames.add(newtab);
                String etab = "ex" + i;
                response = client.callProcedure("@AdHoc", "create stream " + newtab +
                        " partition on column i export to target " + newtab + " (i integer not null)");
                assertEquals(response.getStatus(), ClientResponse.SUCCESS);
                response = client.callProcedure("@AdHoc", "insert into " + etab + " values(444)");
                assertEquals(response.getStatus(), ClientResponse.SUCCESS);
            }
            //We should consume all again.
            waitForExportAllRowsDelivered(client, m_streamNames);
        }

        //drop a stream view table
        for (int i = 0; i < numOfStreams; i++) {
            String view = "v_" + i;
            String etab = "EX" + i;
            response = client.callProcedure("@AdHoc", "drop view  " + view);
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
            response = client.callProcedure("@AdHoc", "insert into " + etab + " values(555)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        //We should consume all again.
        waitForExportAllRowsDelivered(client, m_streamNames);


        for (int i = 0; i < numOfStreams; i++) {
            String tab = "EX" + i;
            m_streamNames.remove(tab);
            response = client.callProcedure("@AdHoc", "drop stream " + tab);
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }

        if (MiscUtils.isPro()) {
            // In the Pro path of the test the NEX streams don't get dropped
            waitForExportAllRowsDelivered(client, m_streamNames);
        }
        else {
            //After drop there should be no stats rows for export.
            waitForStreamedTargetDeallocated(client);
        }

        //recreate tables and export again
        for (int i = 0; i < numOfStreams; i++) {
            String tab = "EX" + i;
            m_streamNames.add(tab);
            response = client.callProcedure("@AdHoc", "create stream " + tab +
                    " export to target " + tab + " (i integer)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
            response = client.callProcedure("@AdHoc", "insert into " + tab + " values(111)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        //We should consume all again.
        waitForExportAllRowsDelivered(client, m_streamNames);

        // must still be able to verify the export data.
        client.close();
        client = getClient();
        //Make sure server is still hanging in there.
        assertNotNull(client);
    }

    //This tests if catalog changes are not applied in EE export continues to function with current generation.
    public void testExportDataAfterNonEEUpdate() throws Exception {
        if (isValgrind()) {
            return;
        }
        System.out.println("testExportDataAfterNonEEUpdate");
        Client client = getClient();
        ClientResponse response;
        response = client.callProcedure("@AdHoc", "create table funny (i integer, j integer);");
        assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        response = client.callProcedure("@AdHoc", "create stream ex partition on column i export to target ex (i integer not null)");
        assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        m_streamNames.add("EX");

        //export some data
        for (int i = 0; i < 5; i++) {
            response = client.callProcedure("@AdHoc", "insert into ex values(111)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        //We should consume all again.
        waitForExportAllRowsDelivered(client, m_streamNames);

        //create procedure and then insert data again.
        response = client.callProcedure("@AdHoc", "create PROCEDURE CountFunny AS SELECT COUNT(*) FROM funny WHERE j=?;");
        assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        for (int i = 0; i < 5; i++) {
            response = client.callProcedure("@AdHoc", "insert into ex " + " values(111)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        quiesce(client);
        //We should consume all again.
        waitForExportAllRowsDelivered(client, m_streamNames);


        //create procedure and then insert data again.
        response = client.callProcedure("@AdHoc", "drop PROCEDURE CountFunny;");
        assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        for (int i = 0; i < 5; i++) {
            response = client.callProcedure("@AdHoc", "insert into ex " + " values(111)");
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        quiesce(client);
        //We should consume all again.
        waitForExportAllRowsDelivered(client, m_streamNames);


        // must still be able to verify the export data.
        client.close();
        client = getClient();
        //Make sure server is still hanging in there.
        assertNotNull(client);
    }

    public void testInsertDataBeforeCatalogUpdate() throws Exception {
        if (isValgrind()) {
            return;
        }
        System.out.println("testInsertDataBeforeCatalogUpdate");
        Client client = getClient();

        client.callProcedure("@AdHoc", "create stream ex partition on column i (i integer not null)");
        client.callProcedure("@AdHoc", "insert into ex values(111)");

        client.callProcedure("@AdHoc", "drop table ex");

        quiesce(client);

        client.callProcedure("@AdHoc", "create stream ex0 (i integer)");
        client.callProcedure("@AdHoc", "create stream ex partition on column i (i integer not null)");

        quiesce(client);

        // must still be able to verify the export data.
        client.close();
        client = getClient();
        //Make sure server is still hanging in there.
        assertNotNull(client);
    }

    public void testCatalogUpdateNonEmptyExport() throws Exception {
        if (isValgrind()) {
            return;
        }
        System.out.println("testCatalogUpdateNonEmptyExport");
        Client client = getClient();
        closeSocketExporterClientAndServer();
        client.callProcedure("@AdHoc", "create stream ex partition on column i export to target ex (i integer not null)");
        m_streamNames.add("EX");
        StringBuilder insertSql = new StringBuilder();
        Object[] param = new Object[2];
        Arrays.fill(param, 1);
        for (int i=0;i<50;i++) {
            param[1] = i;
            m_verifier.addRow(client, "ex", i, param);
            insertSql.append("insert into ex values(" + i + ");");
        }
        client.callProcedure("@AdHoc", insertSql.toString());

        startListener();
        quiesceAndVerifyTarget(client, m_streamNames, m_verifier, 60_000, true);

        // must still be able to verify the export data.
        client.close();
        client = getClient();
        //Make sure server is still hanging in there.
        assertNotNull(client);
    }

    public void testLongTableSignature() throws Exception {
        if (isValgrind()) {
            return;
        }
        System.out.println("testLongTableSignature");
        Client client = getClient();
        closeSocketExporterClientAndServer();
        client.callProcedure("@AdHoc", "create stream tenletters (" +
                "c1  varchar(20)," +
                "c2  tinyint," +
                "c3  tinyint," +
                "c4  tinyint," +
                "c5  tinyint," +
                "c6  tinyint," +
                "c7  tinyint," +
                "c8  tinyint," +
                "c9  tinyint," +
                "c10 tinyint," +
                "c11 varchar(20)," +
                "c12 tinyint," +
                "c13 tinyint," +
                "c14 tinyint," +
                "c15 tinyint," +
                "c16 tinyint," +
                "c17 tinyint," +
                "c18 tinyint," +
                "c19 tinyint," +
                "c20 tinyint," +
                "c21 varchar(20)," +
                "c22 tinyint," +
                "c23 tinyint," +
                "c24 tinyint," +
                "c25 tinyint," +
                "c26 tinyint," +
                "c27 tinyint," +
                "c28 tinyint," +
                "c29 tinyint," +
                "c30 tinyint," +
                "c31 varchar(20)," +
                "c32 tinyint," +
                "c33 tinyint," +
                "c34 tinyint," +
                "c35 tinyint," +
                "c36 tinyint," +
                "c37 tinyint," +
                "c38 tinyint," +
                "c39 tinyint," +
                "c40 tinyint," +
                "c41 varchar(20)," +
                "c42 tinyint," +
                "c43 tinyint," +
                "c44 tinyint," +
                "c45 tinyint," +
                "c46 tinyint," +
                "c47 tinyint," +
                "c48 tinyint," +
                "c49 tinyint," +
                "c50 tinyint," +
                "c51 varchar(20)," +
                "c52 tinyint," +
                "c53 tinyint," +
                "c54 tinyint," +
                "c55 tinyint," +
                "c56 tinyint," +
                "c57 tinyint," +
                "c58 tinyint," +
                "c59 tinyint," +
                "c60 tinyint," +
                "c61 varchar(20)," +
                "c62 tinyint," +
                "c63 tinyint," +
                "c64 tinyint," +
                "c65 tinyint," +
                "c66 tinyint," +
                "c67 tinyint," +
                "c68 tinyint," +
                "c69 tinyint," +
                "c70 tinyint," +
                "c71 varchar(20)," +
                "c72 tinyint," +
                "c73 tinyint," +
                "c74 tinyint," +
                "c75 tinyint," +
                "c76 tinyint," +
                "c77 tinyint," +
                "c78 tinyint," +
                "c79 tinyint," +
                "c80 tinyint," +
                "c81 varchar(20)," +
                "c82 tinyint," +
                "c83 tinyint," +
                "c84 tinyint," +
                "c85 tinyint," +
                "c86 tinyint," +
                "c87 tinyint," +
                "c88 tinyint," +
                "c89 tinyint," +
                "c90 tinyint," +
                "c91 varchar(20)," +
                "c92 tinyint," +
                "c93 tinyint," +
                "c94 tinyint," +
                "c95 tinyint," +
                "c96 tinyint," +
                "c97 tinyint," +
                "c98 tinyint," +
                "c99 tinyint," +
                "c100 tinyint," +
                "c101 varchar(20)," +
                "c102 tinyint," +
                "c103 tinyint," +
                "c104 tinyint," +
                "c105 tinyint," +
                "c106 tinyint," +
                "c107 tinyint," +
                "c108 tinyint," +
                "c109 tinyint," +
                "c110 tinyint," +
                "c111 varchar(20)," +
                "c112 tinyint," +
                "c113 tinyint," +
                "c114 tinyint," +
                "c115 tinyint," +
                "c116 tinyint," +
                "c117 tinyint)");
        client.callProcedure("@AdHoc", "create stream ex (i integer)");
        quiesce(client);
    }

    public void testExportTableWithGeoTypes() throws Exception {
        if (isValgrind()) {
            return;
        }
        System.out.println("testExportTableWithGeoTypes");
        Client client = getClient();

        client.callProcedure("@AdHoc", "create stream foo ( id integer not null," +
                                                         " region geography not null);" +
                                       "create stream foobar partition on column id ( id integer not null," +
                                                            " place geography_point not null);");
        // drop the tables
        String ddl = "drop stream foo;\n" +
                "drop stream foobar;\n";
        ClientResponse response = client.callProcedure("@AdHoc", ddl);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        quiesce(client);

        client.close();
        client = getClient();
        //Make sure server is still hanging in there.
        assertNotNull(client);
    }

    public TestExportLiveDDLSuiteLegacy(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporterLegacy");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporterLegacy");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportLiveDDLSuiteLegacy.class);

        project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        wireupExportTableToSocketExport("ex");
        int numOfStreams = 2;
        if (MiscUtils.isPro()) {
            numOfStreams += 2;
        }
        for (int i = 0; i < numOfStreams; i++) {
            wireupExportTableToSocketExport("ex" + i);
        }

        if (MiscUtils.isPro()) {
            for (int i = 0; i < numOfStreams; i++) {
                wireupExportTableToSocketExport("nex" + i);
            }
        }

        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 4, 1, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, false);

        return builder;
    }
}
