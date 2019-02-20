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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportTestClient;
import org.voltdb.export.ExportTestExpectedData;
import org.voltdb.export.TestExportBaseSocketExport;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.SnapshotVerifier;
import org.voltdb.utils.VoltFile;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportSuite extends TestExportBaseSocketExport {
    private static final int k_factor = 1;

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


    // Test Export of an ADDED table.
    //
    public void testExportAndAddedTable() throws Exception {
        System.out.println("testExportAndAddedTable");
        final Client client = getClient();
        System.out.println("Seen Verifiers: " + m_verifier.m_seen_verifiers);
        // add a new table
        final String newCatalogURL = Configuration.getPathToCatalogForTest("export-ddl-addedtable.jar");
        final String deploymentURL = Configuration.getPathToCatalogForTest("export-ddl-addedtable.xml");
        final ClientResponse callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        // verify that it exports
        for (int i = 0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "ADDED_TABLE", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client, "ADDED_TABLE_GRP", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("ADDED_TABLE", i, rowdata);
            final Object[] paramsGrp = convertValsToParams("ADDED_TABLE_GRP", i, rowdata);
            client.callProcedure("InsertAddedTable", params);
            client.callProcedure("InsertAddedTable", paramsGrp);
        }

        System.out.println("Again Seen Verifiers: " + m_verifier.m_seen_verifiers);
        quiesceAndVerifyTarget(client, m_verifier);
    }

    //  Test Export of a DROPPED table.  Queues some data to a table.
    //  Then drops the table and verifies that Export can successfully
    //  drain the dropped table. IE, drop table doesn't lose Export data.
    //
    public void testExportAndDroppedTable() throws Exception {
        System.out.println("testExportAndDroppedTable");
        Client client = getClient();
        for (int i = 0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client, "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            final Object[] paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);
        }
        waitForStreamedTargetAllocatedMemoryZero(client);

        // now drop the no-nulls table
        final String newCatalogURL = Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.jar");
        final String deploymentURL = Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml");
        final ClientResponse callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        client = getClient();

        // must still be able to verify the export data.
        quiesceAndVerifyTarget(client, m_verifier);
    }

    // Test that a table w/o Export enabled does not produce Export content
    public void testThatTablesOptIn() throws Exception {
        System.out.println("testThatTablesOptIn");
        final Client client = getClient();

        final Object params[] = new Object[TestSQLTypesSuite.COLS + 2];
        params[0] = "WITH_DEFAULTS";  // this table should not produce Export output

        // populate the row data
        for (int i = 0; i < TestSQLTypesSuite.COLS; ++i) {
            params[i + 2] = TestSQLTypesSuite.m_midValues[i];
        }
        long icnt = m_verifier.getExportedDataCount();
        for (int i = 0; i < 10; i++) {
            params[1] = i; // pkey
            // do NOT add row to TupleVerfier as none should be produced
            client.callProcedure("Insert", params);
        }
        //Make sure that we have not recieved any new data.
        waitForStreamedTargetAllocatedMemoryZero(client);
        assertEquals(icnt, ExportTestClient.getExportedDataCount());
        quiesceAndVerifyTarget(client, m_verifier);
    }

    // Verify that planner rejects updates to append-only tables
    //
    public void testExportUpdateAppendOnly() throws IOException {
        System.out.println("testExportUpdateAppendOnly");
        final Client client = getClient();
        boolean threw = false;
        try {
            client.callProcedure("@AdHoc", "Update NO_NULLS SET A_TINYINT=0 WHERE PKEY=0;");
        }
        catch (final ProcCallException e) {
            assertTrue("Updating an export table with adhoc returned a strange message",
                       e.getMessage().contains("Illegal to update a stream."));
            threw = true;
        }
        assertTrue("Updating an export-only table failed to throw an exception",
                   threw);
    }

    //
    // Verify that planner rejects reads of append-only tables.
    //
    public void testExportSelectAppendOnly() throws IOException {
        System.out.println("testExportSelectAppendOnly");
        final Client client = getClient();
        boolean passed = false;
        try {
            client.callProcedure("@AdHoc", "Select PKEY from NO_NULLS WHERE PKEY=0;");
        }
        catch (final ProcCallException e) {
            if (e.getMessage().contains("Illegal to read a stream.")) {
                passed = true;
            }
        }
        assertTrue(passed);
    }

    //
    //  Verify that planner rejects deletes of append-only tables
    //
    public void testExportDeleteAppendOnly() throws IOException {
        System.out.println("testExportDeleteAppendOnly");
        final Client client = getClient();
        boolean passed = false;
        try {
            client.callProcedure("@AdHoc", "DELETE from NO_NULLS WHERE PKEY=0;");
        }
        catch (final ProcCallException e) {
            if (e.getMessage().contains("Illegal to delete from a stream.")) {
                passed = true;
            }
        }
        assertTrue(passed);
    }

    //
    // Multi-table test
    //
    public void testExportMultiTable() throws Exception
    {
        System.out.println("testExportMultiTable");
        final Client client = getClient();
        long icnt = m_verifier.getExportedDataCount();
        for (int i=0; i < 10; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client,
                    "ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            // Grp tables added to verifier because they are needed
            m_verifier.addRow(client,
                    "ALLOW_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            Object[] paramsGrp = convertValsToParams("ALLOW_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_verifier.addRow(client,
                    "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client,
                    "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("NO_NULLS", i, rowdata);
            paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);
        }
        // Make sure some are exported and seen by me
        assertTrue((m_verifier.getExportedDataCount() - icnt > 0));
        quiesceAndVerifyTarget(client, m_verifier);
    }

    //
    // Verify that snapshot can be enabled with a streamed table present
    //
    public void testExportPlusSnapshot() throws Exception {
        System.out.println("testExportPlusSnapshot");
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client,
                    "ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client,
                    "ALLOW_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            Object[] paramsGrp = convertValsToParams("ALLOW_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_verifier.addRow(client,
                    "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client,
                    "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("NO_NULLS", i, rowdata);
            paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);
        }
        // this blocks until the snapshot is complete
        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testExportPlusSnapshot", (byte)1).getResults();

        // verify. copped from TestSaveRestoreSysprocSuite
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream ps = new PrintStream(baos);
        final PrintStream original = System.out;
        new java.io.File("/tmp/" + System.getProperty("user.name")).mkdir();
        try {
            System.setOut(ps);
            final String args[] = new String[] {
                    "testExportPlusSnapshot",
                    "--dir",
                    "/tmp/" + System.getProperty("user.name")
            };
            SnapshotVerifier.main(args);
            ps.flush();
            final String reportString = baos.toString("UTF-8");
            assertTrue(reportString.startsWith("Snapshot valid\n"));
        } catch (final UnsupportedEncodingException e) {}
        finally {
            System.setOut(original);
        }

        // verify the el data
        quiesceAndVerifyTarget(client, m_verifier);
    }

    public void testSwapTables() throws Exception {
        System.out.println("testExportSwapTables");
        final Client client = getClient();
        verifyProcFails(client, "Illegal to swap a stream", "@SwapTables", "ALLOW_NULLS", "LOOPBACK_ALLOW_NULLS");
    }

    public TestExportSuite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportSuite.class);

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

        // JNI, single server
        // Use the cluster only config. Multiple topologies with the extra catalog for the
        // Add drop tests is harder. Restrict to the single (complex) topology.
        //
        //        config = new LocalSingleProcessServer("export-ddl.jar", 2,
        //                                              BackendTarget.NATIVE_EE_JNI);
        //        config.compile(project);
        //        builder.addServerConfig(config);


        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, false);


        /*
         * compile a catalog without the NO_NULLS table for add/drop tests
         */
        config = new LocalCluster("export-ddl-sans-nonulls.jar", 2, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        project = new VoltProjectBuilder();
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));

        wireupExportTableToSocketExport("ALLOW_NULLS");
        wireupExportTableToSocketExport("ALLOW_NULLS_GRP");
        project.addProcedures(PROCEDURES2);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml"));
        assertTrue(compile);

        /*
         * compile a catalog with an added table for add/drop tests
         */
        config = new LocalCluster("export-ddl-addedtable.jar", 2, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        project = new VoltProjectBuilder();
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-addedtable-export-ddl-with-target.sql"));

        wireupExportTableToSocketExport("ALLOW_NULLS");
        wireupExportTableToSocketExport("ADDED_TABLE");
        wireupExportTableToSocketExport("NO_NULLS");  // streamed table
        wireupExportTableToSocketExport("ALLOW_NULLS_GRP");
        wireupExportTableToSocketExport("ADDED_TABLE_GRP");
        wireupExportTableToSocketExport("NO_NULLS_GRP"); // streamed table

        project.addProcedures(PROCEDURES);
        project.addProcedures(PROCEDURES3);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-addedtable.xml"));
        assertTrue(compile);


        return builder;
    }
}
