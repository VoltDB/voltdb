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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.SnapshotVerifier;

import com.google_voltpatches.common.collect.ImmutableMap;

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

    //  Test Export of a DROPPED table.  Queues some data to a table.
    //  Then drops the table and verifies that Export can successfully
    //  drain the dropped table. IE, drop table doesn't lose Export data.
    //
    public void testExportAndDroppedTable() throws Exception {
        System.out.println("testExportAndDroppedTable");
        Client client = getClient();
        for (int i = 0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "S_NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("S_NO_NULLS", i, rowdata);
            client.callProcedure("ExportInsertNoNulls", params);
        }
        waitForExportRowsToBeDelivered(client, ImmutableMap.of("S_NO_NULLS", 10L));

        // now drop the no-nulls table
        final String newCatalogURL = Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.jar");
        final String deploymentURL = Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml");
        final ClientResponse callProcedure =
            UpdateApplicationCatalog.update(client, new File(newCatalogURL), new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        client = getClient();

        // must still be able to verify the export data.
        m_verifier.waitForTuplesAndVerify(client);
    }

    // Test that a table w/o Export enabled does not produce Export content
    public void testThatTablesOptIn() throws Exception {
        System.out.println("testThatTablesOptIn");
        final Client client = getClient();

        final Object[] rowdata = TestSQLTypesSuite.m_midValues;
        // populate the row data
        for (int i = 0; i < 10; i++) {
            // do NOT add row to TupleVerfier as none should be produced
            client.callProcedure("TableInsertLoopback", convertValsToLoaderRow(i, rowdata));
        }
        // Make sure that we have not received any new data.
        quiesce(client);
        assertEquals(0, ExportTestClient.getExportedDataCount());
        m_verifier.verifyRows();
    }

    // Verify that planner rejects updates to append-only tables
    //
    public void testExportUpdateAppendOnly() throws IOException {
        System.out.println("testExportUpdateAppendOnly");
        final Client client = getClient();
        boolean threw = false;
        try {
            client.callProcedure("@AdHoc", "Update S_NO_NULLS SET A_TINYINT=0 WHERE PKEY=0;");
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
            client.callProcedure("@AdHoc", "Select PKEY from S_NO_NULLS WHERE PKEY=0;");
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
            client.callProcedure("@AdHoc", "DELETE from S_NO_NULLS WHERE PKEY=0;");
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
                    "S_ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("S_ALLOW_NULLS", i, rowdata);
            client.callProcedure("ExportInsertAllowNulls", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_verifier.addRow(client,
                    "S_NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("S_NO_NULLS", i, rowdata);
            client.callProcedure("ExportInsertNoNulls", params);
        }
        // Make sure some are exported and seen by me
        assertTrue((m_verifier.getExportedDataCount() - icnt > 0));
        m_verifier.waitForTuplesAndVerify(client);
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
                    "S_ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("S_ALLOW_NULLS", i, rowdata);
            client.callProcedure("ExportInsertAllowNulls", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_verifier.addRow(client,
                    "S_NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("S_NO_NULLS", i, rowdata);
            client.callProcedure("ExportInsertNoNulls", params);
        }
        String snapshotDir = "testExportPlusSnapshot";
        // this blocks until the snapshot is complete
        client.callProcedure("@SnapshotSave", snapshotDir, "testExportPlusSnapshot", (byte) 1).getResults();

        LocalCluster ccluster = (LocalCluster)m_config;
        for (int i = 0; i < ccluster.getNodeCount(); i++) {
            String sdir = ccluster.getServerSpecificScratchDir(String.valueOf(i)) + File.separator + snapshotDir;
            // verify. copied from TestSaveRestoreSysprocSuite
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final PrintStream ps = new PrintStream(baos);
            final PrintStream original = System.out;
            try {
                System.setOut(ps);
                final String args[] = new String[]{
                        "testExportPlusSnapshot",
                        "--dir",
                        sdir
                };
                SnapshotVerifier.main(args);
                ps.flush();
                final String reportString = baos.toString("UTF-8");
                assertTrue(reportString, reportString.contains("Snapshot valid\n"));
            } finally {
                System.setOut(original);
            }
        }

        // verify the el data
        m_verifier.waitForTuplesAndVerify(client);
    }

    public void testSwapTables() throws Exception {
        System.out.println("testExportSwapTables");
        final Client client = getClient();
        verifyProcFails(client, "Illegal to swap a stream", "@SwapTables", "S_ALLOW_NULLS", "LOOPBACK_NO_NULLS");
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
                new MultiConfigSuiteBuilder(TestExportSuite.class, "testExportPlusSnapshot"::equals);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-nonulls-ddl-with-target.sql"));
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-allownulls-ddl-with-target.sql"));
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-nonullsloopback-table-ddl.sql"));
        project.addPartitionInfo("LOOPBACK_NO_NULLS", "PKEY");

        wireupExportTableToSocketExport("S_ALLOW_NULLS");
        wireupExportTableToSocketExport("S_NO_NULLS");

        project.addProcedures(ALLOWNULLS_PROCEDURES);
        project.addProcedures(NONULLS_PROCEDURES);
        project.addProcedures(LOOPBACK_PROCEDURES);

        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setEnableVoltSnapshotPrefix(true);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls-and-allownulls.xml"));
        assertTrue(compile);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);


        /*
         * compile a catalog without the NO_NULLS table for add/drop tests
         */
        config = new LocalCluster("export-ddl-sans-nonulls.jar", 2, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setEnableVoltSnapshotPrefix(true);
        config.setMaxHeap(1024);
        project = new VoltProjectBuilder();
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-nonulls-ddl-with-target.sql"));

        wireupExportTableToSocketExport("S_NO_NULLS");
        project.addProcedures(NONULLS_PROCEDURES);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml"));
        assertTrue(compile);

        return builder;
    }
}
