/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.quarantine;

import java.io.File;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.export.ExportTestClient;
import org.voltdb.exportclient.ExportClientException;
import org.voltdb.quarantine.TestExportSuite;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.TestOrderBySuite;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.regressionsuites.VoltServerConfig;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Insert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertAddedTable;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertBase;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.RollbackInsert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Update_Export;

/**
 *  End to end Export tests using the RawProcessor and the ExportSinkServer.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportSuiteTestExportAndDroppedTableThenShutdown extends RegressionSuite {

    private ExportTestClient m_tester;

    /** Shove a table name and pkey in front of row data */
    private Object[] convertValsToParams(String tableName, final int i,
            final Object[] rowdata)
    {
        final Object[] params = new Object[rowdata.length + 2];
        params[0] = tableName;
        params[1] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            params[ii+2] = rowdata[ii];
        return params;
    }

    /** Push pkey into expected row data */
    private Object[] convertValsToRow(final int i, final char op,
            final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 2];
        row[0] = (byte)(op == 'I' ? 1 : 0);
        row[1] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            row[ii+2] = rowdata[ii];
        return row;
    }

    /** Push pkey into expected row data */
    @SuppressWarnings("unused")
    private Object[] convertValsToLoaderRow(final int i, final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 1];
        row[0] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            row[ii+1] = rowdata[ii];
        return row;
    }

    private void quiesce(final Client client)
    throws Exception
    {
        client.drain();
        client.callProcedure("@Quiesce");
    }

    private void quiesceAndVerifyRetryWorkOnIOException(final Client client, ExportTestClient tester)
    throws Exception
    {
        quiesce(client);
        while (true) {
            try {
                tester.work();
            } catch (ExportClientException e) {
                boolean success = reconnect(tester);
                assertTrue(success);
                System.out.println(e.toString());
                continue;
            }
            break;
        }
        assertTrue(tester.allRowsVerified());
        assertTrue(tester.verifyExportOffsets());
    }

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();

        callbackSucceded = true;
        m_tester = new ExportTestClient(getServerConfig().getNodeCount());
        m_tester.addCredentials("export", "export");
        try {
            m_tester.connect();
        } catch (ExportClientException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        m_tester.disconnect();
        assertTrue(callbackSucceded);
    }

    private boolean callbackSucceded = true;
    class RollbackCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() != ClientResponse.USER_ABORT) {
                callbackSucceded = false;
                System.err.println(clientResponse.getException());
            }
        }
    }

    private boolean reconnect(ExportTestClient client) throws ExportClientException {
        for (int ii = 0; ii < 3; ii++) {
            m_tester.disconnect();
            m_tester.reserveVerifiers();
            boolean success = client.connect();
            if (success) return true;
        }
        return false;
    }

    //  Test Export of a DROPPED table.  Queues some data to a table.
    //  Then drops the table and restarts the server. Verifies that Export can successfully
    //  drain the dropped table. IE, drop table doesn't lose Export data.
    //
    public void testExportAndDroppedTableThenShutdown() throws Exception {
        System.out.println("testExportAndDroppedTableThenShutdown");
        Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.first(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }

        // now drop the no-nulls table
        final String newCatalogURL = Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.jar");
        final String deploymentURL = Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml");
        final ClientResponse callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                                                                             new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        quiesce(client);

        m_config.shutDown();
        m_config.startUp(false);

        client = getClient();

        /**
         * There will be 3 disconnects. Once for the shutdown, once for first generation,
         * another for the 2nd generation created by the catalog change. The predicate is a complex
         * way of saying make sure that the tester has created verifiers for
         */
        for (int ii = 0; m_tester.m_generationsSeen.size() < 3 ||
                m_tester.m_verifiers.get(m_tester.m_generationsSeen.last()).size() < 6; ii++) {
            Thread.sleep(500);
            boolean threwException = false;
            try {
                m_tester.work(1000);
            } catch (ExportClientException e) {
                boolean success = reconnect(m_tester);
                assertTrue(success);
                System.out.println(e.toString());
                threwException = true;
            }
            if (ii < 3) {
                assertTrue(threwException);
            }
        }

        for (int i=10; i < 20; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        client.drain();

        // must still be able to verify the export data.
        quiesceAndVerifyRetryWorkOnIOException(client, m_tester);
    }


    static final GroupInfo GROUPS[] = new GroupInfo[] {
        new GroupInfo("export", false, false),
        new GroupInfo("proc", true, true),
        new GroupInfo("admin", true, true)
    };

    static final UserInfo[] USERS = new UserInfo[] {
        new UserInfo("export", "export", new String[]{"export"}),
        new UserInfo("default", "password", new String[]{"proc"}),
        new UserInfo("admin", "admin", new String[]{"proc", "admin"})
    };

    /*
     * Test suite boilerplate
     */
    static final ProcedureInfo[] PROCEDURES = {
        new ProcedureInfo( new String[]{"proc"}, Insert.class),
        new ProcedureInfo( new String[]{"proc"}, InsertBase.class),
        new ProcedureInfo( new String[]{"proc"}, RollbackInsert.class),
        new ProcedureInfo( new String[]{"proc"}, Update_Export.class)
    };

    static final ProcedureInfo[] PROCEDURES2 = {
        new ProcedureInfo( new String[]{"proc"}, Update_Export.class)
    };

    static final ProcedureInfo[] PROCEDURES3 = {
        new ProcedureInfo( new String[]{"proc"}, InsertAddedTable.class)
    };

    public TestExportSuiteTestExportAndDroppedTableThenShutdown(final String name) {
        super(name);
    }

    public static void main(final String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestOrderBySuite.class);
    }

    static public junit.framework.Test suite() throws Exception
    {
        VoltServerConfig config;

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportSuiteTestExportAndDroppedTableThenShutdown.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setSecurityEnabled(true);
        project.addGroups(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-ddl.sql"));
        project.addExport("org.voltdb.export.processors.RawProcessor",
                true  /*enabled*/,
                java.util.Arrays.asList(new String[]{"export"}));
        // "WITH_DEFAULTS" is a non-exported persistent table
        project.setTableAsExportOnly("ALLOW_NULLS");
        project.setTableAsExportOnly("NO_NULLS");
        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
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
        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);


        /*
         * compile a catalog without the NO_NULLS table for add/drop tests
         */
        config = new LocalCluster("export-ddl-sans-nonulls.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI,  LocalCluster.FailureState.ALL_RUNNING, true);
        project = new VoltProjectBuilder();
        project.addGroups(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addExport("org.voltdb.export.processors.RawProcessor",
                true,  //enabled
                java.util.Arrays.asList(new String[]{"export"}));
        // "WITH_DEFAULTS" is a non-exported persistent table
        project.setTableAsExportOnly("ALLOW_NULLS");

        // and then project builder as normal
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addProcedures(PROCEDURES2);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml"));
        assertTrue(compile);

        /*
         * compile a catalog with an added table for add/drop tests
         */
        config = new LocalCluster("export-ddl-addedtable.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI,  LocalCluster.FailureState.ALL_RUNNING, true);
        project = new VoltProjectBuilder();
        project.addGroups(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-addedtable-ddl.sql"));
        project.addExport("org.voltdb.export.processors.RawProcessor",
                true  /*enabled*/,
                java.util.Arrays.asList(new String[]{"export"}));
        // "WITH_DEFAULTS" is a non-exported persistent table
        project.setTableAsExportOnly("ALLOW_NULLS");   // persistent table
        project.setTableAsExportOnly("ADDED_TABLE");   // persistent table
        project.setTableAsExportOnly("NO_NULLS");      // streamed table

        // and then project builder as normal
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("ADDED_TABLE", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addProcedures(PROCEDURES);
        project.addProcedures(PROCEDURES3);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-addedtable.xml"));
        assertTrue(compile);


        return builder;
    }
}
