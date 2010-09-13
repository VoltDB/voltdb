/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.regressionsuites;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.elt.ExportTestClient;
import org.voltdb.utils.SnapshotVerifier;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Delete;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Insert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertAddedTable;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.RollbackInsert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Update_ELT;

/**
 *  End to end ELT tests using the RawProcessor and the ELSinkServer.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an ELT round trip test.
 */

public class TestExportSuite extends RegressionSuite {

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

    private void quiesceAndVerify(final Client client, ExportTestClient tester)
    throws Exception
    {
        quiesce(client);
        tester.work();
        assertTrue(tester.allRowsVerified());
        assertTrue(tester.verifyEltOffsets());
    }

    private void quiesceAndVerifyFalse(final Client client, ExportTestClient tester)
    throws Exception
    {
        quiesce(client);
        tester.work();
        assertFalse(tester.allRowsVerified());
    }

    @Override
    public void setUp()
    {
        super.setUp();
        callbackSucceded = true;
        m_tester = new ExportTestClient(getServerConfig().getNodeCount());
        try {
            m_tester.connectToELServers(null, null);
        }
        catch (final IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        assertTrue(callbackSucceded);
    }

    /*
     * Test ELT of an ADDED table.
     */
    public void testELTAndAddedTable() throws Exception {
        final Client client = getClient();

        // add a new table
        final String newCatalogURL = Configuration.getPathToCatalogForTest("elt-ddl-addedtable.jar");
        final String deploymentURL = Configuration.getPathToCatalogForTest("elt-ddl-addedtable.xml");
        final ClientResponse callProcedure = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL,
                deploymentURL);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        // make a new tester and see if it gets the new advertisement!
        m_tester.disconnectFromELServers();
        m_tester = new ExportTestClient(getServerConfig().getNodeCount());
        m_tester.connectToELServers(null, null);

        // verify that it exports
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ADDED_TABLE", i, convertValsToRow(i, 'I', rowdata));
            final Object[]  params = convertValsToParams("ADDED_TABLE", i, rowdata);
            client.callProcedure("InsertAddedTable", params);
        }

        quiesceAndVerify(client, m_tester);
    }

    /*
     *  Test ELT of a DROPPED table.  Queues some data to a table.
     *  Then drops the table and verifies that ELT can successfully
     *  drain the dropped table. IE, drop table doesn't lose elt data.
     */
    public void testELTAndDroppedTable() throws Exception {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }

        // now drop the no-nulls table
        final String newCatalogURL = Configuration.getPathToCatalogForTest("elt-ddl-sans-nonulls.jar");
        final String deploymentURL = Configuration.getPathToCatalogForTest("elt-ddl-sans-nonulls.xml");
        final ClientResponse callProcedure = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL,
                deploymentURL);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        // must still be able to verify the elt data.
        quiesceAndVerify(client, m_tester);
    }

    /**
     * Verify safe startup (we can connect clients and poll empty tables)
     */
    public void testELTSafeStartup() throws Exception
    {
        final Client client = getClient();
        quiesceAndVerify(client, m_tester);
    }

    /**
     * Sends ten tuples to an EL enabled VoltServer and verifies the receipt
     * of those tuples after a quiesce (shutdown). Base case.
     */
    public void testELTRoundTripPersistentTable() throws Exception
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));

            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_tester);
    }

    public void testELTLocalServerTooMany() throws Exception
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    public void testELTLocalServerTooMany2() throws Exception
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            // register only even rows with tester
            if ((i % 2) == 0)
            {
                m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            }
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    /** Verify test infrastructure fails a test that sends too few rows */
    public void testELTLocalServerTooFew() throws Exception
    {
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            // Only do the first 7 inserts
            if (i < 7)
            {
                client.callProcedure("Insert", params);
            }
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    /** Verify test infrastructure fails a test that sends mismatched data */
    public void testELTLocalServerBadData() throws Exception
    {
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            // add wrong pkeys on purpose!
            m_tester.addRow("ALLOW_NULLS", i + 10, convertValsToRow(i+10, 'I', rowdata));
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    /**
     * Sends ten tuples to an EL enabled VoltServer and verifies the receipt
     * of those tuples after a quiesce (shutdown). Base case.
     */
    public void testELTRoundTripStreamedTable() throws Exception
    {
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_tester);
    }


    /** Test that a table w/o ELT enabled does not produce ELT content */
    public void testThatTablesOptIn() throws Exception
    {
        final Client client = getClient();

        final Object params[] = new Object[TestSQLTypesSuite.COLS + 2];
        params[0] = "WITH_DEFAULTS";  // this table should not produce ELT output

        // populate the row data
        for (int i=0; i < TestSQLTypesSuite.COLS; ++i) {
            params[i+2] = TestSQLTypesSuite.m_midValues[i];
        }

        for (int i=0; i < 10; i++) {
            params[1] = i; // pkey
            // do NOT add row to TupleVerfier as none should be produced
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_tester);
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

    /*
     * Sends many tuples to an EL enabled VoltServer and verifies the receipt
     * of each in the EL stream. Some procedures rollback (after a real insert).
     * Tests that streams are correct in the face of rollback.
     */
    public void testELTRollback() throws Exception {
        final Client client = getClient();

        final double rollbackPerc = 0.15;
        double random = Math.random(); // initializes the generator

        // eltxxx: should pick more random data
        final Object[] rowdata = TestSQLTypesSuite.m_midValues;

        // roughly 10k rows is a full buffer it seems
        for (int pkey=0; pkey < 175000; pkey++) {
            if ((pkey % 1000) == 0) {
                System.out.println("Rollback test added " + pkey + " rows");
            }
            final Object[] params = convertValsToParams("ALLOW_NULLS", pkey, rowdata);
            random = Math.random();
            if (random <= rollbackPerc) {
                // note - do not update the el verifier as this rollsback
                boolean done;
                do {
                    done = client.callProcedure(new RollbackCallback(), "RollbackInsert", params);
                    if (done == false) {
                        client.backpressureBarrier();
                    }
                } while (!done);
            }
            else {
                m_tester.addRow("ALLOW_NULLS", pkey, convertValsToRow(pkey, 'I', rowdata));
                // the sync call back isn't synchronous if it isn't explicitly blocked on...
                boolean done;
                do {
                    done = client.callProcedure(new NullCallback(), "Insert", params);
                    if (done == false) {
                        client.backpressureBarrier();
                    }
                } while (!done);
            }
        }
        client.drain();
        quiesceAndVerify(client, m_tester);
    }

    private VoltTable createLoadTableTable(boolean addToVerifier, ExportTestClient tester) {

        final VoltTable loadTable = new VoltTable(new ColumnInfo("PKEY", VoltType.INTEGER),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[0], TestSQLTypesSuite.m_types[0]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[1], TestSQLTypesSuite.m_types[1]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[2], TestSQLTypesSuite.m_types[2]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[3], TestSQLTypesSuite.m_types[3]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[4], TestSQLTypesSuite.m_types[4]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[5], TestSQLTypesSuite.m_types[5]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[6], TestSQLTypesSuite.m_types[6]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[7], TestSQLTypesSuite.m_types[7]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[8], TestSQLTypesSuite.m_types[8]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[9], TestSQLTypesSuite.m_types[9]),
          new ColumnInfo(TestSQLTypesSuite.m_columnNames[10], TestSQLTypesSuite.m_types[10]));

        for (int i=0; i < 100; i++) {
            if (addToVerifier) {
                tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'I', TestSQLTypesSuite.m_midValues));
            }
            loadTable.addRow(convertValsToLoaderRow(i, TestSQLTypesSuite.m_midValues));
        }
        return loadTable;
    }

    /*
     * Verify that allowELT = no is obeyed for @LoadMultipartitionTable
     */
    public void testLoadMultiPartitionTableELTOff() throws Exception
    {
        // allow ELT is off. no rows added to the verifier
        final VoltTable loadTable = createLoadTableTable(false, m_tester);
        final Client client = getClient();
        client.callProcedure("@LoadMultipartitionTable", "ALLOW_NULLS", loadTable, 0);
        quiesceAndVerify(client, m_tester);
    }

    /*
     * Verify that allowELT = yes is obeyed for @LoadMultipartitionTable
     */
    public void testLoadMultiPartitionTableELTOn() throws Exception
    {
        // allow ELT is on. rows added to the verifier
        final VoltTable loadTable = createLoadTableTable(true, m_tester);
        final Client client = getClient();
        client.callProcedure("@LoadMultipartitionTable", "ALLOW_NULLS", loadTable, 1);
        quiesceAndVerify(client, m_tester);
    }

    /*
     * Verify that allowELT = yes is obeyed for @LoadMultipartitionTable
     */
    public void testLoadMultiPartitionTableELTOn2() throws Exception
    {
        // allow ELT is on but table is not opted in to ELT.
        final VoltTable loadTable = createLoadTableTable(false, m_tester);
        final Client client = getClient();
        client.callProcedure("@LoadMultipartitionTable", "WITH_DEFAULTS", loadTable, 1);
        quiesceAndVerify(client, m_tester);
    }

    /*
     * Verify that planner rejects updates to append-only tables
     */
    public void testELTUpdateAppendOnly() throws IOException {
        final Client client = getClient();
        boolean passed = false;
        try {
            client.callProcedure("@AdHoc", "Update NO_NULLS SET A_TINYINT=0 WHERE PKEY=0;");
        }
        catch (final ProcCallException e) {
            if (e.getMessage().contains("Illegal to update an export-only table.")) {
                passed = true;
            }
        }
        assertTrue(passed);
    }

    /*
     * Verify that planner rejects reads of append-only tables.
     */
    public void testELTSelectAppendOnly() throws IOException {
        final Client client = getClient();
        boolean passed = false;
        try {
            client.callProcedure("@AdHoc", "Select PKEY from NO_NULLS WHERE PKEY=0;");
        }
        catch (final ProcCallException e) {
            if (e.getMessage().contains("Illegal to read an export-only table.")) {
                passed = true;
            }
        }
        assertTrue(passed);
    }

    /*
     *  Verify that planner rejects deletes of append-only tables
     */
    public void testELTDeleteAppendOnly() throws IOException {
        final Client client = getClient();
        boolean passed = false;
        try {
            client.callProcedure("@AdHoc", "DELETE from NO_NULLS WHERE PKEY=0;");
        }
        catch (final ProcCallException e) {
            if (e.getMessage().contains("Illegal to delete from an export-only table.")) {
                passed = true;
            }
        }
        assertTrue(passed);
    }

    /**
     * Verify round trips of updates to a persistent table.
     */
    public void testELTDeletes() throws Exception
    {
        final Client client = getClient();

        // insert
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }

        for (int i=0; i < 10; i++) {
            // add the full 'D' row
            final Object[] rowdata_d = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'D', rowdata_d));

            // perform the delete
            client.callProcedure("Delete", "ALLOW_NULLS", i);
        }
        quiesceAndVerify(client, m_tester);
    }

    /**
     * Verify round trips of updates to a persistent table.
     */
    public void testELTUpdates() throws Exception
    {
        final Client client = getClient();

        // insert
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }

        // update
        for (int i=0; i < 10; i++) {
            // add the 'D' row
            final Object[] rowdata_d = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'D', rowdata_d));

            // calculate the update and add that to the m_tester
            final Object[] rowdata_i = TestSQLTypesSuite.m_defaultValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata_i));

            // perform the update
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata_i);
            client.callProcedure("Update_ELT", params);
        }

        // delete
        for (int i=0; i < 10; i++) {
            // add the full 'D' row
            final Object[] rowdata_d = TestSQLTypesSuite.m_defaultValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'D', rowdata_d));

            // perform the delete
            client.callProcedure("Delete", "ALLOW_NULLS", i);
        }

        quiesceAndVerify(client, m_tester);
    }

    /**
     * Multi-table test
     */
    public void testELTMultiTable() throws Exception
    {
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_tester.addRow("NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_tester);
    }

    /*
     * Verify that snapshot can be enabled with a streamed table present
     */
    public void testELTPlusSnapshot() throws Exception {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_tester.addRow("NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        // this blocks until the snapshot is complete
        client.callProcedure("@SnapshotSave", "/tmp", "testELTPlusSnapshot", (byte)1).getResults();

        // verify. copped from TestSaveRestoreSysprocSuite
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream ps = new PrintStream(baos);
        final PrintStream original = System.out;
        try {
            System.setOut(ps);
            final String args[] = new String[] {
                    "testELTPlusSnapshot",
                    "--dir",
                    "/tmp"
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
        quiesceAndVerify(client, m_tester);
    }


    /*
     * Test suite boilerplate
     */
    static final Class<?>[] PROCEDURES = {
        Insert.class,
        RollbackInsert.class,
        Update_ELT.class,
        Delete.class
    };

    static final Class<?>[] PROCEDURES2 = {
        Update_ELT.class
    };

    static final Class<?>[] PROCEDURES3 = {
        InsertAddedTable.class
    };

    public TestExportSuite(final String name) {
        super(name);
    }

    public static void main(final String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestOrderBySuite.class);
    }

    static public junit.framework.Test suite()
    {
        VoltServerConfig config;

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestExportSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addSchema(TestExportSuite.class.getResource("sqltypessuite-nonulls-ddl.sql"));
        project.addELT("org.voltdb.elt.processors.RawProcessor",
                       true  /*enabled*/,
                       null  /* authGroups (off) */);
        // "WITH_DEFAULTS" is a non-elt'd persistent table
        project.addELTTable("ALLOW_NULLS", false);   // persistent table
        project.addELTTable("NO_NULLS", true);       // streamed table
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
//        config = new LocalSingleProcessServer("elt-ddl.jar", 2,
//                                              BackendTarget.NATIVE_EE_JNI);
//        config.compile(project);
//        builder.addServerConfig(config);


        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("elt-ddl-cluster-rep.jar", 2, 3, 1,
                                  BackendTarget.NATIVE_EE_JNI);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);


        /*
         * compile a catalog without the NO_NULLS table for add/drop tests
         */
        config = new LocalCluster("elt-ddl-sans-nonulls.jar", 2, 3, 1,
                                              BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project.addSchema(TestExportSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addELT("org.voltdb.elt.processors.RawProcessor",
                       true  /*enabled*/,
                       null  /* authGroups (off) */);
        // "WITH_DEFAULTS" is a non-elt'd persistent table
        project.addELTTable("ALLOW_NULLS", false);   // persistent table

        // and then project builder as normal
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addProcedures(PROCEDURES2);
        compile = config.compile(project);
        TestCatalogUpdateSuite.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("elt-ddl-sans-nonulls.xml"));
        assertTrue(compile);

        /*
         * compile a catalog with an added table for add/drop tests
         */
        config = new LocalCluster("elt-ddl-addedtable.jar", 2, 3, 1,
                                  BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project.addSchema(TestExportSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addSchema(TestExportSuite.class.getResource("sqltypessuite-nonulls-ddl.sql"));
        project.addSchema(TestExportSuite.class.getResource("sqltypessuite-addedtable-ddl.sql"));
        project.addELT("org.voltdb.elt.processors.RawProcessor",
                       true  /*enabled*/,
                       null  /* authGroups (off) */);
        // "WITH_DEFAULTS" is a non-elt'd persistent table
        project.addELTTable("ALLOW_NULLS", false);   // persistent table
        project.addELTTable("ADDED_TABLE", false);   // persistent table
        project.addELTTable("NO_NULLS", true);       // streamed table

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
        TestCatalogUpdateSuite.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("elt-ddl-addedtable.xml"));
        assertTrue(compile);


        return builder;
    }
}
