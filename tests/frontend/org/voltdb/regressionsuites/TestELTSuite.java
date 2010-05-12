/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.elt.ELTestClient;
import org.voltdb.regressionsuites.sqltypesprocs.Delete;
import org.voltdb.regressionsuites.sqltypesprocs.Insert;
import org.voltdb.regressionsuites.sqltypesprocs.RollbackInsert;
import org.voltdb.regressionsuites.sqltypesprocs.Update_ELT;

/**
 *  End to end ELT tests using the RawProcessor and the ELSinkServer.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an ELT round trip test.
 */

public class TestELTSuite extends RegressionSuite {

    private ELTestClient m_tester;

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
    private Object[] convertValsToRow(final int i, final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 1];
        row[0] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            row[ii+1] = rowdata[ii];
        return row;
    }

    private void quiesce(final Client client)
    throws ProcCallException, NoConnectionsException, IOException
    {
        client.drain();
        client.callProcedure("@Quiesce");
    }

    private void quiesceAndVerify(final Client client, ELTestClient tester)
    throws ProcCallException, IOException
    {
        quiesce(client);
        tester.work();
        assertTrue(tester.allRowsVerified());
        assertTrue(tester.verifyEltOffsets());
    }

    private void quiesceAndVerifyFalse(final Client client, ELTestClient tester)
    throws ProcCallException, IOException
    {
        quiesce(client);
        tester.work();
        assertFalse(tester.allRowsVerified());
    }

    @Override
    public void setUp()
    {
        super.setUp();
        m_tester = new ELTestClient(getServerConfig().getNodeCount());
        m_tester.connectToELServers();
    }

    /**
     * Verify safe startup (we can connect clients and poll empty tables)
     */
    public void testELTSafeStartup() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();
        quiesceAndVerify(client, m_tester);
    }

    /**
     * Sends ten tuples to an EL enabled VoltServer and verifies the receipt
     * of those tuples after a quiesce (shutdown). Base case.
     */
    public void testELTRoundTripPersistentTable() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, rowdata));

            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_tester);
    }

    public void testELTLocalServerTooMany() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    public void testELTLocalServerTooMany2() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            // register only even rows with tester
            if ((i % 2) == 0)
            {
                m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, rowdata));
            }
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    /** Verify test infrastructure fails a test that sends too few rows */
    public void testELTLocalServerTooFew() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, rowdata));
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
    public void testELTLocalServerBadData() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            // add wrong pkeys on purpose!
            m_tester.addRow("ALLOW_NULLS", i + 10, convertValsToRow(i+10, rowdata));
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    /**
     * Sends ten tuples to an EL enabled VoltServer and verifies the receipt
     * of those tuples after a quiesce (shutdown). Base case.
     */
    public void testELTRoundTripStreamedTable() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("NO_NULLS", i, convertValsToRow(i, rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_tester);
    }

    /** Test that a table w/o ELT enabled does not produce ELT content */
    public void testThatTablesOptIn() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();

        Object params[] = new Object[TestSQLTypesSuite.COLS + 2];
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


    class RollbackCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() != ClientResponse.USER_ABORT) {
                fail();
            }
        }
    }

    /*
     * Sends many tuples to an EL enabled VoltServer and verifies the receipt
     * of each in the EL stream. Some procedures rollback (after a real insert).
     * Tests that streams are correct in the face of rollback.
     */
    public void testELTRollback() throws IOException, ProcCallException, InterruptedException {
        final Client client = getClient();

        double rollbackPerc = 0.15;
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
                m_tester.addRow("ALLOW_NULLS", pkey, convertValsToRow(pkey, rowdata));
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

    private VoltTable createLoadTableTable(boolean addToVerifier, ELTestClient tester) {

        VoltTable loadTable = new VoltTable(new ColumnInfo("PKEY", VoltType.INTEGER),
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
                tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, TestSQLTypesSuite.m_midValues));
            }
            loadTable.addRow(convertValsToRow(i, TestSQLTypesSuite.m_midValues));
        }
        return loadTable;
    }

    /*
     * Verify that allowELT = no is obeyed for @LoadMultipartitionTable
     */
    public void testLoadMultiPartitionTableELTOff() throws IOException, ProcCallException
    {
        // allow ELT is off. no rows added to the verifier
        VoltTable loadTable = createLoadTableTable(false, m_tester);
        Client client = getClient();
        client.callProcedure("@LoadMultipartitionTable", "ALLOW_NULLS", loadTable, 0);
        quiesceAndVerify(client, m_tester);
    }

    /*
     * Verify that allowELT = yes is obeyed for @LoadMultipartitionTable
     */
    public void testLoadMultiPartitionTableELTOn() throws IOException, ProcCallException
    {
        // allow ELT is on. rows added to the verifier
        VoltTable loadTable = createLoadTableTable(true, m_tester);
        Client client = getClient();
        client.callProcedure("@LoadMultipartitionTable", "ALLOW_NULLS", loadTable, 1);
        quiesceAndVerify(client, m_tester);
    }

    /*
     * Verify that allowELT = yes is obeyed for @LoadMultipartitionTable
     */
    public void testLoadMultiPartitionTableELTOn2() throws IOException, ProcCallException
    {
        // allow ELT is on but table is not opted in to ELT.
        VoltTable loadTable = createLoadTableTable(false, m_tester);
        Client client = getClient();
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
        catch (ProcCallException e) {
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
        catch (ProcCallException e) {
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
        catch (ProcCallException e) {
            if (e.getMessage().contains("Illegal to delete from an export-only table.")) {
                passed = true;
            }
        }
        assertTrue(passed);
    }


    /**
     * Verify round trips of updates to a persistent table.
     */
    public void testELTDeletes() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();

        // insert
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, rowdata));
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }

        for (int i=0; i < 10; i++) {
            // add the full 'D' row
            Object[] rowdata_d = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, rowdata_d));

            // perform the delete
            client.callProcedure("Delete", "ALLOW_NULLS", i);
        }
        quiesceAndVerify(client, m_tester);
    }

    /**
     * Verify round trips of updates to a persistent table.
     */
    public void testELTUpdates() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();

        // insert
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, rowdata));
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }

        // update
        for (int i=0; i < 10; i++) {
            // add the 'D' row
            Object[] rowdata_d = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, rowdata_d));

            // calculate the update and add that to the m_tester
            Object[] rowdata_i = TestSQLTypesSuite.m_defaultValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i,rowdata_i));

            // perform the update
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata_i);
            client.callProcedure("Update_ELT", params);
        }

        // delete
        for (int i=0; i < 10; i++) {
            // add the full 'D' row
            Object[] rowdata_d = TestSQLTypesSuite.m_defaultValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, rowdata_d));

            // perform the delete
            client.callProcedure("Delete", "ALLOW_NULLS", i);
        }

        quiesceAndVerify(client, m_tester);
    }

    /**
     * Multi-table test
     */
    /**
     * Verify round trips of updates to a persistent table.
     */
    public void testELTMultiTable() throws IOException, ProcCallException, InterruptedException
    {
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow("ALLOW_NULLS", i, convertValsToRow(i, rowdata));
            Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_tester.addRow("NO_NULLS", i, convertValsToRow(i, rowdata));
            params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
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

    public TestELTSuite(final String name) {
        super(name);
    }

    public static void main(final String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestOrderBySuite.class);
    }

    static public junit.framework.Test suite()
    {
        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestELTSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestELTSuite.class.getResource("sqltypessuite-ddl.sql"));
        // add the connector/processor (name, host, port)
        // and the exportable tables (name, export-only)
        project.addELT("org.voltdb.elt.processors.RawProcessor", true /*enabled*/);
        // "WITH_DEFAULTS" is a non-elt'd persistent table
        project.addELTTable("ALLOW_NULLS", false);   // persistent table
        project.addELTTable("NO_NULLS", true);       // streamed table
        // and then project builder as normal
        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addProcedures(PROCEDURES);

        VoltServerConfig config;
        // JNI, single server
        config = new LocalSingleProcessServer("elt-ddl.jar", 2,
                                              BackendTarget.NATIVE_EE_JNI);

        config.compile(project);
        builder.addServerConfig(config);

        // three host, two site-per-host, k=1 replication config
        config = new LocalCluster("elt-ddl-cluster-rep.jar", 2, 3, 1,
                                  BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        return builder;
    }
}
