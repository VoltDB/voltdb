/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestExplainCommandSuite extends RegressionSuite {

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestExplainCommandSuite(String name) {
        super(name);
    }

    public void testExplain() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        String[] strs = {"SELECT COUNT(*) FROM T1 order by A_INT", "SELECT COUNT(*) FROM T1 order by A_INT"};

        vt = client.callProcedure("@Explain", (Object[]) strs ).getResults()[0];
        while( vt.advanceRow() ) {
            System.out.println(vt);
            String plan = vt.getString("EXEcution_PlaN");
            assertTrue( plan.contains( "RETURN RESULTS TO STORED PROCEDURE" ));
            // Validate bypass of no-op sort on single-row result.
            assertFalse( plan.contains( "ORDER BY (SORT)"));
            assertTrue( plan.contains( "TABLE COUNT" ));
        }

        //test the index count node
        vt = client.callProcedure("@Explain", "SELECT COUNT(*) FROM t3 where I3 < 100" ).getResults()[0];
        while( vt.advanceRow() ) {
            System.out.println(vt);
            String plan = vt.getString(0);
            assertTrue( plan.contains("INDEX COUNT") );
        }

        //test expression index usage
        vt = client.callProcedure("@Explain", "SELECT * FROM t3 where I3 + I4 < 100" ).getResults()[0];
        while( vt.advanceRow() ) {
            System.out.println(vt);
            String plan = vt.getString(0);
            assertTrue( plan.contains("INDEX SCAN") );
        }
}

    public void testExplainProc() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        vt = client.callProcedure("@ExplainProc", "T1.insert" ).getResults()[0];
        while( vt.advanceRow() ) {
            System.out.println(vt);
            String sql = vt.getString(0);
            String plan = vt.getString(1);
            assertTrue( sql.contains( "INSERT INTO T1 VALUES (?, ?, ?)" ));
            assertTrue( plan.contains( "INSERT into \"T1\"" ));
            assertTrue( plan.contains( "MATERIALIZE TUPLE from parameters and/or literals" ));
        }
    }

    public void testExplainSingleTableView() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        // Test if the error checking is working properly.
        verifyProcFails(client, "View T does not exist.", "@ExplainView", "T");
        verifyProcFails(client, "Table T1 is not a view.", "@ExplainView", "T1");

        final String[] aggTypes = {"MAX", "MIN"};
        final int numOfMinMaxColumns = 12;

        // -1- At this point there is no auxiliary indices at all,
        //     all min/max view columns should use built-in sequential scan to refresh.
        vt = client.callProcedure("@ExplainView", "V1" ).getResults()[0];
        assertEquals(numOfMinMaxColumns, vt.getRowCount());
        for (int i = 0; i < numOfMinMaxColumns; i++) {
            vt.advanceRow();
            String task = vt.getString(0);
            String plan = vt.getString(1);
            assertEquals("Refresh " + aggTypes[i % 2] + " column \"C" + i + "\"", task);
            assertEquals("Built-in sequential scan.", plan);
        }

        // -2- Create an index on TSRC1(G1), then all columns will use built-in index scan now.
        client.callProcedure("@AdHoc", "CREATE INDEX IDX_TSRC1_G1 ON TSRC1(G1);");
        vt = client.callProcedure("@ExplainView", "V1" ).getResults()[0];
        assertEquals(numOfMinMaxColumns, vt.getRowCount());
        for (int i = 0; i < numOfMinMaxColumns; i++) {
            vt.advanceRow();
            String task = vt.getString(0);
            String plan = vt.getString(1);
            assertEquals("Refresh " + aggTypes[i % 2] + " column \"C" + i + "\"", task);
            assertEquals("Built-in index scan \"IDX_TSRC1_G1\".", plan);
        }

        // -3- Create an index on TSRC1(G1, C1), C1 will pick up the new index.
        client.callProcedure("@AdHoc", "CREATE INDEX IDX_TSRC1_G1C1 ON TSRC1(G1, C1);");
        vt = client.callProcedure("@ExplainView", "V1" ).getResults()[0];
        assertEquals(numOfMinMaxColumns, vt.getRowCount());
        for (int i = 0; i < numOfMinMaxColumns; i++) {
            vt.advanceRow();
            String task = vt.getString(0);
            String plan = vt.getString(1);
            assertEquals("Refresh " + aggTypes[i % 2] + " column \"C" + i + "\"", task);
            if (i != 1) {
                assertEquals("Built-in index scan \"IDX_TSRC1_G1\".", plan);
            }
            else {
                assertEquals("Built-in index scan \"IDX_TSRC1_G1C1\".", plan);
            }
        }

        // -4- Remove index IDX_TSRC1_G1.
        //     C1 will continue to use IDX_TSRC1_G1C1,
        //     The rest columns will start to use query plans.
        //     The query plans are index scans on IDX_TSRC1_G1C1 with range-scan setting.
        client.callProcedure("@AdHoc", "DROP INDEX IDX_TSRC1_G1;");
        vt = client.callProcedure("@ExplainView", "V1" ).getResults()[0];
        assertEquals(numOfMinMaxColumns, vt.getRowCount());
        for (int i = 0; i < numOfMinMaxColumns; i++) {
            vt.advanceRow();
            String task = vt.getString(0);
            String plan = vt.getString(1);
            assertEquals("Refresh " + aggTypes[i % 2] + " column \"C" + i + "\"", task);
            if (i != 1) {
                assertTrue(plan.contains("INDEX SCAN of \"TSRC1\" using \"IDX_TSRC1_G1C1\""));
                assertTrue(plan.contains("range-scan on 1 of 2 cols from (G1 >= ?0) while (G1 = ?0)"));
            }
            else {
                assertEquals("Built-in index scan \"IDX_TSRC1_G1C1\".", plan);
            }
        }
        client.callProcedure("@AdHoc", "DROP INDEX IDX_TSRC1_G1C1;");
    }

    public void testExplainMultiTableView() throws IOException, ProcCallException {
        Client client = getClient();
        final String[] aggTypes = {"MAX", "MIN"};
        final int numOfMinMaxColumns = 12;
        VoltTable vt = client.callProcedure("@ExplainView", "V2" ).getResults()[0];
        assertEquals(numOfMinMaxColumns + 1, vt.getRowCount());

        // -1- Check the join evaluation query plan
        // -2- Check the query plans for refreshing MIN / MAX columns
        for (int i = 0; i <= numOfMinMaxColumns; i++) {
            vt.advanceRow();
            String task = vt.getString(0);
            String plan = vt.getString(1);
            if (i == 0) {
                assertEquals("Join Evaluation", task);
            }
            else {
                assertEquals("Refresh " + aggTypes[(i-1) % 2] + " column \"C" + (i-1) + "\"", task);
            }
            assertTrue(plan.contains("NESTLOOP INDEX INNER JOIN"));
            assertTrue(plan.contains("inline INDEX SCAN of \"TSRC1\" using its primary key index"));
            assertTrue(plan.contains("SEQUENTIAL SCAN of \"TSRC2\""));
        }
    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        VoltServerConfig config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestExplainCommandSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestExplainCommandSuite.class.getResource("testExplainCommand-ddl.sql"));
        project.addPartitionInfo("t1", "PKEY");
        project.addPartitionInfo("t2", "PKEY");
        project.addPartitionInfo("t3", "PKEY");
        project.setUseDDLSchema(true);

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("testExplainCommand-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
