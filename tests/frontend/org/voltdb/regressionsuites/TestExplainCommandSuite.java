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
package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.Test;

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

        String[] strs = {"SELECT COUNT(*) FROM T1", "SELECT COUNT(*) FROM T1"};

        vt = client.callProcedure("@Explain", (Object[]) strs ).getResults()[0];
        while (vt.advanceRow()) {
            System.out.println(vt);
            String plan = vt.getString("EXEcution_PlaN");
            assertTrue( plan.contains( "RETURN RESULTS TO STORED PROCEDURE" ));
            // Validate bypass of no-op sort on single-row result.
            assertFalse( plan.contains( "ORDER BY (SORT)"));
            assertTrue( plan.contains( "TABLE COUNT" ));
        }

        //test the index count node
        vt = client.callProcedure("@Explain", "SELECT COUNT(*) FROM t3 where I3 < 100" ).getResults()[0];
        while (vt.advanceRow()) {
            System.out.println(vt);
            String plan = vt.getString(0);
            assertTrue( plan.contains("INDEX COUNT") );
        }

        //test expression index usage
        vt = client.callProcedure("@Explain", "SELECT * FROM t3 where I3 + I4 < 100" ).getResults()[0];
        while (vt.advanceRow()) {
            System.out.println(vt);
            String plan = vt.getString(0);
            assertTrue( plan.contains("INDEX SCAN") );
        }
    }

    public void testExplainProc() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        vt = client.callProcedure("@ExplainProc", "T1.insert" ).getResults()[0];
        while (vt.advanceRow()) {
            System.out.println(vt);
            String name = vt.getString(0);
            String sql = vt.getString(1);
            String plan = vt.getString(2);
            assertEquals("sql0", name);
            assertEquals("INSERT INTO T1 VALUES (?, ?, ?);", sql);
            assertTrue(plan.contains("INSERT into \"T1\""));
            assertTrue(plan.contains("MATERIALIZE TUPLE from parameters and/or literals"));
        }

        //test stored procedure loaded from Java class
        vt = client.callProcedure("@ExplainProc", "JavaProcedure").getResults()[0];
        for (int i = 0; i < 2; i++) {
            vt.advanceRow();
            String name = vt.getString(0);
            String task = vt.getString(1);
            String plan = vt.getString(2);
            if (i == 0) {
                assertEquals("insert", name);
                assertEquals("insert into t4 values(?);", task);
                assertTrue(plan.contains("RECEIVE FROM ALL PARTITIONS"));
                assertTrue(plan.contains("SEND PARTITION RESULTS TO COORDINATOR"));
                assertTrue(plan.contains("INSERT into \"T4\""));
                assertTrue(plan.contains("MATERIALIZE TUPLE from parameters and/or literals"));
            } else {
                assertEquals("select", name);
                assertEquals("select * from t4 where A>=?;", task);
                assertTrue(plan.contains("RETURN RESULTS TO STORED PROCEDURE"));
                assertTrue(plan.contains("SEQUENTIAL SCAN of \"T4\""));
            }
        }
    }

    public void testExplainView() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        // Test if the error checking is working properly.
        verifyProcFails(client, "View T does not exist.", "@ExplainView", "T");
        verifyProcFails(client, "Table T1 is not a view.", "@ExplainView", "T1");

        vt = client.callProcedure("@ExplainView", "v" ).getResults()[0];
        vt.advanceRow();
        String task = vt.getString(0);
        String plan = vt.getString(1);
        assertEquals("", task);
        assertEquals("No query plan is being used.", plan);
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

    public void testExplainMultiStmtProc() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        // test for multi partition query
        String[] sql = new String[]{
                "insert into t1 values (?, ?, ?);",
                "select * from t1;"};
        vt = client.callProcedure("@ExplainProc", "MultiSP" ).getResults()[0];
        assertEquals(sql.length, vt.getRowCount());

        // -1- insert into t1
        // -2- select * from t1
        for (int i = 0; i < sql.length; i++) {
            vt.advanceRow();
            String name = vt.getString(0);
            String task = vt.getString(1);
            String plan = vt.getString(2);
            assertEquals("sql" + i, name);
            assertEquals(sql[i], task);
            assertTrue(plan.contains("RECEIVE FROM ALL PARTITIONS"));
            assertTrue(plan.contains("SEND PARTITION RESULTS TO COORDINATOR"));
            if (i == 0) {
                assertTrue(plan.contains("INSERT into \"T1\""));
                assertTrue(plan.contains("MATERIALIZE TUPLE from parameters and/or literals"));
            } else {
                assertTrue(plan.contains("SEQUENTIAL SCAN of \"T1\""));
            }
        }

        // test for single partition query
        sql = new String[]{
                "insert into t2 values (?, ?, ?);",
                "select * from t2;"};
        vt = client.callProcedure("@ExplainProc", "MultiSPSingle" ).getResults()[0];
        assertEquals(sql.length, vt.getRowCount());

        // -1- insert into t2
        // -2- select * from t2
        // t2 is partitioned on PKEY
        for (int i = 0; i < sql.length; i++) {
            vt.advanceRow();
            String name = vt.getString(0);
            String task = vt.getString(1);
            String plan = vt.getString(2);
            assertEquals("sql" + i, name);
            assertEquals(sql[i], task);
            // note that there is no send and receive data from all partitions unlike above query
            assertFalse(plan.contains("RECEIVE FROM ALL PARTITIONS"));
            assertFalse(plan.contains("SEND PARTITION RESULTS TO COORDINATOR"));
            if (i == 0) {
                assertTrue(plan.contains("INSERT into \"T2\""));
                assertTrue(plan.contains("MATERIALIZE TUPLE from parameters and/or literals"));
            } else {
                assertTrue(plan.contains("SEQUENTIAL SCAN of \"T2\""));
            }
        }

        // test for single partition query
        sql = new String[]{
                "select * from t2 where PKEY = ? AND A_INT = ?;",
                "insert into t2 values (?, ?, ?);",
                "select * from t2;"};
        vt = client.callProcedure("@ExplainProc", "MultiSPSingle1" ).getResults()[0];
        assertEquals(sql.length, vt.getRowCount());

        // -1- select * from t2 where PKEY = ? AND A_INT = ?
        // -2- insert into t2
        // -3- select * from t2
        // t2 is partitioned on PKEY
        for (int i = 0; i < sql.length; i++) {
            vt.advanceRow();
            String name = vt.getString(0);
            String task = vt.getString(1);
            String plan = vt.getString(2);
            assertEquals("sql" + i, name);
            assertEquals(sql[i], task);
            // note that there is no send and receive data from all partitions unlike above query
            assertFalse(plan.contains("RECEIVE FROM ALL PARTITIONS"));
            assertFalse(plan.contains("SEND PARTITION RESULTS TO COORDINATOR"));
            if (i == 0) {
                assertTrue(plan.contains("INDEX SCAN of \"T2\" using its primary key index"));
            } else if (i == 1) {
                assertTrue(plan.contains("INSERT into \"T2\""));
                assertTrue(plan.contains("MATERIALIZE TUPLE from parameters and/or literals"));
            } else {
                assertTrue(plan.contains("SEQUENTIAL SCAN of \"T2\""));
            }
        }
    }

    private void assertExplainOutput(VoltTable result, String[] expectedExplainations) {
        while (result.advanceRow()) {
            System.out.println(result);
            String plan = result.getString("EXEcution_PlaN");
            for (String expectedExplaination : expectedExplainations) {
                assertTrue(plan.contains(expectedExplaination));
            }
        }
    }

    public void testExplainCTE() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        String RCTE = // recursive test query
        "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS (\n" +
        "    SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME FROM EMPLOYEES WHERE MANAGER_ID = 0\n" +
        "    UNION ALL\n" +
        "    SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME\n" +
        "      FROM EMPLOYEES E JOIN EMP_PATH EP ON E.MANAGER_ID = EP.EMP_ID\n" +
        ")\n" +
        "SELECT * FROM EMP_PATH a JOIN EMP_PATH b ON 1=1 WHERE a.LAST_NAME IS NOT NULL;\n";
        String[] RCTEExplaination = new String[] {
        "RETURN RESULTS TO STORED PROCEDURE\n" +
        " NEST LOOP INNER JOIN\n" +
        "  SEQUENTIAL SCAN of COMMON TABLE \"EMP_PATH (A)\"\n",
        // "   filter by (NOT (column#0 IS NULL))\n",
        "   MATERIALIZE COMMON TABLE \"EMP_PATH\"\n" +
        "   START WITH SEQUENTIAL SCAN of \"EMPLOYEES\"\n",
        // "    filter by (column#2 = 0)\n",
        "   ITERATE UNTIL EMPTY NEST LOOP INNER JOIN\n",
        // "    filter by (inner-table.column#0 = column#2)\n",
        "    SEQUENTIAL SCAN of \"EMPLOYEES (E)\"\n" +
        "    SEQUENTIAL SCAN of COMMON TABLE \"EMP_PATH (EP)\"\n" +
        "     only fetch results from the previous iteration\n\n" +
        "  SEQUENTIAL SCAN of COMMON TABLE \"EMP_PATH (B)\"\n"};

        String NRCTE = // non-recursive test query
        "WITH EMP_BASE(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS (\n" +
        "    SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME FROM EMPLOYEES WHERE MANAGER_ID = 0\n" +
        ")\n" +
        "SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EB.LEVEL+1, EB.PATH || '/' || E.LAST_NAME\n" +
        "  FROM EMPLOYEES E JOIN EMP_BASE EB ON E.MANAGER_ID = EB.EMP_ID;\n";
        String[] NRCTEExplaination = new String[] {
        "RETURN RESULTS TO STORED PROCEDURE\n" +
        " NEST LOOP INNER JOIN\n",
        // "  filter by (inner-table.column#0 = column#2)\n",
        "  SEQUENTIAL SCAN of \"EMPLOYEES (E)\"\n" +
        "  SEQUENTIAL SCAN of COMMON TABLE \"EMP_BASE (EB)\"\n" +
        "   MATERIALIZE COMMON TABLE \"EMP_BASE\"\n" +
        "   FROM SEQUENTIAL SCAN of \"EMPLOYEES\"\n"
        // "    filter by (column#2 = ?1)\n"
        };

        vt = client.callProcedure("@Explain", RCTE).getResults()[0];
        assertExplainOutput(vt, RCTEExplaination);
        vt = client.callProcedure("@Explain", NRCTE).getResults()[0];
        assertExplainOutput(vt, NRCTEExplaination);
        vt = client.callProcedure("@ExplainProc", "RCTE").getResults()[0];
        assertExplainOutput(vt, RCTEExplaination);
        vt = client.callProcedure("@ExplainProc", "NRCTE").getResults()[0];
        assertExplainOutput(vt, NRCTEExplaination);
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
