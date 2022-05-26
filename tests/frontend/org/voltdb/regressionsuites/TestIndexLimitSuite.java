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
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.BatchedMultiPartitionTest;

import junit.framework.Test;
public class TestIndexLimitSuite extends RegressionSuite {
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestIndexLimitSuite(String name) {
        super(name);
    }

    void callWithExpectedResult(Client client, Integer ret, String procName, Object... params)
            throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = client.callProcedure(procName, params);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        if (ret == null) {
            long unexpected = result.getLong(0);
            assertTrue("Unexpected non-null value: " + unexpected, result.wasNull());
        }
        else {
            assertEquals(ret.intValue(), result.getLong(0));
        }
    }

    void callWithExpectedResult(Client client, String ret, String procName, Object... params)
            throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = client.callProcedure(procName, params);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(ret, result.getString(0));
    }

    void callWithExpectedResult(Client client, boolean ret, String procName, Object... params)
            throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = client.callProcedure(procName, params);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        if (ret) {
            assertTrue(result.getString(0).contains("LIMIT"));
        } else {
            assertFalse(result.getString(0).contains("LIMIT"));
        }
    }

    public void testPureColumnIndex() throws Exception {
        Client client = getClient();

        client.callProcedure("TU1.insert", 1, 1);
        client.callProcedure("TU1.insert", 2, 2);
        client.callProcedure("TU1.insert", 3, 3);
        client.callProcedure("TU2.insert", 1, 1, "jim");
        client.callProcedure("TU2.insert", 2, 2, "thea");
        client.callProcedure("TU2.insert", 3, 3, "betty");
        client.callProcedure("TU3.insert", 1, 1, 1);
        client.callProcedure("TU3.insert", 2, 2, 1);
        client.callProcedure("TU3.insert", 3, 3, 1);
        client.callProcedure("TU3.insert", 4, 1, 2);
        client.callProcedure("TU3.insert", 5, 2, 2);
        client.callProcedure("TU3.insert", 6, 3, 2);
        client.callProcedure("TU3.insert", 7, 1, 3);
        client.callProcedure("TU3.insert", 8, 2, 3);
        client.callProcedure("TU3.insert", 9, 3, 3);
        client.callProcedure("TU4.insert", 1, 1, "jim", 0);
        client.callProcedure("TU4.insert", 2, 2, "jim", 0);
        client.callProcedure("TU4.insert", 3, 3, "jim", 0);
        client.callProcedure("TU4.insert", 4, 4, "thea", 1);
        client.callProcedure("TU4.insert", 5, 5, "betty", 1);

        callWithExpectedResult(client, 1, "COL_TU1_MIN_POINTS");
        callWithExpectedResult(client, "thea", "COL_TU2_MAX_UNAME");
        callWithExpectedResult(client, 3, "COL_TU3_MAX_POINTS", 3);
        callWithExpectedResult(client, 2, "COL_TU3_MIN_TEL", 2);
        callWithExpectedResult(client, 1, "COL_TU4_MIN_POINTS_WHERE", "jim", 0);
    }

    public void testExpressionIndex() throws Exception {
        Client client = getClient();

        client.callProcedure("TU1.insert", 1, 1);
        client.callProcedure("TU1.insert", 2, 2);
        client.callProcedure("TU1.insert", 3, -3);
        client.callProcedure("TU2.insert", 1, -1, "jim");
        client.callProcedure("TU2.insert", 2, 2, "jim");
        client.callProcedure("TU2.insert", 3, 3, "jim");
        client.callProcedure("TU2.insert", 4, -1, "thea");
        client.callProcedure("TU2.insert", 5, 2, "thea");
        client.callProcedure("TU2.insert", 6, 3, "thea");
        client.callProcedure("TU2.insert", 7, -1, "betty");
        client.callProcedure("TU2.insert", 8, 2, "betty");
        client.callProcedure("TU2.insert", 9, 3, "betty");

        callWithExpectedResult(client, 1, "EXPR_TU1_MIN_FUNC");
        callWithExpectedResult(client, 0, "EXPR_TU1_MIN_ARTH");
        callWithExpectedResult(client, "thea", "EXPR_TU2_MAX_UNAME", 4);
        callWithExpectedResult(client, 2, "EXPR_TU2_MIN_ABS_POINTS", 2);
        callWithExpectedResult(client, 3, "EXPR_TU2_MAX_POINTS", "betty");
        callWithExpectedResult(client, 5, "EXPR_TU2_MAX_LENGTH_UNAME", 1);
    }

    public void testSpecialCases() throws Exception {
        Client client = getClient();

        client.callProcedure("TU1.insert", 1, 1);
        client.callProcedure("TU1.insert", 2, 2);
        client.callProcedure("TU1.insert", 3, -3);
        client.callProcedure("TU2.insert", 1, -1, "jim");
        client.callProcedure("TU2.insert", 2, 2, "jim");
        client.callProcedure("TU2.insert", 3, 3, "jim");
        client.callProcedure("TU2.insert", 4, -1, "thea");
        client.callProcedure("TU2.insert", 5, 2, "thea");
        client.callProcedure("TU2.insert", 6, 3, "thea");
        client.callProcedure("TU2.insert", 7, -1, "betty");
        client.callProcedure("TU2.insert", 8, 2, "betty");
        client.callProcedure("TU2.insert", 9, 3, "betty");

        callWithExpectedResult(client, 1, "EDGE_TU1_MIN_POINTS1", 0);
        callWithExpectedResult(client, 2, "EDGE_TU1_MIN_POINTS2", 2);
        callWithExpectedResult(client, -3, "EDGE_TU1_MIN_POINTS3", 1);
        callWithExpectedResult(client, -3, "EDGE_TU1_MIN_POINTS4", 2);
        callWithExpectedResult(client, 2, "EDGE_TU1_MAX_POINTS1", 0);
        callWithExpectedResult(client, 2, "EDGE_TU1_MAX_POINTS2", 2);
        callWithExpectedResult(client, 1, "EDGE_TU1_MAX_POINTS3", 2);
        callWithExpectedResult(client, 1, "EDGE_TU1_MAX_POINTS4", 1);
        callWithExpectedResult(client, -1, "EDGE_TU2_MIN_POINTS1", "jim", 5);
        callWithExpectedResult(client, 3, "EDGE_TU2_MAX_POINTS1", "thea", 1);
        callWithExpectedResult(client, -1, "EDGE_TU2_MIN_POINTS2", 5, "jim");
        callWithExpectedResult(client, 3, "EDGE_TU2_MAX_POINTS2", 1, "thea");
        callWithExpectedResult(client, 2, "EDGE_TU2_MIN_POINTS3", -1, "jim");
        callWithExpectedResult(client, 2, "EDGE_TU2_MAX_POINTS3", 3, "jim");
        callWithExpectedResult(client, -1, "EDGE_TU2_MIN_POINTS3", -2, "jim");
        callWithExpectedResult(client, 3, "EDGE_TU2_MAX_POINTS3", 4, "jim");
        callWithExpectedResult(client, (Integer)null, "EDGE_TU2_MIN_POINTS3", 4, "jim");
        callWithExpectedResult(client, (Integer)null, "EDGE_TU2_MAX_POINTS3", -1, "jim");
        callWithExpectedResult(client, -1, "EDGE_TU2_MIN_POINTS4", -2, "jim");
        callWithExpectedResult(client, 3, "EDGE_TU2_MAX_POINTS4", 4, "jim");
        callWithExpectedResult(client, -1, "EDGE_TU2_MIN_POINTS4", -1, "jim");
        callWithExpectedResult(client, 3, "EDGE_TU2_MAX_POINTS4", 3, "jim");
        callWithExpectedResult(client, (Integer)null, "EDGE_TU2_MIN_POINTS4", 4, "jim");
        callWithExpectedResult(client, -1, "EDGE_TU2_MAX_POINTS4", -1, "jim");
        callWithExpectedResult(client, (Integer)null, "EDGE_TU2_MIN_POINTS5", 4, "jim");
        callWithExpectedResult(client, -1, "EDGE_TU2_MAX_POINTS5", -1, "jim");
        callWithExpectedResult(client, (Integer)null, "EDGE_TU2_MIN_POINTS6", "betty");
        callWithExpectedResult(client, (Integer)null, "EDGE_TU2_MAX_POINTS6", "jim");
        callWithExpectedResult(client, 9, "EDGE_TU2_MIN_POINTS_EXPR", "jim", 5);
        callWithExpectedResult(client, 13, "EDGE_TU2_MAX_POINTS_EXPR", "betty", 7);
    }

    public void testAdHoc() throws Exception {
        Client client = getClient();

        client.callProcedure("TU1.insert", 1, 1);
        client.callProcedure("TU1.insert", 2, 2);
        client.callProcedure("TU1.insert", 3, -3);
        client.callProcedure("T1.insert", 1, 1);
        client.callProcedure("T1.insert", 2, 2);
        client.callProcedure("T1.insert", 3, -3);
        client.callProcedure("TU2.insert", 1, -1, "jim");
        client.callProcedure("TU2.insert", 2, 2, "jim");
        client.callProcedure("TU2.insert", 3, 3, "jim");
        client.callProcedure("TU2.insert", 4, -1, "thea");
        client.callProcedure("TU2.insert", 5, 2, "thea");
        client.callProcedure("TU2.insert", 6, 3, "thea");
        client.callProcedure("TU2.insert", 7, -1, "betty");
        client.callProcedure("TU2.insert", 8, 2, "betty");
        client.callProcedure("TU2.insert", 9, 3, "betty");
        client.callProcedure("TU3.insert", 1, 1, 1);
        client.callProcedure("TU3.insert", 2, 2, 1);
        client.callProcedure("TU3.insert", 3, 3, 1);
        client.callProcedure("TU3.insert", 4, 1, 2);
        client.callProcedure("TU3.insert", 5, 2, 2);
        client.callProcedure("TU3.insert", 6, 3, 2);
        client.callProcedure("TU3.insert", 7, 1, 3);
        client.callProcedure("TU3.insert", 8, 2, 3);
        client.callProcedure("TU3.insert", 9, 3, 3);
        client.callProcedure("TU4.insert", 1, 1, "jim", 0);
        client.callProcedure("TU4.insert", 2, 2, "jim", 0);
        client.callProcedure("TU4.insert", 3, 3, "jim", 0);
        client.callProcedure("TU4.insert", 4, 4, "thea", 1);
        client.callProcedure("TU4.insert", 5, 5, "betty", 1);

        callWithExpectedResult(client, -3, "@AdHoc", "SELECT MIN(POINTS) FROM TU1");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS) FROM TU1");

        callWithExpectedResult(client, "thea", "@AdHoc", "SELECT MAX(UNAME) FROM TU2");
        callWithExpectedResult(client, true, "@Explain", "SELECT MAX(UNAME) FROM TU2");

        callWithExpectedResult(client, 3, "@AdHoc", "SELECT MAX(POINTS) FROM TU3 WHERE TEL = 3");
        callWithExpectedResult(client, false, "@Explain", "SELECT MAX(POINTS) FROM TU3 WHERE TEL = 3");

        callWithExpectedResult(client, 3, "@AdHoc", "SELECT MIN(TEL) FROM TU3 WHERE TEL = 3");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(TEL) FROM TU3 WHERE TEL = 3");

        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(POINTS) FROM TU4 WHERE UNAME = 'jim' AND SEX = 0");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS) FROM TU4 WHERE UNAME = 'jim' AND SEX = 0");

        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(ABS(POINTS)) FROM TU1");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(ABS(POINTS)) FROM TU1");

        callWithExpectedResult(client, 0, "@AdHoc", "SELECT MIN(POINTS + ID) FROM TU1");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS + ID) FROM TU1");

        // MAX() with filter (not on aggExpr) is not optimized
        callWithExpectedResult(client, 3, "@AdHoc", "SELECT MAX(POINTS) FROM TU2 WHERE UNAME = 'betty'");
        callWithExpectedResult(client, false, "@Explain", "SELECT MAX(POINTS) FROM TU2 WHERE UNAME = 'betty'");

        // MAX() with filter (not on aggExpr) is not optimized
        callWithExpectedResult(client, "thea", "@AdHoc", "SELECT MAX(UNAME) FROM TU2 WHERE POINTS * 2 = 4");
        callWithExpectedResult(client, false, "@Explain", "SELECT MAX(UNAME) FROM TU2 WHERE POINTS * 2 = 4");

        callWithExpectedResult(client, 2, "@AdHoc", "SELECT MIN(ABS(POINTS)) FROM TU2 WHERE ABS(POINTS) = 2");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(ABS(POINTS)) FROM TU2 WHERE ABS(POINTS) = 2");

        // MAX() with filter (not on aggExpr) is not optimized
        callWithExpectedResult(client, 5, "@AdHoc", "SELECT MAX(CHAR_LENGTH(UNAME)) FROM TU2 WHERE ABS(POINTS) = 1");
        callWithExpectedResult(client, false, "@Explain", "SELECT MAX(CHAR_LENGTH(UNAME)) FROM TU2 WHERE ABS(POINTS) = 1");

        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS > 0");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS > 0");

        callWithExpectedResult(client, 2, "@AdHoc", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS >= 2");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS >= 2");

        callWithExpectedResult(client, -3, "@AdHoc", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS < 1");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS < 1");


        callWithExpectedResult(client, -3, "@AdHoc", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS <= 2");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS <= 2");

        callWithExpectedResult(client, 2, "@AdHoc", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS > 0");
        callWithExpectedResult(client, true, "@Explain", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS > 0");

        callWithExpectedResult(client, 2, "@AdHoc", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS >= 2");
        callWithExpectedResult(client, true, "@Explain", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS >= 2");

        // NEW: optimizable after adding reserve scan
        // MAX() with upper bound is not optimized
        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS < 2");
        callWithExpectedResult(client, true, "@Explain", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS < 2");

        // NEW: optimizable after adding reserve scan
        // MAX() with upper bound is not optimized
        callWithExpectedResult(client, 2, "@AdHoc", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS <= 2");
        callWithExpectedResult(client, true, "@Explain", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS <= 2");

        callWithExpectedResult(client, -1, "@AdHoc", "SELECT MIN(POINTS) FROM TU2 WHERE UNAME = 'jim' AND POINTS < 5");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS) FROM TU2 WHERE UNAME = 'jim' AND POINTS < 5");

        // MAX() with filters on cols / exprs other than aggExpr is not optimized
        callWithExpectedResult(client, 3, "@AdHoc", "SELECT MAX(POINTS) FROM TU2 WHERE UNAME = 'thea' AND POINTS > 1");
        callWithExpectedResult(client, false, "@Explain", "SELECT MAX(POINTS) FROM TU2 WHERE UNAME = 'thea' AND POINTS > 1");

        callWithExpectedResult(client, -1, "@AdHoc", "SELECT MIN(POINTS) FROM TU2 WHERE POINTS < 5 AND UNAME = 'jim'");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS) FROM TU2 WHERE POINTS < 5 AND UNAME = 'jim'");

        // MAX() with filters on cols / exprs other than aggExpr is not optimized
        callWithExpectedResult(client, 3, "@AdHoc", "SELECT MAX(POINTS) FROM TU2 WHERE POINTS > 1 AND UNAME = 'thea'");
        callWithExpectedResult(client, false, "@Explain", "SELECT MAX(POINTS) FROM TU2 WHERE POINTS > 1 AND UNAME = 'thea'");

        // receive optimizable query first, then non-optimizable query
        // will generate optimized plan for optimizable query, and generate another plan for non-optimizable query
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS * 2) FROM TU2");
        callWithExpectedResult(client, -3, "@AdHoc", "SELECT MIN(POINTS * 3) FROM TU2");
        callWithExpectedResult(client, -2, "@AdHoc", "SELECT MIN(POINTS * 2) FROM TU2");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS * 2) FROM TU2");
        callWithExpectedResult(client, false, "@Explain", "SELECT MIN(POINTS * 3) FROM TU2");

        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS * 2) FROM TU2 WHERE POINTS * 2 = 100");

        // receive non-optimizable query first, then optimizable query
        // the optimizable query will use not-optimized plan but should still execute correctly
        callWithExpectedResult(client, false, "@Explain", "SELECT MIN(POINTS + 200) FROM TU3");
        callWithExpectedResult(client, 201, "@AdHoc", "SELECT MIN(POINTS + 200) FROM TU3");
        callWithExpectedResult(client, false, "@Explain", "SELECT MIN(POINTS + 100) FROM TU3");
        callWithExpectedResult(client, 101, "@AdHoc", "SELECT MIN(POINTS + 100) FROM TU3");

        callWithExpectedResult(client, 9, "@AdHoc", "SELECT MIN(POINTS + 10) FROM TU2 WHERE UNAME = 'jim' AND POINTS + 10 > 5");
        callWithExpectedResult(client, true, "@Explain", "SELECT MIN(POINTS + 10) FROM TU2 WHERE UNAME = 'jim' AND POINTS + 10 > 5");
        callWithExpectedResult(client, false, "@Explain", "SELECT MIN(POINTS + 11) FROM TU2 WHERE UNAME = 'jim' AND POINTS + 10 > 5");

        callWithExpectedResult(client, 13, "@AdHoc", "SELECT MAX(POINTS + 10) FROM TU2 WHERE UNAME = 'betty' AND POINTS + 10 > 7");
        callWithExpectedResult(client, false, "@Explain", "SELECT MAX(POINTS + 10) FROM TU2 WHERE UNAME = 'betty' AND POINTS + 10 > 7");

        // do not optimize using hash index
        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(ID) FROM T1;");
        callWithExpectedResult(client, false, "@Explain", "SELECT MIN(ID) FROM T1");
        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(POINTS) FROM T1 WHERE ID = 1;");
        callWithExpectedResult(client, false, "@Explain", "SELECT MIN(POINTS) FROM T1 WHERE ID = 1");

        callWithExpectedResult(client, -1, "@AdHoc", "SELECT POINTS FROM TU2 WHERE UNAME = 'jim' ORDER BY POINTS ASC LIMIT 1;");
        callWithExpectedResult(client, true, "@Explain", "SELECT POINTS FROM TU2 WHERE UNAME = 'jim' ORDER BY POINTS ASC LIMIT 1;");
        callWithExpectedResult(client, 3, "@AdHoc", "SELECT POINTS FROM TU2 WHERE UNAME = 'jim' ORDER BY POINTS DESC LIMIT 1;");
        callWithExpectedResult(client, true, "@Explain", "SELECT POINTS FROM TU2 WHERE UNAME = 'jim' ORDER BY POINTS DESC LIMIT 1;");

    }

    public void testENG6176_MIN_NULL() throws Exception {
        Client client = getClient();

        client.callProcedure("TMIN.insert", 1, 1,    1);
        client.callProcedure("TMIN.insert", 1, null, -5);
        client.callProcedure("TMIN.insert", 1, 1,    null);
        client.callProcedure("TMIN.insert", 1, -2,   0);

        // Test simple index
        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(A) FROM TMIN;");
        callWithExpectedResult(client, -2, "@AdHoc", "SELECT MIN(B) FROM TMIN;");
        callWithExpectedResult(client, -5, "@AdHoc", "SELECT MIN(C) FROM TMIN;");
        callWithExpectedResult(client, -2, "@AdHoc", "SELECT MIN(B) FROM TMIN WHERE A > 0;");
        callWithExpectedResult(client, -2, "@AdHoc", "SELECT MIN(B) FROM TMIN WHERE A = 1;");
        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(C) FROM TMIN WHERE A = 1 AND B = 1;");
        callWithExpectedResult(client, -2, "@AdHoc", "SELECT MIN(B) FROM TMIN WHERE B <= 3;");
        callWithExpectedResult(client, -2, "@AdHoc", "SELECT MIN(B) FROM TMIN WHERE A = 1 AND B <= 3;");

        // Test expression index
        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(ABS(B)) FROM TMIN;");
        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(ABS(B)) FROM TMIN WHERE A = 1;");
        callWithExpectedResult(client, -2, "@AdHoc", "SELECT MIN(B) FROM TMIN WHERE ABS(A) = 1;");
        callWithExpectedResult(client, 1, "@AdHoc", "SELECT MIN(ABS(B)) FROM TMIN WHERE ABS(A) = 1;");
        callWithExpectedResult(client, -2, "@AdHoc", "SELECT MIN(B) FROM TMIN WHERE ABS(A) = 1;");
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
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestIndexLimitSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(BatchedMultiPartitionTest.class.getResource("sqlindex-ddl.sql"));

        // pure column index queries
        project.addStmtProcedure("COL_TU1_MIN_POINTS", "SELECT MIN(POINTS) FROM TU1;");
        project.addStmtProcedure("COL_TU2_MAX_UNAME", "SELECT MAX(UNAME) FROM TU2");
        project.addStmtProcedure("COL_TU3_MAX_POINTS", "SELECT MAX(POINTS) FROM TU3 WHERE TEL = ?");
        project.addStmtProcedure("COL_TU3_MIN_TEL", "SELECT MIN(TEL) FROM TU3 WHERE TEL = ?");
        project.addStmtProcedure("COL_TU4_MIN_POINTS_WHERE", "SELECT MIN(POINTS) FROM TU4 WHERE UNAME = ? AND SEX = ?");

        // expression index queries
        project.addStmtProcedure("EXPR_TU1_MIN_FUNC", "SELECT MIN(ABS(POINTS)) FROM TU1");
        project.addStmtProcedure("EXPR_TU1_MIN_ARTH", "SELECT MIN(POINTS + ID) FROM TU1");
        project.addStmtProcedure("EXPR_TU2_MAX_POINTS", "SELECT MAX(POINTS) FROM TU2 WHERE UNAME = ?");
        project.addStmtProcedure("EXPR_TU2_MAX_UNAME", "SELECT MAX(UNAME) FROM TU2 WHERE POINTS * 2 = ?");
        project.addStmtProcedure("EXPR_TU2_MIN_ABS_POINTS", "SELECT MIN(ABS(POINTS)) FROM TU2 WHERE ABS(POINTS) = ?");
        project.addStmtProcedure("EXPR_TU2_MAX_LENGTH_UNAME", "SELECT MAX(CHAR_LENGTH(UNAME)) FROM TU2 WHERE ABS(POINTS) = ?");

        // edge cases:
        project.addStmtProcedure("EDGE_TU1_MIN_POINTS1", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS > ?");
        project.addStmtProcedure("EDGE_TU1_MIN_POINTS2", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS >= ?");
        project.addStmtProcedure("EDGE_TU1_MIN_POINTS3", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS < ?");
        project.addStmtProcedure("EDGE_TU1_MIN_POINTS4", "SELECT MIN(POINTS) FROM TU1 WHERE POINTS <= ?");
        project.addStmtProcedure("EDGE_TU1_MAX_POINTS1", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS > ?");
        project.addStmtProcedure("EDGE_TU1_MAX_POINTS2", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS >= ?");
        project.addStmtProcedure("EDGE_TU1_MAX_POINTS3", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS < ?");
        project.addStmtProcedure("EDGE_TU1_MAX_POINTS4", "SELECT MAX(POINTS) FROM TU1 WHERE POINTS <= ?");
        project.addStmtProcedure("EDGE_TU2_MIN_POINTS1", "SELECT MIN(POINTS) FROM TU2 WHERE UNAME = ? AND POINTS < ?");
        project.addStmtProcedure("EDGE_TU2_MAX_POINTS1", "SELECT MAX(POINTS) FROM TU2 WHERE UNAME = ? AND POINTS > ?");
        project.addStmtProcedure("EDGE_TU2_MIN_POINTS2", "SELECT MIN(POINTS) FROM TU2 WHERE POINTS < ? AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MAX_POINTS2", "SELECT MAX(POINTS) FROM TU2 WHERE POINTS > ? AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MIN_POINTS3", "SELECT MIN(POINTS) FROM TU2 WHERE POINTS > ? AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MAX_POINTS3", "SELECT MAX(POINTS) FROM TU2 WHERE POINTS < ? AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MIN_POINTS4", "SELECT MIN(POINTS) FROM TU2 WHERE POINTS >= ? AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MAX_POINTS4", "SELECT MAX(POINTS) FROM TU2 WHERE POINTS <= ? AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MIN_POINTS5", "SELECT MIN(POINTS) FROM TU2 WHERE POINTS = ? AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MAX_POINTS5", "SELECT MAX(POINTS) FROM TU2 WHERE POINTS = ? AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MIN_POINTS6", "SELECT MIN(POINTS) FROM TU2 WHERE POINTS IS NULL AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MAX_POINTS6", "SELECT MAX(POINTS) FROM TU2 WHERE POINTS IS NULL AND UNAME = ?");
        project.addStmtProcedure("EDGE_TU2_MIN_POINTS_EXPR", "SELECT MIN(POINTS + 10) FROM TU2 WHERE UNAME = ? AND POINTS + 10 > ?");
        project.addStmtProcedure("EDGE_TU2_MAX_POINTS_EXPR", "SELECT MAX(POINTS + 10) FROM TU2 WHERE UNAME = ? AND POINTS + 10 > ?");

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("sqlIndexLimit-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlIndexLimit-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("sqlIndexLimit-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestIndexLimitSuite.class);
    }
}
