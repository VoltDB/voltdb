/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.BatchedMultiPartitionTest;
public class TestIndexLimitSuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestIndexLimitSuite(String name) {
        super(name);
    }

    void callWithExpectedResult(Client client,int ret, String procName, Object... params)
            throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = client.callProcedure(procName, params);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(ret, result.getLong(0));
    }

    void callWithExpectedResult(Client client,String ret, String procName, Object... params)
            throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = client.callProcedure(procName, params);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(ret, result.getString(0));
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
        callWithExpectedResult(client, "thea", "EXPR_TU2_MIN_UNAME", 4);
        callWithExpectedResult(client, 2, "EXPR_TU2_MIN_ABS_POINTS", 2);
        callWithExpectedResult(client, 3, "EXPR_TU2_MAX_POINTS", "betty");
        callWithExpectedResult(client, 5, "EXPR_TU2_MAX_LENGTH_UNAME", 1);

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
        project.addProcedures(PROCEDURES);
        project.addPartitionInfo("TU1", "ID");
        project.addPartitionInfo("TU2", "UNAME");
        project.addPartitionInfo("TU3", "TEL");
        project.addPartitionInfo("TU4", "UNAME");

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
        project.addStmtProcedure("EXPR_TU2_MIN_UNAME", "SELECT MAX(UNAME) FROM TU2 WHERE POINTS * 2 = ?");
        project.addStmtProcedure("EXPR_TU2_MIN_ABS_POINTS", "SELECT MIN(ABS(POINTS)) FROM TU2 WHERE ABS(POINTS) = ?");
        project.addStmtProcedure("EXPR_TU2_MAX_LENGTH_UNAME", "SELECT MAX(CHAR_LENGTH(UNAME)) FROM TU2 WHERE ABS(POINTS) = ?");
        project.addStmtProcedure("EXPR_TU2_LENGTH_UNAME", "SELECT CHAR_LENGTH(UNAME) FROM TU2 WHERE ABS(POINTS) = ?");
        project.addStmtProcedure("EXPR_TU2_LENGTH_UNAME_LIMIT", "SELECT CHAR_LENGTH(UNAME) FROM TU2 WHERE ABS(POINTS) = ? ORDER BY CHAR_LENGTH(UNAME) DESC LIMIT 1");

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
