/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.BatchedMultiPartitionTest;
public class TestIndexColumnLessThanSuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestIndexColumnLessThanSuite(String name) {
        super(name);
    }

    public void testFailedSqlcoverageCase() throws Exception {
        Client client = getClient();

        client.callProcedure("@AdHoc","INSERT INTO P1 VALUES(0, 'JOUmtGnsKFCGOaWvg', -27392, 7.30754864900605705103e-01);");
        client.callProcedure("@AdHoc","INSERT INTO P1 VALUES(1, 'aXwwPrkDwabomDdAZ', 30036, 1.09425040281800223241e-01);");
        client.callProcedure("@AdHoc","INSERT INTO P2 VALUES(4, 'xJAmmYpKGJJyflOMw', -7060, 14977);");
        client.callProcedure("@AdHoc","INSERT INTO P2 VALUES(5, 'xybOXumtYDvBNhbUs', 13529, -2025);");

        VoltTable table;

        table = client.callProcedure("@AdHoc","SELECT P1.ID, P2.P2_ID from P1, P2 where P1.ID >= P2.P2_ID order by P1.ID, P2.P2_ID limit 10;").getResults()[0];
        assertTrue(table.advanceRow() == false);

        table = client.callProcedure("@AdHoc","SELECT P1.ID, P2.P2_ID from P1, P2 where P2.P2_ID <= P1.ID order by P1.ID, P2.P2_ID limit 10;").getResults()[0];
        assertTrue(table.advanceRow() == false);

        table = client.callProcedure("@AdHoc","SELECT P1.ID, P2.P2_ID from P1, P2 where P1.ID <= P2.P2_ID order by P1.ID, P2.P2_ID limit 10;").getResults()[0];
        assertTrue(table.getRowCount() == 4);
        assertTrue(table.advanceRow());
        //System.err.println("RESULT01:\n" + table);

        table = client.callProcedure("@AdHoc","SELECT P1.ID, P2.P2_ID from P1, P2 where P2.P2_ID >= P1.ID order by P1.ID, P2.P2_ID limit 10;").getResults()[0];
        assertTrue(table.getRowCount() == 4);
        assertTrue(table.advanceRow());
        //System.err.println("RESULT02:\n" + table);
    }

    public void testTwoTableJoinBug() throws Exception {
        Client client = getClient();

        client.callProcedure("P2.insert", 1, "1", 1, 1);
        client.callProcedure("P2.insert", 2, "1", 1, 2);
        client.callProcedure("P2.insert", 3, "1", 1, 3);

        client.callProcedure("P3.insert", 1, "1", 1, 1);
        client.callProcedure("P3.insert", 2, "1", 1, 2);
        client.callProcedure("P3.insert", 3, "1", 1, 3);

        VoltTable table;

        table = client.callProcedure("@AdHoc","SELECT * from P2, P3 where P2_NUM1 = P3_NUM1 AND P2_NUM2 > P3_NUM2 order by P2.P2_ID, P3.P3_ID limit 10").getResults()[0];
        assertTrue(table.getRowCount() == 3);
        assertTrue(table.advanceRow());
        System.err.println("RESULT11:\n" + table);

        table = client.callProcedure("@AdHoc","SELECT * from P2, P3 where P2_NUM1 = P3_NUM1 AND P3_NUM2 < P2_NUM2 order by P2.P2_ID, P3.P3_ID limit 10").getResults()[0];
        assertTrue(table.getRowCount() == 3);
        assertTrue(table.advanceRow());
        System.err.println("RESULT12:\n" + table);
    }

    /**
     * Build a list of the tests that will be run when TestIndexColumnLess gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        VoltServerConfig config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestIndexColumnLessThanSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(BatchedMultiPartitionTest.class.getResource("sqlindex-ddl.sql"));
        project.addProcedures(PROCEDURES);

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("sql-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlCountingIndex-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("sql-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);


        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestIndexCountSuite.class);
    }
}
