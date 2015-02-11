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
public class TestTableCountSuite extends RegressionSuite {
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestTableCountSuite(String name) {
        super(name);
    }

    public void testTableCountsWithOperation() throws Exception {
        Client client = getClient();

        client.callProcedure("TU1.insert", 1, 1);
        client.callProcedure("TU1.insert", 2, 2);
        client.callProcedure("TU1.insert", 3, 3);
        client.callProcedure("TU1.insert", 6, 6);
        client.callProcedure("TU1.insert", 8, 8);

        VoltTable table = client.callProcedure("@AdHoc","SELECT (COUNT(*)+1) FROM TU1").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(6, table.getLong(0));

        // subquery temp table count
        table = client.callProcedure("@AdHoc","select count(*) from (SELECT * FROM TU1) Temp").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(5, table.getLong(0));
    }

    public void testTableCounts() throws Exception {
        Client client = getClient();

        client.callProcedure("TU1.insert", 1, 1);
        client.callProcedure("TU1.insert", 2, 2);
        client.callProcedure("TU1.insert", 3, 3);
        client.callProcedure("TU1.insert", 6, 6);
        client.callProcedure("TU1.insert", 8, 8);

        VoltTable table;

        table = client.callProcedure("@AdHoc","SELECT COUNT(*) FROM TU1").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(5, table.getLong(0));

        // subquery temp table count
        table = client.callProcedure("@AdHoc","select count(*) from (SELECT * FROM TU1) Temp").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(5, table.getLong(0));

        table = client.callProcedure("@AdHoc","select count(*) " +
                "from (SELECT * FROM TU1 where id > 3) Temp").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(2, table.getLong(0));


        // Unique Map, two column index
        client.callProcedure("TU3.insert", 1, 1, 123);
        client.callProcedure("TU3.insert", 2, 2, 123);
        client.callProcedure("TU3.insert", 3, 3, 123);
        client.callProcedure("TU3.insert", 4, 6, 123);
        client.callProcedure("TU3.insert", 5, 8, 123);
        client.callProcedure("TU3.insert", 6, 1, 456);
        client.callProcedure("TU3.insert", 7, 2, 456);
        client.callProcedure("TU3.insert", 8, 3, 456);
        client.callProcedure("TU3.insert", 9, 6, 456);
        client.callProcedure("TU3.insert", 10, 8, 456);

        table = client.callProcedure("@AdHoc","SELECT COUNT(*) FROM TU3").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(10, table.getLong(0));

        // subquery temp table count
        table = client.callProcedure("@AdHoc","select count(*) from (SELECT * FROM TU3) Temp").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(10, table.getLong(0));

     // Multi-map, two column index
        client.callProcedure("TM2.insert", 1, 1, "xin");
        client.callProcedure("TM2.insert", 2, 2, "xin");
        client.callProcedure("TM2.insert", 3, 3, "xin");
        client.callProcedure("TM2.insert", 4, 3, "xin");
        client.callProcedure("TM2.insert", 5, 3, "xin");
        client.callProcedure("TM2.insert", 6, 5, "xin");
        client.callProcedure("TM2.insert", 7, 6, "xin");
        client.callProcedure("TM2.insert", 8, 6, "xin");
        client.callProcedure("TM2.insert", 9, 8, "xin");
        client.callProcedure("TM2.insert", 10, 8, "xin");

        client.callProcedure("TM2.insert", 11, 1, "jia");
        client.callProcedure("TM2.insert", 12, 2, "jia");
        client.callProcedure("TM2.insert", 13, 3, "jia");
        client.callProcedure("TM2.insert", 14, 3, "jia");
        client.callProcedure("TM2.insert", 15, 3, "jia");
        client.callProcedure("TM2.insert", 16, 5, "jia");
        client.callProcedure("TM2.insert", 17, 6, "jia");
        client.callProcedure("TM2.insert", 18, 6, "jia");
        client.callProcedure("TM2.insert", 19, 8, "jia");
        client.callProcedure("TM2.insert", 20, 8, "jia");

        table = client.callProcedure("@AdHoc","SELECT COUNT(*) FROM TM2").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(20, table.getLong(0));

        // subquery temp table count
        table = client.callProcedure("@AdHoc","select count(*) from (SELECT * FROM TM2) Temp").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(20, table.getLong(0));
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
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestTableCountSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(BatchedMultiPartitionTest.class.getResource("sqlindex-ddl.sql"));

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("sqlCountingIndex-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

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
