/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import org.voltdb.client.ClientResponse;
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

    public void testOneColumnIndexLessThan() throws Exception {
        Client client = getClient();

//        client.callProcedure("P1.insert", 1, "1", 1, 1);
//        client.callProcedure("P1.insert", 2, "1", 1, 2);
//        client.callProcedure("P1.insert", 3, "1", 1, 3);
//
//        client.callProcedure("P2.insert", 1, "1", 1, 1);
//        client.callProcedure("P2.insert", 2, "1", 1, 2);
//        client.callProcedure("P2.insert", 3, "1", 1, 3);

        client.callProcedure("P1.insert",0, "VsjXIIYQnwoFIpNob", -16556, 5.47315658172998098507e-01);
        client.callProcedure("P1.insert",1, "yGSKfTbpaJATfvBvh", -27838, 4.08313508830880467215e-01);
        client.callProcedure("P2.insert",4, "vIwoHCHtWbYaJaNeJ", 28494, 23683);
        client.callProcedure("P2.insert",5, "jPiUvMLcKGrrsMXrT", 23951, 25679);



        VoltTable table;

        ClientResponse cr = client.callProcedure("@AdHoc","SELECT P1.ID, P2.P2_ID from P1, P2 where P1.ID >= P2.P2_ID order by P1.ID, P2.P2_ID limit 10");
        //assertTrue(table.advanceRow());
        System.err.println(cr.getStatusString());
        //System.err.println("RESULT0:\n" + table);
        assertTrue(true);

//        table = client.callProcedure("@AdHoc","SELECT * from P1, P2 where P1_NUM1 = P2_NUM1 AND P1_NUM2 >= P2_NUM2 order by P1.ID, P2.P2_ID limit 10").getResults()[0];
//        assertTrue(table.advanceRow());
//        System.err.println("RESULT1:\n" + table);
//        assertTrue(true);
//
//        table = client.callProcedure("@AdHoc","SELECT * from P1, P2 where P1_NUM1 = P2_NUM1 AND P2_NUM2 <= P1_NUM2 order by P1.ID, P2.P2_ID limit 10").getResults()[0];
//        assertTrue(table.advanceRow());
//        System.err.println("RESULT2:\n" + table);
//        assertTrue(true);


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
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestIndexColumnLessThanSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(BatchedMultiPartitionTest.class.getResource("sqlindex-ddl.sql"));
        project.addProcedures(PROCEDURES);
        project.addPartitionInfo("TU1", "ID");
        project.addPartitionInfo("TU2", "UNAME");
        project.addPartitionInfo("TU3", "TEL");
        project.addPartitionInfo("TU4", "UNAME");
        project.addPartitionInfo("TM1", "ID");
        project.addPartitionInfo("TM2", "UNAME");

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
/*
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlCountingIndex-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: Local Cluster (of processes) with failed node
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlCountingIndex-cluster-rejoin.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ONE_FAILURE, false);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        */
        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestIndexCountSuite.class);
    }
}
