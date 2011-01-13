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

package org.voltdb.regressionsuites;

import java.io.IOException;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.*;
import org.voltdb_testprocs.regressionsuites.failureprocs.InsertLotsOfData;

public class TestSQLFeaturesSuite extends RegressionSuite {

    /*
     *  See also TestPlansGroupBySuite for tests of distinct, group by, basic aggregates
     */

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        FeaturesSelectAll.class, UpdateTests.class,
        SelfJoinTest.class, SelectOrderLineByDistInfo.class,
        BatchedMultiPartitionTest.class, WorkWithBigString.class, PassByteArrayArg.class,
        PassAllArgTypes.class, InsertLotsOfData.class, SelectWithJoinOrder.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestSQLFeaturesSuite(String name) {
        super(name);
    }

    public void testUpdates() throws Exception {
        Client client = getClient();

        client.callProcedure("InsertOrderLine", (byte)1, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1.5, "poo");
        client.callProcedure("UpdateTests", (byte)1);
        VoltTable[] results = client.callProcedure("FeaturesSelectAll").getResults();

        assertEquals(5, results.length);

        // get the order line table
        VoltTable table = results[2];
        assertEquals(table.getColumnName(0), "OL_O_ID");
        assertTrue(table.getRowCount() == 1);
        VoltTableRow row = table.fetchRow(0);
        assertEquals(row.getLong("OL_O_ID"), 1);
        assertEquals(row.getLong("OL_D_ID"), 6);
        assertEquals(row.getLong("OL_W_ID"), 1);
        assertEquals(row.getLong("OL_QUANTITY"), 1);
        assertEquals(row.getLong("OL_SUPPLY_W_ID"), 5);

        assertTrue(true);
    }

    public void testSelfJoins() throws Exception {
        Client client = getClient();

        client.callProcedure("InsertNewOrder", (byte)1, 3L, 1L);
        VoltTable[] results = client.callProcedure("SelfJoinTest", (byte)1).getResults();

        assertEquals(results.length, 1);

        // get the new order table
        VoltTable table = results[0];
        assertTrue(table.getRowCount() == 1);
        VoltTableRow row = table.fetchRow(0);
        assertEquals(row.getLong("NO_D_ID"), 3);
    }

    /** Verify that non-latin-1 characters can be stored and retrieved */
    public void testUTF8Storage() throws IOException {
        Client client = getClient();
        final String testString = "並丧";
        try {
            client.callProcedure("InsertOrderLine", 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1.5, testString);
            VoltTable[] results = client.callProcedure("FeaturesSelectAll").getResults();

            assertEquals(5, results.length);

            // get the order line table
            VoltTable table = results[2];
            assertEquals(table.getColumnName(0), "OL_O_ID");
            assertTrue(table.getRowCount() == 1);
            VoltTableRow row = table.fetchRow(0);
            String resultString = row.getString("OL_DIST_INFO");
            assertEquals(testString, resultString);
        }
        catch (ProcCallException e) {
            e.printStackTrace();
            fail();
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    /** Verify that non-latin-1 characters can be used in expressions */
    public void testUTF8Predicate() throws IOException {
        Client client = getClient();
        final String testString = "袪被";
        try {
            // Intentionally using a one byte string to make sure length preceded strings are handled correctly in the EE.
            client.callProcedure("InsertOrderLine", 2L, 1L, 1L, 2L, 2L, 2L, 2L, 2L, 1.5, "a");
            client.callProcedure("InsertOrderLine", 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1.5, testString);
            client.callProcedure("InsertOrderLine", 3L, 1L, 1L, 3L, 3L, 3L, 3L, 3L, 1.5, "def");
            VoltTable[] results = client.callProcedure("SelectOrderLineByDistInfo", testString).getResults();
            assertEquals(1, results.length);
            VoltTable table = results[0];
            assertTrue(table.getRowCount() == 1);
            VoltTableRow row = table.fetchRow(0);
            String resultString = row.getString("OL_DIST_INFO");
            assertEquals(testString, resultString);
        }
        catch (ProcCallException e) {
            e.printStackTrace();
            fail();
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testBatchedMultipartitionTxns() throws IOException, ProcCallException {
        Client client = getClient();

        VoltTable[] results = client.callProcedure("BatchedMultiPartitionTest").getResults();
        assertEquals(5, results.length);
        assertEquals(1, results[0].asScalarLong());
        assertEquals(1, results[1].asScalarLong());
        assertEquals(1, results[2].asScalarLong());
        assertEquals(2, results[3].getRowCount());
        assertEquals(1, results[4].getRowCount());
    }

    public void testLongStringUsage() throws IOException {
        final int STRLEN = 5000;

        Client client = getClient();

        String longStringPart = "volt!";
        StringBuilder sb = new StringBuilder();
        while(sb.length() < STRLEN)
            sb.append(longStringPart);
        String longString = sb.toString();
        assertEquals(STRLEN, longString.length());

        VoltTable[] results = null;
        try {
            results = client.callProcedure("WorkWithBigString", 1, longString).getResults();
        } catch (ProcCallException e) {
            e.printStackTrace();
            fail();
        }
        assertEquals(1, results.length);
        VoltTableRow row = results[0].fetchRow(0);

        assertEquals(1, row.getLong(0));
        assertEquals(0, row.getString(2).compareTo(longString));
    }

    public void testStringAsByteArrayParam() throws Exception {
        final int STRLEN = 5000;

        Client client = getClient();

        String longStringPart = "volt!";
        StringBuilder sb = new StringBuilder();
        while(sb.length() < STRLEN)
            sb.append(longStringPart);
        String longString = sb.toString();
        assertEquals(STRLEN, longString.length());


        VoltTable[] results = client.callProcedure("PassByteArrayArg", (byte)1, 2, longString.getBytes("UTF-8")).getResults();
        assertEquals(1, results.length);
        VoltTableRow row = results[0].fetchRow(0);

        assertEquals(1, row.getLong(0));
        assertEquals(0, row.getString(2).compareTo(longString));
    }

    public void testPassAllArgTypes() throws IOException {
        byte b = 100;
        byte bArray[] = new byte[] { 100, 101, 102 };
        short s = 32000;
        short sArray[] = new short[] { 32000, 32001, 32002 };
        int i = 2147483640;
        int iArray[] = new int[] { 2147483640, 2147483641, 2147483642 };
        long l = Long.MAX_VALUE - 10;
        long lArray[] = new long[] { Long.MAX_VALUE - 10, Long.MAX_VALUE - 9, Long.MAX_VALUE - 8 };
        String str = "foo";
        byte bString[] = "bar".getBytes("UTF-8");

        Client client = getClient();
        try {
            client.callProcedure("PassAllArgTypes", b, bArray, s, sArray, i, iArray, l, lArray, str, bString);
        } catch (Exception e) {
            fail();
        }
    }

    public void testJoinOrder() throws Exception {
        if (isHSQL() || isValgrind()) return;

        Client client = getClient();

        VoltTable[] results = null;
        int nextId = 0;
        for (int mb = 0; mb < 25; mb += 5) {
            results = client.callProcedure("InsertLotsOfData", 0, nextId).getResults();
            assertEquals(1, results.length);
            assertTrue(nextId < results[0].asScalarLong());
            nextId = (int) results[0].asScalarLong();
            System.err.println("Inserted " + (mb + 5) + "mb");
        }

        for (int ii = 0; ii < 1000; ii++) {
            client.callProcedure("InsertT1", ii);
        }
        client.callProcedure("InsertT2", 0);

        //Right join order
        client.callProcedure("SelectWithJoinOrder", 0);

        //Wrong join order
        boolean exception = false;
        try {
            client.callProcedure("SelectWithJoinOrder", 1);
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception);

        //Right join order
        client.callProcedure("SelectRightOrder");

        exception = false;
        try {
            client.callProcedure("SelectWrongOrder");
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception);


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
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSQLFeaturesSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(BatchedMultiPartitionTest.class.getResource("sqlfeatures-ddl.sql"));
        project.addPartitionInfo("NEW_ORDER", "NO_W_ID");
        project.addPartitionInfo("ORDER_LINE", "OL_W_ID");
        project.addPartitionInfo("FIVEK_STRING", "P");
        project.addPartitionInfo("FIVEK_STRING_WITH_INDEX", "ID");
        project.addPartitionInfo("WIDE", "P");
        project.addPartitionInfo("MANY_COLUMNS", "P");
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("InsertOrderLine",
                "INSERT INTO ORDER_LINE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
        project.addStmtProcedure("InsertNewOrder",
                "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");
        project.addStmtProcedure("InsertT1",
                "INSERT INTO T1 VALUES (?);");
        project.addStmtProcedure("InsertT2",
                "INSERT INTO T2 VALUES (?);");
        project.addStmtProcedure("SelectRightOrder",
                "SELECT * FROM WIDE, T1, T2 WHERE T2.ID = T1.ID", null, "T1,T2,WIDE");
        project.addStmtProcedure("SelectWrongOrder",
                "SELECT * FROM WIDE, T1, T2 WHERE T2.ID = T1.ID", null, "WIDE,T1,T2");

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalSingleProcessServer("sqlfeatures-onesite.jar", 1, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with two sites/partitions
        config = new LocalSingleProcessServer("sqlfeatures-twosites.jar", 2, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile (note the reuse of the TPCC project)
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalSingleProcessServer("sqlfeatures-hsql.jar", 1, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);


        /////////////////////////////////////////////////////////////
        // CONFIG #4: Local Cluster (of processes)
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlfeatures-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #5: Local Cluster (of processes) with recovering node
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlfeatures-cluster-rejoin.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ONE_RECOVERING, false);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestSQLFeaturesSuite.class);
    }
}
