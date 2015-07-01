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

import java.io.IOException;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;
import org.voltdb_testprocs.regressionsuites.failureprocs.InsertLotsOfData;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.BatchedMultiPartitionTest;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.FeaturesSelectAll;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.PassAllArgTypes;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.PassByteArrayArg;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.SelectOrderLineByDistInfo;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.SelectWithJoinOrder;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.SelfJoinTest;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.UpdateTests;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.WorkWithBigString;

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

        client.callProcedure("ORDER_LINE.insert", (byte)1, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1.5, "poo");
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

        client.callProcedure("NEW_ORDER.insert", (byte)1, 3L, 1L);
        VoltTable[] results = client.callProcedure("SelfJoinTest", (byte)1).getResults();

        assertEquals(results.length, 1);

        // get the new order table
        VoltTable table = results[0];
        assertTrue(table.getRowCount() == 1);
        VoltTableRow row = table.fetchRow(0);
        assertEquals(row.getLong("NO_D_ID"), 3);
    }

    /** Verify that non-latin-1 characters can be stored and retrieved */
    public void testUTF8() throws IOException {
        Client client = getClient();
        final String testString = "並丧";
        try {
            client.callProcedure("ORDER_LINE.insert", 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1.5, testString);
            VoltTable[] results = client.callProcedure("FeaturesSelectAll").getResults();

            assertEquals(5, results.length);

            // get the order line table
            VoltTable table = results[2];
            assertEquals(table.getColumnName(0), "OL_O_ID");
            assertTrue(table.getRowCount() == 1);
            VoltTableRow row = table.fetchRow(0);
            String resultString = row.getString("OL_DIST_INFO");
            assertEquals(testString, resultString);

            // reset
            client.callProcedure("@AdHoc", "delete from ORDER_LINE;");

            // Intentionally using a one byte string to make sure length preceded strings are handled correctly in the EE.
            client.callProcedure("ORDER_LINE.insert", 2L, 1L, 1L, 2L, 2L, 2L, 2L, 2L, 1.5, "a");
            client.callProcedure("ORDER_LINE.insert", 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1.5, testString);
            client.callProcedure("ORDER_LINE.insert", 3L, 1L, 1L, 3L, 3L, 3L, 3L, 3L, 1.5, "def");
            results = client.callProcedure("SelectOrderLineByDistInfo", testString).getResults();
            assertEquals(1, results.length);
            table = results[0];
            assertTrue(table.getRowCount() == 1);
            row = table.fetchRow(0);
            resultString = row.getString("OL_DIST_INFO");
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

        TimestampType tst = new TimestampType(PassAllArgTypes.MILLISECONDS_SINCE_EPOCH_TEST_VALUE*1000);
        java.util.Date utild = new java.util.Date(PassAllArgTypes.MILLISECONDS_SINCE_EPOCH_TEST_VALUE);
        java.sql.Date sqld = new java.sql.Date(PassAllArgTypes.MILLISECONDS_SINCE_EPOCH_TEST_VALUE);
        java.sql.Timestamp ts = new java.sql.Timestamp(PassAllArgTypes.MILLISECONDS_SINCE_EPOCH_TEST_VALUE);

        Client client = getClient();
        try {
            ClientResponse cr = client.callProcedure("PassAllArgTypes", b, bArray, s, sArray, i, iArray, l, lArray, str, bString,
                    tst, utild, sqld, ts);
            assert(cr.getStatus() == ClientResponse.SUCCESS);
            VoltTable[] result = cr.getResults();
            assert(result.length == 4);
            VoltTable vt = result[0];
            assert(vt.getRowCount() == 1);
            VoltTableRow row = vt.fetchRow(0);
            assertEquals(row.getLong("b"), b);
            byte[] gotArray = row.getVarbinary("bArray");
            assertEquals(gotArray.length, bArray.length);
            for (int j = 0; j < gotArray.length; j++) {
                assertEquals(gotArray[j], bArray[j]);
            }
            assertEquals(gotArray.length, bArray.length);
            assertEquals(row.getLong("s"), s);
            assertEquals(row.getLong("i"), i);
            assertEquals(row.getLong("l"), l);
            assertEquals(row.getString("str"), str);
            byte[] gotString = row.getVarbinary("bString");
            assertEquals(gotString.length, bString.length);
            for (int j = 0; j < gotString.length; j++) {
                assertEquals(gotString[j], bString[j]);
            }

            String tsColName;
            int tsColIndex;
            tsColName = "tst";
            assertEquals(row.getTimestampAsLong(tsColName), tst.getTime());
            assertEquals(row.getTimestampAsTimestamp(tsColName), tst);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName), ts);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName).getTime(), sqld.getTime());
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName).getTime(), utild.getTime());
            tsColIndex = vt.getColumnIndex(tsColName);
            assertEquals(row.getTimestampAsLong(tsColIndex), tst.getTime());
            assertEquals(row.getTimestampAsTimestamp(tsColIndex), tst);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex), ts);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex).getTime(), sqld.getTime());
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex).getTime(), utild.getTime());

            tsColName = "sqld";
            assertEquals(row.getTimestampAsLong(tsColName), tst.getTime());
            assertEquals(row.getTimestampAsTimestamp(tsColName), tst);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName), ts);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName).getTime(), sqld.getTime());
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName).getTime(), utild.getTime());
            tsColIndex = vt.getColumnIndex(tsColName);
            assertEquals(row.getTimestampAsLong(tsColIndex), tst.getTime());
            assertEquals(row.getTimestampAsTimestamp(tsColIndex), tst);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex), ts);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex).getTime(), sqld.getTime());
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex).getTime(), utild.getTime());

            tsColName = "utild";
            assertEquals(row.getTimestampAsLong(tsColName), tst.getTime());
            assertEquals(row.getTimestampAsTimestamp(tsColName), tst);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName), ts);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName).getTime(), sqld.getTime());
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName).getTime(), utild.getTime());
            tsColIndex = vt.getColumnIndex(tsColName);
            assertEquals(row.getTimestampAsLong(tsColIndex), tst.getTime());
            assertEquals(row.getTimestampAsTimestamp(tsColIndex), tst);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex), ts);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex).getTime(), sqld.getTime());
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex).getTime(), utild.getTime());

            tsColName = "ts";
            assertEquals(row.getTimestampAsLong(tsColName), tst.getTime());
            assertEquals(row.getTimestampAsTimestamp(tsColName), tst);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName), ts);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName).getTime(), sqld.getTime());
            assertEquals(row.getTimestampAsSqlTimestamp(tsColName).getTime(), utild.getTime());
            tsColIndex = vt.getColumnIndex(tsColName);
            assertEquals(row.getTimestampAsLong(tsColIndex), tst.getTime());
            assertEquals(row.getTimestampAsTimestamp(tsColIndex), tst);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex), ts);
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex).getTime(), sqld.getTime());
            assertEquals(row.getTimestampAsSqlTimestamp(tsColIndex).getTime(), utild.getTime());

            vt = result[1];
            assert(vt.getRowCount() == sArray.length);
            for (int j = 0; j < sArray.length; j++) {
                assertEquals(vt.fetchRow(j).getLong("sArray"), sArray[j]);
            }
            vt = result[2];
            assert(vt.getRowCount() == iArray.length);
            for (int j = 0; j < iArray.length; j++) {
                assertEquals(vt.fetchRow(j).getLong("iArray"), iArray[j]);
            }
            vt = result[3];
            assert(vt.getRowCount() == lArray.length);
            for (int j = 0; j < lArray.length; j++) {
                assertEquals(vt.fetchRow(j).getLong("lArray"), lArray[j]);
            }

        } catch (Exception e) {
            fail("An argument value was mishandled in PassAllArgTypes");
        }

        // Now, go overboard, trying to preserve nano accuracy.
        // XXX: The following test is a little controversial.
        // Some would prefer a gentler response -- just truncating/rounding to the nearest microsecond.
        // When these voices of reason prevail, this test should be replaced by a test that nano-noise
        // gets filtered out but the result is still correct to microsecond granularity.
        java.sql.Timestamp ts_nano = new java.sql.Timestamp(PassAllArgTypes.MILLISECONDS_SINCE_EPOCH_TEST_VALUE);
        assertEquals(ts, ts_nano);
        // Extract the 1000000 nanos (doubly-counted milliseconds)
        assertEquals(1000000, ts_nano.getNanos());
        // and explicitly add in 1001 nanos (1 microsecond + 1 nanosecond)
        ts_nano.setNanos(ts_nano.getNanos()+1001);

        boolean caught;
        try {
            caught = false;
            // system-defined CRUD inputs
            client.callProcedure("PassAllArgTypes", b, bArray, s, sArray, i, iArray, l, lArray, str, bString,
                    ts_nano /* Here's the problem! */, utild, sqld, ts);
        } catch (RuntimeException e) {
            caught = true;
        } catch (Exception e) {
            caught = true; // but not quite how it was expected to be.
            fail("Some other exception while testing nano noise in PassAllArgTypes" + e);
        }
        assert(caught);
    }

    public void testSetOpsThatFail() throws Exception {
        Client client = getClient();

        boolean caught;

        caught = false;
        try {
            client.callProcedure("@AdHoc", "(SELECT NO_O_ID FROM NEW_ORDER WHERE NO_O_ID < 100) UNION (SELECT NO_O_ID FROM NEW_ORDER WHERE NO_O_ID < 100);");
        } catch (ProcCallException e) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            client.callProcedure("@AdHoc", "(SELECT NO_O_ID FROM NEW_ORDER WHERE NO_O_ID < 100) INTERSECT (SELECT NO_O_ID FROM NEW_ORDER WHERE NO_O_ID < 100);");
        } catch (ProcCallException e) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            client.callProcedure("@AdHoc", "(SELECT NO_O_ID FROM NEW_ORDER WHERE NO_O_ID < 100) EXCEPT (SELECT NO_O_ID FROM NEW_ORDER WHERE NO_O_ID < 100);");
        } catch (ProcCallException e) {
            caught = true;
        }
        assertTrue(caught);
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
        LocalCluster config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSQLFeaturesSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(BatchedMultiPartitionTest.class.getResource("sqlfeatures-ddl.sql"));
        project.addProcedures(PROCEDURES);

        boolean success;

        //* <-- Change this comment to 'block style' to toggle over to just the one single-server IPC DEBUG config.
        // IF (! DEBUG config) ...

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("sqlfeatures-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        config.setMaxHeap(3300);

        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlfeatures-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        config.setMaxHeap(3300);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: Local Cluster (of processes)
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlfeatures-cluster-rejoin.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        config.setMaxHeap(3800);
        // Commented out until ENG-3076, ENG-3434 are resolved.
        //config = new LocalCluster("sqlfeatures-cluster-rejoin.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI,
        //                          LocalCluster.FailureState.ONE_FAILURE, false);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /*/ // ... ELSE (DEBUG config) ... [ FRAGILE! This is a structured comment. Do not break it. ]

        /////////////////////////////////////////////////////////////
        // CONFIG #0: DEBUG Local Site/Partition running on IPC backend
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("sqlfeatures-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
        // build the jarfile
        success = config.compile(project);
        assert(success);
        // add this config to the set of tests to run
        builder.addServerConfig(config);

        // ... ENDIF (DEBUG config) [ FRAGILE! This is a structured comment. Do not break it. ] */

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestSQLFeaturesSuite.class);
    }
}
