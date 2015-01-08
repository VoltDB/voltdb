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
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.regressionsuites.failureprocs.BadDecimalToVarcharCompare;
import org.voltdb_testprocs.regressionsuites.failureprocs.BadFloatToVarcharCompare;
import org.voltdb_testprocs.regressionsuites.failureprocs.BadVarcharCompare;
import org.voltdb_testprocs.regressionsuites.failureprocs.BatchTooBig;
import org.voltdb_testprocs.regressionsuites.failureprocs.CleanupFail;
import org.voltdb_testprocs.regressionsuites.failureprocs.DivideByZero;
import org.voltdb_testprocs.regressionsuites.failureprocs.FetchTooMuch;
import org.voltdb_testprocs.regressionsuites.failureprocs.InsertBigString;
import org.voltdb_testprocs.regressionsuites.failureprocs.InsertLotsOfData;
import org.voltdb_testprocs.regressionsuites.failureprocs.ReturnAppStatus;
import org.voltdb_testprocs.regressionsuites.failureprocs.SelectBigString;
import org.voltdb_testprocs.regressionsuites.failureprocs.TooFewParams;
import org.voltdb_testprocs.regressionsuites.failureprocs.ViolateUniqueness;
import org.voltdb_testprocs.regressionsuites.failureprocs.ViolateUniquenessAndCatchException;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.WorkWithBigString;

public class TestFailuresSuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        BadVarcharCompare.class, BadFloatToVarcharCompare.class,
        BadDecimalToVarcharCompare.class,
        ViolateUniqueness.class, ViolateUniquenessAndCatchException.class,
        DivideByZero.class, WorkWithBigString.class, InsertBigString.class,
        InsertLotsOfData.class, FetchTooMuch.class, CleanupFail.class, TooFewParams.class,
        ReturnAppStatus.class, BatchTooBig.class, SelectBigString.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestFailuresSuite(String name) {
        super(name);
    }

    // Subcase of ENG-800
    public void testBadVarcharToAnyCompare() throws IOException
    {
        System.out.println("STARTING testBadVarcharToAnyCompare");
        Client client = getClient();

        boolean threw = false;
        try
        {
            client.callProcedure("BadVarcharCompare", 1).getResults();
        }
        catch (ProcCallException e)
        {
            if (!isHSQL())
            {
                if ((e.getMessage().contains("SQL ERROR")) &&
                        (e.getMessage().contains("cannot be cast for comparison to type VARCHAR")))
                {
                    threw = true;
                }
                else
                {
                    e.printStackTrace();
                }
            }
            else
            {
                threw = true;
            }
        }
        assertTrue(threw);
    }

    // Subcase of ENG-800
    public void testBadFloatToVarcharCompare() throws IOException
    {
        System.out.println("STARTING testBadFloatToVarcharCompare");
        Client client = getClient();

        boolean threw = false;
        try
        {
            client.callProcedure("BadFloatToVarcharCompare", 1).getResults();
        }
        catch (ProcCallException e)
        {
            if (!isHSQL())
            {
                if ((e.getMessage().contains("SQL ERROR")) &&
                        (e.getMessage().contains("VARCHAR cannot be cast for comparison to type FLOAT")))
                {
                    threw = true;
                }
                else
                {
                    e.printStackTrace();
                }
            }
            else
            {
                threw = true;
            }
        }
        assertTrue(threw);
    }

    // Subcase of ENG-800
    public void testBadDecimalToVarcharCompare() throws IOException
    {
        System.out.println("STARTING testBadDecimalToVarcharCompare");
        Client client = getClient();

        boolean threw = false;
        try
        {
            client.callProcedure("BadDecimalToVarcharCompare", 1).getResults();
        }
        catch (ProcCallException e)
        {
            if (!isHSQL())
            {
                if ((e.getMessage().contains("SQL ERROR")) &&
                        (e.getMessage().contains("VARCHAR cannot be cast for comparison to type DECIMAL")))
                {
                    threw = true;
                }
                else
                {
                    e.printStackTrace();
                }
            }
            else
            {
                threw = true;
            }
        }
        assertTrue(threw);
    }

    public void testViolateUniqueness() throws IOException {
        System.out.println("STARTING testVU");
        Client client = getClient();

        VoltTable[] results = null;
        try {
            results = client.callProcedure("ViolateUniqueness", 1L, 1L, 1L).getResults();
            System.out.println("testVU client response received");
            assertEquals(results.length, 0);
        } catch (ProcCallException e) {
            try {
                results = client.callProcedure("InsertNewOrder", 2L, 2L, 2L).getResults();
            } catch (ProcCallException e1) {
                fail(e1.toString());
            } catch (IOException e1) {
                fail(e1.toString());
            }
            System.out.println("second client response received");
            assertEquals(1, results.length);
            assertEquals(1, results[0].asScalarLong());

            return;
        } catch (IOException e) {
            fail(e.toString());
            return;
        }
        fail("testViolateUniqueness should return while catching ProcCallException");
    }

    public void testViolateUniquenessAndCatchException() throws IOException {
        Client client = getClient();
        System.out.println("STARTING testViolateUniquenessAndCatchException");
        VoltTable[] results = null;
        try {
            results = client.callProcedure("ViolateUniquenessAndCatchException", 1L, 1L, (byte)1).getResults();
            assertTrue(results.length == 1);
            System.out.println("PASSED testViolateUandCE");
        } catch (ProcCallException e) {
            System.out.println("FAIL(1) testViolateUandCE");
            e.printStackTrace();
            fail("ViolateUniquenessAndCatchException should not have thrown a ProcCallException");
        } catch (IOException e) {
            System.out.println("FAIL(2) testViolateUandCE");
            fail(e.toString());
            return;
        }

        try {
            results = client.callProcedure("InsertNewOrder", 2L, 2L, 2L).getResults();
        } catch (ProcCallException e1) {
            fail(e1.toString());
        } catch (IOException e1) {
            fail(e1.toString());
        }
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());

        return;
    }

    //
    // Check that a very large ConstraintFailureException can serialize correctly.
    //
    public void testTicket511_ViolateUniquenessWithLargeString() throws Exception {
        Client client = getClient();
        System.out.println("STARTING testTicket511_ViolateUniquenessWithLargeString");
        byte stringData[] = new byte[60000];
        java.util.Arrays.fill(stringData, (byte)'a');

        client.callProcedure("InsertBigString", 0, new String(stringData, "UTF-8"));

        java.util.Arrays.fill(stringData, (byte)'b');
        boolean threwException = false;
        try {
            client.callProcedure("InsertBigString", 0, new String(stringData, "UTF-8"));
        } catch (ProcCallException e) {
            threwException = true;
            assertTrue(e.getMessage().contains("CONSTRAINT VIOLATION"));
            assertTrue(e.getClientResponse().getStatusString().toUpperCase().contains("UNIQUE"));
            String msg = e.getClientResponse().getStatusString();
            System.err.println(msg);
        }
        assertTrue(threwException);
    }

    public void testDivideByZero() throws IOException {
        System.out.println("STARTING DivideByZero");
        Client client = getClient();

        VoltTable[] results = null;
        try {
            results = client.callProcedure("DivideByZero", 0L, 0L, (byte)1).getResults();
            System.out.println("DivideByZero client response received");
            assertEquals(results.length, 0);
        } catch (ProcCallException e) {
            System.out.println(e.getMessage());
            if (e.getMessage().contains("SQL ERROR"))
                return;
            if (isHSQL())
                if (e.getMessage().contains("HSQL-BACKEND ERROR"))
                    return;
        } catch (IOException e) {
            fail(e.toString());
            return;
        }
        fail("testDivideByZero should return while catching ProcCallException");
    }

    //
    // Note: this test looks like it should be testing the 50MB buffer serialization
    // limit between the EE and Java but watching it run, it really fails on max
    // temp table serialization sizes. This needs more investigation.
    //
    public void testMemoryOverload() throws IOException, ProcCallException {
        if (isHSQL() || isValgrind()) return;

        final int STRLEN = 30000;

        int totalBytes = 0;
        int expectedMaxSuccessBytes = 40000000; // less than the 50*1024*1024 limit.
        int expectedRows = 0;

        System.out.println("STARTING testMemoryOverload");
        Client client = getClient();

        String longStringPart = "volt!";
        StringBuilder sb = new StringBuilder();
        while(sb.length() < STRLEN)
            sb.append(longStringPart);
        String longString = sb.toString();
        assertEquals(STRLEN, longString.length());

        VoltTable[] results = null;

        while (totalBytes < expectedMaxSuccessBytes) {
            results = client.callProcedure("InsertBigString", expectedRows++, longString).getResults();
            assertEquals(1, results.length);
            assertEquals(1, results[0].asScalarLong());
            totalBytes += STRLEN;
        }

        results = client.callProcedure("WorkWithBigString", expectedRows++, longString).getResults();
        assertEquals(1, results.length);
        assertEquals(expectedRows, results[0].getRowCount());
        totalBytes += STRLEN;

        // 51MB exceeds the response buffer limit.
        while (totalBytes < (50 * 1024 * 1024)) {
            results = client.callProcedure("InsertBigString", expectedRows++, longString).getResults();
            assertEquals(1, results.length);
            assertEquals(1, results[0].asScalarLong());
            totalBytes += STRLEN;
        }

        // Some tests are run with a different effective partition count on community builds,
        // due to a k-factor downgrade, so allow for a possible per partition row count scale difference.
        int kFactorScaleDown;
        if (MiscUtils.isPro()) {
            kFactorScaleDown = 1;
        } else {
            kFactorScaleDown = 2;
        }

        for (int ii = 0; ii < 4; ii++) {
            results = client.callProcedure("SelectBigString", ii).getResults();
            System.out.println(results[0].getRowCount());
            long rowCount = results[0].getRowCount();
            //With elastic hashing the numbers are a little fuzzy
            if ( ! ((rowCount > 800 && rowCount < 950) ||
                    (rowCount > 800/kFactorScaleDown && rowCount < 950/kFactorScaleDown))) {
                System.out.println("Unexpected row count: " + rowCount);
            }
            assertTrue((rowCount > 800 && rowCount < 950) ||
                (rowCount > 800/kFactorScaleDown && rowCount < 950/kFactorScaleDown));
        }

        //System.out.printf("Fail Bytes: %d, Expected Rows %d\n", totalBytes, expectedRows);
        //System.out.flush();
        try {
            results = client.callProcedure("WorkWithBigString", expectedRows++, longString).getResults();
            fail();
        } catch (ProcCallException e) {
            // this should eventually happen
            assertTrue(totalBytes > expectedMaxSuccessBytes);
            return;
        } catch (IOException e) {
            fail(e.toString());
            return;
        }
        fail();
    }

    public void testPerPlanFragmentMemoryOverload() throws IOException, ProcCallException {
        if (isHSQL() || isValgrind()) return;

        System.out.println("STARTING testPerPlanFragmentMemoryOverload");
        Client client = getClient();

        VoltTable[] results = null;

        int nextId = 0;

        for (int mb = 0; mb < 75; mb += 5) {
            results = client.callProcedure("InsertLotsOfData", 0, nextId).getResults();
            assertEquals(1, results.length);
            assertTrue(nextId < results[0].asScalarLong());
            nextId = (int) results[0].asScalarLong();
            System.err.println("Inserted " + (mb + 5) + "mb");
        }

        results = client.callProcedure("FetchTooMuch", 0).getResults();
        assertEquals(1, results.length);
        System.out.println("Fetched the 75 megabytes");

        for (int mb = 0; mb < 75; mb += 5) {
            results = client.callProcedure("InsertLotsOfData", 0, nextId).getResults();
            assertEquals(1, results.length);
            assertTrue(nextId < results[0].asScalarLong());
            nextId = (int) results[0].asScalarLong();
            System.err.println("Inserted " + (mb + 80) + "mb");
        }

        try {
            results = client.callProcedure("FetchTooMuch", 0).getResults();
        } catch (ProcCallException e) {
            e.printStackTrace();
            return;
        }
        fail("Should gracefully fail from using too much temp table memory, but didn't.");
    }

    public void testQueueCleanupFailure() throws IOException, ProcCallException {
        System.out.println("STARTING testQueueCleanupFailure");
        Client client = getClient();

        VoltTable[] results = null;

        results = client.callProcedure("CleanupFail", 0, 0, (byte)0).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());

        results = client.callProcedure("CleanupFail", 2, 2, (byte)2).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());
    }

    public void testTooFewParamsOnSinglePartitionProc() throws IOException {
        System.out.println("STARTING testTooFewParamsOnSinglePartitionProc");
        Client client = getClient();

        try {
            client.callProcedure("TooFewParams", 1);
            fail();
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().startsWith("Error sending"));
        }

        try {
            client.callProcedure("TooFewParams");
            fail();
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().startsWith("Error sending"));
        }
    }

    public void testTooFewParamsOnSQLStmt() throws IOException {
        System.out.println("STARTING testTooFewParamsOnSQLStmt");
        Client client = getClient();

        try {
            client.callProcedure("TooFewParams", 1, 1);
            fail();
        } catch (ProcCallException e) {
        }
    }

    public void testAppStatus() throws Exception {
        System.out.println("STARTING testAppStatus");
        Client client = getClient();

        ClientResponse response = client.callProcedure( "ReturnAppStatus", 0, "statusstring", (byte)0);
        assertNull(response.getAppStatusString());
        assertEquals(response.getAppStatus(), Byte.MIN_VALUE);
        assertEquals(response.getResults()[0].getStatusCode(), Byte.MIN_VALUE);

        response = client.callProcedure( "ReturnAppStatus", 1, "statusstring", (byte)1);
        assertTrue("statusstring".equals(response.getAppStatusString()));
        assertEquals(response.getAppStatus(), 1);
        assertEquals(response.getResults()[0].getStatusCode(), 1);

        response = client.callProcedure( "ReturnAppStatus", 2, "statusstring", (byte)2);
        assertNull(response.getAppStatusString());
        assertEquals(response.getAppStatus(), 2);
        assertEquals(response.getResults()[0].getStatusCode(), 2);

        response = client.callProcedure( "ReturnAppStatus", 3, "statusstring", (byte)3);
        assertTrue("statusstring".equals(response.getAppStatusString()));
        assertEquals(response.getAppStatus(), Byte.MIN_VALUE);
        assertEquals(response.getResults()[0].getStatusCode(), 3);

        boolean threwException = false;
        try {
            response = client.callProcedure( "ReturnAppStatus", 4, "statusstring", (byte)4);
        } catch (ProcCallException e) {
            threwException = true;
            response = e.getClientResponse();
        }
        assertTrue(threwException);
        assertTrue("statusstring".equals(response.getAppStatusString()));
        assertEquals(response.getAppStatus(), 4);
    }

    /**
     * Build a list of the tests that will be run when TestFailuresSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestFailuresSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(DivideByZero.class.getResource("failures-ddl.sql"));
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with two sites/partitions
        VoltServerConfig config = new LocalCluster("failures-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile (note the reuse of the TPCC project)
        if (!config.compile(project)) fail();

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        // get a server config that similar, but doesn't use the same backend
        config = new LocalCluster("failures-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);

        // build the jarfile (note the reuse of the TPCC project)
        if (!config.compile(project)) fail();

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: N=2 K=1 Cluster
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("failures-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }
}
