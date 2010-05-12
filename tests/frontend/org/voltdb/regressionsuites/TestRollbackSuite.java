/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import org.voltdb.client.Client;
import org.voltdb.VoltTable;
import org.voltdb.client.ProcCallException;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.*;
import org.voltdb.regressionsuites.rollbackprocs.*;

public class TestRollbackSuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        SinglePartitionJavaError.class,
        SinglePartitionJavaAbort.class,
        SinglePartitionConstraintError.class,
        MultiPartitionJavaError.class,
        MultiPartitionJavaAbort.class,
        MultiPartitionConstraintError.class,
        SinglePartitionUpdateConstraintError.class,
        SinglePartitionConstraintFailureAndContinue.class,
        SelectAll.class,
        ReadMatView.class,
        FetchNORowUsingIndex.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestRollbackSuite(String name) {
        super(name);
    }

    public void testSinglePartitionJavaFailure() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("SinglePartitionJavaError", 2);
            fail();
        }
        catch (ProcCallException e) {}
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("SelectAll").getResults();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[7];
            assertTrue(table.getRowCount() == 0);

            // check the mat view
            results = client.callProcedure("ReadMatView", 2).getResults();
            assertEquals(results.length, 1);
            assertTrue(results[0].getRowCount() == 0);

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

    public void testSinglePartitionJavaAbort() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("SinglePartitionJavaAbort", 2);
            fail();
        }
        catch (ProcCallException e) {}
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("SelectAll").getResults();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[7];
            assertTrue(table.getRowCount() == 0);

            // check the mat view
            results = client.callProcedure("ReadMatView", 2).getResults();
            assertEquals(results.length, 1);
            assertTrue(results[0].getRowCount() == 0);
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

    public void testSinglePartitionConstraintFailure() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("SinglePartitionConstraintError", 2);
            fail();
        }
        catch (ProcCallException e) {}
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("SelectAll").getResults();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[7];
            assertTrue(table.getRowCount() == 0);

            // check the mat view
            results = client.callProcedure("ReadMatView", 2).getResults();
            assertEquals(results.length, 1);
            assertTrue(results[0].getRowCount() == 0);
        }
        catch (ProcCallException e) {
            e.printStackTrace();
            fail();
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            client.callProcedure("InsertNewOrder", 2, 2, 2);
        } catch (ProcCallException e1) {
            e1.printStackTrace();
            fail();
        }

        try {

            client.callProcedure("SinglePartitionUpdateConstraintError", 2);
            fail();
        }
        catch (ProcCallException e) {}
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("SelectAll").getResults();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[7];
            assertTrue(table.getRowCount() == 1);
            table.advanceRow();
            assertEquals(2, table.getLong(0));
            assertEquals(2, table.getLong(1));
            assertEquals(2, table.getLong(2));

            // check the mat view
            results = client.callProcedure("ReadMatView", 2).getResults();
            assertEquals(results.length, 1);
            assertTrue(results[0].getRowCount() == 1);
            results[0].advanceRow();
            assertEquals(2, results[0].getLong(0));
            assertEquals(1, results[0].getLong(1));
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

    public void testTooLargeStringInsertAndUpdate() throws IOException {
        final java.util.Random r = new java.util.Random();
        StringBuilder sb = new StringBuilder(300);
        for (int ii = 0; ii < 300; ii++) {
            sb.append(r.nextInt(9));
        }

        final String insertQuery = "INSERT INTO CUSTOMER_NAME VALUES ( 0, 0, 0, 'foo','" + sb.toString() + "')";
        final String updateQuery = "UPDATE CUSTOMER_NAME SET C_ID = 0, C_D_ID = 0, C_W_ID = 0, C_FIRST = 'foo', C_LAST ='" + sb.toString() + "'";
        final Client client = getClient();

        boolean threwException = false;
        try {
            client.callProcedure("@AdHoc", insertQuery);
        } catch (ProcCallException e) {
            threwException = true;
        }
        assertTrue(threwException);

        VoltTable results[] = null;
        try {
            results = client.callProcedure("@AdHoc", "SELECT * FROM CUSTOMER_NAME").getResults();
        } catch (ProcCallException e) {
            fail();
        }
        assertNotNull(results);
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());


        try {
            client.callProcedure("@AdHoc", "INSERT INTO CUSTOMER_NAME VALUES ( 0, 0, 0, 'foo', 'bar')");
        } catch (ProcCallException e) {
            fail();
        }

        threwException = false;
        try {
            client.callProcedure("@AdHoc", updateQuery);
        } catch (ProcCallException e) {
            e.printStackTrace();
            threwException = true;
        }
        assertTrue(threwException);

        results = null;
        try {
            results = client.callProcedure("@AdHoc", "SELECT * FROM CUSTOMER_NAME").getResults();
        } catch (ProcCallException e) {
            fail();
        }
        assertNotNull(results);
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        VoltTable result = results[0];
        result.advanceRow();
        assertEquals( 0, result.getLong(0));
        assertEquals( 0, result.getLong(1));
        assertEquals( 0, result.getLong(2));
        assertTrue( "foo".equals(result.getString(3)));
        assertTrue( "bar".equals(result.getString(4)));
    }

//    public void testSinglePartitionConstraintFailureAndContinue() throws IOException {
//        Client client = getClient();
//        VoltTable results[] = null;
//        try {
//            results = client.callProcedure("SinglePartitionConstraintFailureAndContinue", 2);
//        }
//        catch (ProcCallException e) {
//            fail("Didn't expect a ProcCallException if the user caught the constraint failure");
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//            fail();
//        }
//        assertNotNull(results);
//        assertEquals( 1, results.length);
//        assertEquals( 0, results[0].getRowCount());
//
//        try {
//            results = client.callProcedure("SelectAll");
//
//            assertEquals(results.length, 9);
//
//            // get the new order table
//            VoltTable table = results[7];
//            assertTrue(table.getRowCount() == 0);
//
//        }
//        catch (ProcCallException e) {
//            e.printStackTrace();
//            fail();
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//            fail();
//        }
//    }

    public void testMultiPartitionJavaFailure() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("MultiPartitionJavaError", 2);
            fail();
        }
        catch (ProcCallException e) {}
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("SelectAll").getResults();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[2];
            assertTrue(table.getRowCount() == 0);

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

    public void testMultiPartitionJavaAbort() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("MultiPartitionJavaAbort", 2);
            fail();
        }
        catch (ProcCallException e) {}
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("SelectAll").getResults();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[2];
            assertTrue(table.getRowCount() == 0);

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

    public void testMultiPartitionConstraintFailure() throws IOException {
        Client client = getClient();

        try {
            System.out.println("Calling MultiPartitionConstraintError");
            System.out.flush();
            client.callProcedure("MultiPartitionConstraintError", 2);
            System.out.println("Called MultiPartitionConstraintError");
            System.out.flush();
            fail();
        }
        catch (ProcCallException e) {}
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            System.out.println("Calling SelectAll");
            System.out.flush();
            VoltTable[] results = client.callProcedure("SelectAll").getResults();
            System.out.println("Called SelectAll");
            System.out.flush();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[2];
            System.out.println(table.toString());
            assertTrue(table.getRowCount() == 0);

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

    public void testOverflowMatView() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("InsertNewOrder", Long.MAX_VALUE - 2, 2, 2);
            client.callProcedure("InsertNewOrder", 1, 3, 2);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        try {
            String update = "UPDATE NEW_ORDER SET NO_O_ID = " + Long.MAX_VALUE + " WHERE NO_D_ID = 3;";
            client.callProcedure("@AdHoc", update);
            fail();
        }
        catch (ProcCallException e) {}
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("SelectAll").getResults();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[7];
            assertTrue(table.getRowCount() == 2);
            table.advanceRow();
            assertEquals(Long.MAX_VALUE - 2, table.getLong(0));
            assertEquals(2, table.getLong(1));
            assertEquals(2, table.getLong(2));
            table.advanceRow();
            assertEquals(1, table.getLong(0));
            assertEquals(3, table.getLong(1));
            assertEquals(2, table.getLong(2));

            // check the mat view
            results = client.callProcedure("ReadMatView", 2).getResults();
            assertEquals(results.length, 1);
            table = results[0];
            table.advanceRow();
            System.out.println(table);
            assertTrue(table.getRowCount() == 1);
            long col1 = table.getLong(0);
            assertEquals(2, col1);
            long col2 = table.getLong(1);
            assertEquals(2, col2);
            long col3 = table.getLong(2);
            assertEquals(Long.MAX_VALUE - 1, col3);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("FetchNORowUsingIndex", 2, 3, 1).getResults();

            assertEquals(results.length, 1);
            VoltTable table = results[0];
            int rowCount = table.getRowCount();
            assertEquals(1, rowCount);
            table.advanceRow();
            assertEquals(1, table.getLong(0));
            assertEquals(3, table.getLong(1));
            assertEquals(2, table.getLong(2));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testUpdateViolatesUniqueConstraint() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("InsertNewOrder", 2, 2, 2);
            client.callProcedure("InsertNewOrder", 3, 4, 2);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        try {
            String update = "UPDATE NEW_ORDER SET NO_O_ID = " + 2 + ", NO_D_ID = 2 WHERE NO_D_ID = 4;";
            client.callProcedure("@AdHoc", update);
            fail();
        }
        catch (ProcCallException e) {
           e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("SelectAll").getResults();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[7];
            System.out.println(table);
            assertTrue(table.getRowCount() == 2);
            table.advanceRow();
            assertEquals(2, table.getLong(0));
            assertEquals(2, table.getLong(1));
            assertEquals(2, table.getLong(2));
            table.advanceRow();
            assertEquals(3, table.getLong(0));
            assertEquals(4, table.getLong(1));
            assertEquals(2, table.getLong(2));

            // check the mat view
            results = client.callProcedure("ReadMatView", 2).getResults();
            assertEquals(results.length, 1);
            table = results[0];
            table.advanceRow();
            System.out.println(table);
            assertTrue(table.getRowCount() == 1);
            long col1 = table.getLong(0);
            assertEquals(2, col1);
            long col2 = table.getLong(1);
            assertEquals(2, col2);
            long col3 = table.getLong(2);
            assertEquals(5, col3);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("FetchNORowUsingIndex", 2, 4, 3).getResults();

            assertEquals(results.length, 1);
            VoltTable table = results[0];
            int rowCount = table.getRowCount();
            assertEquals(1, rowCount);
            table.advanceRow();
            assertEquals(3, table.getLong(0));
            assertEquals(4, table.getLong(1));
            assertEquals(2, table.getLong(2));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testUpdateViolatesNotNullConstraint() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("InsertNewOrder", 2, 2, 2);
            client.callProcedure("InsertNewOrder", 3, 4, 2);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        try {
            String update = "UPDATE NEW_ORDER SET NO_O_ID = -9223372036854775808, NO_D_ID = -9223372036854775808 WHERE NO_D_ID = 4;";
            client.callProcedure("@AdHoc", update);
            fail();
        }
        catch (ProcCallException e) {
//           e.printStackTrace();
           e.getCause().printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("SelectAll").getResults();

            assertEquals(results.length, 9);

            // get the new order table
            VoltTable table = results[7];
            System.out.println(table);
            assertTrue(table.getRowCount() == 2);
            table.advanceRow();
            assertEquals(2, table.getLong(0));
            assertEquals(2, table.getLong(1));
            assertEquals(2, table.getLong(2));
            table.advanceRow();
            assertEquals(3, table.getLong(0));
            assertEquals(4, table.getLong(1));
            assertEquals(2, table.getLong(2));

            // check the mat view
            results = client.callProcedure("ReadMatView", 2).getResults();
            assertEquals(results.length, 1);
            table = results[0];
            table.advanceRow();
            System.out.println(table);
            assertTrue(table.getRowCount() == 1);
            long col1 = table.getLong(0);
            assertEquals(2, col1);
            long col2 = table.getLong(1);
            assertEquals(2, col2);
            long col3 = table.getLong(2);
            assertEquals(5, col3);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("FetchNORowUsingIndex", 2, 4, 3).getResults();

            assertEquals(results.length, 1);
            VoltTable table = results[0];
            int rowCount = table.getRowCount();
            assertEquals(1, rowCount);
            table.advanceRow();
            assertEquals(3, table.getLong(0));
            assertEquals(4, table.getLong(1));
            assertEquals(2, table.getLong(2));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
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
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestRollbackSuite.class);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addSchema(SinglePartitionJavaError.class.getResource("tpcc-extraview-ddl.sql"));
        project.addDefaultPartitioning();
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalSingleProcessServer("rollback-onesite.jar", 1, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with two sites/partitions
        config = new LocalSingleProcessServer("rollback-twosites.jar", 2, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile (note the reuse of the TPCC project)
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        //config = new LocalSingleProcessServer("rollback-hsql.jar", 1, BackendTarget.HSQLDB_BACKEND);
        //success = config.compile(project);
        //assert(success);
        //builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #4: Local Cluster (of processes)
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("rollback-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI, false);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestRollbackSuite.class);
    }
}
