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
import java.math.BigDecimal;
import java.math.BigInteger;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.SelectAll;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.types.TimestampType;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.AllTypesJavaAbort;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.AllTypesJavaError;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.AllTypesMultiOpsJavaError;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.AllTypesUpdateJavaAbort;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.AllTypesUpdateJavaError;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.FetchNORowUsingIndex;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.InsertAllTypes;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.MultiPartitionConstraintError;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.MultiPartitionJavaAbort;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.MultiPartitionJavaError;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.MultiPartitionParamSerializationError;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.ReadMatView;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.SinglePartitionConstraintError;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.SinglePartitionConstraintFailureAndContinue;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.SinglePartitionJavaAbort;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.SinglePartitionJavaError;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.SinglePartitionParamSerializationError;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.SinglePartitionUpdateConstraintError;

public class TestRollbackSuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        SinglePartitionJavaError.class,
        SinglePartitionJavaAbort.class,
        SinglePartitionConstraintError.class,
        MultiPartitionJavaError.class,
        MultiPartitionJavaAbort.class,
        MultiPartitionConstraintError.class,
        MultiPartitionParamSerializationError.class,
        SinglePartitionUpdateConstraintError.class,
        SinglePartitionConstraintFailureAndContinue.class,
        SinglePartitionParamSerializationError.class,
        SelectAll.class,
        ReadMatView.class,
        FetchNORowUsingIndex.class,
        InsertAllTypes.class,
        AllTypesJavaError.class,
        AllTypesJavaAbort.class,
        AllTypesUpdateJavaError.class,
        AllTypesUpdateJavaAbort.class,
        AllTypesMultiOpsJavaError.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestRollbackSuite(String name) {
        super(name);
    }

    public void testParameterSetSerializationErrorRollback()
    throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();

        try
        {
            client.callProcedure("MultiPartitionParamSerializationError", 0);
            fail("MultiPartitionParamSerializationError should have thrown a ProcCallException, not succeeded");
        }
        catch (ProcCallException e) {
        }

        try
        {
            client.callProcedure("SinglePartitionParamSerializationError", 0);
            fail("SinglePartitionParamSerializationError should have thrown a ProcCallException, not succeeded");
        }
        catch (ProcCallException e) {
        }

        VoltTable results = client.callProcedure("@Statistics", "procedure", 0).getResults()[0];
        System.out.println("results: " + results);
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
            results = client.callProcedure("ReadMatView", (byte)2).getResults();
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
            client.callProcedure("SinglePartitionJavaAbort", (byte)2);
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
            results = client.callProcedure("ReadMatView", (byte)2).getResults();
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
            client.callProcedure("SinglePartitionConstraintError", (byte)2);
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
            results = client.callProcedure("ReadMatView", (byte)2).getResults();
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

            client.callProcedure("SinglePartitionUpdateConstraintError", (byte)2);
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
            results = client.callProcedure("ReadMatView", (byte)2).getResults();
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

    // ENG-487
    public void allTypesTestHelper(String procName, int[] order, int id)
    throws IOException {
        Client client = getClient();
        final double EPSILON = 0.00001;
        final BigDecimal moneyOne = new BigDecimal(BigInteger.valueOf(7700000000000L), 12);
        final BigDecimal moneyTwo = new BigDecimal(BigInteger.valueOf(1100000000000L), 12);

        try {
            client.callProcedure("InsertAllTypes", 1, 2, 3, 4, new TimestampType(5),
                                 0.6, moneyOne, "inlined", "uninlined");
            client.callProcedure("InsertAllTypes", 7, 6, 5, 4, new TimestampType(3),
                                 0.2, moneyTwo, "INLINED", "UNINLINED");
        } catch (ProcCallException e1) {
            e1.printStackTrace();
            fail();
        }

        try {
            if (procName.contains("MultiOps"))
                client.callProcedure("AllTypesMultiOpsJavaError", order, id);
            else if (procName.contains("Update"))
                client.callProcedure(procName, 7);
            else
                client.callProcedure(procName);
            fail();
        }
        catch (ProcCallException e) {}
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            VoltTable[] results = client.callProcedure("@AdHoc", "SELECT * FROM ALL_TYPES ORDER BY ID ASC;").getResults();

            // get the table
            VoltTable table = results[0];
            assertTrue(table.getRowCount() == 2);

            table.advanceRow();
            assertEquals(1, table.getLong(0));
            assertEquals(2, table.getLong(1));
            assertEquals(3, table.getLong(2));
            assertEquals(4, table.getLong(3));
            assertEquals(new TimestampType(5), table.getTimestampAsTimestamp(4));
            assertTrue(Math.abs(0.6 - table.getDouble(5)) < EPSILON);
            assertEquals(moneyOne, table.getDecimalAsBigDecimal(6));
            assertTrue(table.getString(7).equals("inlined"));
            assertTrue(table.getString(8).equals("uninlined"));

            table.advanceRow();
            assertEquals(7, table.getLong(0));
            assertEquals(6, table.getLong(1));
            assertEquals(5, table.getLong(2));
            assertEquals(4, table.getLong(3));
            assertEquals(new TimestampType(3), table.getTimestampAsTimestamp(4));
            assertTrue(Math.abs(0.2 - table.getDouble(5)) < EPSILON);
            assertEquals(moneyTwo, table.getDecimalAsBigDecimal(6));
            assertTrue(table.getString(7).equals("INLINED"));
            assertTrue(table.getString(8).equals("UNINLINED"));

            // Check by using the indexes
            results = client.callProcedure("@AdHoc", "SELECT * FROM ALL_TYPES WHERE ID > 3;").getResults();

            // get the table
            table = results[0];
            assertTrue(table.getRowCount() == 1);

            table.advanceRow();
            assertEquals(7, table.getLong(0));
            assertEquals(6, table.getLong(1));
            assertEquals(5, table.getLong(2));
            assertEquals(4, table.getLong(3));
            assertEquals(new TimestampType(3), table.getTimestampAsTimestamp(4));
            assertTrue(Math.abs(0.2 - table.getDouble(5)) < EPSILON);
            assertEquals(moneyTwo, table.getDecimalAsBigDecimal(6));
            assertTrue(table.getString(7).equals("INLINED"));
            assertTrue(table.getString(8).equals("UNINLINED"));

            // unique hash index
            results = client.callProcedure("@AdHoc", "SELECT * FROM ALL_TYPES WHERE ID = 1;").getResults();

            // get the table
            table = results[0];
            assertTrue(table.getRowCount() == 1);

            table.advanceRow();
            assertEquals(1, table.getLong(0));
            assertEquals(2, table.getLong(1));
            assertEquals(3, table.getLong(2));
            assertEquals(4, table.getLong(3));
            assertEquals(new TimestampType(5), table.getTimestampAsTimestamp(4));
            assertTrue(Math.abs(0.6 - table.getDouble(5)) < EPSILON);
            assertEquals(moneyOne, table.getDecimalAsBigDecimal(6));
            assertTrue(table.getString(7).equals("inlined"));
            assertTrue(table.getString(8).equals("uninlined"));

            // multimap tree index
            results = client.callProcedure("@AdHoc", "SELECT * FROM ALL_TYPES ORDER BY TINY, SMALL, BIG, T, RATIO, MONEY, INLINED DESC;").getResults();

            // get the table
            table = results[0];
            assertTrue(table.getRowCount() == 2);

            table.advanceRow();
            assertEquals(1, table.getLong(0));
            assertEquals(2, table.getLong(1));
            assertEquals(3, table.getLong(2));
            assertEquals(4, table.getLong(3));
            assertEquals(new TimestampType(5), table.getTimestampAsTimestamp(4));
            assertTrue(Math.abs(0.6 - table.getDouble(5)) < EPSILON);
            assertEquals(moneyOne, table.getDecimalAsBigDecimal(6));
            assertTrue(table.getString(7).equals("inlined"));
            assertTrue(table.getString(8).equals("uninlined"));

            table.advanceRow();
            assertEquals(7, table.getLong(0));
            assertEquals(6, table.getLong(1));
            assertEquals(5, table.getLong(2));
            assertEquals(4, table.getLong(3));
            assertEquals(new TimestampType(3), table.getTimestampAsTimestamp(4));
            assertTrue(Math.abs(0.2 - table.getDouble(5)) < EPSILON);
            assertEquals(moneyTwo, table.getDecimalAsBigDecimal(6));
            assertTrue(table.getString(7).equals("INLINED"));
            assertTrue(table.getString(8).equals("UNINLINED"));

            // multimap hash index
            results = client.callProcedure("@AdHoc", "SELECT * FROM ALL_TYPES WHERE TINY = 6 AND SMALL = 5 AND BIG = 4;").getResults();

            // get the table
            table = results[0];
            assertTrue(table.getRowCount() == 1);

            table.advanceRow();
            assertEquals(7, table.getLong(0));
            assertEquals(6, table.getLong(1));
            assertEquals(5, table.getLong(2));
            assertEquals(4, table.getLong(3));
            assertEquals(new TimestampType(3), table.getTimestampAsTimestamp(4));
            assertTrue(Math.abs(0.2 - table.getDouble(5)) < EPSILON);
            assertEquals(moneyTwo, table.getDecimalAsBigDecimal(6));
            assertTrue(table.getString(7).equals("INLINED"));
            assertTrue(table.getString(8).equals("UNINLINED"));

            // clean up
            client.callProcedure("@AdHoc", "DELETE FROM ALL_TYPES");
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

    public void testAllTypesJavaError() throws IOException {
        allTypesTestHelper("AllTypesJavaError", null, 0);
    }

    public void testAllTypesJavaAbort() throws IOException {
        allTypesTestHelper("AllTypesJavaAbort", null, 0);
    }

    public void testAllTypesUpdateJavaError() throws IOException {
        allTypesTestHelper("AllTypesUpdateJavaError", null, 0);
    }

    public void testAllTypesUpdateJavaAbort() throws IOException {
        allTypesTestHelper("AllTypesUpdateJavaAbort", null, 0);
    }

    // ENG-488
    public void testAllTypesMultiOpsJavaError() throws IOException {
        allTypesTestHelper("AllTypesMultiOpsJavaError",
                           new int[] {AllTypesMultiOpsJavaError.INSERT,
                                      AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.DELETE},
                           3);
        allTypesTestHelper("AllTypesMultiOpsJavaError",
                           new int[] {AllTypesMultiOpsJavaError.INSERT,
                                      AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.DELETE},
                           3);
        allTypesTestHelper("AllTypesMultiOpsJavaError",
                           new int[] {AllTypesMultiOpsJavaError.INSERT,
                                      AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.UPDATE},
                           3);
        allTypesTestHelper("AllTypesMultiOpsJavaError",
                           new int[] {AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.UPDATE},
                           7);
        allTypesTestHelper("AllTypesMultiOpsJavaError",
                           new int[] {AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.DELETE},
                           7);
        allTypesTestHelper("AllTypesMultiOpsJavaError",
                           new int[] {AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.DELETE,
                                      AllTypesMultiOpsJavaError.INSERT},
                           7);
        allTypesTestHelper("AllTypesMultiOpsJavaError",
                           new int[] {AllTypesMultiOpsJavaError.DELETE,
                                      AllTypesMultiOpsJavaError.INSERT,
                                      AllTypesMultiOpsJavaError.UPDATE},
                           7);
        allTypesTestHelper("AllTypesMultiOpsJavaError",
                           new int[] {AllTypesMultiOpsJavaError.DELETE,
                                      AllTypesMultiOpsJavaError.INSERT,
                                      AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.UPDATE},
                           7);
        allTypesTestHelper("AllTypesMultiOpsJavaError",
                           new int[] {AllTypesMultiOpsJavaError.DELETE,
                                      AllTypesMultiOpsJavaError.INSERT,
                                      AllTypesMultiOpsJavaError.UPDATE,
                                      AllTypesMultiOpsJavaError.DELETE},
                           7);
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
            results = client.callProcedure("ReadMatView", (byte)2).getResults();
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
            VoltTable[] results = client.callProcedure("FetchNORowUsingIndex", (byte)2, 3, 1).getResults();

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
            results = client.callProcedure("ReadMatView", (byte)2).getResults();
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
            VoltTable[] results = client.callProcedure("FetchNORowUsingIndex", (byte)2, 4, 3).getResults();

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
           System.out.println(e);
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
            results = client.callProcedure("ReadMatView", (byte)2).getResults();
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
            VoltTable[] results = client.callProcedure("FetchNORowUsingIndex", (byte)2, 4, 3).getResults();

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
        project.addPartitionInfo("ALL_TYPES", "ID");
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with two sites/partitions
        config = new LocalCluster("rollback-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile (note the reuse of the TPCC project)
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: Local Cluster (of processes)
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("rollback-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestRollbackSuite.class);
    }
}
