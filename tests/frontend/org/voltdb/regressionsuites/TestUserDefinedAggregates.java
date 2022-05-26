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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import org.junit.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testfuncs.UserDefinedTestFunctions.UserDefinedTestException;

/**
 * Tests of SQL statements that use User-Defined Aggregate Functions (UDAF's).
 */
public class TestUserDefinedAggregates extends RegressionSuite {

    static protected Random random = new Random();
    static protected final double EPSILON = 1.0E-12;  // Acceptable difference, for FLOAT (Double) tests

    public TestUserDefinedAggregates(String name) {
        super(name);
    }

    protected void testFunction(String functionCall, Object[] expected, VoltType returnType,
            String[] columnNames, String[][] columnValues)
            throws IOException, ProcCallException {
        // randomly decide which table to test
        String tableName = "R1";
        if (random.nextInt(100) < 50) {
            tableName = "P1";
        }

        // Set the expected result of the SELECT query using the UDAF
        int expectedTableLength = expected == null ? 0 : expected.length;
        Object[][] expectedTable = new Object[1][expectedTableLength];
        for (int i = 0; i < expectedTableLength; ++i) {
            expectedTable[0][i] = expected[i];
        }

        // initialize the client response and client
        ClientResponse cr = null;
        Client client = getClient();

        // INSERT rows into the table that we are using for testing
        StringBuilder allColumnNames = new StringBuilder("ID");
        StringBuilder allColumnValues = new StringBuilder();
        for (int i = 0; i < columnValues.length; ++i) {
            allColumnNames.append(", ").append(columnNames[i]);
        }
        for (int i = 0; i < columnValues[0].length; ++i) {
            allColumnValues = new StringBuilder(Integer.toString(i));
            for (int j = 0; j < columnValues.length; ++j) {
                allColumnValues.append(", ").append(columnValues[j][i]);
            }
            String insertStatement = "INSERT INTO "+ tableName
                    + " ("+allColumnNames+") VALUES" + " ("+allColumnValues+")";
            cr = client.callProcedure("@AdHoc", insertStatement);
            assertEquals(insertStatement+" failed", ClientResponse.SUCCESS, cr.getStatus());
        }

        // Get the actual result of the SELECT query using the UDF
        String selectStatement = "SELECT "+functionCall+" FROM "+tableName;
        cr = client.callProcedure("@AdHoc", selectStatement);
        assertEquals(selectStatement+" failed", ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];

        // Compare the expected to the actual result
        if (VoltType.FLOAT.equals(returnType)) {
            RegressionSuite.assertApproximateContentOfTable(expectedTable, vt, EPSILON);
        } else {
            RegressionSuite.assertContentOfTable(expectedTable, vt);
        }

        // Clean-up
        String truncateStatement = "TRUNCATE TABLE "+tableName;
        cr = client.callProcedure("@AdHoc", truncateStatement);
        assertEquals(truncateStatement+" (clean-up) failed", ClientResponse.SUCCESS, cr.getStatus());
    }

    /** Tests the specified <i>functionCall</i>, and confirms that an Exception
     *  is thrown, of type ProcCallException.class, with a cause (possibly null)
     *  of <i>expectedExcepCauseType</i>.
     *  It does this by first INSERT-ing one row, and then SELECT-ing the
     *  <i>functionCall</i> value (as well as the ID) from that row, and then
     *  catching any Exception (or other Throwable) thrown.
     *  Optionally, you can also specify <i>columnNames</i> and corresponding
     *  <i>columnValues</i> (in addition to ID=0) to be specified in the
     *  initial INSERT statement.
     *  The table to INSERT into and SELECT from is chosen, randomly, as either
     *  R1 or P1. */
    protected void testFunctionThrowsException(String functionCall, VoltType returnType,
            Class<? extends Throwable> expectedExcepCauseType,
            String[] columnNames, String[][] columnValues) {
        Class<? extends Throwable> expectedExceptionType = ProcCallException.class;
        try {
            testFunction(functionCall, null, returnType, columnNames, columnValues);
        } catch (Throwable ex) {
            Class<? extends Throwable> actualExceptionType = ex.getClass();
            Class<? extends Throwable> actualExcepCauseType = null;
            Throwable exceptionCause = ex.getCause();
            if (exceptionCause != null) {
                actualExcepCauseType = exceptionCause.getClass();
            }
            assertEquals("Unexpected Exception type for: " + functionCall,
                    expectedExceptionType, actualExceptionType);
            // TODO: delete, once UDFs throwing exceptions with causes works (ENG-12863):
            if (exceptionCause != null) {
                assertEquals("Unexpected Exception *cause* type for " + functionCall,
                        expectedExcepCauseType, actualExcepCauseType);
            }
            return;
        }
        fail(functionCall + " did not throw expected exception: " + expectedExceptionType +
                " (with " + expectedExcepCauseType + " cause)");
    }

    protected void testCreateFunctionException(String functionCall, String errorMessage) throws IOException {
        Client client = getClient();
        try {
            client.callProcedure("@AdHoc", functionCall);
            fail();
        }
        catch (ProcCallException ex) {
            assertEquals(errorMessage, ex.getMessage());
        }
    }

    // Unit tests for UDAFs
    @Test
    public void testUavg() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1", "2", "3", "4"}};
        Object[] expected = {2.5D};
        testFunction("uavg(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUavgAndAbs() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1", "-2", "3", "-4"}};
        Object[] expected = {2.5D};
        testFunction("uavg(abs(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUavgAndFloor() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.2", "2.8", "3.3", "4.6"}};
        Object[] expected = {2.5D};
        testFunction("uavg(floor(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUavgTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1", "2", "3", "4"}, {"2", "3", "4", "5"}};
        Object[] expected = {2.5D, 3.5D};
        testFunction("uavg(NUM), uavg(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUavgAddColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1", "2", "3", "4"}, {"2", "3", "4", "5"}};
        Object[] expected = {6D};
        testFunction("uavg(NUM) + uavg(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUCount() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}};
        Object[] expected = {8};
        testFunction("ucount(NUM)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    @Test
    public void testUCountTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}, {"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}};
        Object[] expected = {8, 8};
        testFunction("ucount(NUM), ucount(DEC)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    @Test
    public void testUCountSubtractColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}, {"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}};
        Object[] expected = {0D};
        testFunction("ucount(NUM) - ucount(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUCountThreeColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}, {"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}};
        Object[] expected = {8, 8, 8};
        testFunction("ucount(NUM), ucount(DEC), count(DEC)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    @Test
    public void testUmax() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.0", "-1000.4", "999"}};
        Object[] expected = {999.0D};
        testFunction("umax(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmaxAndAbs() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.0", "-1000.4", "999"}};
        Object[] expected = {1000.4D};
        testFunction("umax(abs(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmaxTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.0", "-1000.4", "999"}, {"-0.5", "11000.2", "100.4", "999.8", "-1000.4", "998.3"}};
        Object[] expected = {999.0D, 11000.2D};
        testFunction("umax(NUM), umax(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmaxDivideColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.8", "-1000.4", "999"}, {"-0.5", "100.2", "100.4", "999.8", "-1000.4", "998.3"}};
        Object[] expected = {1.0D};
        testFunction("umax(NUM)/umax(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmaxAndCeilingDivideColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.8", "-1000.4", "999"}, {"-0.5", "100.2", "100.4", "1999.8", "-1000.4", "998.3"}};
        Object[] expected = {0.5D};
        testFunction("umax(ceiling(NUM))/umax(ceiling(DEC))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmedian() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"2", "4", "5", "10"}};
        Object[] expected = {4.5D};
        testFunction("umedian(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmedianTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"2", "4", "5", "10"}, {"1", "10", "5", "5"}};
        Object[] expected = {4.5D, 5.0D};
        testFunction("umedian(NUM), umedian(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmedianAndDivide() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"2", "9", "9", "10"}, {"1", "10", "4", "4"}};
        Object[] expected = {2.25D};
        testFunction("umedian(NUM)/umedian(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmedianThreeColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"2", "4", "5", "10"}, {"1", "10", "5", "5"}};
        Object[] expected = {4.5D, 5.0D, 21.0D};
        testFunction("umedian(NUM), umedian(DEC), sum(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmedianEndUnsupportedReturn() throws IOException {
        String functionCall = "CREATE AGGREGATE FUNCTION umedianEndUnsupportedReturn FROM CLASS org.voltdb_testfuncs.UmedianEndUnsupportedReturn";
        String errorMessage = "Unexpected condition occurred applying DDL statements: Unsupported return value type: java.util.List";
        testCreateFunctionException(functionCall, errorMessage);
    }

    @Test
    public void testUmin() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"0.4", "1", "100.3", "999.9", "-1000.0", "999.1"}};
        Object[] expected = {-1000.0};
        testFunction("umin(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUminWithException() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"0.4", "1", "100.3", "999.9", "-1000.0", "999.1"}};
        Object[] expected = {-1000.0};
        try {
            testFunction("uminwithexception(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
        } catch (ProcCallException e) {
            // In debug run, exception also has stacktrace appended.
            assertTrue(e.getMessage().startsWith(
                    "VOLTDB ERROR: SQL ERROR UserDefinedAggregate::assemble() failed: Minimum value negative"));
        }
    }

    @Test
    public void testUminAndUDF() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"0.4", "1", "100.3", "999.9", "-1000.0", "999.1"}};
        Object[] expected = {1.0D};
        testFunction("add2Float(umin(NUM),1001.0)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUmode() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"1", "3", "3", "3", "5", "5", "7"}};
        Object[] expected = {3};
        testFunction("umode(INT)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    @Test
    public void testUmodeTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"INT", "BIG"};
        String[][] columnValues = {{"1", "3", "3", "3", "5", "5", "7"}, {"0", "0", "0", "23", "13", "9", "12"}};
        Object[] expected = {3, 0};
        testFunction("umode(INT), umode(BIG)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    @Test
    public void testUmodeAndUDF() throws IOException, ProcCallException {
        String[] columnNames = {"INT", "BIG"};
        String[][] columnValues = {{"1", "3", "3", "3", "5", "5", "7"}, {"0", "0", "0", "23", "13", "9", "12"}};
        Object[] expected = {3};
        testFunction("add2Integer(umode(INT), umode(BIG))", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    @Test
    public void testUmodeAssembleUnsupportedParameter() throws IOException {
        String functionCall = "CREATE AGGREGATE FUNCTION umodeAssembleUnsupportedParameter FROM CLASS org.voltdb_testfuncs.UmodeAssembleUnsupportedParameter";
        String errorMessage = "Unexpected condition occurred applying DDL statements: Unsupported parameter value type: java.util.ArrayList";
        testCreateFunctionException(functionCall, errorMessage);
    }

    @Test
    public void testUprimesum() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"0", "2", "13", "25", "37", "14", "87"}};
        Object[] expected = {52};
        testFunction("uprimesum(INT)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    @Test
    public void testUprimesumTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"INT", "BIG"};
        String[][] columnValues = {{"0", "2", "13", "25", "37", "14", "87"}, {"2", "3", "5", "7", "11", "13", "17"}};
        Object[] expected = {52, 58};
        testFunction("uprimesum(INT), uprimesum(BIG)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    @Test
    public void testUprimesumAndPower() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"0", "2", "13", "25", "37", "14", "87"}};
        Object[] expected = {2704D};
        testFunction("power(uprimesum(INT), 2)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUprimesumAndSubtractTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"INT", "BIG"};
        String[][] columnValues = {{"0", "2", "13", "25", "37", "14", "87"}, {"2", "3", "5", "7", "11", "13", "17"}};
        Object[] expected = {-6};
        testFunction("uprimesum(INT) - uprimesum(BIG)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    @Test
    public void testUprimesumCombineWrongParameter() throws IOException {
        String functionCall = "CREATE AGGREGATE FUNCTION uprimesumCombineWrongParameter FROM CLASS org.voltdb_testfuncs.UprimesumCombineWrongParameter";
        String errorMessage = "Unexpected condition occurred applying DDL statements: Parameter type must be instance of Class: org.voltdb_testfuncs.UprimesumCombineWrongParameter";
        testCreateFunctionException(functionCall, errorMessage);
    }

    @Test
    public void testUsum() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.2", "2.5", "-4.6", "6.4", "-5.5"}};
        Object[] expected = {0.0D};
        testFunction("usum(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUsumAndSqrt() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.45", "2.5", "-4.6", "6.4", "-5.5"}};
        Object[] expected = {0.5D};
        testFunction("sqrt(usum(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUsumAndSqrtAndCeilingTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.45", "2.5", "-4.6", "6.4", "-5.5"}};
        Object[] expected = {0.5D, 1.0D};
        testFunction("sqrt(usum(NUM)), ceiling(usum(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    @Test
    public void testUminOverflow() {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"-99999999999999999999999999.999999999999, -0.000000000001"}};
        testFunctionThrowsException("umin(NUM)", VoltType.FLOAT, RuntimeException.class, columnNames, columnValues);
    }

    @Test
    public void testUmaxOverflow() {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"99999999999999999999999999.999999999999, 0.000000000001"}};
        testFunctionThrowsException("umax(NUM)", VoltType.FLOAT, RuntimeException.class, columnNames, columnValues);
    }

    @Test
    public void testUcountNullPointerException() {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"UDF_TEST.THROW_NullPointerException", "1.3", "-3.55"}};
        testFunctionThrowsException("ucount(NUM)", VoltType.INTEGER, NullPointerException.class, columnNames, columnValues);
    }

    @Test
    public void testUmedianIllegalArgumentException() {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"UDF_TEST.THROW_IllegalArgumentException", "2", "-3"}};
        testFunctionThrowsException("umedian(INT)", VoltType.FLOAT, IllegalArgumentException.class, columnNames, columnValues);
    }

    @Test
    public void testUmodeNumberFormatException() {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"UDF_TEST.THROW_NumberFormatException", "4", "-6"}};
        testFunctionThrowsException("umode(INT)", VoltType.FLOAT, NumberFormatException.class, columnNames, columnValues);
    }

    @Test
    public void testUavgArrayIndexOutOfBoundsException() {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"UDF_TEST.THROW_ArrayIndexOutOfBoundsException", "2.8", "28"}};
        testFunctionThrowsException("uavg(NUM)", VoltType.FLOAT, ArrayIndexOutOfBoundsException.class, columnNames, columnValues);
    }

    @Test
    public void testUaddstrClassCastException() {
        String[] columnNames = {"VCHAR"};
        String[][] columnValues = {{"UDF_TEST.THROW_ClassCastException", "'Kawhi'", "'Paul'"}};
        testFunctionThrowsException("uaddstr(VCHAR)", VoltType.STRING, ClassCastException.class, columnNames, columnValues);
    }

    @Test
    public void testUprimesumArithmeticException() {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"UDF_TEST.THROW_ArithmeticException", "2", "3"}};
        testFunctionThrowsException("uprimesum(INT)", VoltType.INTEGER, ArithmeticException.class, columnNames, columnValues);
    }

    @Test
    public void testUcountUnsupportedOperationException() {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"UDF_TEST.THROW_UnsupportedOperationException", "1.3", "-3.55"}};
        testFunctionThrowsException("ucount(NUM)", VoltType.INTEGER, UnsupportedOperationException.class, columnNames, columnValues);
    }

    @Test
    public void testUmedianVoltTypeException() {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"UDF_TEST.THROW_VoltTypeException", "2", "-3"}};
        testFunctionThrowsException("umedian(INT)", VoltType.FLOAT, VoltTypeException.class, columnNames, columnValues);
    }

    @Test
    public void testUmodeUserDefinedTestException() {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"UDF_TEST.THROW_UserDefinedTestException", "4", "-6"}};
        testFunctionThrowsException("umode(INT)", VoltType.FLOAT, UserDefinedTestException.class, columnNames, columnValues);
    }

    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestUserDefinedAggregates.class);

        // build up a project builder for the workload
       VoltProjectBuilder project = new VoltProjectBuilder();
       project.setUseDDLSchema(true);

       byte[] createFunctionsDDL = null;
       try {
           createFunctionsDDL = Files.readAllBytes(Paths.get("tests/testfuncs/org/voltdb_testfuncs/UserDefinedTestAggregates-DDL.sql"));
       } catch (IOException e) {
           fail(e.getMessage());
       }

       final String tableDefinition = " ("
                + "  ID      INTEGER NOT NULL PRIMARY KEY,"
                + "  TINY    TINYINT,"
                + "  SMALL   SMALLINT,"
                + "  INT     INTEGER,"
                + "  BIG     BIGINT,"
                + "  NUM     FLOAT,"
                + "  DEC     DECIMAL,"
                + "  VCHAR_INLINE_MAX VARCHAR(63 BYTES),"
                + "  VCHAR            VARCHAR(64 BYTES),"
                + "  TIME    TIMESTAMP,"
                + "  VARBIN1 VARBINARY(100),"
                + "  VARBIN2 VARBINARY(100),"
                + "  POINT1  GEOGRAPHY_POINT,"
                + "  POINT2  GEOGRAPHY_POINT,"
                + "  POLYGON GEOGRAPHY );\n";
        final String literalSchema = new String(createFunctionsDDL) + "\n"
                + "CREATE TABLE R1" + tableDefinition
                + "CREATE TABLE P1" + tableDefinition
                + "PARTITION TABLE P1 ON COLUMN ID;\n";

        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 2 Local Sites/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        LocalCluster config;

        config = new LocalCluster("tudf-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        /*
        config = new LocalCluster("tudf-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);
        */

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 3-node k=1 cluster
        /////////////////////////////////////////////////////////////


        config = new LocalCluster("tudf-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        return builder;
    }

}
