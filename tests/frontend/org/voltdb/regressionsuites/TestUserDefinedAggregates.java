/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testfuncs.UserDefinedTestFunctions.UDF_TEST;
import org.voltdb_testfuncs.UserDefinedTestFunctions.UserDefinedTestException;

/**
 * Tests of SQL statements that use User-Defined Aggregate Functions (UDAF's).
 */
public class TestUserDefinedAggregates extends TestUserDefinedFunctions {

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
        int expectedTableLength = expected.length;
        Object[][] expectedTable = new Object[1][expectedTableLength];
        for (int i = 0; i < expectedTableLength; ++i) {
            expectedTable[0][i] = expected[i];
        }

        // initialize the client response and client
        ClientResponse cr = null;
        Client client = getClient();

        // INSERT rows into the table that we are using for testing
        String allColumnNames = "ID";
        String allColumnValues = "";
        for (int i = 0; i < columnValues.length; ++i) {
            allColumnNames += ", " + columnNames[i];
        }
        for (int i = 0; i < columnValues[0].length; ++i) {
            allColumnValues = Integer.toString(i);
            for (int j = 0; j < columnValues.length; ++j) {
                allColumnValues += ", " + columnValues[j][i];
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

    protected void testCreateFunctionException(String functionCall, String errorMessage) throws IOException, ProcCallException {
        Client client = getClient();
        try {
            client.callProcedure("@AdHoc", functionCall);
            fail();
        }
        catch (ProcCallException ex) {
            assertEquals(errorMessage, ex.getMessage());
        }
    }

    public void testUavg() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1", "2", "3", "4"}};
        Object[] expected = {2.5D};
        testFunction("uavg(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUavgAndAbs() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1", "-2", "3", "-4"}};
        Object[] expected = {2.5D};
        testFunction("uavg(abs(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUavgAndFloor() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.2", "2.8", "3.3", "4.6"}};
        Object[] expected = {2.5D};
        testFunction("uavg(floor(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUavgTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1", "2", "3", "4"}, {"2", "3", "4", "5"}};
        Object[] expected = {2.5D, 3.5D};
        testFunction("uavg(NUM), uavg(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUavgAddColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1", "2", "3", "4"}, {"2", "3", "4", "5"}};
        Object[] expected = {6D};
        testFunction("uavg(NUM) + uavg(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUCount() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}};
        Object[] expected = {8};
        testFunction("ucount(NUM)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUCountTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}, {"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}};
        Object[] expected = {8, 8};
        testFunction("ucount(NUM), ucount(DEC)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUCountSubtractColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}, {"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}};
        Object[] expected = {0D};
        testFunction("ucount(NUM) - ucount(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUCountThreeColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}, {"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"}};
        Object[] expected = {8, 8, 8};
        testFunction("ucount(NUM), ucount(DEC), count(DEC)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUmax() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.0", "-1000.4", "999"}};
        Object[] expected = {999.0D};
        testFunction("umax(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmaxAndAbs() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.0", "-1000.4", "999"}};
        Object[] expected = {1000.4D};
        testFunction("umax(abs(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmaxTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.0", "-1000.4", "999"}, {"-0.5", "11000.2", "100.4", "999.8", "-1000.4", "998.3"}};
        Object[] expected = {999.0D, 11000.2D};
        testFunction("umax(NUM), umax(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmaxDivideColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.8", "-1000.4", "999"}, {"-0.5", "100.2", "100.4", "999.8", "-1000.4", "998.3"}};
        Object[] expected = {1.0D};
        testFunction("umax(NUM)/umax(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmaxAndCeilingDivideColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"0", "1.1", "100.4", "999.8", "-1000.4", "999"}, {"-0.5", "100.2", "100.4", "1999.8", "-1000.4", "998.3"}};
        Object[] expected = {0.5D};
        testFunction("umax(ceiling(NUM))/umax(ceiling(DEC))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmaxNoSerializable() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION umaxNoSerializable FROM CLASS org.voltdb_testfuncs.UmaxNoSerializable";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"Cannot define a aggregate function without implementing Serializable in the class declaration\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUmedian() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"2", "4", "5", "10"}};
        Object[] expected = {4.5D};
        testFunction("umedian(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmedianTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"2", "4", "5", "10"}, {"1", "10", "5", "5"}};
        Object[] expected = {4.5D, 5.0D};
        testFunction("umedian(NUM), umedian(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmedianAndDivide() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"2", "9", "9", "10"}, {"1", "10", "4", "4"}};
        Object[] expected = {2.25D};
        testFunction("umedian(NUM)/umedian(DEC)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmedianThreeColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM", "DEC"};
        String[][] columnValues = {{"2", "4", "5", "10"}, {"1", "10", "5", "5"}};
        Object[] expected = {4.5D, 5.0D, 21.0D};
        testFunction("umedian(NUM), umedian(DEC), sum(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmedianEndMissing() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION umedianEndMissing FROM CLASS org.voltdb_testfuncs.UmedianEndMissing";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UmedianEndMissing for user-defined aggregate function umedianendmissing, you do not have the correctly formatted method end\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUmedianEndNoReturn() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION umedianEndNoReturn FROM CLASS org.voltdb_testfuncs.UmedianEndNoReturn";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UmedianEndNoReturn for user-defined aggregate function umedianendnoreturn, you do not have the correctly formatted method end\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUmedianEndUnsupportedReturn() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION umedianEndUnsupportedReturn FROM CLASS org.voltdb_testfuncs.UmedianEndUnsupportedReturn";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UmedianEndUnsupportedReturn for user-defined aggregate function umedianendunsupportedreturn, you do not have the correctly formatted method end\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUmedianEndWithParameter() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION umedianEndWithParameter FROM CLASS org.voltdb_testfuncs.UmedianEndWithParameter";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UmedianEndWithParameter for user-defined aggregate function umedianendwithparameter, you do not have the correctly formatted method end\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUmin() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"0.4", "1", "100.3", "999.9", "-1000.0", "999.1"}};
        Object[] expected = {-1000.0};
        testFunction("umin(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUminAndUDF() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"0.4", "1", "100.3", "999.9", "-1000.0", "999.1"}};
        Object[] expected = {1.0D};
        testFunction("add2Float(umin(NUM),1001.0)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUminStartMissing() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION uminStartMissing FROM CLASS org.voltdb_testfuncs.UminStartMissing";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UminStartMissing for user-defined aggregate function uminstartmissing, you do not have the correctly formatted method start\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUminStartWithParameter() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION uminStartWithParameter FROM CLASS org.voltdb_testfuncs.UminStartWithParameter";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UminStartWithParameter for user-defined aggregate function uminstartwithparameter, you do not have the correctly formatted method start\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUminStartWithReturn() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION uminStartWithReturn FROM CLASS org.voltdb_testfuncs.UminStartWithReturn";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UminStartWithReturn for user-defined aggregate function uminstartwithreturn, you do not have the correctly formatted method start\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUmode() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"1", "3", "3", "3", "5", "5", "7"}};
        Object[] expected = {3};
        testFunction("umode(INT)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUmodeTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"INT", "BIG"};
        String[][] columnValues = {{"1", "3", "3", "3", "5", "5", "7"}, {"0", "0", "0", "23", "13", "9", "12"}};
        Object[] expected = {3, 0};
        testFunction("umode(INT), umode(BIG)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUmodeAndUDF() throws IOException, ProcCallException {
        String[] columnNames = {"INT", "BIG"};
        String[][] columnValues = {{"1", "3", "3", "3", "5", "5", "7"}, {"0", "0", "0", "23", "13", "9", "12"}};
        Object[] expected = {3};
        testFunction("add2Integer(umode(INT), umode(BIG))", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUmodeAssembleMissing() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION umodeAssembleMissing FROM CLASS org.voltdb_testfuncs.UmodeAssembleMissing";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UmodeAssembleMissing for user-defined aggregate function umodeassemblemissing, you do not have the correctly formatted method assemble\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUmodeAssembleNoParameter() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION umodeAssembleNoParameter FROM CLASS org.voltdb_testfuncs.UmodeAssembleNoParameter";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UmodeAssembleNoParameter for user-defined aggregate function umodeassemblenoparameter, you do not have the correctly formatted method assemble\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUmodeAssembleUnsupportedParameter() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION umodeAssembleUnsupportedParameter FROM CLASS org.voltdb_testfuncs.UmodeAssembleUnsupportedParameter";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UmodeAssembleUnsupportedParameter for user-defined aggregate function umodeassembleunsupportedparameter, you do not have the correctly formatted method assemble\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUmodeAssembleWithReturn() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION umodeAssembleWithReturn FROM CLASS org.voltdb_testfuncs.UmodeAssembleWithReturn;";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UmodeAssembleWithReturn for user-defined aggregate function umodeassemblewithreturn, you do not have the correctly formatted method assemble\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUprimesum() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"0", "2", "13", "25", "37", "14", "87"}};
        Object[] expected = {52};
        testFunction("uprimesum(INT)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUprimesumTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"INT", "BIG"};
        String[][] columnValues = {{"0", "2", "13", "25", "37", "14", "87"}, {"2", "3", "5", "7", "11", "13", "17"}};
        Object[] expected = {52, 58};
        testFunction("uprimesum(INT), uprimesum(BIG)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUprimesumAndPower() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String[][] columnValues = {{"0", "2", "13", "25", "37", "14", "87"}};
        Object[] expected = {2704D};
        testFunction("power(uprimesum(INT), 2)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUprimesumAndSubtractTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"INT", "BIG"};
        String[][] columnValues = {{"0", "2", "13", "25", "37", "14", "87"}, {"2", "3", "5", "7", "11", "13", "17"}};
        Object[] expected = {-6};
        testFunction("uprimesum(INT) - uprimesum(BIG)", expected, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUprimesumCombineMissing() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION uprimesumCombineMissing FROM CLASS org.voltdb_testfuncs.UprimesumCombineMissing";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UprimesumCombineMissing for user-defined aggregate function uprimesumcombinemissing, you do not have the correctly formatted method combine\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUprimesumCombineNoParameter() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION uprimesumCombineNoParameter FROM CLASS org.voltdb_testfuncs.UprimesumCombineNoParameter";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UprimesumCombineNoParameter for user-defined aggregate function uprimesumcombinenoparameter, you do not have the correctly formatted method combine\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUprimesumCombineWithReturn() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION uprimesumCombineWithReturn FROM CLASS org.voltdb_testfuncs.UprimesumCombineWithReturn";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UprimesumCombineWithReturn for user-defined aggregate function uprimesumcombinewithreturn, you do not have the correctly formatted method combine\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUprimesumCombineWrongParameter() throws IOException, ProcCallException {
        String functionCall = "CREATE AGGREGATE FUNCTION uprimesumCombineWrongParameter FROM CLASS org.voltdb_testfuncs.UprimesumCombineWrongParameter";
        String errorMessage = "[Ad Hoc DDL Input]: VoltDB DDL Error: \"In the class UprimesumCombineWrongParameter for user-defined aggregate function uprimesumcombinewrongparameter, you do not have the correctly formatted method combine\"";
        testCreateFunctionException(functionCall, errorMessage);
    }

    public void testUsum() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.2", "2.5", "-4.6", "6.4", "-5.5"}};
        Object[] expected = {0.0D};
        testFunction("usum(NUM)", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUsumAndSqrt() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.45", "2.5", "-4.6", "6.4", "-5.5"}};
        Object[] expected = {0.5D};
        testFunction("sqrt(usum(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUsumAndSqrtAndCeilingTwoColumn() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[][] columnValues = {{"1.45", "2.5", "-4.6", "6.4", "-5.5"}};
        Object[] expected = {0.5D, 1.0D};
        testFunction("sqrt(usum(NUM)), ceiling(usum(NUM))", expected, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUminOverflow() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[] columnValues = {"-99999999999999999999999999.999999999999, -0.000000000001"};
        testFunctionThrowsException("umin(NUM)", VoltType.FLOAT, RuntimeException.class, columnNames, columnValues);
    }

    public void testUmaxOverflow() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[] columnValues = {"99999999999999999999999999.999999999999, 0.000000000001"};
        testFunctionThrowsException("umax(NUM)", VoltType.FLOAT, RuntimeException.class, columnNames, columnValues);
    }

    public void testUcountNullPointerException() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String exception = Integer.toString(UDF_TEST.THROW_NullPointerException);
        String[] columnValues = {exception, "1.3", "-3.55"};
        testFunctionThrowsException("ucount(NUM)", VoltType.INTEGER, NullPointerException.class, columnNames, columnValues);
    }

    public void testUmedianIllegalArgumentException() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String exception = Integer.toString(UDF_TEST.THROW_IllegalArgumentException);
        String[] columnValues = {exception, "2", "-3"};
        testFunctionThrowsException("umedian(INT)", VoltType.FLOAT, IllegalArgumentException.class, columnNames, columnValues);
    }

    public void testUmodeNumberFormatException() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String exception = Integer.toString(UDF_TEST.THROW_NumberFormatException);
        String[] columnValues = {exception, "4", "-6"};
        testFunctionThrowsException("umode(INT)", VoltType.FLOAT, NumberFormatException.class, columnNames, columnValues);
    }

    public void testUavgArrayIndexOutOfBoundsException() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String exception = Integer.toString(UDF_TEST.THROW_ArrayIndexOutOfBoundsException);
        String[] columnValues = {exception, "2.8", "28"};
        testFunctionThrowsException("uavg(NUM)", VoltType.FLOAT, ArrayIndexOutOfBoundsException.class, columnNames, columnValues);
    }

    public void testUaddstrClassCastException() throws IOException, ProcCallException {
        String[] columnNames = {"VCHAR"};
        String exception = Integer.toString(UDF_TEST.THROW_ClassCastException);
        String[] columnValues = {exception, "'Kawhi'", "'Paul'"};
        testFunctionThrowsException("uaddstr(VCHAR)", VoltType.STRING, ClassCastException.class, columnNames, columnValues);
    }

    public void testUprimesumArithmeticException() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String exception = Integer.toString(UDF_TEST.THROW_ArithmeticException);
        String[] columnValues = {exception, "2", "3"};
        testFunctionThrowsException("uprimesum(INT)", VoltType.INTEGER, ArithmeticException.class, columnNames, columnValues);
    }

    public void testUcountUnsupportedOperationException() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String exception = Integer.toString(UDF_TEST.THROW_UnsupportedOperationException);
        String[] columnValues = {exception, "1.3", "-3.55"};
        testFunctionThrowsException("ucount(NUM)", VoltType.INTEGER, UnsupportedOperationException.class, columnNames, columnValues);
    }

    public void testUmedianVoltTypeException() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String exception = Integer.toString(UDF_TEST.THROW_VoltTypeException);
        String[] columnValues = {exception, "2", "-3"};
        testFunctionThrowsException("umedian(INT)", VoltType.FLOAT, VoltTypeException.class, columnNames, columnValues);
    }

    public void testUmodeUserDefinedTestException() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String exception = Integer.toString(UDF_TEST.THROW_UserDefinedTestException);
        String[] columnValues = {exception, "4", "-6"};
        testFunctionThrowsException("umode(INT)", VoltType.FLOAT, UserDefinedTestException.class, columnNames, columnValues);
    }

    static public Test suite() {
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