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

    @Override
    protected void testFunction(String functionCall, Object expected, VoltType returnType,
            String[] columnNames, String[] columnValues, String tableName)
            throws IOException, ProcCallException {
        // If table not specified, randomly decide which one to test
        if (tableName == null) {
            tableName = "R1";
            if (random.nextInt(100) < 50) {
                tableName = "P1";
            }
        }

        // Set the expected result of the SELECT query using the UDF
        Object[][] expectedTable = new Object[1][1];
        expectedTable[0][0] = expected;

        // initialize the client response and client
        ClientResponse cr = null;
        Client client = getClient();

        // INSERT rows into the table that we are using for testing
        for (int i = 0; i < columnValues.length; ++i) {
            String allColumnNames  = "ID";
            String allColumnValues = Integer.toString(i);
            if (columnNames != null && columnNames.length > 0) {
                allColumnNames += ", " + columnNames[0];
            }
            if (columnValues != null && columnValues.length > 0) {
                allColumnValues += ", " + columnValues[i];
            }
            String insertStatement = "INSERT INTO "+tableName
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

    public void testUaddstr() throws IOException, ProcCallException {
        String[] columnNames = {"VCHAR"};
        String[] columnValues = {"'Westbrook'", "'Ferguson'", "'George'", "'Grant'", "'Adams'"};
        testFunction("uaddstr(VCHAR)", "Westbrook Ferguson George Grant Adams", VoltType.STRING, columnNames, columnValues);
    }

    public void testUavg() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[] columnValues = {"1", "2", "3", "4"};
        testFunction("uavg(NUM)", 2.5D, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUCount() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[] columnValues = {"1.6", "2.2", "3.5", "4", "5", "6.4", "7", "8.0"};
        testFunction("ucount(NUM)", 8, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUmax() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[] columnValues = {"0", "1.1", "100.4", "999.0", "-1000.4", "999"};
        testFunction("umax(NUM)", 999.0D, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmedian() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[] columnValues = {"2", "4", "5", "10"};
        testFunction("umedian(NUM)", 4.5D, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmin() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[] columnValues = {"0.4", "1", "100.3", "999.9", "-1000.0", "999.1"};
        testFunction("umin(NUM)", -1000.0, VoltType.FLOAT, columnNames, columnValues);
    }

    public void testUmode() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String[] columnValues = {"1", "3", "3", "3", "5", "5", "7"};
        testFunction("umode(INT)", 3, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUprimesum() throws IOException, ProcCallException {
        String[] columnNames = {"INT"};
        String[] columnValues = {"-10", "0", "2", "13", "25", "37", "14", "87"};
        testFunction("uprimesum(INT)", 139, VoltType.INTEGER, columnNames, columnValues);
    }

    public void testUsum() throws IOException, ProcCallException {
        String[] columnNames = {"NUM"};
        String[] columnValues = {"1.2", "2.5", "-4.6", "6.4", "-5.5"};
        testFunction("usum(NUM)", 0.0D, VoltType.FLOAT, columnNames, columnValues);
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