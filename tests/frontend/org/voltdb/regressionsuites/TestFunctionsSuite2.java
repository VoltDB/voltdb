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
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.DoubleFunction;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestFunctionsSuite2 extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    // Padding used to purposely exercise non-inline strings.
    private static final String paddedToNonInlineLength =
            "will you still free me (will memcheck see me) when Im sixty-four";

    private static final String literalSchema =
            "CREATE TABLE P1 ( " +
                    "ID INTEGER DEFAULT 0 NOT NULL, " +
                    "DESC VARCHAR(300), " +
                    "NUM INTEGER, " +
                    "RATIO FLOAT, " +
                    "PAST TIMESTAMP DEFAULT NULL, " +
                    "PRIMARY KEY (ID) ); " +

                    "PARTITION TABLE P1 ON COLUMN ID;" +

                    // Test generalized index on a function of a non-indexed column.
                    "CREATE INDEX P1_ABS_NUM ON P1 ( ABS(NUM) ); " +

                    // Test generalized index on an expression of multiple columns.
                    "CREATE INDEX P1_ABS_ID_PLUS_NUM ON P1 ( ABS(ID) + NUM ); " +

                    // Test generalized indexes on a string function and various combos.
                    "CREATE INDEX P1_SUBSTRING_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2) ); " +
                    "CREATE INDEX P1_SUBSTRING_WITH_COL_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), DESC ); " +
                    "CREATE INDEX P1_NUM_EXPR_WITH_STRING_COL ON P1 ( ABS(ID), DESC ); " +
                    "CREATE INDEX P1_MIXED_TYPE_EXPRS1 ON P1 ( ABS(ID+2), SUBSTRING(DESC FROM 1 FOR 2) ); " +
                    "CREATE INDEX P1_MIXED_TYPE_EXPRS2 ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), ABS(ID+2) ); " +

                    "CREATE TABLE R1 ( " +
                    "ID INTEGER DEFAULT 0 NOT NULL, " +
                    "DESC VARCHAR(300), " +
                    "NUM INTEGER, " +
                    "RATIO FLOAT, " +
                    "PAST TIMESTAMP, " +
                    "PRIMARY KEY (ID) ); " +

                    // Test unique generalized index on a function of an already indexed column.
                    "CREATE UNIQUE INDEX R1_ABS_ID_DESC ON R1 ( ABS(ID), DESC ); " +

                    // Test generalized expression index with a constant argument.
                    "CREATE INDEX R1_ABS_ID_SCALED ON R1 ( ID / 3 ); " +

                    //Test generalized expression index with case when.
                    "CREATE INDEX R1_CASEWHEN ON R1 (CASE WHEN num < 3 THEN num/2 ELSE num + 10 END); " +


                    "CREATE TABLE R2 ( " +
                    "ID INTEGER DEFAULT 0 NOT NULL, " +
                    "DESC VARCHAR(300), " +
                    "NUM INTEGER, " +
                    "RATIO FLOAT, " +
                    "PAST TIMESTAMP DEFAULT NULL, " +
                    "PRIMARY KEY (ID) ); " +

                    //Another table that has all numeric types, for testing numeric column functions.
                    "CREATE TABLE NUMBER_TYPES ( " +
                    "INTEGERNUM INTEGER DEFAULT 0 NOT NULL, " +
                    "TINYNUM TINYINT, " +
                    "SMALLNUM SMALLINT, " +
                    "BIGNUM BIGINT, " +
                    "FLOATNUM FLOAT, " +
                    "DECIMALNUM DECIMAL, " +
                    "PRIMARY KEY (INTEGERNUM) );" +

                    "CREATE INDEX NUMBER_TYPES_BITAND_IDX ON NUMBER_TYPES ( bitand(bignum, 3) ); " +

                    "CREATE TABLE R_TIME ( " +
                    "ID INTEGER DEFAULT 0 NOT NULL, " +
                    "C1 INTEGER DEFAULT 2 NOT NULL, " +
                    "T1 TIMESTAMP DEFAULT NULL, " +
                    "T2 TIMESTAMP DEFAULT NOW, " +
                    "T3 TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                    "T4 TIMESTAMP DEFAULT '2012-12-12 12:12:12.121212', " +
                    "PRIMARY KEY (ID) ); " +

                    "CREATE TABLE C_NULL ( " +
                    "ID INTEGER DEFAULT 0 NOT NULL, " +
                    "S1 SMALLINT DEFAULT NULL, " +
                    "I1 INTEGER DEFAULT NULL, " +
                    "F1 FLOAT DEFAULT NULL, " +
                    "D1 DECIMAL DEFAULT NULL, " +
                    "V1 VARCHAR(10) DEFAULT NULL, " +
                    "T1 TIMESTAMP DEFAULT NULL, " +
                    "I2 INTEGER DEFAULT NULL, " +
                    "F2 FLOAT DEFAULT NULL, " +
                    "D2 DECIMAL DEFAULT NULL, " +
                    "V2 VARCHAR(10) DEFAULT NULL, " +
                    "T2 TIMESTAMP DEFAULT NULL, " +
                    "I3 INTEGER DEFAULT NULL, " +
                    "F3 FLOAT DEFAULT NULL, " +
                    "D3 DECIMAL DEFAULT NULL, " +
                    "V3 VARCHAR(10) DEFAULT NULL, " +
                    "T3 TIMESTAMP DEFAULT NULL, " +
                    "PRIMARY KEY (ID) ); " +
                    "PARTITION TABLE C_NULL ON COLUMN ID;" +

                    "CREATE TABLE INLINED_VC_VB_TABLE (" +
                    "ID INTEGER DEFAULT 0 NOT NULL," +
                    "VC1 VARCHAR(6)," +     // inlined
                    "VC2 VARCHAR(16)," +    // not inlined
                    "VB1 VARBINARY(6)," +   // inlined
                    "VB2 VARBINARY(64));" + // not inlined
                    "";
    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestFunctionsSuite2(String name) {
        super(name);
    }

    private static String expectedErrorMessage(String fun) {
        return USING_CALCITE ? String.format("Cannot apply '%s' to arguments of type", fun) :
                "incompatible data type in operation";
    }

    @Test
    public void testMod() throws Exception {
        System.out.println("STARTING testMod");
        Client client = getClient();

        client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (2, '', -10, 2.3, NULL)");

        // integral types
        validateTableOfScalarLongs(client, "select MOD(25,7) from R1", new long[]{4});
        validateTableOfScalarLongs(client, "select MOD(25,-7) from R1", new long[]{4});
        validateTableOfScalarLongs(client, "select MOD(-25,7) from R1", new long[]{-4});
        validateTableOfScalarLongs(client, "select MOD(-25,-7) from R1", new long[]{-4});

        validateTableOfScalarLongs(client, "select MOD(id,7) from R1", new long[]{2});
        validateTableOfScalarLongs(client, "select MOD(id * 4,id+2) from R1", new long[]{0});

        // Edge case: MOD 0
        verifyStmtFails(client, "select MOD(-25,0) from R1", "division by zero");

        validateTableOfScalarDecimals(client,"select MOD(CAST(3.0 as decimal), CAST(2.0 as decimal)) from R1",
                new BigDecimal[]{new BigDecimal("1.000000000000")});
        validateTableOfScalarDecimals(client, "select MOD(CAST(-25.32 as decimal), CAST(ratio as decimal)) from R1",
                new BigDecimal[]{new BigDecimal("-0.020000000000")});

        // Test MOD with NULL values
        validateTableOfScalarDecimals(client, "select MOD(NULL, CAST(ratio as decimal)) from R1", new BigDecimal[]{null});
        validateTableOfScalarDecimals(client, "select MOD(CAST(3.12 as decimal), NULL) from R1", new BigDecimal[]{null});
        validateTableOfScalarDecimals(client, "select MOD(CAST(NULL AS decimal), CAST(ratio as decimal)) from R1", new BigDecimal[]{null});
        verifyStmtFails(client, "select MOD(NULL, NULL) from R1",
                "data type cast needed for parameter or null literal");

        // Mix of decimal and ints
        verifyStmtFails(client, "select MOD(25.32, 2) from R1", expectedErrorMessage("MOD"));
        verifyStmtFails(client, "select MOD(2, 25.32) from R1", expectedErrorMessage("MOD"));
        verifyStmtFails(client, "select MOD('-25.32', 2.5) from R1", expectedErrorMessage("MOD"));
        verifyStmtFails(client, "select MOD(-25.32, ratio) from R1", expectedErrorMessage("MOD"));
    }

    @Test
    public void testUnaryMinus() throws Exception {
        System.out.println("STARTING testUnaryMinus");
        Client client = getClient();

        ClientResponse cr;
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID) );
                // Test generalized indexes on a string function and combos.
        CREATE INDEX P1_SUBSTRING_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2) );
        CREATE INDEX P1_SUBSTRING_WITH_COL_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), DESC );
        CREATE INDEX P1_NUM_EXPR_WITH_STRING_COL ON P1 ( DESC, ABS(ID) );
        CREATE INDEX P1_MIXED_TYPE_EXPRS1 ON P1 ( ABS(ID+2), SUBSTRING(DESC FROM 1 FOR 2) );
        CREATE INDEX P1_MIXED_TYPE_EXPRS2 ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), ABS(ID+2) );
        */

        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (0, 'wEoiXIuJwSIKBujWv', -405636, 1.38145922788945552107e-01, NULL)");

        VoltTable r;
        cr = client.callProcedure("@AdHoc", "SELECT -id from P1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(1, r.getRowCount());

        r.advanceRow();
        // check if unary minus equals 0-value
        // EDGE CASE -0 integer
        assertEquals( 0, r.get("C1", VoltType.INTEGER));

        // invalid data type for unary minus
        verifyStmtFails(client, "select -desc from P1", expectedErrorMessage("-"));

        // check -(-var) = var
        cr = client.callProcedure("@AdHoc", "select num, -(-num) from P1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        assertEquals(r.get("NUM", VoltType.INTEGER), r.get("C2", VoltType.INTEGER));

        // unary minus returns NULL for NULL numeric values like other arithmetic operators
        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (2, 'nulltest', NULL, 1.38145922788945552107e-01, NULL)");
        cr = client.callProcedure("@AdHoc",
                "select -num from P1 where desc='nulltest'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        assertEquals( VoltType.NULL_INTEGER, r.get("C1", VoltType.INTEGER));

        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (3, 'maxvalues', 0, " + Double.MAX_VALUE + " , NULL)");
        cr = client.callProcedure("@AdHoc",
                "select -ratio from P1 where desc='maxvalues'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        // actually returns NULL but when we do r.get("C1", VoltType.FLOAT) it returns more precision
        assertTrue( (Double) r.get("C1", VoltType.FLOAT) <= VoltType.NULL_FLOAT );

        // testing the same behavior for 0-Double.MAX_VALUE
        cr = client.callProcedure("@AdHoc", "select 0-ratio from P1 where desc='maxvalues'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        // actually returns NULL but when we do r.get("C1", VoltType.FLOAT) it returns more precision
        assertTrue( (Double) r.get("C1", VoltType.FLOAT) <= VoltType.NULL_FLOAT );

        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (4, 'minvalues', 0 , " + Double.MIN_VALUE + " , NULL)");

        cr = client.callProcedure("@AdHoc", "select -ratio from P1 where desc='minvalues'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        assertTrue( (Double) r.get("C1", VoltType.FLOAT) <= -Double.MIN_VALUE );

        /*
         * //Another table that has all numeric types, for testing numeric column functions.
                "CREATE TABLE NUMBER_TYPES ( " +
                "INTEGERNUM INTEGER DEFAULT 0 NOT NULL, " +
                "TINYNUM TINYINT, " +
                "SMALLNUM SMALLINT, " +
                "BIGNUM BIGINT, " +
                "FLOATNUM FLOAT, " +
                "DECIMALNUM DECIMAL, " +
                "PRIMARY KEY (INTEGERNUM) );"
         */
        client.callProcedure("NUMBER_TYPES.insert", 1, 2, 3, 4, 1.523, 2.53E09);

        VoltTable rA;
        VoltTable rB;
        cr = client.callProcedure("@AdHoc",
                "select -integernum, -tinynum, -smallnum, -bignum, -floatnum, -decimalnum from NUMBER_TYPES");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        rA = cr.getResults()[0];
        assertEquals(1, rA.getRowCount());

        cr = client.callProcedure("@AdHoc",
                "select 0-integernum, 0-tinynum, 0-smallnum, 0-bignum, 0-floatnum, 0-decimalnum from NUMBER_TYPES");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        rB = cr.getResults()[0];
        assertEquals(1, rB.getRowCount());

        rA.advanceRow();
        rB.advanceRow();
        // check if unary minus equals 0-value
        assertEquals( rA.get( "C1", VoltType.INTEGER), rB.get( "C1", VoltType.INTEGER ));
        assertEquals( rA.get( "C2", VoltType.TINYINT), rB.get( "C2", VoltType.TINYINT ));
        assertEquals( rA.get( "C3", VoltType.SMALLINT), rB.get( "C3", VoltType.SMALLINT ));
        assertEquals( rA.get( "C4", VoltType.BIGINT), rB.get( "C4", VoltType.BIGINT ));
        assertEquals( rA.get( "C5", VoltType.FLOAT), rB.get( "C5", VoltType.FLOAT ));
        assertEquals( rA.get( "C6", VoltType.DECIMAL), rB.get( "C6", VoltType.DECIMAL ));

        client.callProcedure("@AdHoc", "delete from NUMBER_TYPES where INTEGERNUM = 1");
        client.callProcedure("NUMBER_TYPES.insert", Integer.MAX_VALUE, Byte.MAX_VALUE, Short.MAX_VALUE,
                Long.MAX_VALUE, 0, 0);

        String sql = "select -integernum, -tinynum, -smallnum, -bignum, -floatnum, -decimalnum from NUMBER_TYPES;";
        validateTableOfLongs(client, sql, new long[][]{{ -Integer.MAX_VALUE, -Byte.MAX_VALUE,
                -Short.MAX_VALUE, -Long.MAX_VALUE, 0, 0 }});

        client.callProcedure("@AdHoc",
                "delete from NUMBER_TYPES where INTEGERNUM = " + Integer.MAX_VALUE);
        //client.callProcedure("NUMBER_TYPES.insert", 1, 2, 3, 4, 5.0, -99999999999999999999999999.999999999999);
        client.callProcedure("@AdHoc",
                "Insert into NUMBER_TYPES values(1, 2, 3, 4, 5.0, -99999999999999999999999999.999999999999);");

        cr = client.callProcedure("@AdHoc", "select -decimalnum from NUMBER_TYPES");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.out.println(r);
        assertEquals(1, r.getRowCount());
        r.advanceRow();
        // Java converts big numbers - so comparing strings which have the values preserved
        assertEquals( "99999999999999999999999999.999999999999",
                r.get("C1", VoltType.DECIMAL).toString());
    }

    // Test some false alarm cases in HSQLBackend that were interfering with sqlcoverage.
    @Test
    public void testFoundHSQLBackendOutOfRange() throws IOException, ProcCallException {
        System.out.println("STARTING testFoundHSQLBackendOutOfRange");
        Client client = getClient();
        ClientResponse cr = null;
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID) );
        */

        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (0, 'wEoiXIuJwSIKBujWv', -405636, 1.38145922788945552107e-01, NULL)");
        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (2, 'wEoiXIuJwSIKBujWv', -29914, 8.98500019539639316335e-01, NULL)");
        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (4, 'WCfDDvZBPoqhanfGN', -1309657, 9.34160160574919795629e-01, NULL)");
        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (6, 'WCfDDvZBPoqhanfGN', 1414568, 1.14383710279231887164e-01, NULL)");
        cr = client.callProcedure("@AdHoc","select (5.25 + NUM) from P1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc", "SELECT FLOOR(NUM + 5.25) NUMSUM FROM P1 ORDER BY NUMSUM");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // This test case requires HSQL to be taught to do (truncating) integer division of integers as VoltDB does.
        // While not strictly required by the SQL standard, integer division is at least technically compliant,
        // where HSQL's use of floating point division is not.
        // cr = client.callProcedure("@AdHoc", "SELECT SUM(DISTINCT SQRT(ID / (NUM))) AS Q22 FROM P1");
        // assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    @Test
    public void testStringExpressionIndex() throws Exception {
        System.out.println("STARTING testStringExpressionIndex");
        Client client = getClient();
        initialLoad(client, "P1");

        ClientResponse cr;
        VoltTable result;
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID) );
                // Test generalized indexes on a string function and combos.
        CREATE INDEX P1_SUBSTRING_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2) );
        CREATE INDEX P1_SUBSTRING_WITH_COL_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), DESC );
        CREATE INDEX P1_NUM_EXPR_WITH_STRING_COL ON P1 ( DESC, ABS(ID) );
        CREATE INDEX P1_MIXED_TYPE_EXPRS1 ON P1 ( ABS(ID+2), SUBSTRING(DESC FROM 1 FOR 2) );
        CREATE INDEX P1_MIXED_TYPE_EXPRS2 ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), ABS(ID+2) );
        */

        // Do some rudimentary indexed queries -- the real challenge to string expression indexes is defining and loading/maintaining them.
        // TODO: For that reason, it might make sense to break them out into their own suite to make their specific issues easier to isolate.
        cr = client.callProcedure("@AdHoc",
                "select ID from P1 where SUBSTRING(DESC FROM 1 for 2) = 'X1' and ABS(ID+2) > 7 order by NUM, ID");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(5, result.getRowCount());

        VoltTable r;
        long resultA;
        long resultB;

        // Filters intended to be close enough to bring two different indexes to the same result as no index at all.
        cr = client.callProcedure("@AdHoc","select count(*) from P1 where ABS(ID+3) = 7");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc",
                "select count(*) from P1 where SUBSTRING(DESC FROM 1 for 2) >= 'X1' and ABS(ID+2) = 8");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc",
                "select count(*) from P1 where SUBSTRING(DESC FROM 1 for 2) = 'X1' and ABS(ID+2) > 7 and ABS(ID+2) < 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        // Do some updates intended to be non-corrupting and inconsequential to the test query results.
        cr = client.callProcedure("@AdHoc", "delete from P1 where ABS(ID+3) <> 7");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        long killCount = r.asScalarLong();
        assertEquals(7, killCount);

        // Repeat the queries on updated indexes.
        cr = client.callProcedure("@AdHoc",
                "select count(*) from P1 where SUBSTRING(DESC FROM 1 for 2) >= 'X1' and ABS(ID+2) = 8");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc",
                "select count(*) from P1 where SUBSTRING(DESC FROM 1 for 2) = 'X1' and ABS(ID+2) > 7 and ABS(ID+2) < 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

    }

    @Test
    public void testNumericExpressionIndex() throws Exception {
        System.out.println("STARTING testNumericExpressionIndex");
        Client client = getClient();
        initialLoad(client, "R1");

        ClientResponse cr;
        VoltTable result;
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID) );
        // Test generalized index on a function of a non-indexed column.
        CREATE INDEX P1_ABS_NUM ON P1 ( ABS(NUM) );
        // Test generalized index on an expression of multiple columns.
        CREATE INDEX P1_ABS_ID_PLUS_NUM ON P1 ( ABS(ID) + NUM );
        // Test generalized index on a string function.
        // CREATE INDEX P1_SUBSTRING_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2) );
        CREATE TABLE R1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID) );
        // Test unique generalized index on a function of an already indexed column.
        CREATE UNIQUE INDEX R1_ABS_ID ON R1 ( ABS(ID) );
        // Test generalized expression index with a constant argument.
        CREATE INDEX R1_ABS_ID_SCALED ON R1 ( ID / 3 );
        */

        cr = client.callProcedure("@AdHoc",
                "select ID from R1 where ABS(ID) = 9 and DESC > 'XYZ' order by ID");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(0, result.getRowCount());


        cr = client.callProcedure("@AdHoc", "select ID from R1 where ABS(ID) > 9 order by NUM, ID");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(5, result.getRowCount());

        VoltTable r;
        long resultA;
        long resultB;

        cr = client.callProcedure("@AdHoc", "select count(*) from R1 where (ID+ID) / 6 = -3");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        // Here's some hard-won functionality -- matching expression indexes with only the right constants in them.
        cr = client.callProcedure("@AdHoc", "select count(*) from R1 where ID / 3 = -3");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from R1 where (ID+ID) / 6 = -2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        // Expecting to use the cached index plan and still get a correct result.
        cr = client.callProcedure("@AdHoc", "select count(*) from R1 where ID / 3 = -2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from R1 where (ID+ID) / 4 = -3");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        // Not expecting to use the index -- that's the whole point.
        cr = client.callProcedure("@AdHoc", "select count(*) from R1 where ID / 2 = -3");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

    }

    @Test
    public void testAbsWithLimit_ENG3572() throws Exception {
        System.out.println("STARTING testAbsWithLimit_ENG3572");
        Client client = getClient();
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID)
                );
        */
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", "select abs(NUM) from P1 where ID = 0 limit 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void initialLoad(Client client, String tableName) throws IOException, InterruptedException {
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException("Failed with response: " + clientResponse.getStatusString());
                }
            }
        };

        /*
        CREATE TABLE ??? (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID)
                );
        */
        for(int id=7; id < 15; id++) {
            client.callProcedure(callback, tableName+".insert",
                    - id, // ID
                    "X"+ id +paddedToNonInlineLength, // DESC
                    10, // NUM
                    1.1, // RATIO
                    new Timestamp(100000000L)); // PAST
            client.drain();
        }
    }

    @Test
    public void testAbs() throws Exception {
        System.out.println("STARTING testAbs");
        Client client = getClient();
        initialLoad(client, "P1");

        ClientResponse cr;
        VoltTable r;

        // The next two queries used to fail due to ENG-3913,
        // abuse of compound indexes for partial GT filters.
        // An old issue only brought to light by the addition of a compound index to this suite.
        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(ID) > 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong()); // used to get 6, matching like >=

        initialLoad(client, "R1");

        cr = client.callProcedure("WHERE_ABS");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong()); // used to get 6, matching like >=

        try {
            // test decimal support and non-column expressions
            cr = client.callProcedure("WHERE_ABSFF");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            r = cr.getResults()[0];
            assertEquals(5, r.asScalarLong());
        } catch (ProcCallException hsqlFailed) {
            // Give HSQLDB a pass on this query.
            String msg = hsqlFailed.getMessage();
            assertTrue(msg.matches(".*ExpectedProcedureException.*HSQLDB.*"));
        }

        // Test type promotions
        cr = client.callProcedure("WHERE_ABSIF");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

        try {
            cr = client.callProcedure("WHERE_ABSFI");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            r = cr.getResults()[0];
            assertEquals(5, r.asScalarLong());
        } catch (ProcCallException hsqlFailed) {
            // Give HSQLDB a pass on this query.
            String msg = hsqlFailed.getMessage();
            assertTrue(msg.matches(".*ExpectedProcedureException.*HSQLDB.*"));
        }


        // Test application to weakly typed NUMERIC constants
        try {
            cr = client.callProcedure("WHERE_ABSWEAK");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            r = cr.getResults()[0];
            assertEquals(5, r.asScalarLong());
        } catch (ProcCallException hsqlFailed) {
            // Give HSQLDB a pass on this query.
            String msg = hsqlFailed.getMessage();
            assertTrue(msg.matches(".*ExpectedProcedureException.*HSQLDB.*"));
        }

        cr = client.callProcedure("DISPLAY_ABS");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

        cr = client.callProcedure("ORDER_ABS");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        long value = r.getLong(0);
        assertEquals(5, value);
/*
        cr = client.callProcedure("GROUP_ABS");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());
*/
        cr = client.callProcedure("AGG_OF_ABS");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());
/*
        cr = client.callProcedure("ABS_OF_AGG");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());
*/

        initialLoad(client, "R1");

        initialLoad(client, "R2");

        // The next 2 queries failed in 3.4 with a runtime type exception about casting from VARCHAR reported in ENG-5004
        cr = client.callProcedure("@AdHoc", "select * from P1, R2 where P1.ID = R2.ID AND ABS(P1.NUM) > 0");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.out.println(r);
        assertEquals(8, r.getRowCount());

        cr = client.callProcedure("@AdHoc", "select * from P1, R2 where P1.ID = R2.ID AND ABS(P1.NUM+0) > 0");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.out.println(r);
        assertEquals(8, r.getRowCount());

        // These next queries fail in 3.5 with a runtime type exception about unrecognized type related?/similar? to ENG-5004?
        cr = client.callProcedure("@AdHoc", "select count(*) from P1, R2 where P1.ID = R2.ID AND ABS(R2.NUM+0) > 0");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.out.println(r);
        assertEquals(8, r.asScalarLong());

        cr = client.callProcedure("@AdHoc", "select count(*) from P1, R2 where P1.ID = R2.ID AND ABS(R2.NUM) > 0");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.out.println(r);
        assertEquals(8, r.asScalarLong());

        // Test null propagation
        cr = client.callProcedure("INSERT_NULL", 99);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 98);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 97);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 96);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 95);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        long resultA;
        long resultB;

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where NUM > 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(NUM) > 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(0-NUM) > 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not NUM > 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(0-NUM) > 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(NUM) > 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ID = -2 - NUM");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        // These cases were originally failed attempts to trigger ENG-3191, but they still seem worth trying.
        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(ID) = 2 + NUM");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(NUM) = (2 - ID)");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ID < 0");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(ID) = (0 - ID)");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        // Here's the ENG-3191 case, all better now.
        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ID = (0 - ABS(ID))");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        // Here's the ENG-3196 case, all better now
        cr = client.callProcedure("@AdHoc", "SELECT ABS(ID) AS ENG3196 FROM R1 ORDER BY (ID) LIMIT 5;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.out.println("DEBUG ENG-3196: " + r);
        long resultCount = r.getRowCount();
        assertEquals(5, resultCount);
        r.advanceRow();
        resultB = r.getLong(0);
        assertEquals(14, resultB);
        r.advanceToRow(4);
        resultB = r.getLong(0);
        assertEquals(10, resultB);
        final String expectedErrorMessage = expectedErrorMessage("ABS");

        boolean caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(DESC) > 9");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains(expectedErrorMessage));
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(DESC) > 'ABC'");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains(expectedErrorMessage));
            caught = true;
        }
        assertTrue(caught);

        client.callProcedure("@AdHoc", "insert into R1 values (1, null, null, null, null)");

        caught = false;
        try {
            // This should violate the UNIQUE ABS constraint without violating the primary key constraint.
            client.callProcedure("@AdHoc", "insert into R1 values (-1, null, null, null, null)");
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("violation of constraint"));
            caught = true;
        }
        // If the insert succeeds on VoltDB, the constraint failed to trigger.
        // If the insert fails on HSQL, the test is invalid -- HSQL should not detect the subtle constraint violation we are trying to trigger.
        assertEquals(! isHSQL(), caught);
    }

    @Test
    public void testSubstring() throws Exception {
        System.out.println("STARTING testSubstring");
        Client client = getClient();
        initialLoad(client, "P1");

        ClientResponse cr;
        VoltTable r;

        // test where support
        cr = client.callProcedure("WHERE_SUBSTRING2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

        // Test that commas work just like keyword separators
        cr = client.callProcedure("ALT_WHERE_SUBSTRING2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

        cr = client.callProcedure("WHERE_SUBSTRING3");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

        // Test that commas work just like keyword separators
        cr = client.callProcedure("ALT_WHERE_SUBSTRING3");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

        // Test select support
        cr = client.callProcedure("DISPLAY_SUBSTRING");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        assertEquals("12"+paddedToNonInlineLength, r.getString(0));

        cr = client.callProcedure("DISPLAY_SUBSTRING2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        assertEquals("12", r.getString(0));

        // Test ORDER BY by support
        cr = client.callProcedure("ORDER_SUBSTRING");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        long value = r.getLong(0);
        assertEquals(5, value);

        // Test GROUP BY by support
        cr = client.callProcedure("AGG_OF_SUBSTRING");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        assertEquals("10"+paddedToNonInlineLength, r.getString(0));

        cr = client.callProcedure("AGG_OF_SUBSTRING2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        assertEquals("10", r.getString(0));

        // Test null propagation
        cr = client.callProcedure("INSERT_NULL", 99);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 98);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 97);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 96);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 95);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        long resultA;
        long resultB;

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where DESC >= 'X11'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc",
                "select count(*) from P1 where SUBSTRING (DESC FROM 2) >= '11'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not DESC >= 'X12'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc",
                "select count(*) from P1 where not SUBSTRING( DESC FROM 2) >= '12'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where DESC >= 'X2'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc",
                "select count(*) from P1 where not SUBSTRING( SUBSTRING (DESC FROM 2) FROM 1 FOR 1) < '2'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        boolean caught = false;
        try {
            cr = client.callProcedure("@AdHoc",
                    "select count(*) from P1 where not SUBSTRING( DESC FROM 2) > 9");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("incompatible data type"));
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        final String expectedErrorMessage = expectedErrorMessage("SUBSTRING");
        try {
            cr = client.callProcedure("@AdHoc",
                    "select count(*) from P1 where not SUBSTRING (1 FROM 2) > 9");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains(expectedErrorMessage));
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc",
                    "select count(*) from P1 where not SUBSTRING (1 FROM DESC) > '9'");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains(expectedErrorMessage));
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc",
                    "select count(*) from P1 where not SUBSTRING (DESC FROM DESC) > 'ABC'");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains(expectedErrorMessage));
            caught = true;
        }
        assertTrue(caught);

    }

    @Test
    public void testCurrentTimestamp() throws Exception {
        System.out.println("STARTING testCurrentTimestamp");
        /**
         *      "CREATE TABLE R_TIME ( " +
         "ID INTEGER DEFAULT 0 NOT NULL, " +
         "C1 INTEGER DEFAULT 2 NOT NULL, " +
         "T1 TIMESTAMP DEFAULT NULL, " +
         "T2 TIMESTAMP DEFAULT NOW, " +
         "T3 TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
         "PRIMARY KEY (ID) ); " +
         */
        Client client = getClient();
        ClientResponse cr;
        VoltTable vt;
        Date before;
        Date after;

        // Test Default value with functions.
        before = new Date();
        cr = client.callProcedure("@AdHoc", "Insert into R_TIME (ID) VALUES(1);");
        after = new Date();
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = client.callProcedure("@AdHoc", "SELECT C1, T1, T2, T3 FROM R_TIME WHERE ID = 1;").getResults()[0];
        assertTrue(vt.advanceRow());

        assertEquals(2, vt.getLong(0));
        // Test NULL

        long t2FirstRow = vt.getTimestampAsLong(2);
        long t3FirstRow = vt.getTimestampAsLong(3);

        assertTrue(after.getTime()*1000 >= t2FirstRow);
        assertTrue(before.getTime()*1000 <= t2FirstRow);
        assertEquals(t2FirstRow, t3FirstRow);

        // execute the same insert again, to assert that we get a newer timestamp
        // even if we are re-using the same plan (ENG-6755)

        // sleep a quarter of a second just to be certain we get a different timestamp
        Thread.sleep(250);

        cr = client.callProcedure("@AdHoc", "Insert into R_TIME (ID) VALUES(2);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = client.callProcedure("@AdHoc", "SELECT C1, T1, T2, T3 FROM R_TIME WHERE ID = 2;").getResults()[0];
        assertTrue(vt.advanceRow());
        long t2SecondRow = vt.getTimestampAsLong(2);
        assertTrue(t2FirstRow < t2SecondRow);

        before = new Date();
        vt = client.callProcedure("@AdHoc", "SELECT NOW, CURRENT_TIMESTAMP FROM R_TIME;").getResults()[0];
        after = new Date();
        assertTrue(vt.advanceRow());

        assertTrue(after.getTime()*1000 >= vt.getTimestampAsLong(0));
        assertTrue(before.getTime()*1000 <= vt.getTimestampAsLong(0));
        assertEquals(vt.getTimestampAsLong(0), vt.getTimestampAsLong(1));
    }

    @Test
    public void testTimestampConversions() throws Exception {
        Client client = getClient();
        ClientResponse cr;
        VoltTable r;

        // Giving up on hsql testing until timestamp precision behavior can be normalized.
        if ( ! isHSQL()) {
            System.out.println("STARTING test CAST between string and timestamp.");
            /*
            CREATE TABLE R2 (
                    ID INTEGER DEFAULT '0' NOT NULL,
                    DESC VARCHAR(300),
                    NUM INTEGER,
                    RATIO FLOAT,
                    PAST TIMESTAMP DEFAULT NULL,
                    PRIMARY KEY (ID) );
            */
            String strTime;
            // Normal test case 2001-9-9 01:46:40.789000
            cr = client.callProcedure("R2.insert", 1, "2001-09-09 01:46:40.789000", 10, 1.1, new Timestamp(1000000000789L));
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            strTime = "2001-10-30 21:46:40.000789";
            cr = client.callProcedure("R2.insert", 2, strTime, 12, 1.1, strTime);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            strTime = "1601-01-01 00:00:00.000789";
            cr = client.callProcedure("R2.insert", 3, strTime, 13, 1.1, strTime);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            strTime = "2013-12-31 23:59:59.999999";
            cr = client.callProcedure("R2.insert", 4, strTime, 14, 1.1, strTime);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            // test only given date
            strTime = "2014-07-02";
            cr = client.callProcedure("R2.insert", 5, strTime + " 00:00:00.000000", 15, 1.1, strTime);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            strTime = "2014-07-03";
            cr = client.callProcedure("R2.insert", 6, strTime, 16, 1.1, strTime +" 00:00:00.000000");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            strTime = "2014-07-04";
            cr = client.callProcedure("R2.insert", 7, strTime, 17, 1.1, strTime);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            // test AdHoc cast
            cr = client.callProcedure("@AdHoc",
                    "select cast('2014-07-04 00:00:00.000000' as timestamp) from R2 where id = 1;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            r = cr.getResults()[0];
            r.advanceRow();
            assertEquals(r.getTimestampAsTimestamp(0).toString(), "2014-07-04 00:00:00.000000");
            cr = client.callProcedure("@AdHoc",
                    "select cast('2014-07-05' as timestamp) from R2 where id = 1;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            r = cr.getResults()[0];
            r.advanceRow();
            assertEquals(r.getTimestampAsTimestamp(0).toString(), "2014-07-05 00:00:00.000000");

            cr = client.callProcedure("VERIFY_TIMESTAMP_STRING_EQ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            r = cr.getResults()[0];
            if (r.getRowCount() != 0) {
                System.out.println("VERIFY_TIMESTAMP_STRING_EQ failed on " + r.getRowCount() + " rows:");
                System.out.println(r.toString());
                fail("VERIFY_TIMESTAMP_STRING_EQ failed on " + r.getRowCount() + " rows");
            }

            cr = client.callProcedure("VERIFY_STRING_TIMESTAMP_EQ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            r = cr.getResults()[0];
            // there should be 2 rows wrong, because the cast always return a long format string, but we
            // have two rows containing short format strings
            if (r.getRowCount() != 2) {
                System.out.println("VERIFY_STRING_TIMESTAMP_EQ failed on " + r.getRowCount() +
                        " rows, where only 2 were expected:");
                System.out.println(r.toString());
                fail("VERIFY_TIMESTAMP_STRING_EQ failed on " + r.getRowCount() +
                        " rows, where only 2 were expected:");
            }

            cr = client.callProcedure("DUMP_TIMESTAMP_STRING_PATHS");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            r = cr.getResults()[0];
            System.out.println(r);
        }

        System.out.println("STARTING test Extract");
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID)
                );
        */
        // Test Null timestamp
//        cr = client.callProcedure("P1.insert", 0, "X0", 10, 1.1, null);
//        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
//        cr = client.callProcedure("EXTRACT_TIMESTAMP", 0);
//        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
//        r = cr.getResults()[0];
//        r.advanceRow();
//        assertNull(r.get(8, VoltType.TIMESTAMP));


        subtestExtract(client, "EXTRACT_TIMESTAMP");
        // Test that commas work just like keyword separators
        subtestExtract(client, "ALT_EXTRACT_TIMESTAMP");
    }

    @Test
    void subtestExtract(Client client, String extractProc) throws IOException, ProcCallException {
        ClientResponse cr;
        VoltTable r;
        long result;
        String extractFailed = extractProc + " got a wrong answer";

        cr = client.callProcedure("@AdHoc", "TRUNCATE TABLE P1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Normal test case 2001-9-9 01:46:40
        cr = client.callProcedure("P1.insert", 1, "X0", 10, 1.1, new Timestamp(1000000000789L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(extractProc, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        int columnIndex = 0;

        int EXPECTED_YEAR = 2001;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_YEAR, result);

        int EXPECTED_MONTH = 9;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_MONTH, result);

        int EXPECTED_DAY = 9;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DAY, result);

        int EXPECTED_DOW = 1;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOW, result);

        int EXPECTED_DOM = EXPECTED_DAY;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOM, result);

        int EXPECTED_DOY = 252;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOY, result);

        int EXPECTED_QUARTER = 3;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_QUARTER, result);

        int EXPECTED_HOUR = 1;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_HOUR, result);

        int EXPECTED_MINUTE = 46;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_MINUTE, result);

        BigDecimal EXPECTED_SECONDS = new BigDecimal("40.789000000000");
        BigDecimal decimalResult = r.getDecimalAsBigDecimal(columnIndex++);
        assertEquals(extractFailed, EXPECTED_SECONDS, decimalResult);

        // ISO 8601 regards Sunday as the last day of a week
        int EXPECTED_WEEK = 36;
        if (isHSQL()) {
            // hsql get answer 37, because it believes a week starts with Sunday
            EXPECTED_WEEK = 37;
        }
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_WEEK, result);
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_WEEK, result);

        // VoltDB has a special function to handle WEEKDAY, and it is not the same as DAY_OF_WEEK
        int EXPECTED_WEEKDAY = 6;
        if (isHSQL()) {
            // We map WEEKDAY keyword to DAY_OF_WEEK in hsql parser
            EXPECTED_WEEKDAY = EXPECTED_DOW;
        }
        result = r.getLong(columnIndex);
        assertEquals(extractFailed, EXPECTED_WEEKDAY, result);

        // test timestamp before epoch, Human time (GMT): Thu, 18 Nov 1948 16:32:02 GMT
        // Leap year!
        // http://disc.gsfc.nasa.gov/julian_calendar.shtml
        cr = client.callProcedure("P1.insert", 2, "X0", 10, 1.1, new Timestamp(-666430077123L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(extractProc, 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        columnIndex = 0;

        EXPECTED_YEAR = 1948;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_YEAR, result);

        EXPECTED_MONTH = 11;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_MONTH, result);

        EXPECTED_DAY = 18;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DAY, result);

        EXPECTED_DOW = 5;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOW, result);

        EXPECTED_DOM = EXPECTED_DAY;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOM, result);

        EXPECTED_DOY = 323;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOY, result);

        EXPECTED_QUARTER = 4;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_QUARTER, result);

        EXPECTED_HOUR = 16;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_HOUR, result);

        EXPECTED_MINUTE = 32;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_MINUTE, result);

        EXPECTED_SECONDS = new BigDecimal("2.877000000000");
        decimalResult = r.getDecimalAsBigDecimal(columnIndex++);
        assertEquals(extractFailed, EXPECTED_SECONDS, decimalResult);

        EXPECTED_WEEK = 47;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_WEEK, result);
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_WEEK, result);

        // VoltDB has a special function to handle WEEKDAY, and it is not the same as DAY_OF_WEEK
        EXPECTED_WEEKDAY = 3;
        if (isHSQL()) {
            // We map WEEKDAY keyword to DAY_OF_WEEK in hsql parser
            EXPECTED_WEEKDAY = EXPECTED_DOW;
        }
        result = r.getLong(columnIndex);
        assertEquals(extractFailed, EXPECTED_WEEKDAY, result);

        // test timestamp with a very old date, Human time (GMT): Fri, 05 Jul 1658 14:22:27 GMT
        cr = client.callProcedure("P1.insert", 3, "X0", 10, 1.1, new Timestamp(-9829676252456L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(extractProc, 3);
        assertEquals(extractFailed, ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        columnIndex = 0;

        EXPECTED_YEAR = 1658;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_YEAR, result);

        EXPECTED_MONTH = 7;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_MONTH, result);

        EXPECTED_DAY = 5;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DAY, result);

        EXPECTED_DOW = 6;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOW, result);

        EXPECTED_DOM = EXPECTED_DAY;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOM, result);

        EXPECTED_DOY = 186;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOY, result);

        EXPECTED_QUARTER = 3;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_QUARTER, result);

        EXPECTED_HOUR = 14;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_HOUR, result);

        EXPECTED_MINUTE = 22;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_MINUTE, result);

        EXPECTED_SECONDS = new BigDecimal("27.544000000000");
        decimalResult = r.getDecimalAsBigDecimal(columnIndex++);
        assertEquals(extractFailed, EXPECTED_SECONDS, decimalResult);

        EXPECTED_WEEK = 27;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_WEEK, result);
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_WEEK, result);

        // VoltDB has a special function to handle WEEKDAY, and it is not the same as DAY_OF_WEEK
        EXPECTED_WEEKDAY = 4;
        if (isHSQL()) {
            // We map WEEKDAY keyword to DAY_OF_WEEK in hsql parser
            EXPECTED_WEEKDAY = EXPECTED_DOW;
        }
        result = r.getLong(columnIndex);
        assertEquals(extractFailed, EXPECTED_WEEKDAY, result);

        // Move in this testcase of quickfix-extract(), Human time (GMT): Mon, 02 Jul 1956 12:53:37 GMT
        cr = client.callProcedure("P1.insert", 4, "X0", 10, 1.1, new Timestamp(-425991982877L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("EXTRACT_TIMESTAMP", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        columnIndex = 0;
        //System.out.println("Result: " + r);

        EXPECTED_YEAR = 1956;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_YEAR, result);

        EXPECTED_MONTH = 7;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_MONTH, result);

        EXPECTED_DAY = 2;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DAY, result);

        EXPECTED_DOW = 2;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOW, result);

        EXPECTED_DOM = EXPECTED_DAY;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOM, result);

        EXPECTED_DOY = 184;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_DOY, result);

        EXPECTED_QUARTER = 3;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_QUARTER, result);

        EXPECTED_HOUR = 12;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_HOUR, result);

        EXPECTED_MINUTE = 53;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_MINUTE, result);

        EXPECTED_SECONDS = new BigDecimal("37.123000000000");
        decimalResult = r.getDecimalAsBigDecimal(columnIndex++);
        assertEquals(extractFailed, EXPECTED_SECONDS, decimalResult);

        EXPECTED_WEEK = 27;
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_WEEK, result);
        result = r.getLong(columnIndex++);
        assertEquals(extractFailed, EXPECTED_WEEK, result);

        // VoltDB has a special function to handle WEEKDAY, and it is not the same as DAY_OF_WEEK
        EXPECTED_WEEKDAY = 0;
        if (isHSQL()) {
            // We map WEEKDAY keyword to DAY_OF_WEEK in hsql parser
            EXPECTED_WEEKDAY = EXPECTED_DOW;
        }
        result = r.getLong(columnIndex);
        assertEquals(extractFailed, EXPECTED_WEEKDAY, result);
    }

    @Test
    public void testParams() throws IOException, ProcCallException {
        System.out.println("STARTING testParams");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "foo", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // next one disabled until ENG-3486
        /*cr = client.callProcedure("PARAM_SUBSTRING", "eeoo");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertEquals("eoo", result.fetchRow(0).getString(0));*/

        cr = client.callProcedure("PARAM_ABS", -2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertEquals(1, result.asScalarLong());
    }

    // NOTE: Avoid trouble by keeping values in order, half-negative, then zero, then half-positive.
    private static final double orderedIds[] = { -125, -25, 0, 25, 125 };
    private static final int ROWCOUNT = orderedIds.length;

    private static final double orderedByStringIds[] = { -125, -25, 0, 125, 25 };
    private static final double orderedByFloatStringIds[] = { -125, -25, 0, 125, 25 };
    private static final double orderedByDecimalStringIds[] = { -25, -125, 0, 25, 125  };

    // NOTE: Be careful about how this array may be indexed by hard-coded column numbers sprinkled through the code
    // -- and especially keep the non-integer columns last.
    // These correspond to the column types of table NUMBER_TYPES, in the order that they are defined and typically selected.
    private static String numTypeNames[] = { "INTEGER", "TINYINT", "SMALLINT", "BIGINT", "FLOAT", "DECIMAL" };
    private static String numFormatNames[] = { "LONG",  "LONG", "LONG", "LONG", "DOUBLE", "DECIMAL" };
    private static final int COLUMNCOUNT = numTypeNames.length;
    private static final int FLOATCOLINDEX = 4;
    private static final int DECIMALCOLINDEX = 5;

    private static final int VALUECOUNT = ROWCOUNT * COLUMNCOUNT;
    private static final double values[] = new double[VALUECOUNT];
    private static final int NONNEGCOUNT = ((ROWCOUNT + 1) / 2) * COLUMNCOUNT;
    private static final double nonnegs[] = new double[NONNEGCOUNT];
    private static final int POSCOUNT = ((ROWCOUNT - 1) / 2) * COLUMNCOUNT;
    private static final double nonnegnonzeros[] = new double[POSCOUNT];
    static {
        assert(numTypeNames[FLOATCOLINDEX].equals("FLOAT"));
        assert(numTypeNames[DECIMALCOLINDEX].equals("DECIMAL"));
        int kk = 0;
        int nn = 0;
        int pp = 0;
        for (int jj = 0; jj < ROWCOUNT; ++jj) {
            for (int ii = 0; ii < COLUMNCOUNT; ++ii) {
                double rawValue = orderedIds[jj] * ((ii < FLOATCOLINDEX) ? 1.0 : 0.01);
                values[kk++] = rawValue;
                if (rawValue >= 0.0) {
                    nonnegs[nn++] = rawValue;
                    if (rawValue > 0.0) {
                        nonnegnonzeros[pp++] = rawValue;
                    }
                }
            }
        }
        assert(NONNEGCOUNT == nn);
        assert(POSCOUNT == pp);
    }

    private static void insertNumbers(Client client, double[] rawData, int nRaws) throws Exception {
        client.callProcedure("@AdHoc", "delete from NUMBER_TYPES;");
        // Insert non-negative or all input values with and without whole number and fractional parts.
        for (int kk = 0; kk < nRaws; kk += COLUMNCOUNT) {
            client.callProcedure("NUMBER_TYPES.insert", (int)rawData[kk], (int)rawData[kk+1],
                    (int)rawData[kk+2], (int)rawData[kk+3],
                    rawData[kk+FLOATCOLINDEX], String.valueOf(rawData[kk+DECIMALCOLINDEX]));
        }
    }

    private static double normalizeZero(double result) { return (result == -0.0) ? 0.0 : result; }

    private static class FunctionTestCase {
        final String m_case;
        final double m_result;
        final double m_filter;

        FunctionTestCase(String proc, double result) {
            m_case = proc;
            m_filter = 0.0;
            // Believe it or not, "negative 0" results were causing problems.
            m_result = normalizeZero(result);
        }
        FunctionTestCase(String proc, double filter, long result) {
            m_case = proc;
            m_filter = normalizeZero(filter);
            m_result = result;
        }
    };

    /**
     * @param expectedFormat
     * @param result
     * @param jj
     * @return
     */
    private double getColumnValue(String expectedFormat, VoltTable result, int jj) {
        double value;
        if (expectedFormat == null) {
            if (jj < FLOATCOLINDEX) {
                value = result.getLong(jj);
            } else if (jj == FLOATCOLINDEX) {
                value = result.getDouble(jj);
            } else {
                value = result.getDecimalAsBigDecimal(jj).doubleValue();
            }
        } else if (expectedFormat.equals("LONG")) {
            value = result.getLong(jj);
        } else if (expectedFormat.equals("DOUBLE")) {
            value = result.getDouble(jj);
        } else {
            value = result.getDecimalAsBigDecimal(jj).doubleValue();
        }
        return value;
    }

    /**
     * @param expectedFormat
     * @param client
     * @param proc
     * @param jj
     * @param filter
     * @return
     * @throws IOException
     * @throws ProcCallException
     */
    private static ClientResponse callWithParameter(String expectedFormat, Client client, String proc, int jj, double filter)
            throws IOException, ProcCallException {
        ClientResponse cr;
        if (expectedFormat == null) {
            if (jj < FLOATCOLINDEX) {
                cr = client.callProcedure(proc, (int)filter);
            } else if (jj == FLOATCOLINDEX) {
                cr = client.callProcedure(proc, filter);
            } else {
                cr = client.callProcedure(proc, BigDecimal.valueOf(filter));
            }
        } else if (expectedFormat.equals("LONG")) {
            cr = client.callProcedure(proc, (int)filter);
        } else if (expectedFormat.equals("DOUBLE")) {
            cr = client.callProcedure(proc, filter);
        } else {
            cr = client.callProcedure(proc, BigDecimal.valueOf(filter));
        }
        return cr;
    }

    private FunctionTestCase[] displayFunctionRun(
            Client client, String fname, int rowCount, String expectedFormat) throws Exception {
        ClientResponse cr;
        VoltTable result;

        FunctionTestCase[] resultSet = new FunctionTestCase[numTypeNames.length * rowCount];
        int ii = 0;

        String proc = "DISPLAY_" + fname;
        cr = client.callProcedure(proc);
        result = cr.getResults()[0];
        assertEquals(rowCount, result.getRowCount());
        while (result.advanceRow()) {
            int jj = 0;
            for (String numTypeName : numTypeNames) {
                double value = getColumnValue(expectedFormat, result, jj);
                resultSet[ii++] = new FunctionTestCase(proc + " " + numTypeName, value);
                ++jj;
            }
        }
        return resultSet;
    }

    private FunctionTestCase[] orderFunctionRun(Client client, String fname, int rowCount) throws Exception {
        ClientResponse cr;
        VoltTable result;

        FunctionTestCase[] resultSet = new FunctionTestCase[numTypeNames.length * rowCount];
        int ii = 0;

        for (String numTypeName : numTypeNames) {
            String proc = "ORDER_" + fname + "_" + numTypeName;
            cr = client.callProcedure(proc);
            result = cr.getResults()[0];
            assertEquals(rowCount, result.getRowCount());
            int jj = 0;
            while (result.advanceRow()) {
                try {
                    resultSet[ii] = new FunctionTestCase(proc + " ROW " + jj, result.getLong(0));
                    ii++;
                } catch (IllegalArgumentException iae) {
                    // HSQL has been known to claim that the INTEGERNUM column is being returned as a float -- WTF!
                    resultSet[ii] = new FunctionTestCase(proc + " ROW " + jj, result.getDouble(0));
                    ii++;
                }
                // Extraneous columns beyond the first are provided for debug purposes only
                for (int kk = 1; kk < result.getColumnCount(); ++kk) {
                    if (result.getColumnType(kk) == VoltType.FLOAT) {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getDouble(kk));
                    } else if (result.getColumnType(kk) == VoltType.DECIMAL) {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getDecimalAsBigDecimal(kk));
                    } else if (result.getColumnType(kk) == VoltType.STRING) {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getString(kk));
                    } else {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getLong(kk));
                    }
                }
                ++jj;
            }
        }
        return resultSet;
    }

    private FunctionTestCase[] whereFunctionRun(
            Client client, String fname, Set<Double> filters, String expectedFormat) throws Exception {
        ClientResponse cr;
        VoltTable result;

        FunctionTestCase[] resultSet = new FunctionTestCase[numTypeNames.length * filters.size()];
        int kk = 0;
        int jj = 0;
        for (String numTypeName : numTypeNames) {
            for (double filter : filters) {
                String proc = "WHERE_" + fname + "_" + numTypeName;
                cr = callWithParameter(expectedFormat, client, proc, jj, filter);
                result = cr.getResults()[0];
                int rowCount = result.getRowCount();
                assertEquals(rowCount, 1);
                resultSet[kk++] = new FunctionTestCase(proc, filter, result.asScalarLong());
            }
            ++jj;
        }
        return resultSet;
    }

    private static int complaintCount = 1;
    private static void complain(String complaint) {
        System.out.println("Complaint #" + complaintCount + ": " + complaint);
        ++complaintCount; // NICE PLACE FOR A BREAKPOINT.
    }

    static String formatForFuzziness = "%14e";
    private void functionTest(String fname, double rawData[], double[] resultValues, Set<Double> filters,
                              boolean monotonic, boolean ascending, String expectedFormat) throws Exception {
        System.out.println("STARTING test of " + fname);

        Client client = getClient();
        insertNumbers(client, rawData, resultValues.length);

        FunctionTestCase[] results;

        results = displayFunctionRun(client, fname, resultValues.length / COLUMNCOUNT, expectedFormat);

        assertEquals(results.length, resultValues.length);

        Map<String, Integer> valueBag = new HashMap<>();
        int kk = 0;
        for (FunctionTestCase result : results) {
            double expected = resultValues[kk++];
            if (expected != result.m_result) {
                // Compromise: accuracy errors get complaints but not asserts.
                complain("Failed " + result.m_case + " expected " + expected + " got " + result.m_result);
            }
            // Use precision-limited string formatting to forgive accuracy errors between the C++ and java floating point function implementations.
            String asExpected = String.format(formatForFuzziness, expected);
            String asResulted = String.format(formatForFuzziness, result.m_result);
            assertEquals(asExpected, asResulted);
            // count occurrences of expected values in anticipation of the WHERE_ tests.
            Integer count = valueBag.get(asExpected);
            if (count == null) {
                count = 0;
            }
            valueBag.put(asExpected, count + 1);
            //*VERBOSIFY TO DEBUG:*/ System.out.println("UPDATING " + result.m_case + " found count of " + asExpected + " to " + (count+1) );
        }

        if (monotonic) {
            // Validate that sorting on the function value does not alter the ordering of its input values.
            results = orderFunctionRun(client, fname, resultValues.length/COLUMNCOUNT);

            // The total number of ordered INTEGERNUM values returned should be the same as the number of stored values.
            assertEquals(results.length, resultValues.length);
            // If not using ALL values of orderedIds for this run, skip early (negative) values to only match later ones.
            int skippedIds = ROWCOUNT - resultValues.length / COLUMNCOUNT;
            if (ascending) {
                kk = skippedIds;
            } else {
                kk = ROWCOUNT-1;
            }
            for (FunctionTestCase result : results) {
                int idIndex;
                if (ascending) {
                    idIndex = kk++;
                    // skip early id values again at the end of each order by query result.
                    if (kk == ROWCOUNT) {
                        kk = skippedIds;
                    }
                } else {
                    idIndex = kk--;
                    if (kk == skippedIds-1) {
                        kk = ROWCOUNT-1;
                    }
                }
                double expected = orderedIds[idIndex];
                if (expected != result.m_result) {
                    complain("Failed " + result.m_case + " expected " + expected + " got " + result.m_result);
                }
                assertEquals(expected, result.m_result);
            }
        }

        if (filters != null) {
            results = whereFunctionRun(client, fname, filters, expectedFormat);

            assertEquals(results.length, COLUMNCOUNT*filters.size());
            // If filters represents all the values in resultValues,
            // the filtered counts should total to resultValues.length.
            int coveringCount = resultValues.length;
            //*VERBOSIFY TO DEBUG:*/ System.out.println("EXPECTING total count" + coveringCount);
            for (FunctionTestCase result : results) {
                if (result.m_result == 0.0) {
                    // complain("NONMATCHING filter " + result.m_case + " " + result.m_filter);
                    continue;
                }
                Integer count = valueBag.get(String.format(formatForFuzziness, result.m_filter));
                if (count == null) {
                    complain("Function " + fname + " got unexpected result " + result.m_filter + ".");
                }
                assertNotNull(count);
                //*VERBOSIFY TO DEBUG:*/ System.out.println("REDUCING " + result.m_case + " unfound " + result.m_filter + " count " + count + " by " + result.m_result );
                if (count < result.m_result) {
                    complain(result.m_case + " value " + result.m_filter + " not expected or previously deleted from " + valueBag + ".");
                }
                assertTrue(count >= result.m_result);
                valueBag.put(String.format(formatForFuzziness, result.m_filter), count-(int)result.m_result);
                coveringCount -= (int)result.m_result;
                //*VERBOSIFY TO DEBUG:*/ System.out.println("DROPPING TOTAL TO " + coveringCount);
            }
            for (Entry<String, Integer> entry : valueBag.entrySet()) {
                int count = entry.getValue();
                if (count != 0) {
                    complain("Function " + fname + " expected result " + entry.getKey() + " lacks " + count + " matches.");
                }
                assertEquals(0, count);
            }
            assertEquals(0, coveringCount);
        }

        System.out.println("ENDING test of " + fname);
    }

    @Test
    public void testCeiling() throws Exception {
        String fname = "CEILING";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            // Believe it or not, "negative 0" results were causing problems.
            resultValues[kk] = normalizeZero(Math.ceil(values[kk]));
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = null; // column/parameter values are variously typed.
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    @Test
    public void testExp() throws Exception {
        String fname = "EXP";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.exp(values[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    @Test
    public void testFloor() throws Exception {
        String fname = "FLOOR";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.floor(values[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = null;
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);

    }

    @Test
    public void testPowerx7() throws Exception {
        final String fname = "POWERX7";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.pow(values[kk], 7.0);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    @Test
    public void testPowerx07() throws Exception {
        final String fname = "POWERX07";
        final double[] resultValues = new double[nonnegnonzeros.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.pow(nonnegnonzeros[kk], 0.7);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, nonnegnonzeros, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    @Test
    public void testPower7x() throws Exception {
        final String fname = "POWER7X";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.pow(7.0, values[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    @Test
    public void testPower07x() throws Exception {
        final String fname = "POWER07X";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.pow(0.7, values[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = false;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    @Test
    public void testSqrt() throws Exception {
        final String fname = "SQRT";
        final double[] resultValues = new double[nonnegs.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.sqrt(nonnegs[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, nonnegs, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    @Test
    public void testTrig() throws Exception {
        final boolean monotonic = false;
        final boolean ascending = false;
        final String expectedFormat = "DOUBLE";
        final Set<Double> filters = null;

        Map<String, DoubleFunction<Double>> funcInfo = new HashMap<>();
        funcInfo.put("SIN", Math::sin);
        funcInfo.put("COS", Math::cos);
        funcInfo.put("TAN", Math::tan);
        funcInfo.put("COT", x -> 1.0 / Math.tan(x));
        funcInfo.put("SEC", x -> 1.0 / Math.cos(x));
        funcInfo.put("CSC", x -> 1.0 / Math.sin(x));

        double[] valuesCopy = Arrays.copyOf(values, values.length);
        for (Entry<String, DoubleFunction<Double>> entry : funcInfo.entrySet()) {
            final String fname = entry.getKey();
            final double[] resultValues = new double[valuesCopy.length];
            for (int kk = 0; kk < resultValues.length; ++kk) {
                resultValues[kk] = entry.getValue().apply(valuesCopy[kk]);
                if (Double.isInfinite(resultValues[kk])) {
                    // The EE throws an exception when a nonfinite would be produced.
                    valuesCopy[kk] += 1;
                    resultValues[kk] = entry.getValue().apply(valuesCopy[kk]);
                    assert(Double.isFinite(resultValues[kk]));
                }
            }
            functionTest(fname, valuesCopy, resultValues, filters, monotonic, ascending, expectedFormat);
        }

        if (!isHSQL()) {
            // Also verify that trig functions that produce non-finites throw an exception
            String[] stmts = {"select cot(0.0) from number_types", "select csc(0.0) from number_types"};

            for (String stmt : stmts) {
                verifyStmtFails(getClient(), stmt, "Invalid result value");
            }
        }
    }

    @Test
    public void testDegrees() throws Exception {
        String fname = "DEGREES";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = values[kk]*(180.0/Math.PI);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    @Test
    public void testRadians() throws Exception {
        String fname = "RADIANS";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = values[kk]*(Math.PI / 180.0);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);

    }

    @Test
    public void testNaturalLog() throws Exception {
        final String[] fname = {"LOG", "LN"};
        final double[] resultValues = new double[nonnegnonzeros.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.log(nonnegnonzeros[kk]);
            filters.add(resultValues[kk]);
        }

        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        for (String log : fname) {
            functionTest(log, nonnegnonzeros, resultValues, filters, monotonic, ascending, expectedFormat);
        }

        // Adhoc Queries
        Client client = getClient();

        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (5, 'wEoiXIuJwSIKBujWv', -405636, 1.38145922788945552107e-01, NULL)");
        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (2, 'wEoiXIuJwSIKBujWv', -29914, 8.98500019539639316335e-01, NULL)");
        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (4, 'WCfDDvZBPoqhanfGN', -1309657, 9.34160160574919795629e-01, NULL)");

        // valid adhoc SQL query
        String sql = "select * from P1 where ID > LOG(1)";
        client.callProcedure("@AdHoc", sql);

        // execute Log() with invalid arguments
        try {
            sql = "select LOG(0) from P1";
            client.callProcedure("@AdHoc", sql);
            fail("Expected for Log(zero) result: invalid result value (-inf)");
        } catch (ProcCallException excp) {
            if (isHSQL()) {
                assertTrue(excp.getMessage().contains("invalid argument for natural logarithm"));
            } else {
                assertTrue(excp.getMessage().contains("Invalid result value (-inf)"));
            }
        }

        try {
            sql = "select LOG(-10) from P1";
            client.callProcedure("@AdHoc", sql);
            fail("Expected resultfor Log(negative #): invalid result value (nan) or (-nan)");
        } catch (ProcCallException excp) {
            if (isHSQL()) {
                assertTrue(excp.getMessage().contains("invalid argument for natural logarithm"));
            } else {
                final String msg = excp.getMessage();
                assertTrue(msg.contains("Invalid result value (nan)") ||
                        msg.contains("Invalid result value (-nan)"));
            }
        }
    }

    @Test
    public void testNaturalLog10() throws Exception {
        final String[] fname = {"LOG10"};
        final double[] resultValues = new double[nonnegnonzeros.length];
        final Set<Double> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.log10(nonnegnonzeros[kk]);
            filters.add(resultValues[kk]);
        }

        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        for (String log : fname) {
            functionTest(log, nonnegnonzeros, resultValues, filters, monotonic, ascending, expectedFormat);
        }

        // Adhoc Queries
        Client client = getClient();

        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (510, 'wEoiXIuJwSIKBujWv', -405636, 1.38145922788945552107e-01, NULL)");
        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (210, 'wEoiXIuJwSIKBujWv', -29914, 8.98500019539639316335e-01, NULL)");
        client.callProcedure("@AdHoc",
                "INSERT INTO P1 VALUES (410, 'WCfDDvZBPoqhanfGN', -1309657, 9.34160160574919795629e-01, NULL)");

        // valid adhoc SQL query
        String sql = "select * from P1 where ID > LOG10(1)";
        client.callProcedure("@AdHoc", sql);

        // execute Log10() with invalid arguments
        try {
            sql = "select LOG10(0) from P1";
            client.callProcedure("@AdHoc", sql);
            fail("Expected for Log10(zero) result: invalid result value (-inf)");
        } catch (ProcCallException excp) {
            if (isHSQL()) {
                assertTrue(excp.getMessage().contains("invalid argument for natural logarithm"));
            } else {
                assertTrue(excp.getMessage().contains("Invalid result value (-inf)"));
            }
        }

        try {
            sql = "select LOG10(-10) from P1";
            client.callProcedure("@AdHoc", sql);
            fail("Expected resultfor Log10(negative #): invalid result value (nan)");
        } catch (ProcCallException excp) {
            if (isHSQL()) {
                assertTrue(excp.getMessage().contains("invalid argument for natural logarithm"));
            } else {
                assertTrue(excp.getMessage().contains("Invalid result value (nan)"));
            }
        }
    }

    @Test
    public void testLeftAndRight() throws IOException, ProcCallException {
        System.out.println("STARTING Left and Right");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "贾鑫Vo", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // test LEFT function
        cr = client.callProcedure("LEFT", 0, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("", result.getString(1));

        cr = client.callProcedure("LEFT", 1, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("贾", result.getString(1));

        cr = client.callProcedure("LEFT", 2, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("贾鑫", result.getString(1));

        cr = client.callProcedure("LEFT", 3, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("贾鑫V", result.getString(1));

        cr = client.callProcedure("LEFT", 4, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("贾鑫Vo", result.getString(1));

        cr = client.callProcedure("LEFT", 5, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("贾鑫Vo", result.getString(1));

        // invalid case
        Exception ex = null;
        try {
            client.callProcedure("LEFT", -1, 1);
        } catch (Exception e) {
            assertTrue(e instanceof ProcCallException);
            ex = e;
        } finally {
            assertNotNull(ex);
        }

        // test RIGHT function
        cr = client.callProcedure("RIGHT", 0, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("", result.getString(1));

        cr = client.callProcedure("RIGHT", 1, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("o", result.getString(1));

        cr = client.callProcedure("RIGHT", 2, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Vo", result.getString(1));

        cr = client.callProcedure("RIGHT", 3, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("鑫Vo", result.getString(1));

        cr = client.callProcedure("RIGHT", 4, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("贾鑫Vo", result.getString(1));

        cr = client.callProcedure("RIGHT", 5, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("贾鑫Vo", result.getString(1));

        ex = null;
        try {
            client.callProcedure("RIGHT", -1, 1);
        } catch (Exception e) {
            assertTrue(e instanceof ProcCallException);
            ex = e;
        } finally {
            assertNotNull(ex);
        }
    }

    @Test
    public void testSpace() throws IOException, ProcCallException {
        System.out.println("STARTING test Space");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "foo", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("SPACE", 0, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("", result.getString(1));

        cr = client.callProcedure("SPACE", 1, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(" ", result.getString(1));

        cr = client.callProcedure("SPACE", 5, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("     ", result.getString(1));
    }

    @Test
    public void testLowerUpper() throws IOException, ProcCallException {
        System.out.println("STARTING test Space");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("LOWER_UPPER", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("voltdb", result.getString(1));
        assertEquals("VOLTDB", result.getString(2));

        cr = client.callProcedure("P1.insert", 2, "VoltDB贾鑫", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("LOWER_UPPER", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("voltdb贾鑫", result.getString(1));
        assertEquals("VOLTDB贾鑫", result.getString(2));

        cr = client.callProcedure("P1.insert", 3, null, 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("LOWER_UPPER", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertNull(result.getString(1));
        assertNull(result.getString(2));

        // Edge case: UTF-8 string can have Upper and Lower cases
        String grussen = "grüßEN";
        cr = client.callProcedure("P1.insert", 4, grussen, 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("LOWER_UPPER", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());

        // Turn on this test case when EE supports the locale CASE conversion
//        if (isHSQL()) {
//            assertEquals(grussen, result.getString(1));
//            assertEquals(grussen, result.getString(2));
//        } else {
//            assertEquals("GRÜSSEN", result.getString(1));
//            assertEquals("grüßen", result.getString(2));
//        }
    }

    @Test
    public void testTrim() throws IOException, ProcCallException {
        System.out.println("STARTING test Trim");
        Client client = getClient();

        subtestTrimSpace(client, "TRIM_SPACE");
        // Test that commas work just like keyword separators
        subtestTrimSpace(client, "ALT_TRIM_SPACE");

        subtestTrimAny(client, "TRIM_ANY");
        // Test that commas work just like keyword separators
        subtestTrimAny(client, "ALT_TRIM_ANY");
    }

    private void subtestTrimSpace(Client client, String trimProc) throws IOException, ProcCallException {
        ClientResponse cr;
        VoltTable result;

        String trimFailed = trimProc + " got a wrong answer";

        cr = client.callProcedure("@AdHoc", "TRUNCATE TABLE P1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("P1.insert", 1, "  VoltDB   ", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        if (! USING_CALCITE) {  // TODO ENG-15490 enable null & ? as UDF param
            result = client.callProcedure("@AdHoc",
                    "select trim(LEADING null from desc) from P1").getResults()[0];
            assertTrue(result.advanceRow());
            assertNull(result.getString(0));
        }

        cr = client.callProcedure("TRIM_SPACE", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(trimFailed, "VoltDB   ", result.getString(1));
        assertEquals(trimFailed, "VoltDB   ", result.getString(2));
        assertEquals(trimFailed, "  VoltDB",  result.getString(3));
        assertEquals(trimFailed, "  VoltDB",  result.getString(4));
        assertEquals(trimFailed, "VoltDB",  result.getString(5));
        assertEquals(trimFailed, "VoltDB",  result.getString(6));
    }

    private void subtestTrimAny(Client client, String trimProc) throws IOException, ProcCallException {
        ClientResponse cr;
        VoltTable result;

        String trimFailed = trimProc + " got a wrong answer";

        cr = client.callProcedure("@AdHoc", "TRUNCATE TABLE P1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("P1.insert", 1, "  VoltDB   ", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure(trimProc, " ", " ", " ", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(trimFailed, "VoltDB   ", result.getString(1));
        assertEquals(trimFailed, "  VoltDB",  result.getString(2));
        assertEquals(trimFailed, "VoltDB",  result.getString(3));

        try {
            client.callProcedure(trimProc, "", "", "", 1);
            fail();
        } catch (Exception ex) {
            String exceptionMsg = ex.getMessage();
            assertTrue(exceptionMsg.contains("data exception"));
            assertTrue(exceptionMsg.contains("trim error"));
        }

        // Test TRIM with other character
        cr = client.callProcedure("P1.insert", 2, "vVoltDBBB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure(trimProc, "v", "B", "B", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(trimFailed, "VoltDBBB", result.getString(1));
        assertEquals(trimFailed, "vVoltD", result.getString(2));
        assertEquals(trimFailed, "vVoltD", result.getString(3));

        // Multiple character trim, Hsql does not support
        if (!isHSQL()) {
            cr = client.callProcedure(trimProc, "vV", "BB", "Vv", 2);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals(trimFailed, "oltDBBB", result.getString(1));
            assertEquals(trimFailed, "vVoltDB", result.getString(2));
            assertEquals(trimFailed, "vVoltDBBB", result.getString(3));
        }

        // Test null trim character
        cr = client.callProcedure(trimProc, null, null, null, 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertNull(result.getString(1));
        assertNull(result.getString(2));
        assertNull(result.getString(3));


        // Test non-ASCII trim_char
        cr = client.callProcedure("P1.insert", 3, "贾vVoltDBBB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure(trimProc, "贾", "v", "贾", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(trimFailed, "vVoltDBBB", result.getString(1));
        assertEquals(trimFailed, "贾vVoltDBBB", result.getString(2));
        assertEquals(trimFailed, "vVoltDBBB", result.getString(3));

        if (!isHSQL()) {
            // Complete match
            cr = client.callProcedure(trimProc, "贾vVoltDBBB", "贾vVoltDBBB", "贾vVoltDBBB", 3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals(trimFailed, "", result.getString(1));
            assertEquals(trimFailed, "", result.getString(2));
            assertEquals(trimFailed, "", result.getString(3));

            cr = client.callProcedure(trimProc, "贾vVoltDBBB_TEST", "贾vVoltDBBB贾vVoltDBBB", "贾vVoltDBBBT", 3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals(trimFailed, "贾vVoltDBBB", result.getString(1));
            assertEquals(trimFailed, "贾vVoltDBBB", result.getString(2));
            assertEquals(trimFailed, "贾vVoltDBBB", result.getString(3));
        }

        // Complicated test
        cr = client.callProcedure("P1.insert", 4, "贾贾vVoltDBBB贾贾贾", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // UTF-8 hex, 贾: 0xe8 0xb4 0xbe, 辴: 0xe8 0xbe 0xb4
        cr = client.callProcedure(trimProc, "辴", "辴", "辴", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(trimFailed, "贾贾vVoltDBBB贾贾贾", result.getString(1));
        assertEquals(trimFailed, "贾贾vVoltDBBB贾贾贾", result.getString(2));
        assertEquals(trimFailed, "贾贾vVoltDBBB贾贾贾", result.getString(3));

        cr = client.callProcedure(trimProc, "贾", "贾", "贾", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(trimFailed, "vVoltDBBB贾贾贾", result.getString(1));
        assertEquals(trimFailed, "贾贾vVoltDBBB", result.getString(2));
        assertEquals(trimFailed, "vVoltDBBB", result.getString(3));

        if (!isHSQL()) {
            cr = client.callProcedure(trimProc, "贾辴", "贾辴", "贾辴", 4);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals(trimFailed, "贾贾vVoltDBBB贾贾贾", result.getString(1));
            assertEquals(trimFailed, "贾贾vVoltDBBB贾贾贾", result.getString(2));
            assertEquals(trimFailed, "贾贾vVoltDBBB贾贾贾", result.getString(3));

            cr = client.callProcedure(trimProc, "贾贾vV", "贾贾", "B贾贾贾", 4);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals(trimFailed, "oltDBBB贾贾贾", result.getString(1));
            assertEquals(trimFailed, "贾贾vVoltDBBB贾", result.getString(2));
            assertEquals(trimFailed, "贾贾vVoltDBB", result.getString(3));
        }
    }

    @Test
    public void testRepeat() throws IOException, ProcCallException {
        System.out.println("STARTING test Repeat");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "foo", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("REPEAT", 0, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("", result.getString(1));

        cr = client.callProcedure("REPEAT", 1, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("foo", result.getString(1));

        cr = client.callProcedure("REPEAT", 3, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("foofoofoo", result.getString(1));
        if (!isHSQL()) {
            String expectedError = "VOLTDB ERROR: SQL ERROR\\s*+The result of the REPEAT function is larger than the maximum size allowed "
                    + "for strings \\(1048576 bytes\\)\\. Reduce either the string size or repetition count\\.";
            verifyProcFails(client, expectedError, "REPEAT", 10000000, 1);
            // The multiply needed to do the size check for this call to REPEAT will
            // overflow a 64-bit signed int.  This was ticket ENG-11559.
            verifyProcFails(client, expectedError, "REPEAT", 4611686018427387903L, 1);
        }

        // Make sure that repeat of an empty string doesn't take a long time
        // This verifies the fix for ENG-12118
        long startTime = System.nanoTime();
        cr = client.callProcedure("@AdHoc", "select repeat('', 10000000000000) from P1 limit 1");
        assertContentOfTable(new Object[][] {{""}}, cr.getResults()[0]);
        long elapsedNanos = System.nanoTime() - startTime;
        // It should take less than a minute (much much less) to complete this query
        assertTrue("Repeat with empty string took too long!", elapsedNanos < 1000000000L * 60L);
    }

    @Test
    public void testReplace() throws IOException, ProcCallException {
        System.out.println("STARTING test Replace");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "foo", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        result = client.callProcedure("REPLACE", "o", "XX", 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("fXXXX", result.getString(1));

        result = client.callProcedure("REPLACE", "o", null, 1).getResults()[0];
        assertTrue(result.advanceRow());
        if (isHSQL()) {
            // NULL means empty string for Hsql
            assertEquals("f", result.getString(1));
        } else {
            assertNull(result.getString(1));
        }

        result = client.callProcedure("REPLACE", null, "XX", 1).getResults()[0];
        assertTrue(result.advanceRow());
        if (isHSQL()) {
            // NULL means not change for the original string for Hsql
            assertEquals("foo", result.getString(1));
        } else {
            assertNull(result.getString(1));
        }

        result = client.callProcedure("REPLACE", "fo", "V", 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("Vo", result.getString(1));

        // UTF-8 String
        cr = client.callProcedure("P1.insert", 2, "贾鑫@VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        result = client.callProcedure("REPLACE", "鑫", "XX", 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾XX@VoltDB", result.getString(1));
    }

    @Test
    public void testOverlay() throws IOException, ProcCallException {
        System.out.println("STARTING test Overlay");
        Client client = getClient();
        subtestOverlay(client, "OVERLAY", "OVERLAY_FULL_LENGTH");
        // Test that commas work just like keyword separators
        subtestOverlay(client, "ALT_OVERLAY", "ALT_OVERLAY_FULL_LENGTH");
    }

    private void subtestOverlay(Client client, String overlayProc, String overlayFullLengthProc)
            throws IOException, ProcCallException {
        ClientResponse cr;
        VoltTable result;
        String overlayFailed = overlayProc + " got a wrong answer";
        String overlayFullLengthFailed = overlayFullLengthProc + " got a wrong answer";

        cr = client.callProcedure("@AdHoc", "TRUNCATE TABLE P1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("P1.insert", 1, "Xin@VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        result = client.callProcedure(overlayProc, "Jia", 4, 7, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFailed, "XinJia", result.getString(1));

        result = client.callProcedure(overlayProc, "Jia_", 4, 1, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFailed, "XinJia_VoltDB", result.getString(1));

        try {
            client.callProcedure(overlayProc, "Jia", 4.2, 7, 1).getResults();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("The provided value: (4.2) of type: java.lang.Double "
                    + "is not a match or is out of range for the target parameter type: long"));
        }

        // Test NULL results
        result = client.callProcedure(overlayProc, null, 4, 7, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertNull(overlayFailed, result.getString(1));

        result = client.callProcedure(overlayProc, "Jia", 4, null, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertNull(overlayFailed, result.getString(1));

        result = client.callProcedure(overlayProc, "Jia", null, 7, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertNull(overlayFailed, result.getString(1));

        result = client.callProcedure(overlayFullLengthProc, "Jia", 4, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFullLengthFailed, "XinJialtDB", result.getString(1));

        result = client.callProcedure(overlayFullLengthProc, "J", 4, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFullLengthFailed, "XinJVoltDB", result.getString(1));


        // Test UTF-8 OVERLAY
        cr = client.callProcedure("P1.insert", 2, "贾鑫@VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        result = client.callProcedure(overlayProc, "XinJia", 1, 2, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFailed, "XinJia@VoltDB", result.getString(1));

        result = client.callProcedure(overlayProc, "XinJia", 8, 2, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFailed, "贾鑫@VoltXinJia", result.getString(1));

        result = client.callProcedure(overlayProc, "XinJia", 1, 9, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFailed, "XinJia", result.getString(1));

        result = client.callProcedure(overlayProc, "XinJia", 2, 7, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFailed, "贾XinJiaB", result.getString(1));

        result = client.callProcedure(overlayProc, "XinJia", 2, 8, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFailed, "贾XinJia", result.getString(1));

        result = client.callProcedure(overlayFullLengthProc, "_", 3, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFullLengthFailed, "贾鑫_VoltDB", result.getString(1));

        result = client.callProcedure(overlayFullLengthProc, " at ", 2, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFullLengthFailed, "贾 at ltDB", result.getString(1));


        result = client.callProcedure(overlayProc, "XinJia", 9, 1, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFailed, "贾鑫@VoltDXinJia", result.getString(1));

        result = client.callProcedure(overlayProc, "石宁", 9, 1, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(overlayFailed, "贾鑫@VoltD石宁", result.getString(1));

        // Hsql has bugs on string(substring) index
        if (!isHSQL()) {
            result = client.callProcedure(overlayProc, "XinJia", 9, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(overlayFailed, "贾鑫@VoltDXinJia", result.getString(1));

            result = client.callProcedure(overlayProc, "石宁", 9, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(overlayFailed, "贾鑫@VoltD石宁", result.getString(1));

            result = client.callProcedure(overlayProc, "XinJia", 10, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(overlayFailed, "贾鑫@VoltDBXinJia", result.getString(1));

            result = client.callProcedure(overlayProc, "石宁", 10, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(overlayFailed, "贾鑫@VoltDB石宁", result.getString(1));

            // various start argument tests
            // start from 0, not 1, but treat it at least 1
            result = client.callProcedure(overlayProc, "XinJia", 100, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(overlayFailed, "贾鑫@VoltDBXinJia", result.getString(1));

            // various length argument
            result = client.callProcedure(overlayProc, "XinJia", 2, 0, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(overlayFailed, "贾XinJia鑫@VoltDB", result.getString(1));

            result = client.callProcedure(overlayProc, "XinJia", 1, 10, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(overlayFailed, "XinJia", result.getString(1));

            result = client.callProcedure(overlayProc, "XinJia", 1, 100, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(overlayFailed, "XinJia", result.getString(1));

            result = client.callProcedure(overlayProc, "XinJia", 2, 100, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(overlayFailed, "贾XinJia", result.getString(1));


            // Negative tests
            try {
                client.callProcedure(overlayProc, "XinJia", -10, 2, 2).getResults();
                fail();
            } catch (Exception ex) {
                assertTrue(ex.getMessage().contains("data exception -- OVERLAY error, not positive start argument -10"));
            }

            try {
                client.callProcedure(overlayProc, "XinJia", 0, 2, 2).getResults();
                fail();
            } catch (Exception ex) {
                assertTrue(ex.getMessage().contains("data exception -- OVERLAY error, not positive start argument 0"));
            }

            try {
                client.callProcedure(overlayProc, "XinJia", 1, -1, 2).getResults();
                fail();
            } catch (Exception ex) {
                assertTrue(ex.getMessage().contains("data exception -- OVERLAY error, negative length argument -1"));
            }
        }
    }

    private static String longToHexLiteral(long val) {
        String hexDigits = Long.toHexString(val);
        if (hexDigits.length() % 2 == 1) {
            hexDigits = "0" + hexDigits;
        }
        return "x'" + hexDigits + "'";
    }

    private void validateBitwiseAndOrXor(VoltTable vt, long bignum, long in) {
        if (bignum == Long.MIN_VALUE || in == Long.MIN_VALUE) {
            // Long.MIN_VALUE is NULL for VoltDB, following the rule null in, null out
            validateRowOfLongs(vt, new long[]{Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE});
        } else {
            validateRowOfLongs(vt, new long[]{bignum & in, bignum | in, bignum ^ in});
        }
    }

    private void bitwiseFunctionChecker(long pk, long bignum, long in) throws IOException, ProcCallException {
        VoltTable vt;
        Client client = getClient();
        client.callProcedure("@AdHoc",
                String.format("insert into NUMBER_TYPES(INTEGERNUM, bignum) values(%d,%d);", pk, bignum));

        vt = client.callProcedure("BITWISE_AND_OR_XOR", in, in, in, pk).getResults()[0];
        validateBitwiseAndOrXor(vt, bignum, in);

        // Try again using x'...' syntax
        String hexIn = longToHexLiteral(in);
        vt = client.callProcedure("BITWISE_AND_OR_XOR", hexIn, hexIn, hexIn, pk).getResults()[0];
        validateBitwiseAndOrXor(vt, bignum, in);
    }

    @Test
    public void testBitwiseFunction_AND_OR_XOR() throws IOException, ProcCallException {
        // bigint 1: 01,  input 3: 11
        bitwiseFunctionChecker(1, 1, 3);
        bitwiseFunctionChecker(2, 3, 1);

        bitwiseFunctionChecker(3, -3, 1);
        bitwiseFunctionChecker(4, 1, -3);

        bitwiseFunctionChecker(5, Long.MAX_VALUE, Long.MIN_VALUE + 10);
        bitwiseFunctionChecker(6, Long.MIN_VALUE + 10, Long.MAX_VALUE);

        bitwiseFunctionChecker(7, -100, Long.MIN_VALUE + 10);
        bitwiseFunctionChecker(8, Long.MIN_VALUE + 10, -100);

        VoltTable vt;
        Client client = getClient();
        long in, pk, bignum;
        String sql;

        // out of range tests
        try {
            sql = "select bitand(bignum, 9223372036854775809) from NUMBER_TYPES;";
            client.callProcedure("@AdHoc", sql).getResults();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("numeric value out of range"));
        }

        // test bitwise function index usage
        client.callProcedure("@AdHoc",
                String.format("INSERT INTO NUMBER_TYPES(INTEGERNUM, BIGNUM) VALUES(%d,%d);", 19, 6));
        client.callProcedure("@AdHoc",
                String.format("INSERT INTO NUMBER_TYPES(INTEGERNUM, BIGNUM) VALUES(%d,%d);", 20, 14));

        sql = "select bignum from NUMBER_TYPES where bignum > -100 and bignum < 100 and bitand(bignum,3) = 2 order by bignum;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{6, 14});

        // picked up this index
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("NUMBER_TYPES_BITAND_IDX"));

        if( isHSQL() ) {
            // Hsqldb has different behavior on the NULL and Long.MIN_VALUE handling
            return;
        }

        bitwiseFunctionChecker(21, Long.MAX_VALUE, Long.MIN_VALUE);
        bitwiseFunctionChecker(22, Long.MIN_VALUE, Long.MAX_VALUE);

        // try the out of range exception
        client.callProcedure("@AdHoc",
                "insert into NUMBER_TYPES(INTEGERNUM, bignum) values(50, ?);", Long.MIN_VALUE + 1);
        verifyStmtFails(client, "select BITAND(bignum, -2) from NUMBER_TYPES where INTEGERNUM = 50;",
                "would produce INT64_MIN, which is reserved for SQL NULL values");

        verifyStmtFails(client, "select BITXOR(bignum, 1) from NUMBER_TYPES where INTEGERNUM = 50;",
                "would produce INT64_MIN, which is reserved for SQL NULL values");

        // special case for null, treated as Long.MIN_VALUE
        pk = 100; bignum = Long.MIN_VALUE; in = Long.MAX_VALUE;
        client.callProcedure("NUMBER_TYPES.insert", pk, 1, 1, null, 1.0, 1.0);
        vt = client.callProcedure("BITWISE_AND_OR_XOR", in, in, in, pk).getResults()[0];
        validateRowOfLongs(vt, new long[]{Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE});

        pk = 101; bignum = Long.MAX_VALUE; in = Long.MIN_VALUE;
        client.callProcedure("NUMBER_TYPES.insert", pk, 1, 1, bignum, 1.0, 1.0);
        vt = client.callProcedure("BITWISE_AND_OR_XOR", null, null, null, pk).getResults()[0];
        validateRowOfLongs(vt, new long[]{Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE});
    }

    @Test
    public void testPi() throws Exception {
        System.out.println("STARTING testPi");

        Client client = getClient();
        /*
         *      "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PAST TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
         */
        ClientResponse cr;
        VoltTable vt;

        cr = client.callProcedure("@AdHoc","INSERT INTO P1 (ID) VALUES(1)");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = client.callProcedure("@AdHoc", "SELECT PI() * ID FROM P1 WHERE ID = 1;").getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(Math.abs(vt.getDouble(0) - Math.PI) <= 1.0e-16);

        vt = client.callProcedure("@AdHoc", "SELECT PI * ID FROM P1 WHERE ID = 1;").getResults()[0];
        assertTrue(vt.advanceRow());
        assertTrue(Math.abs(vt.getDouble(0) - Math.PI) <= 1.0e-16);

        cr = client.callProcedure("@AdHoc", "TRUNCATE TABLE P1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    @Test
    public void testMultiSignatureFunctionInStoredProcedure() throws Exception {
        // ENG-10939
        System.out.println("STARTING testMultiSignatureFunctionInStoredProcedure");
        Client client = getClient();
        ClientResponse cr = client.callProcedure("TEST_SUBSTRING_INPROC", 12, "string");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    static public junit.framework.Test suite() throws IOException {
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(literalSchema);

        project.addStmtProcedure("WHERE_ABS", "select count(*) from P1 where ABS(ID) > 9");
        project.addStmtProcedure("WHERE_ABSFF", "select count(*) from P1 where ABS(ID - 0.4) > 9.5");
        project.addStmtProcedure("WHERE_ABSIF", "select count(*) from P1 where ABS(ID) > 9.5");
        project.addStmtProcedure("WHERE_ABSFI", "select count(*) from P1 where ABS(ID + 0.4) > 9");
        project.addStmtProcedure("WHERE_ABSWEAK", "select count(*) from P1 where ABS(ID - 0.4) > ABS(9.5)");

        project.addStmtProcedure("DISPLAY_ABS", "select ABS(ID)-2 from P1 where ID >= -7");
        project.addStmtProcedure("ORDER_ABS", "select ID+12 from P1 order by ABS(ID)");
        // GROUP BY with complex expressions not yet supported
        // project.addStmtProcedure("GROUP_ABS", "select MIN(ID+17) from P1 group by ABS(ID+12) order by ABS(ID+12)");
        project.addStmtProcedure("AGG_OF_ABS", "select MIN(ABS(ID)-2) from P1");
        // RuntimeException seems to stem from parser failure similar to ENG-2901
        // project.addStmtProcedure("ABS_OF_AGG", "select ABS(MIN(ID+9)) from P1");

        project.addStmtProcedure("DISPLAY_CEILING",
                "select CEILING(INTEGERNUM), CEILING(TINYNUM), CEILING(SMALLNUM), CEILING(BIGNUM), CEILING(FLOATNUM), CEILING(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("BITWISE_AND_OR_XOR",
                "select bitand(bignum, ?), bitor(bignum, ?), bitxor(bignum, ?) from NUMBER_TYPES WHERE INTEGERNUM = ?");

        project.addStmtProcedure("ORDER_CEILING_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CEILING(INTEGERNUM), INTEGERNUM");
        project.addStmtProcedure("ORDER_CEILING_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CEILING(TINYNUM), TINYNUM");
        project.addStmtProcedure("ORDER_CEILING_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CEILING(SMALLNUM), SMALLNUM");
        project.addStmtProcedure("ORDER_CEILING_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CEILING(BIGNUM), BIGNUM");
        project.addStmtProcedure("ORDER_CEILING_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CEILING(FLOATNUM), FLOATNUM");
        project.addStmtProcedure("ORDER_CEILING_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CEILING(DECIMALNUM), DECIMALNUM");

        project.addStmtProcedure("WHERE_CEILING_INTEGER",
                "select count(*) from NUMBER_TYPES where CEILING(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_TINYINT",
                "select count(*) from NUMBER_TYPES where CEILING(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_SMALLINT",
                "select count(*) from NUMBER_TYPES where CEILING(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_BIGINT",
                "select count(*) from NUMBER_TYPES where CEILING(BIGNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_FLOAT",
                "select count(*) from NUMBER_TYPES where CEILING(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_DECIMAL",
                "select count(*) from NUMBER_TYPES where CEILING(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_DEGREES",
                "select DEGREES(INTEGERNUM), DEGREES(TINYNUM), DEGREES(SMALLNUM), DEGREES(BIGNUM), DEGREES(FLOATNUM), DEGREES(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_DEGREES_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by DEGREES(INTEGERNUM)");
        project.addStmtProcedure("ORDER_DEGREES_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by DEGREES(TINYNUM)");
        project.addStmtProcedure("ORDER_DEGREES_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by DEGREES(SMALLNUM)");
        project.addStmtProcedure("ORDER_DEGREES_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by DEGREES(BIGNUM)");
        project.addStmtProcedure("ORDER_DEGREES_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by DEGREES(FLOATNUM)");
        project.addStmtProcedure("ORDER_DEGREES_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by DEGREES(DECIMALNUM)");

        project.addStmtProcedure("WHERE_DEGREES_INTEGER",
                "select count(*) from NUMBER_TYPES where DEGREES(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_DEGREES_TINYINT",
                "select count(*) from NUMBER_TYPES where DEGREES(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_DEGREES_SMALLINT",
                "select count(*) from NUMBER_TYPES where DEGREES(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_DEGREES_BIGINT",
                "select count(*) from NUMBER_TYPES where DEGREES(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_DEGREES_FLOAT",
                "select count(*) from NUMBER_TYPES where DEGREES(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_DEGREES_DECIMAL",
                "select count(*) from NUMBER_TYPES where DEGREES(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_EXP",
                "select EXP(INTEGERNUM), EXP(TINYNUM), EXP(SMALLNUM), EXP(BIGNUM), EXP(FLOATNUM), EXP(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_EXP_INTEGER",
                "select INTEGERNUM, EXP(INTEGERNUM) from NUMBER_TYPES order by EXP(INTEGERNUM)");
        project.addStmtProcedure("ORDER_EXP_TINYINT",
                "select INTEGERNUM, EXP(TINYNUM) from NUMBER_TYPES order by EXP(TINYNUM)");
        project.addStmtProcedure("ORDER_EXP_SMALLINT",
                "select INTEGERNUM, EXP(SMALLNUM) from NUMBER_TYPES order by EXP(SMALLNUM)");
        project.addStmtProcedure("ORDER_EXP_BIGINT",
                "select INTEGERNUM, EXP(BIGNUM) from NUMBER_TYPES order by EXP(BIGNUM)");
        project.addStmtProcedure("ORDER_EXP_FLOAT",
                "select INTEGERNUM, EXP(FLOATNUM) from NUMBER_TYPES order by EXP(FLOATNUM)");
        project.addStmtProcedure("ORDER_EXP_DECIMAL",
                "select INTEGERNUM, EXP(DECIMALNUM) from NUMBER_TYPES order by EXP(DECIMALNUM)");

        project.addStmtProcedure("WHERE_EXP_INTEGER",
                "select count(*) from NUMBER_TYPES where EXP(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_TINYINT",
                "select count(*) from NUMBER_TYPES where EXP(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_SMALLINT",
                "select count(*) from NUMBER_TYPES where EXP(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_BIGINT",
                "select count(*) from NUMBER_TYPES where EXP(BIGNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_FLOAT",
                "select count(*) from NUMBER_TYPES where EXP(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_DECIMAL",
                "select count(*) from NUMBER_TYPES where EXP(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_FLOOR",
                "select FLOOR(INTEGERNUM), FLOOR(TINYNUM), FLOOR(SMALLNUM), FLOOR(BIGNUM), FLOOR(FLOATNUM), FLOOR(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_FLOOR_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by FLOOR(INTEGERNUM), INTEGERNUM");
        project.addStmtProcedure("ORDER_FLOOR_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by FLOOR(TINYNUM), TINYNUM");
        project.addStmtProcedure("ORDER_FLOOR_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by FLOOR(SMALLNUM), SMALLNUM");
        project.addStmtProcedure("ORDER_FLOOR_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by FLOOR(BIGNUM), BIGNUM");
        project.addStmtProcedure("ORDER_FLOOR_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by FLOOR(FLOATNUM), FLOATNUM");
        project.addStmtProcedure("ORDER_FLOOR_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by FLOOR(DECIMALNUM), DECIMALNUM");

        project.addStmtProcedure("WHERE_FLOOR_INTEGER",
                "select count(*) from NUMBER_TYPES where FLOOR(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_TINYINT",
                "select count(*) from NUMBER_TYPES where FLOOR(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_SMALLINT",
                "select count(*) from NUMBER_TYPES where FLOOR(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_BIGINT",
                "select count(*) from NUMBER_TYPES where FLOOR(BIGNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_FLOAT",
                "select count(*) from NUMBER_TYPES where FLOOR(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_DECIMAL",
                "select count(*) from NUMBER_TYPES where FLOOR(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_POWER7X",
                "select POWER(7, INTEGERNUM), POWER(7, TINYNUM), POWER(7, SMALLNUM), POWER(7, BIGNUM), POWER(7, FLOATNUM), POWER(7, DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_POWER7X_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(7, INTEGERNUM)");
        project.addStmtProcedure("ORDER_POWER7X_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(7, TINYNUM)");
        project.addStmtProcedure("ORDER_POWER7X_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(7, SMALLNUM)");
        project.addStmtProcedure("ORDER_POWER7X_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(7, BIGNUM)");
        project.addStmtProcedure("ORDER_POWER7X_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(7, FLOATNUM)");
        project.addStmtProcedure("ORDER_POWER7X_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(7, DECIMALNUM)");

        project.addStmtProcedure("WHERE_POWER7X_INTEGER",
                "select count(*) from NUMBER_TYPES where POWER(7, INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_TINYINT",
                "select count(*) from NUMBER_TYPES where POWER(7, TINYNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_SMALLINT",
                "select count(*) from NUMBER_TYPES where POWER(7, SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_BIGINT",
                "select count(*) from NUMBER_TYPES where POWER(7, BIGNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_FLOAT",
                "select count(*) from NUMBER_TYPES where POWER(7, FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_DECIMAL",
                "select count(*) from NUMBER_TYPES where POWER(7, DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_POWER07X",
                "select POWER(0.7, INTEGERNUM), POWER(0.7, TINYNUM), POWER(0.7, SMALLNUM), POWER(0.7, BIGNUM), POWER(0.7, FLOATNUM), POWER(0.7, DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_POWER07X_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, INTEGERNUM)");
        project.addStmtProcedure("ORDER_POWER07X_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, TINYNUM)");
        project.addStmtProcedure("ORDER_POWER07X_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, SMALLNUM)");
        project.addStmtProcedure("ORDER_POWER07X_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, BIGNUM)");
        project.addStmtProcedure("ORDER_POWER07X_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, FLOATNUM)");
        project.addStmtProcedure("ORDER_POWER07X_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, DECIMALNUM)");

        project.addStmtProcedure("WHERE_POWER07X_INTEGER",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, INTEGERNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_TINYINT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, TINYNUM)   ) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_SMALLINT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, SMALLNUM)  ) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_BIGINT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, BIGNUM)    ) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_FLOAT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, FLOATNUM)  ) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_DECIMAL",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, DECIMALNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");

        project.addStmtProcedure("DISPLAY_POWERX7",
                "select POWER(INTEGERNUM, 7), POWER(TINYNUM, 7), POWER(SMALLNUM, 7), POWER(BIGNUM, 7), POWER(FLOATNUM, 7), POWER(DECIMALNUM, 7) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_POWERX7_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(INTEGERNUM, 7)");
        project.addStmtProcedure("ORDER_POWERX7_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(TINYNUM,    7)");
        project.addStmtProcedure("ORDER_POWERX7_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(SMALLNUM,   7)");
        project.addStmtProcedure("ORDER_POWERX7_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(BIGNUM,     7)");
        project.addStmtProcedure("ORDER_POWERX7_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(FLOATNUM,   7)");
        project.addStmtProcedure("ORDER_POWERX7_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(DECIMALNUM, 7)");

        project.addStmtProcedure("WHERE_POWERX7_INTEGER",
                "select count(*) from NUMBER_TYPES where POWER(INTEGERNUM, 7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_TINYINT",
                "select count(*) from NUMBER_TYPES where POWER(TINYNUM,    7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_SMALLINT",
                "select count(*) from NUMBER_TYPES where POWER(SMALLNUM,   7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_BIGINT",
                "select count(*) from NUMBER_TYPES where POWER(BIGNUM,     7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_FLOAT",
                "select count(*) from NUMBER_TYPES where POWER(FLOATNUM,   7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_DECIMAL",
                "select count(*) from NUMBER_TYPES where POWER(DECIMALNUM, 7) = ?");

        // These are intended for application to non-negative values.
        // Failure tests on negative values can be done separately, possibly via ad hoc.
        project.addStmtProcedure("DISPLAY_POWERX07",
                "select POWER(INTEGERNUM, 0.7), POWER(TINYNUM, 0.7), POWER(SMALLNUM, 0.7), POWER(BIGNUM, 0.7), POWER(FLOATNUM, 0.7), POWER(DECIMALNUM, 0.7) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_POWERX07_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(INTEGERNUM, 0.7)");
        project.addStmtProcedure("ORDER_POWERX07_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(TINYNUM,    0.7)");
        project.addStmtProcedure("ORDER_POWERX07_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(SMALLNUM,   0.7)");
        project.addStmtProcedure("ORDER_POWERX07_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(BIGNUM,     0.7)");
        project.addStmtProcedure("ORDER_POWERX07_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(FLOATNUM,   0.7)");
        project.addStmtProcedure("ORDER_POWERX07_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by POWER(DECIMALNUM, 0.7)");

        project.addStmtProcedure("WHERE_POWERX07_INTEGER",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(INTEGERNUM, 0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_TINYINT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(TINYNUM,    0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_SMALLINT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(SMALLNUM,   0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_BIGINT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(BIGNUM,     0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_FLOAT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(FLOATNUM,   0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_DECIMAL",
                "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(DECIMALNUM, 0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");

        project.addStmtProcedure("DISPLAY_RADIANS",
                "select RADIANS(INTEGERNUM), RADIANS(TINYNUM), RADIANS(SMALLNUM), RADIANS(BIGNUM), RADIANS(FLOATNUM), RADIANS(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_RADIANS_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by RADIANS(INTEGERNUM)");
        project.addStmtProcedure("ORDER_RADIANS_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by RADIANS(TINYNUM)");
        project.addStmtProcedure("ORDER_RADIANS_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by RADIANS(SMALLNUM)");
        project.addStmtProcedure("ORDER_RADIANS_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by RADIANS(BIGNUM)");
        project.addStmtProcedure("ORDER_RADIANS_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by RADIANS(FLOATNUM)");
        project.addStmtProcedure("ORDER_RADIANS_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by RADIANS(DECIMALNUM)");
        // These WHERE tests fails without the range in values so changed it like below.
        project.addStmtProcedure("WHERE_RADIANS_INTEGER",
                "select count(*) from NUMBER_TYPES where ((0.0000001+RADIANS(INTEGERNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_RADIANS_TINYINT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+RADIANS(TINYNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_RADIANS_SMALLINT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+RADIANS(SMALLNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_RADIANS_BIGINT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+RADIANS(BIGNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_RADIANS_FLOAT",
                "select count(*) from NUMBER_TYPES where ((0.0000001+RADIANS(FLOATNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_RADIANS_DECIMAL",
                "select count(*) from NUMBER_TYPES where ((0.0000001+RADIANS(DECIMALNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");

        // These are intended for application to non-negative values.
        // Failure tests on negative values can be done separately, possibly via ad hoc.
        project.addStmtProcedure("DISPLAY_SQRT",
                "select SQRT(INTEGERNUM), SQRT(TINYNUM), SQRT(SMALLNUM), SQRT(BIGNUM), SQRT(FLOATNUM), SQRT(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_SQRT_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by SQRT(INTEGERNUM)");
        project.addStmtProcedure("ORDER_SQRT_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by SQRT(TINYNUM)");
        project.addStmtProcedure("ORDER_SQRT_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by SQRT(SMALLNUM)");
        project.addStmtProcedure("ORDER_SQRT_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by SQRT(BIGNUM)");
        project.addStmtProcedure("ORDER_SQRT_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by SQRT(FLOATNUM)");
        project.addStmtProcedure("ORDER_SQRT_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by SQRT(DECIMALNUM)");

        project.addStmtProcedure("WHERE_SQRT_INTEGER",
                "select count(*) from NUMBER_TYPES where SQRT(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_TINYINT",
                "select count(*) from NUMBER_TYPES where SQRT(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_SMALLINT",
                "select count(*) from NUMBER_TYPES where SQRT(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_BIGINT",
                "select count(*) from NUMBER_TYPES where SQRT(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_FLOAT",
                "select count(*) from NUMBER_TYPES where SQRT(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_DECIMAL",
                "select count(*) from NUMBER_TYPES where SQRT(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_SIN",
                "select SIN(INTEGERNUM), SIN(TINYNUM), SIN(SMALLNUM), SIN(BIGNUM), SIN(FLOATNUM), SIN(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");
        project.addStmtProcedure("DISPLAY_COS",
                "select COS(INTEGERNUM), COS(TINYNUM), COS(SMALLNUM), COS(BIGNUM), COS(FLOATNUM), COS(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");
        project.addStmtProcedure("DISPLAY_TAN",
                "select TAN(INTEGERNUM), TAN(TINYNUM), TAN(SMALLNUM), TAN(BIGNUM), TAN(FLOATNUM), TAN(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");
        project.addStmtProcedure("DISPLAY_COT",
                "select COT(INTEGERNUM), COT(TINYNUM), COT(SMALLNUM), COT(BIGNUM), COT(FLOATNUM), COT(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");
        project.addStmtProcedure("DISPLAY_SEC",
                "select SEC(INTEGERNUM), SEC(TINYNUM), SEC(SMALLNUM), SEC(BIGNUM), SEC(FLOATNUM), SEC(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");
        project.addStmtProcedure("DISPLAY_CSC",
                "select CSC(INTEGERNUM), CSC(TINYNUM), CSC(SMALLNUM), CSC(BIGNUM), CSC(FLOATNUM), CSC(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("DISPLAY_LN",
                "select LN(INTEGERNUM), LN(TINYNUM), LN(SMALLNUM), LN(BIGNUM), LN(FLOATNUM), LN(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_LN_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by LN(INTEGERNUM)");
        project.addStmtProcedure("ORDER_LN_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by LN(TINYNUM)");
        project.addStmtProcedure("ORDER_LN_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by LN(SMALLNUM)");
        project.addStmtProcedure("ORDER_LN_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by LN(BIGNUM)");
        project.addStmtProcedure("ORDER_LN_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by LN(FLOATNUM)");
        project.addStmtProcedure("ORDER_LN_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by LN(DECIMALNUM)");

        project.addStmtProcedure("WHERE_LN_INTEGER",
                "select count(*) from NUMBER_TYPES where LN(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_LN_TINYINT",
                "select count(*) from NUMBER_TYPES where LN(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_LN_SMALLINT",
                "select count(*) from NUMBER_TYPES where LN(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_LN_BIGINT",
                "select count(*) from NUMBER_TYPES where LN(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_LN_FLOAT",
                "select count(*) from NUMBER_TYPES where LN(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_LN_DECIMAL",
                "select count(*) from NUMBER_TYPES where LN(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_LOG",
                "select LOG(INTEGERNUM), LOG(TINYNUM), LOG(SMALLNUM), LOG(BIGNUM), LOG(FLOATNUM), LOG(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_LOG_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by LOG(INTEGERNUM)");
        project.addStmtProcedure("ORDER_LOG_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by LOG(TINYNUM)");
        project.addStmtProcedure("ORDER_LOG_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by LOG(SMALLNUM)");
        project.addStmtProcedure("ORDER_LOG_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by LOG(BIGNUM)");
        project.addStmtProcedure("ORDER_LOG_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by LOG(FLOATNUM)");
        project.addStmtProcedure("ORDER_LOG_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by LOG(DECIMALNUM)");

        project.addStmtProcedure("WHERE_LOG_INTEGER",
                "select count(*) from NUMBER_TYPES where LOG(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_LOG_TINYINT",
                "select count(*) from NUMBER_TYPES where LOG(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_LOG_SMALLINT",
                "select count(*) from NUMBER_TYPES where LOG(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_LOG_BIGINT",
                "select count(*) from NUMBER_TYPES where LOG(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_LOG_FLOAT",
                "select count(*) from NUMBER_TYPES where LOG(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_LOG_DECIMAL",
                "select count(*) from NUMBER_TYPES where LOG(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_LOG10",
                "select LOG10(INTEGERNUM), LOG10(TINYNUM), LOG10(SMALLNUM), LOG10(BIGNUM), LOG10(FLOATNUM), LOG10(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");
        project.addStmtProcedure("ORDER_LOG10_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by LOG10(INTEGERNUM)");
        project.addStmtProcedure("ORDER_LOG10_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by LOG10(TINYNUM)");
        project.addStmtProcedure("ORDER_LOG10_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by LOG10(SMALLNUM)");
        project.addStmtProcedure("ORDER_LOG10_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by LOG10(BIGNUM)");
        project.addStmtProcedure("ORDER_LOG10_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by LOG10(FLOATNUM)");
        project.addStmtProcedure("ORDER_LOG10_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by LOG10(DECIMALNUM)");

        project.addStmtProcedure("WHERE_LOG10_INTEGER",
                "select count(*) from NUMBER_TYPES where LOG10(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_LOG10_TINYINT",
                "select count(*) from NUMBER_TYPES where LOG10(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_LOG10_SMALLINT",
                "select count(*) from NUMBER_TYPES where LOG10(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_LOG10_BIGINT",
                "select count(*) from NUMBER_TYPES where LOG10(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_LOG10_FLOAT",
                "select count(*) from NUMBER_TYPES where LOG10(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_LOG10_DECIMAL",
                "select count(*) from NUMBER_TYPES where LOG10(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_INTEGER",
                "select CAST(INTEGERNUM AS INTEGER), CAST(TINYNUM AS INTEGER), CAST(SMALLNUM AS INTEGER), CAST(BIGNUM AS INTEGER), CAST(FLOATNUM AS INTEGER), CAST(DECIMALNUM AS INTEGER) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_INTEGER_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS INTEGER)");
        project.addStmtProcedure("ORDER_INTEGER_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS INTEGER)");
        project.addStmtProcedure("ORDER_INTEGER_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS INTEGER)");
        project.addStmtProcedure("ORDER_INTEGER_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS INTEGER)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_INTEGER_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS INTEGER), INTEGERNUM");
        project.addStmtProcedure("ORDER_INTEGER_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS INTEGER), INTEGERNUM");

        project.addStmtProcedure("WHERE_INTEGER_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS INTEGER) = ?");

        project.addStmtProcedure("DISPLAY_TINYINT",
                "select CAST(INTEGERNUM AS TINYINT), CAST(TINYNUM AS TINYINT), CAST(SMALLNUM AS TINYINT), CAST(BIGNUM AS TINYINT), CAST(FLOATNUM AS TINYINT), CAST(DECIMALNUM AS TINYINT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_TINYINT_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS TINYINT)");
        project.addStmtProcedure("ORDER_TINYINT_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS TINYINT)");
        project.addStmtProcedure("ORDER_TINYINT_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS TINYINT)");
        project.addStmtProcedure("ORDER_TINYINT_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS TINYINT)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_TINYINT_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS TINYINT), INTEGERNUM");
        project.addStmtProcedure("ORDER_TINYINT_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS TINYINT), INTEGERNUM");

        project.addStmtProcedure("WHERE_TINYINT_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS TINYINT) = ?");

        project.addStmtProcedure("DISPLAY_SMALLINT",
                "select CAST(INTEGERNUM AS SMALLINT), CAST(TINYNUM AS SMALLINT), CAST(SMALLNUM AS SMALLINT), CAST(BIGNUM AS SMALLINT), CAST(FLOATNUM AS SMALLINT), CAST(DECIMALNUM AS SMALLINT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_SMALLINT_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS SMALLINT)");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS SMALLINT)");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS SMALLINT)");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS SMALLINT)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_SMALLINT_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS SMALLINT), INTEGERNUM");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS SMALLINT), INTEGERNUM");

        project.addStmtProcedure("WHERE_SMALLINT_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS SMALLINT) = ?");

        project.addStmtProcedure("DISPLAY_BIGINT",
                "select CAST(INTEGERNUM AS BIGINT), CAST(TINYNUM AS BIGINT), CAST(SMALLNUM AS BIGINT), CAST(BIGNUM AS BIGINT), CAST(FLOATNUM AS BIGINT), CAST(DECIMALNUM AS BIGINT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_BIGINT_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS BIGINT)");
        project.addStmtProcedure("ORDER_BIGINT_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS BIGINT)");
        project.addStmtProcedure("ORDER_BIGINT_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS BIGINT)");
        project.addStmtProcedure("ORDER_BIGINT_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS BIGINT)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_BIGINT_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS BIGINT), INTEGERNUM");
        project.addStmtProcedure("ORDER_BIGINT_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS BIGINT), INTEGERNUM");

        project.addStmtProcedure("WHERE_BIGINT_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS BIGINT) = ?");

        project.addStmtProcedure("DISPLAY_FLOAT",
                "select CAST(INTEGERNUM AS FLOAT), CAST(TINYNUM AS FLOAT), CAST(SMALLNUM AS FLOAT), CAST(BIGNUM AS FLOAT), CAST(FLOATNUM AS FLOAT), CAST(DECIMALNUM AS FLOAT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_FLOAT_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS FLOAT)");

        project.addStmtProcedure("WHERE_FLOAT_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS FLOAT) = ?");

        project.addStmtProcedure("DISPLAY_DECIMAL",
                "select CAST(INTEGERNUM AS DECIMAL), CAST(TINYNUM AS DECIMAL), CAST(SMALLNUM AS DECIMAL), CAST(BIGNUM AS DECIMAL), CAST(FLOATNUM AS DECIMAL), CAST(DECIMALNUM AS DECIMAL) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_DECIMAL_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS DECIMAL)");

        project.addStmtProcedure("WHERE_DECIMAL_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS DECIMAL) = ?");

        project.addStmtProcedure("DISPLAY_SUBSTRING",
                "select SUBSTRING (DESC FROM 2) from P1 where ID = -12");
        project.addStmtProcedure("DISPLAY_SUBSTRING2",
                "select SUBSTRING (DESC FROM 2 FOR 2) from P1 where ID = -12");

        project.addStmtProcedure("EXTRACT_TIMESTAMP",
                "select EXTRACT(YEAR FROM PAST), EXTRACT(MONTH FROM PAST), EXTRACT(DAY FROM PAST), " +
                        "EXTRACT(DAY_OF_WEEK FROM PAST), EXTRACT(DAY_OF_MONTH FROM PAST), EXTRACT(DAY_OF_YEAR FROM PAST), EXTRACT(QUARTER FROM PAST), " +
                        "EXTRACT(HOUR FROM PAST), EXTRACT(MINUTE FROM PAST), EXTRACT(SECOND FROM PAST), EXTRACT(WEEK_OF_YEAR FROM PAST), " +
                        "EXTRACT(WEEK FROM PAST), EXTRACT(WEEKDAY FROM PAST) from P1 where ID = ?");

        // Test that commas work just like keyword separators
        project.addStmtProcedure("ALT_EXTRACT_TIMESTAMP",
                "select EXTRACT(YEAR, PAST), EXTRACT(MONTH, PAST), EXTRACT(DAY, PAST), " +
                        "EXTRACT(DAY_OF_WEEK, PAST), EXTRACT(DAY_OF_MONTH, PAST), EXTRACT(DAY_OF_YEAR, PAST), EXTRACT(QUARTER, PAST), " +
                        "EXTRACT(HOUR, PAST), EXTRACT(MINUTE, PAST), EXTRACT(SECOND, PAST), EXTRACT(WEEK_OF_YEAR, PAST), " +
                        "EXTRACT(WEEK, PAST), EXTRACT(WEEKDAY, PAST) from P1 where ID = ?");

        project.addStmtProcedure("VERIFY_TIMESTAMP_STRING_EQ",
                "select PAST, CAST(DESC AS TIMESTAMP), DESC, CAST(PAST AS VARCHAR) from R2 " +
                        "where PAST <> CAST(DESC AS TIMESTAMP)");

        project.addStmtProcedure("VERIFY_STRING_TIMESTAMP_EQ",
                "select PAST, CAST(DESC AS TIMESTAMP), DESC, CAST(PAST AS VARCHAR) from R2 " +
                        "where DESC <> CAST(PAST AS VARCHAR)");

        project.addStmtProcedure("DUMP_TIMESTAMP_STRING_PATHS",
                "select PAST, CAST(DESC AS TIMESTAMP), DESC, CAST(PAST AS VARCHAR) from R2");



        project.addStmtProcedure("DISPLAY_VARCHAR",
                "select CAST(INTEGERNUM AS VARCHAR), CAST(TINYNUM AS VARCHAR), CAST(SMALLNUM AS VARCHAR), CAST(BIGNUM AS VARCHAR), CAST(FLOATNUM AS VARCHAR), CAST(DECIMALNUM AS VARCHAR) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_VARCHAR_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS VARCHAR)");

        project.addStmtProcedure("WHERE_VARCHAR_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS VARCHAR) = ?");

        project.addStmtProcedure("ORDER_SUBSTRING", "select ID+15 from P1 order by SUBSTRING (DESC FROM 2)");

        project.addStmtProcedure("WHERE_SUBSTRING2",
                "select count(*) from P1 " +
                        "where SUBSTRING (DESC FROM 2) > '12"+paddedToNonInlineLength+"'");
        // Test that commas work just like keyword separators
        project.addStmtProcedure("ALT_WHERE_SUBSTRING2",
                "select count(*) from P1 " +
                        "where SUBSTRING (DESC, 2) > '12"+paddedToNonInlineLength+"'");

        project.addStmtProcedure("WHERE_SUBSTRING3",
                "select count(*) from P1 " +
                        "where not SUBSTRING (DESC FROM 2 FOR 1) > '13'");
        // Test that commas work just like keyword separators
        project.addStmtProcedure("ALT_WHERE_SUBSTRING3",
                "select count(*) from P1 " +
                        "where not SUBSTRING (DESC, 2, 1) > '13'");

        // Test GROUP BY by support
        project.addStmtProcedure("AGG_OF_SUBSTRING",
                "select MIN(SUBSTRING (DESC FROM 2)) from P1 where ID < -7");
        project.addStmtProcedure("AGG_OF_SUBSTRING2",
                "select MIN(SUBSTRING (DESC FROM 2 FOR 2)) from P1 where ID < -7");

        // Test parameterizing functions
        // next one disabled until ENG-3486
        //project.addStmtProcedure("PARAM_SUBSTRING", "select SUBSTRING(? FROM 2) from P1");
        project.addStmtProcedure("PARAM_ABS", "select ABS(? + NUM) from P1");

        project.addStmtProcedure("LEFT", "select id, LEFT(DESC,?) from P1 where id = ?");
        project.addStmtProcedure("RIGHT", "select id, RIGHT(DESC,?) from P1 where id = ?");
        project.addStmtProcedure("SPACE", "select id, SPACE(?) from P1 where id = ?");
        project.addStmtProcedure("LOWER_UPPER", "select id, LOWER(DESC), UPPER(DESC) from P1 where id = ?");

        project.addStmtProcedure("TRIM_SPACE", "select id, LTRIM(DESC), TRIM(LEADING ' ' FROM DESC), " +
                "RTRIM(DESC), TRIM(TRAILING ' ' FROM DESC), TRIM(DESC), TRIM(BOTH ' ' FROM DESC) from P1 where id = ?");
        // Test that commas work just like keyword separators
        project.addStmtProcedure("ALT_TRIM_SPACE", "select id, TRIM(LEADING FROM DESC), TRIM(LEADING, ' ', DESC), " +
                "TRIM(TRAILING, DESC), TRIM(TRAILING, ' ', DESC), TRIM(BOTH, DESC), TRIM(BOTH, ' ' , DESC) from P1 where id = ?");
        project.addStmtProcedure("TRIM_ANY", "select id, TRIM(LEADING ? FROM DESC), TRIM(TRAILING ? FROM DESC), " +
                "TRIM(BOTH ? FROM DESC) from P1 where id = ?");
        // Test that commas work just like keyword separators
        project.addStmtProcedure("ALT_TRIM_ANY", "select id, TRIM(LEADING, ?, DESC), TRIM(TRAILING, ?, DESC), " +
                "TRIM(BOTH, ?, DESC) from P1 where id = ?");

        project.addStmtProcedure("REPEAT", "select id, REPEAT(DESC,?) from P1 where id = ?");
        project.addStmtProcedure("REPLACE", "select id, REPLACE(DESC,?, ?) from P1 where id = ?");
        project.addStmtProcedure("OVERLAY", "select id, OVERLAY(DESC PLACING ? FROM ? FOR ?) from P1 where id = ?");
        // Test that commas work just like keyword separators
        project.addStmtProcedure("ALT_OVERLAY", "select id, OVERLAY(DESC, ?, ?, ?) from P1 where id = ?");
        project.addStmtProcedure("OVERLAY_FULL_LENGTH",
                "select id, OVERLAY(DESC PLACING ? FROM ?) from P1 where id = ?");
        // Test that commas work just like keyword separators
        project.addStmtProcedure("ALT_OVERLAY_FULL_LENGTH", "select id, OVERLAY(DESC, ?, ?) from P1 where id = ?");

        project.addStmtProcedure("CHAR", "select id, CHAR(?) from P1 where id = ?");

        project.addStmtProcedure("INSERT_NULL", "insert into P1 values (?, null, null, null, null)");
        // project.addStmtProcedure("UPS", "select count(*) from P1 where UPPER(DESC) > 'L'");

        project.addStmtProcedure("TEST_SUBSTRING_INPROC",
                "SELECT * FROM INLINED_VC_VB_TABLE WHERE ABS(?) > 1 AND SUBSTRING(CAST(? AS VARCHAR),1,3) = 'str';");

        // CONFIG #1: Local Site/Partitions running on JNI backend
        VoltServerConfig config = new LocalCluster(
                "fixedsql-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestFunctionsSuite2.class);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL
        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        // no clustering tests for functions

        return builder;
    }
}
