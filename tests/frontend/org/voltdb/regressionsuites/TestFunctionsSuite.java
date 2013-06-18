/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestFunctionsSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    // Padding used to purposely exercise non-inline strings.
    private static final String paddedToNonInlineLength =
        "will you still free me (will memcheck see me) when Im sixty-four";

    // Test some false alarm cases in HSQLBackend that were interfering with sqlcoverage.
    public void testFoundHSQLBackendOutOfRange() throws IOException, InterruptedException, ProcCallException {
        System.out.println("STARTING testFoundHSQLBackendOutOfRange");
        Client client = getClient();
        ClientResponse cr = null;
        /*
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PAST TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
        */

        client.callProcedure("@AdHoc", "INSERT INTO P1 VALUES (0, 'wEoiXIuJwSIKBujWv', -405636, 1.38145922788945552107e-01, NULL)");
        client.callProcedure("@AdHoc", "INSERT INTO P1 VALUES (2, 'wEoiXIuJwSIKBujWv', -29914, 8.98500019539639316335e-01, NULL)");
        client.callProcedure("@AdHoc", "INSERT INTO P1 VALUES (4, 'WCfDDvZBPoqhanfGN', -1309657, 9.34160160574919795629e-01, NULL)");
        client.callProcedure("@AdHoc", "INSERT INTO P1 VALUES (6, 'WCfDDvZBPoqhanfGN', 1414568, 1.14383710279231887164e-01, NULL)");
        cr = client.callProcedure("@AdHoc", "select (5.25 + NUM) from P1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc", "SELECT FLOOR(NUM + 5.25) NUMSUM FROM P1 ORDER BY NUMSUM");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // This test case requires HSQL to be taught to do (truncating) integer division of integers as VoltDB does.
        // While not strictly required by the SQL standard, integer division is at least technically compliant,
        // where HSQL's use of floating point division is not.
        // cr = client.callProcedure("@AdHoc", "SELECT SUM(DISTINCT SQRT(ID / (NUM))) AS Q22 FROM P1");
        // assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    public void testStringExpressionIndex() throws Exception {
        System.out.println("STARTING testStringExpressionIndex");
        Client client = getClient();
        initialLoad(client, "P1");

        ClientResponse cr = null;
        VoltTable result = null;
        /*
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PAST TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
                // Test generalized indexes on a string function and combos.
                "CREATE INDEX P1_SUBSTRING_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2) ); " +
                "CREATE INDEX P1_SUBSTRING_WITH_COL_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), DESC ); " +
                "CREATE INDEX P1_NUM_EXPR_WITH_STRING_COL ON P1 ( DESC, ABS(ID) ); " +
                "CREATE INDEX P1_MIXED_TYPE_EXPRS1 ON P1 ( ABS(ID+2), SUBSTRING(DESC FROM 1 FOR 2) ); " +
                "CREATE INDEX P1_MIXED_TYPE_EXPRS2 ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), ABS(ID+2) ); " +
        */

        // Do some rudimentary indexed queries -- the real challenge to string expression indexes is defining and loading/maintaining them.
        // TODO: For that reason, it might make sense to break them out into their own suite to make their specific issues easier to isolate.
        cr = client.callProcedure("@AdHoc", "select ID from P1 where SUBSTRING(DESC FROM 1 for 2) = 'X1' and ABS(ID+2) > 7 order by NUM, ID");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(5, result.getRowCount());

        VoltTable r;
        long resultA;
        long resultB;

        // Filters intended to be close enough to bring two different indexes to the same result as no index at all.
        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(ID+3) = 7 order by NUM, ID");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where SUBSTRING(DESC FROM 1 for 2) >= 'X1' and ABS(ID+2) = 8");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where SUBSTRING(DESC FROM 1 for 2) = 'X1' and ABS(ID+2) > 7 and ABS(ID+2) < 9");
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
        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where SUBSTRING(DESC FROM 1 for 2) >= 'X1' and ABS(ID+2) = 8");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where SUBSTRING(DESC FROM 1 for 2) = 'X1' and ABS(ID+2) > 7 and ABS(ID+2) < 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

}

    public void testNumericExpressionIndex() throws Exception {
        System.out.println("STARTING testNumericExpressionIndex");
        Client client = getClient();
        initialLoad(client, "R1");

        ClientResponse cr = null;
        VoltTable result = null;
        /*
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PAST TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
                // Test generalized index on a function of a non-indexed column.
                "CREATE INDEX P1_ABS_NUM ON P1 ( ABS(NUM) ); " +
                // Test generalized index on an expression of multiple columns.
                "CREATE INDEX P1_ABS_ID_PLUS_NUM ON P1 ( ABS(ID) + NUM ); " +
                // Test generalized index on a string function.
                // "CREATE INDEX P1_SUBSTRING_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2) ); " +
                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PAST TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
                // Test unique generalized index on a function of an already indexed column.
                "CREATE UNIQUE INDEX R1_ABS_ID ON R1 ( ABS(ID) ); " +
                // Test generalized expression index with a constant argument.
                "CREATE INDEX R1_ABS_ID_SCALED ON R1 ( ID / 3 ); " +
        */

        cr = client.callProcedure("@AdHoc", "select ID from R1 where ABS(ID) = 9 and DESC > 'XYZ' order by ID");
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

    public void testAbsWithLimit_ENG3572() throws Exception
    {
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
        ClientResponse cr = null;
        cr = client.callProcedure("@AdHoc", "select abs(NUM) from P1 where ID = 0 limit 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    public void testAbs() throws Exception
    {
        System.out.println("STARTING testAbs");
        Client client = getClient();
        initialLoad(client, "P1");

        ClientResponse cr = null;
        VoltTable r = null;

        // The next two queries used to fail due to ENG-3913,
        // abuse of compound indexes for partial GT filters.
        // An old issue only brought to light by the addition of a compound index to this suite.
        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(ID) > 9");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong()); // used to get 6, matching like >=

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

        boolean caught = false;

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(DESC) > 9");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(DESC) > 'ABC'");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

        cr = client.callProcedure("@AdHoc", "insert into R1 values (1, null, null, null, null)");

        caught = false;
        try {
            // This should violate the UNIQUE ABS constraint without violating the primary key constraint.
            cr = client.callProcedure("@AdHoc", "insert into R1 values (-1, null, null, null, null)");
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("violation of constraint") != -1);
            caught = true;
        }
        // If the insert succeeds on VoltDB, the constraint failed to trigger.
        // If the insert fails on HSQL, the test is invalid -- HSQL should not detect the subtle constraint violation we are trying to trigger.
        assertEquals( ! isHSQL(), caught);
    }

    public void testSubstring() throws Exception
    {
        System.out.println("STARTING testSubstring");
        Client client = getClient();
        initialLoad(client, "P1");

        ClientResponse cr = null;
        VoltTable r = null;

        // test where support
        cr = client.callProcedure("WHERE_SUBSTRING2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

        cr = client.callProcedure("WHERE_SUBSTRING3");
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

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where SUBSTRING (DESC FROM 2) >= '11'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not DESC >= 'X12'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING( DESC FROM 2) >= '12'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where DESC >= 'X2'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where SUBSTRING(DESC FROM 2 FOR 1) >= '2'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING( SUBSTRING (DESC FROM 2) FROM 1 FOR 1) < '2'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        boolean caught = false;

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING( DESC FROM 2) > 9");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING (1 FROM 2) > 9");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING (1 FROM DESC) > '9'");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING (DESC FROM DESC) > 'ABC'");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

    }

    public void testExtract() throws Exception
    {
        System.out.println("STARTING testExtract");
        Client client = getClient();
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException("Failed with response: " + clientResponse.getStatusString());
                }
            }
        };
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
        client.callProcedure(callback, "P1.insert", 0, "X0", 10, 1.1, new Timestamp(1000000000000L));
        client.drain();
        ClientResponse cr = null;
        VoltTable r = null;
        long result;

        cr = client.callProcedure("@AdHoc",
                                  "select " +
                                  "EXTRACT(YEAR FROM PAST), EXTRACT(MONTH FROM PAST), EXTRACT(DAY FROM PAST), " +
                                  "EXTRACT(DAY_OF_WEEK FROM PAST), EXTRACT(DAY_OF_YEAR FROM PAST), " +
                                  //"EXTRACT(WEEK_OF_YEAR FROM PAST), " +
                                  "EXTRACT(QUARTER FROM PAST), EXTRACT(HOUR FROM PAST), EXTRACT(MINUTE FROM PAST), EXTRACT(SECOND FROM PAST) " +
                                  "from P1;");

        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        int columnIndex = 0;

        final int EXPECTED_YEAR = 2001;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_YEAR, result);

        final int EXPECTED_MONTH = 9;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MONTH, result);

        final int EXPECTED_DAY = 9;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DAY, result);

        final int EXPECTED_DOW = 1;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOW, result);

        final int EXPECTED_DOY = 252;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOY, result);

//        final int EXPECTED_WOY = 36;
//        result = r.getLong(columnIndex++);
//        assertEquals(EXPECTED_WOY, result);

        final int EXPECTED_QUARTER = 3;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_QUARTER, result);

        final int EXPECTED_HOUR = 1;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_HOUR, result);

        final int EXPECTED_MINUTE = 46;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MINUTE, result);

        final BigDecimal EXPECTED_SECONDS = new BigDecimal("40.000000000000");
        BigDecimal decimalResult = r.getDecimalAsBigDecimal(columnIndex++);
        assertEquals(EXPECTED_SECONDS, decimalResult);

    }

    public void testParams() throws NoConnectionsException, IOException, ProcCallException {
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
    static
    {
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

    private static void insertNumbers(Client client, double[] rawData, int nRaws) throws Exception
    {
        client.callProcedure("@AdHoc", "delete from NUMBER_TYPES;");
        // Insert non-negative or all input values with and without whole number and fractional parts.
        for (int kk = 0; kk < nRaws; kk += COLUMNCOUNT) {
            client.callProcedure("NUMBER_TYPES.insert", (int)rawData[kk+0], (int)rawData[kk+1], (int)rawData[kk+2], (int)rawData[kk+3], rawData[kk+FLOATCOLINDEX], String.valueOf(rawData[kk+DECIMALCOLINDEX]));
        }
    }

    private static double normalizeZero(double result) { return (result == -0.0) ? 0.0 : result; }

    private static class FunctionTestCase
    {
        public final String m_case;
        public final double m_result;
        public final double m_filter;

        public FunctionTestCase(String proc, double result)
        {
            m_case = proc;
            m_filter = 0.0;
            // Believe it or not, "negative 0" results were causing problems.
            m_result = normalizeZero(result);
        }
        public FunctionTestCase(String proc, double filter, long result)
        {
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
    private double getColumnValue(String expectedFormat, VoltTable result, int jj)
    {
        double value;
        if (expectedFormat == null) {
            if (jj < FLOATCOLINDEX) {
                value = result.getLong(jj);
            }
            else if (jj == FLOATCOLINDEX) {
                value = result.getDouble(jj);
            }
            else {
                value = result.getDecimalAsBigDecimal(jj).doubleValue();
            }
        }
        else if (expectedFormat.equals("LONG")) {
            value = result.getLong(jj);
        }
        else if (expectedFormat.equals("DOUBLE")) {
            value = result.getDouble(jj);
        }
        else {
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
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    private static ClientResponse callWithParameter(String expectedFormat, Client client, String proc, int jj, double filter)
            throws IOException, NoConnectionsException, ProcCallException
    {
        ClientResponse cr;
        if (expectedFormat == null) {
            if (jj < FLOATCOLINDEX) {
                cr = client.callProcedure(proc, (int)filter);
            }
            else if (jj == FLOATCOLINDEX) {
                cr = client.callProcedure(proc, filter);
            } else {
                cr = client.callProcedure(proc, BigDecimal.valueOf(filter));
            }
        }
        else if (expectedFormat.equals("LONG")) {
            cr = client.callProcedure(proc, (int)filter);
        }
        else if (expectedFormat.equals("DOUBLE")) {
            cr = client.callProcedure(proc, filter);
        }
        else {
            cr = client.callProcedure(proc, BigDecimal.valueOf(filter));
        }
        return cr;
    }

    private FunctionTestCase[] displayFunctionRun(Client client, String fname, int rowCount, String expectedFormat) throws Exception
    {
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

    private FunctionTestCase[] orderFunctionRun(Client client, String fname, int rowCount) throws Exception
    {
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
                    }
                    else if (result.getColumnType(kk) == VoltType.DECIMAL) {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getDecimalAsBigDecimal(kk));
                    }
                    else if (result.getColumnType(kk) == VoltType.STRING) {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getString(kk));
                    }
                    else {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getLong(kk));
                    }
                }
                ++jj;
            }
        }
        return resultSet;
    }

    private FunctionTestCase[] whereFunctionRun(Client client, String fname, Set<Double> filters, String expectedFormat) throws Exception
    {
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
    private static void complain(String complaint)
    {
        System.out.println("Complaint #" + complaintCount + ": " + complaint);
        ++complaintCount; // NICE PLACE FOR A BREAKPOINT.
    }

    static String formatForFuzziness = "%14e";
    private void functionTest(String fname, double rawData[], double[] resultValues, Set<Double> filters,
                              boolean monotonic, boolean ascending, String expectedFormat) throws Exception
    {
        System.out.println("STARTING test of " + fname);

        Client client = getClient();
        insertNumbers(client, rawData, resultValues.length);

        FunctionTestCase[] results;

        results = displayFunctionRun(client, fname, resultValues.length / COLUMNCOUNT, expectedFormat);

        assertEquals(results.length, resultValues.length);

        Map<String, Integer> valueBag = new HashMap<String, Integer>();
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
                complain(result.m_case + " value " + result.m_filter + " not expected or previously depleted from " + valueBag + ".");
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

        System.out.println("ENDING test of " + fname);
    }

    public void testManyNumericFunctions() throws Exception
    {
        subtestFromVarCharCasts();
        subtestToVarCharCasts();
        subtestNumericCasts();
        subtestCeiling();
        subtestExp();
        subtestFloor();
        subtestPowerx7();
        subtestPowerx07();
        subtestPower7x();
        subtestPower07x();
        subtestSqrt();
    }

    public void subtestCeiling() throws Exception
    {
        String fname = "CEILING";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<Double>();
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

    public void subtestExp() throws Exception
    {
        String fname = "EXP";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<Double>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.exp(values[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    public void subtestFloor() throws Exception
    {
        String fname = "FLOOR";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<Double>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.floor(values[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = null;
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);

    }

    public void subtestPowerx7() throws Exception
    {
        final String fname = "POWERX7";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<Double>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.pow(values[kk], 7.0);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    public void subtestPowerx07() throws Exception
    {
        final String fname = "POWERX07";
        final double[] resultValues = new double[nonnegnonzeros.length];
        final Set<Double> filters = new HashSet<Double>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.pow(nonnegnonzeros[kk], 0.7);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, nonnegnonzeros, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    public void subtestPower7x() throws Exception
    {
        final String fname = "POWER7X";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<Double>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.pow(7.0, values[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    public void subtestPower07x() throws Exception
    {
        final String fname = "POWER07X";
        final double[] resultValues = new double[values.length];
        final Set<Double> filters = new HashSet<Double>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.pow(0.7, values[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = false;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, values, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    public void subtestSqrt() throws Exception
    {
        final String fname = "SQRT";
        final double[] resultValues = new double[nonnegs.length];
        final Set<Double> filters = new HashSet<Double>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = Math.sqrt(nonnegs[kk]);
            filters.add(resultValues[kk]);
        }
        final boolean monotonic = true;
        final boolean ascending = true;
        final String expectedFormat = "DOUBLE";
        functionTest(fname, nonnegs, resultValues, filters, monotonic, ascending, expectedFormat);
    }

    private static class FunctionVarCharTestCase
    {
        public final String m_case;
        public final String m_filter;
        public final long m_result;

        public FunctionVarCharTestCase(String proc, String filter)
        {
            m_case = proc;
            m_filter = filter;
            m_result = 0;
        }
        public FunctionVarCharTestCase(String proc, String filter, long l)
        {
            m_case = proc;
            m_filter = filter;
            m_result = l;
        }
    };

    private FunctionVarCharTestCase[] displayVarCharCastRun(Client client, int rowCount) throws Exception
    {
        ClientResponse cr;
        VoltTable result;

        FunctionVarCharTestCase[] resultSet = new FunctionVarCharTestCase[numTypeNames.length * rowCount];
        int ii = 0;

        String proc = "DISPLAY_VARCHAR";
        cr = client.callProcedure(proc);
        result = cr.getResults()[0];
        assertEquals(rowCount, result.getRowCount());
        while (result.advanceRow()) {
            int jj = 0;
            for (String numTypeName : numTypeNames) {
                String tooSimple = result.getString(jj);
                String value = Double.valueOf(tooSimple).toString();
                //*VERBOSIFY TO DEBUG:*/ System.out.println("DEBUG " + proc + " " + numTypeName + " GOT " + tooSimple + " into " + value);
                resultSet[ii++] = new FunctionVarCharTestCase(proc + " " + numTypeName, value);
                ++jj;
            }
        }
        return resultSet;
    }

    private FunctionVarCharTestCase[] whereVarCharCastRun(Client client, Set<String> filters) throws Exception
    {
        ClientResponse cr;
        VoltTable result;

        FunctionVarCharTestCase[] resultSet = new FunctionVarCharTestCase[numTypeNames.length * filters.size()];
        int kk = 0;
        int jj = 0;
        for (String numTypeName : numTypeNames) {
            for (String filter : filters) {
                String proc = "WHERE_VARCHAR_CAST_" + numTypeName;
                String param = filter;
                String[] decimalParts = filter.split("\\.");
                if (jj < FLOATCOLINDEX) {
                    // Truncate an integer decimal before the decimal point to match an integer column.
                    if (decimalParts.length < 2 || decimalParts[1].equals("0")) {
                        param = decimalParts[0];
                    }
                    // Else fall through to pass a fractional decimal as it is
                    // to purposely force a mismatch with an integer column.
                }
                else if (jj > FLOATCOLINDEX) {
                    // Pad the decimal string.
                    if (decimalParts.length < 2 || decimalParts[1].equals("0")) {
                        param = decimalParts[0] + ".000000000000";
                    }
                    else {
                        param = decimalParts[0] + "." + (decimalParts[1] + "000000000000").substring(0,12);
                    }
                }
                // Handle float-to-string cast formatting
                // TODO: this code may not be right for multiples of 10 or decimals of magnitude < .1
                // which we don't happen to be using currently to drive this numeric test framework.
                else {
                    if (decimalParts.length < 2 || decimalParts[1].equals("0")) {
                        if (decimalParts[0].equals("0")) {
                            param = "0E0";
                        }
                        else {
                            int signedDigitWidth = (decimalParts[0].charAt(0) == '-') ? 2 : 1;
                            param = decimalParts[0].substring(0, signedDigitWidth) +
                                    "." + decimalParts[0].substring(signedDigitWidth) +
                                    "E" + (decimalParts[0].length() - signedDigitWidth);
                        }
                    }
                    else {
                        if (decimalParts[0].equals("0")) {
                            param = decimalParts[1].substring(0, 1) +
                                    "." + decimalParts[1].substring(1) + "E-1";
                        }
                        else if (decimalParts[0].equals("-0") ) {
                            param = "-" + decimalParts[1].substring(0, 1) +
                                    "." + decimalParts[1].substring(1) + "E-1";
                        }
                        else {
                            int signedDigitWidth = (decimalParts[0].charAt(0) == '-') ? 2 : 1;
                            param = decimalParts[0].substring(0, signedDigitWidth) +
                                    "." + decimalParts[0].substring(signedDigitWidth) + decimalParts[1] +
                                    "E" + (decimalParts[0].length() - signedDigitWidth);
                        }
                    }
                }
                cr = client.callProcedure(proc, param);
                result = cr.getResults()[0];
                int rowCount = result.getRowCount();
                assertEquals(rowCount, 1);
                long tupleCount = result.asScalarLong();
                //*VERBOSIFY TO DEBUG:*/ System.out.println("DEBUG " + proc + " " + numTypeName + " GOT count " + tupleCount);
                resultSet[kk++] = new FunctionVarCharTestCase(proc, filter, tupleCount);
            }
            ++jj;
        }
        return resultSet;
    }

    public void subtestNumericCasts() throws Exception
    {
        System.out.println("STARTING test of numeric CAST");
        final double[] rawData = values;
        final double[] resultIntValues = new double[values.length];
        final Set<Double> intFilters = new HashSet<Double>();
        final Set<Double> rawFilters = new HashSet<Double>();
        for (int kk = 0; kk < resultIntValues.length; ++kk) {
            resultIntValues[kk] = (int)values[kk];
            intFilters.add(resultIntValues[kk]);
            rawFilters.add(values[kk]);
        }

        Client client = getClient();
        insertNumbers(client, rawData, rawData.length);

        FunctionTestCase[] results;
        double[] resultValues;
        Set<Double> filters;

        for (int jj = 0; jj < numTypeNames.length ; ++jj) {
            if (numFormatNames[jj].equals("DECIMAL") || numFormatNames[jj].equals("DOUBLE")) {
                results = displayFunctionRun(client, numTypeNames[jj], values.length / COLUMNCOUNT, numFormatNames[jj]);
                resultValues = rawData;
                filters = rawFilters;
            }
            else {
                results = displayFunctionRun(client, numTypeNames[jj], values.length / COLUMNCOUNT, numFormatNames[jj]);
                resultValues = resultIntValues;
                filters = intFilters;
            }
            assertEquals(results.length, values.length);

            Map<String, Integer> valueBag = new HashMap<String, Integer>();
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

            // Validate that sorting on the function value does not alter the ordering of its input values.
            results = orderFunctionRun(client, numTypeNames[jj]  + "_CAST", resultValues.length/COLUMNCOUNT);

            // The total number of ordered INTEGERNUM values returned should be the same as the number of stored values.
            assertEquals(results.length, resultValues.length);
            kk = 0;
            for (FunctionTestCase result : results) {
                int idIndex = kk++;
                if (kk == ROWCOUNT) {
                    kk = 0;
                }
                double expected = orderedIds[idIndex];
                if (expected != result.m_result) {
                    complain("Failed " + result.m_case + " expected " + expected + " got " + result.m_result);
                }
                assertEquals(expected, result.m_result);
            }

            results = whereFunctionRun(client, numTypeNames[jj] + "_CAST", filters, numFormatNames[jj]);

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
                    complain("CAST got unexpected result " + result.m_filter + ".");
                }
                assertNotNull(count);
                //*VERBOSIFY TO DEBUG:*/ System.out.println("REDUCING " + result.m_case + " unfound " + result.m_filter + " count " + count + " by " + result.m_result );
                if (count < result.m_result) {
                    complain(result.m_case + " value " + result.m_filter + " not expected or previously depleted from " + valueBag + ".");
                }
                assertTrue(count >= result.m_result);
                valueBag.put(String.format(formatForFuzziness, result.m_filter), count-(int)result.m_result);
                coveringCount -= (int)result.m_result;
                //*VERBOSIFY TO DEBUG:*/ System.out.println("DROPPING TOTAL TO " + coveringCount);
            }
            for (Entry<String, Integer> entry : valueBag.entrySet()) {
                int count = entry.getValue();
                if (count != 0) {
                    complain("CAST expected result " + entry.getKey() + " lacks " + count + " matches.");
                }
                assertEquals(0, count);
            }
            assertEquals(0, coveringCount);
        }


        System.out.println("ENDING test of numeric CAST");
    }

    private static void insertNumbersViaVarChar(Client client, double[] rawData, int nRaws) throws Exception
    {
        client.callProcedure("@AdHoc", "delete from NUMBER_TYPES;");
        // Insert inputs via string values to test casts from VARCHAR
        for (int kk = 0; kk < nRaws; kk += COLUMNCOUNT) {
            client.callProcedure("@AdHoc",
                                 "INSERT INTO NUMBER_TYPES VALUES (" +
                                 "CAST('" + (int)rawData[kk+0] + "' AS " + numTypeNames[0] + "),  " +
                                 "CAST('" + (int)rawData[kk+1] + "' AS " + numTypeNames[1] + "),  " +
                                 "CAST('" + (int)rawData[kk+2] + "' AS " + numTypeNames[2] + "),  " +
                                 "CAST('" + (int)rawData[kk+3] + "' AS " + numTypeNames[3] + "),  " +
                                 "CAST('" + rawData[kk+FLOATCOLINDEX]   + "' AS FLOAT         ),  " +
                                 "CAST('" + rawData[kk+DECIMALCOLINDEX] + "' AS DECIMAL       ) );");
        }
    }

    private void subtestFromVarCharCasts() throws Exception
    {
        System.out.println("STARTING test of FROM VARCHAR CAST");
        Client client = getClient();
        insertNumbersViaVarChar(client, values, values.length);
        System.out.println("VALIDATING result of 'FROM VARCHAR' CAST via results of 'TO VARCHAR' CASTS");
        subtestVarCharCasts(client);
        System.out.println("ENDING test of FROM VARCHAR CAST");
    }

    private void subtestToVarCharCasts() throws Exception
    {
        System.out.println("STARTING test of TO VARCHAR CAST");
        Client client = getClient();
        insertNumbers(client, values, values.length);
        subtestVarCharCasts(client);
        System.out.println("ENDING test of TO VARCHAR CAST");
    }

    private void subtestVarCharCasts(Client client) throws Exception
    {
        final String[] resultValues = new String[values.length];
        final Set<String> filters = new HashSet<String>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = "" + values[kk];
            filters.add(resultValues[kk]);
        }

        FunctionVarCharTestCase[] results;

        results = displayVarCharCastRun(client, values.length / COLUMNCOUNT);
        assertEquals(results.length, values.length);

        Map<String, Integer> valueBag = new HashMap<String, Integer>();
        int kk = 0;
        for (FunctionVarCharTestCase result : results) {
            String expected = resultValues[kk++];
            if (! expected.equals(result.m_filter)) {
                // Compromise: accuracy errors get complaints but not asserts.
                complain("Failed " + result.m_case + " expected " + expected + " got " + result.m_filter);
            }
            assertEquals(expected, result.m_filter);
            // count occurrences of expected values in anticipation of the WHERE_ tests.
            Integer count = valueBag.get(expected);
            if (count == null) {
                count = 0;
            }
            valueBag.put(expected, count + 1);
            //*VERBOSIFY TO DEBUG:*/ System.out.println("UPDATING " + result.m_case + " found count of " + expected + " to " + (count+1) );
        }

        results = whereVarCharCastRun(client, filters);

        assertEquals(results.length, COLUMNCOUNT*filters.size());
        // If filters represents all the values in resultValues,
        // the filtered counts should total to resultValues.length.
        int coveringCount = resultValues.length;
        //*VERBOSIFY TO DEBUG:*/ System.out.println("EXPECTING total count" + coveringCount);
        for (FunctionVarCharTestCase result : results) {
            Integer count = valueBag.get(result.m_filter);
            if (count == null) {
                complain("CAST got unexpected result " + result.m_filter + ".");
            }
            assertNotNull(count);
            //*VERBOSIFY TO DEBUG:*/ System.out.println("VARCHAR REDUCING " + result.m_case + " unfound " + result.m_filter + " count " + count + " by " + result.m_result );
            if (count < result.m_result) {
                complain(result.m_case + " value " + result.m_filter + " not expected or previously depleted from " + valueBag + ".");
            }
            assertTrue(count >= result.m_result);
            valueBag.put(result.m_filter, count-(int)result.m_result);
            coveringCount -= (int)result.m_result;
            //*VERBOSIFY TO DEBUG:*/ System.out.println("DROPPING TOTAL TO " + coveringCount);
        }
        for (Entry<String, Integer> entry : valueBag.entrySet()) {
            int count = entry.getValue();
            if (count != 0) {
                complain("VARCHAR CAST expected result " + entry.getKey() + " lacks " + count + " matches.");
            }
            assertEquals(0, count); // Ideally FLOAT behaves in some reasonable standard way.
        }
        assertEquals(0, coveringCount); // Ideally FLOAT behaves in some reasonable standard way.
        //assertTrue(0 == coveringCount /*I WISH*/ || 5 == coveringCount /* former near miss */ );


        // Validate how sorting on the string value alters the ordering of its input values.
        FunctionTestCase[] orderedResults = orderFunctionRun(client, "VARCHAR_CAST", values.length/COLUMNCOUNT);

        // The total number of ordered INTEGERNUM values returned should be the same as the number of stored values.
        assertEquals(values.length, orderedResults.length);
        kk = 0;
        int jj = 0;
        for (FunctionTestCase result : orderedResults) {
            int idIndex = kk++;
            if (kk == ROWCOUNT) {
                kk = 0;
            }
            double[] expecteds = (jj/ROWCOUNT < FLOATCOLINDEX) ? orderedByStringIds :
                (jj/ROWCOUNT > FLOATCOLINDEX) ? orderedByDecimalStringIds : orderedByFloatStringIds;
            if (expecteds[idIndex] != result.m_result) {
                complain("Failed " + result.m_case + " expected " + expecteds[idIndex] + " got " + result.m_result);
            }
            assertEquals(expecteds[idIndex], result.m_result);
            ++jj;
        }
    }

    public void testLeftAndRight() throws NoConnectionsException, IOException, ProcCallException {
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
            cr = client.callProcedure("LEFT", -1, 1);
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
            cr = client.callProcedure("RIGHT", -1, 1);
        } catch (Exception e) {
            assertTrue(e instanceof ProcCallException);
            ex = e;
        } finally {
            assertNotNull(ex);
        }
    }

    public void testSpace() throws NoConnectionsException, IOException, ProcCallException {
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

    public void testRepeat() throws NoConnectionsException, IOException, ProcCallException {
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
    }

    public void testConcat() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING test Concat and its Operator");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "Xin", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("CONCAT", "", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Xin", result.getString(1));

        cr = client.callProcedure("CONCAT", "@VoltDB", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Xin@VoltDB", result.getString(1));

        cr = client.callProcedure("ConcatOpt", "", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Xin", result.getString(1));

        cr = client.callProcedure("ConcatOpt", "@VoltDB", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Xin@VoltDB", result.getString(1));
    }

    private void initialLoad(Client client, String tableName) throws IOException, NoConnectionsException, InterruptedException {
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
                                 "X"+String.valueOf(id)+paddedToNonInlineLength, // DESC
                                  10, // NUM
                                  1.1, // RATIO
                                  new Timestamp(100000000L)); // PAST
            client.drain();
        }
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestFunctionsSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestFunctionsSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PAST TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +

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
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PAST TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +

                // Test unique generalized index on a function of an already indexed column.
                "CREATE UNIQUE INDEX R1_ABS_ID_DESC ON R1 ( ABS(ID), DESC ); " +

                // Test generalized expression index with a constant argument.
                "CREATE INDEX R1_ABS_ID_SCALED ON R1 ( ID / 3 ); " +

                //Another table that has all numeric types, for testing numeric column functions.
                "CREATE TABLE NUMBER_TYPES ( " +
                "INTEGERNUM INTEGER DEFAULT '0' NOT NULL, " +
                "TINYNUM TINYINT, " +
                "SMALLNUM SMALLINT, " +
                "BIGNUM BIGINT, " +
                "FLOATNUM FLOAT, " +
                "DECIMALNUM DECIMAL, " +
                "PRIMARY KEY (INTEGERNUM) );" +

                "";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        project.addPartitionInfo("P1", "ID");

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

        project.addStmtProcedure("DISPLAY_CEILING", "select CEILING(INTEGERNUM), CEILING(TINYNUM), CEILING(SMALLNUM), CEILING(BIGNUM), CEILING(FLOATNUM), CEILING(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_CEILING_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by CEILING(INTEGERNUM), INTEGERNUM");
        project.addStmtProcedure("ORDER_CEILING_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by CEILING(TINYNUM), TINYNUM");
        project.addStmtProcedure("ORDER_CEILING_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by CEILING(SMALLNUM), SMALLNUM");
        project.addStmtProcedure("ORDER_CEILING_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by CEILING(BIGNUM), BIGNUM");
        project.addStmtProcedure("ORDER_CEILING_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by CEILING(FLOATNUM), FLOATNUM");
        project.addStmtProcedure("ORDER_CEILING_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by CEILING(DECIMALNUM), DECIMALNUM");

        project.addStmtProcedure("WHERE_CEILING_INTEGER",  "select count(*) from NUMBER_TYPES where CEILING(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_TINYINT",  "select count(*) from NUMBER_TYPES where CEILING(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_SMALLINT", "select count(*) from NUMBER_TYPES where CEILING(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_BIGINT",   "select count(*) from NUMBER_TYPES where CEILING(BIGNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_FLOAT",    "select count(*) from NUMBER_TYPES where CEILING(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_CEILING_DECIMAL",  "select count(*) from NUMBER_TYPES where CEILING(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_EXP", "select EXP(INTEGERNUM), EXP(TINYNUM), EXP(SMALLNUM), EXP(BIGNUM), EXP(FLOATNUM), EXP(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_EXP_INTEGER",  "select INTEGERNUM, EXP(INTEGERNUM) from NUMBER_TYPES order by EXP(INTEGERNUM)");
        project.addStmtProcedure("ORDER_EXP_TINYINT",  "select INTEGERNUM, EXP(TINYNUM) from NUMBER_TYPES order by EXP(TINYNUM)");
        project.addStmtProcedure("ORDER_EXP_SMALLINT", "select INTEGERNUM, EXP(SMALLNUM) from NUMBER_TYPES order by EXP(SMALLNUM)");
        project.addStmtProcedure("ORDER_EXP_BIGINT",   "select INTEGERNUM, EXP(BIGNUM) from NUMBER_TYPES order by EXP(BIGNUM)");
        project.addStmtProcedure("ORDER_EXP_FLOAT",    "select INTEGERNUM, EXP(FLOATNUM) from NUMBER_TYPES order by EXP(FLOATNUM)");
        project.addStmtProcedure("ORDER_EXP_DECIMAL",  "select INTEGERNUM, EXP(DECIMALNUM) from NUMBER_TYPES order by EXP(DECIMALNUM)");

        project.addStmtProcedure("WHERE_EXP_INTEGER",  "select count(*) from NUMBER_TYPES where EXP(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_TINYINT",  "select count(*) from NUMBER_TYPES where EXP(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_SMALLINT", "select count(*) from NUMBER_TYPES where EXP(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_BIGINT",   "select count(*) from NUMBER_TYPES where EXP(BIGNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_FLOAT",    "select count(*) from NUMBER_TYPES where EXP(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_EXP_DECIMAL",  "select count(*) from NUMBER_TYPES where EXP(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_FLOOR", "select FLOOR(INTEGERNUM), FLOOR(TINYNUM), FLOOR(SMALLNUM), FLOOR(BIGNUM), FLOOR(FLOATNUM), FLOOR(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_FLOOR_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by FLOOR(INTEGERNUM), INTEGERNUM");
        project.addStmtProcedure("ORDER_FLOOR_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by FLOOR(TINYNUM), TINYNUM");
        project.addStmtProcedure("ORDER_FLOOR_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by FLOOR(SMALLNUM), SMALLNUM");
        project.addStmtProcedure("ORDER_FLOOR_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by FLOOR(BIGNUM), BIGNUM");
        project.addStmtProcedure("ORDER_FLOOR_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by FLOOR(FLOATNUM), FLOATNUM");
        project.addStmtProcedure("ORDER_FLOOR_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by FLOOR(DECIMALNUM), DECIMALNUM");

        project.addStmtProcedure("WHERE_FLOOR_INTEGER",  "select count(*) from NUMBER_TYPES where FLOOR(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_TINYINT",  "select count(*) from NUMBER_TYPES where FLOOR(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_SMALLINT", "select count(*) from NUMBER_TYPES where FLOOR(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_BIGINT",   "select count(*) from NUMBER_TYPES where FLOOR(BIGNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_FLOAT",    "select count(*) from NUMBER_TYPES where FLOOR(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_FLOOR_DECIMAL",  "select count(*) from NUMBER_TYPES where FLOOR(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_POWER7X", "select POWER(7, INTEGERNUM), POWER(7, TINYNUM), POWER(7, SMALLNUM), POWER(7, BIGNUM), POWER(7, FLOATNUM), POWER(7, DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_POWER7X_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by POWER(7, INTEGERNUM)");
        project.addStmtProcedure("ORDER_POWER7X_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by POWER(7, TINYNUM)");
        project.addStmtProcedure("ORDER_POWER7X_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by POWER(7, SMALLNUM)");
        project.addStmtProcedure("ORDER_POWER7X_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by POWER(7, BIGNUM)");
        project.addStmtProcedure("ORDER_POWER7X_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by POWER(7, FLOATNUM)");
        project.addStmtProcedure("ORDER_POWER7X_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by POWER(7, DECIMALNUM)");

        project.addStmtProcedure("WHERE_POWER7X_INTEGER",  "select count(*) from NUMBER_TYPES where POWER(7, INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_TINYINT",  "select count(*) from NUMBER_TYPES where POWER(7, TINYNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_SMALLINT", "select count(*) from NUMBER_TYPES where POWER(7, SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_BIGINT",   "select count(*) from NUMBER_TYPES where POWER(7, BIGNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_FLOAT",    "select count(*) from NUMBER_TYPES where POWER(7, FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_POWER7X_DECIMAL",  "select count(*) from NUMBER_TYPES where POWER(7, DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_POWER07X", "select POWER(0.7, INTEGERNUM), POWER(0.7, TINYNUM), POWER(0.7, SMALLNUM), POWER(0.7, BIGNUM), POWER(0.7, FLOATNUM), POWER(0.7, DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_POWER07X_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, INTEGERNUM)");
        project.addStmtProcedure("ORDER_POWER07X_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, TINYNUM)");
        project.addStmtProcedure("ORDER_POWER07X_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, SMALLNUM)");
        project.addStmtProcedure("ORDER_POWER07X_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, BIGNUM)");
        project.addStmtProcedure("ORDER_POWER07X_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, FLOATNUM)");
        project.addStmtProcedure("ORDER_POWER07X_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by POWER(0.7, DECIMALNUM)");

        project.addStmtProcedure("WHERE_POWER07X_INTEGER",  "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, INTEGERNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_TINYINT",  "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, TINYNUM)   ) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_SMALLINT", "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, SMALLNUM)  ) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_BIGINT",   "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, BIGNUM)    ) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_FLOAT",    "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, FLOATNUM)  ) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWER07X_DECIMAL",  "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(0.7, DECIMALNUM)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");

        project.addStmtProcedure("DISPLAY_POWERX7", "select POWER(INTEGERNUM, 7), POWER(TINYNUM, 7), POWER(SMALLNUM, 7), POWER(BIGNUM, 7), POWER(FLOATNUM, 7), POWER(DECIMALNUM, 7) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_POWERX7_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by POWER(INTEGERNUM, 7)");
        project.addStmtProcedure("ORDER_POWERX7_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by POWER(TINYNUM,    7)");
        project.addStmtProcedure("ORDER_POWERX7_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by POWER(SMALLNUM,   7)");
        project.addStmtProcedure("ORDER_POWERX7_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by POWER(BIGNUM,     7)");
        project.addStmtProcedure("ORDER_POWERX7_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by POWER(FLOATNUM,   7)");
        project.addStmtProcedure("ORDER_POWERX7_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by POWER(DECIMALNUM, 7)");

        project.addStmtProcedure("WHERE_POWERX7_INTEGER",  "select count(*) from NUMBER_TYPES where POWER(INTEGERNUM, 7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_TINYINT",  "select count(*) from NUMBER_TYPES where POWER(TINYNUM,    7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_SMALLINT", "select count(*) from NUMBER_TYPES where POWER(SMALLNUM,   7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_BIGINT",   "select count(*) from NUMBER_TYPES where POWER(BIGNUM,     7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_FLOAT",    "select count(*) from NUMBER_TYPES where POWER(FLOATNUM,   7) = ?");
        project.addStmtProcedure("WHERE_POWERX7_DECIMAL",  "select count(*) from NUMBER_TYPES where POWER(DECIMALNUM, 7) = ?");

        // These are intended for application to non-negative values.
        // Failure tests on negative values can be done separately, possibly via ad hoc.
        project.addStmtProcedure("DISPLAY_POWERX07", "select POWER(INTEGERNUM, 0.7), POWER(TINYNUM, 0.7), POWER(SMALLNUM, 0.7), POWER(BIGNUM, 0.7), POWER(FLOATNUM, 0.7), POWER(DECIMALNUM, 0.7) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_POWERX07_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by POWER(INTEGERNUM, 0.7)");
        project.addStmtProcedure("ORDER_POWERX07_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by POWER(TINYNUM,    0.7)");
        project.addStmtProcedure("ORDER_POWERX07_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by POWER(SMALLNUM,   0.7)");
        project.addStmtProcedure("ORDER_POWERX07_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by POWER(BIGNUM,     0.7)");
        project.addStmtProcedure("ORDER_POWERX07_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by POWER(FLOATNUM,   0.7)");
        project.addStmtProcedure("ORDER_POWERX07_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by POWER(DECIMALNUM, 0.7)");

        project.addStmtProcedure("WHERE_POWERX07_INTEGER",  "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(INTEGERNUM, 0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_TINYINT",  "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(TINYNUM,    0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_SMALLINT", "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(SMALLNUM,   0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_BIGINT",   "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(BIGNUM,     0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_FLOAT",    "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(FLOATNUM,   0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");
        project.addStmtProcedure("WHERE_POWERX07_DECIMAL",  "select count(*) from NUMBER_TYPES where ((0.0000001+POWER(DECIMALNUM, 0.7)) / (0.0000001+?)) BETWEEN 0.99 and 1.01");

        // These are intended for application to non-negative values.
        // Failure tests on negative values can be done separately, possibly via ad hoc.
        project.addStmtProcedure("DISPLAY_SQRT", "select SQRT(INTEGERNUM), SQRT(TINYNUM), SQRT(SMALLNUM), SQRT(BIGNUM), SQRT(FLOATNUM), SQRT(DECIMALNUM) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_SQRT_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by SQRT(INTEGERNUM)");
        project.addStmtProcedure("ORDER_SQRT_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by SQRT(TINYNUM)");
        project.addStmtProcedure("ORDER_SQRT_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by SQRT(SMALLNUM)");
        project.addStmtProcedure("ORDER_SQRT_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by SQRT(BIGNUM)");
        project.addStmtProcedure("ORDER_SQRT_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by SQRT(FLOATNUM)");
        project.addStmtProcedure("ORDER_SQRT_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by SQRT(DECIMALNUM)");

        project.addStmtProcedure("WHERE_SQRT_INTEGER",  "select count(*) from NUMBER_TYPES where SQRT(INTEGERNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_TINYINT",  "select count(*) from NUMBER_TYPES where SQRT(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_SMALLINT", "select count(*) from NUMBER_TYPES where SQRT(SMALLNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_BIGINT",   "select count(*) from NUMBER_TYPES where SQRT(TINYNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_FLOAT",    "select count(*) from NUMBER_TYPES where SQRT(FLOATNUM) = ?");
        project.addStmtProcedure("WHERE_SQRT_DECIMAL",  "select count(*) from NUMBER_TYPES where SQRT(DECIMALNUM) = ?");

        project.addStmtProcedure("DISPLAY_INTEGER", "select CAST(INTEGERNUM AS INTEGER), CAST(TINYNUM AS INTEGER), CAST(SMALLNUM AS INTEGER), CAST(BIGNUM AS INTEGER), CAST(FLOATNUM AS INTEGER), CAST(DECIMALNUM AS INTEGER) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_INTEGER_CAST_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS INTEGER)");
        project.addStmtProcedure("ORDER_INTEGER_CAST_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS INTEGER)");
        project.addStmtProcedure("ORDER_INTEGER_CAST_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS INTEGER)");
        project.addStmtProcedure("ORDER_INTEGER_CAST_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS INTEGER)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_INTEGER_CAST_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS INTEGER), INTEGERNUM");
        project.addStmtProcedure("ORDER_INTEGER_CAST_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS INTEGER), INTEGERNUM");

        project.addStmtProcedure("WHERE_INTEGER_CAST_INTEGER",  "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_TINYINT",  "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_SMALLINT", "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_BIGINT",   "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_FLOAT",    "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_DECIMAL",  "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS INTEGER) = ?");

        project.addStmtProcedure("DISPLAY_TINYINT", "select CAST(INTEGERNUM AS TINYINT), CAST(TINYNUM AS TINYINT), CAST(SMALLNUM AS TINYINT), CAST(BIGNUM AS TINYINT), CAST(FLOATNUM AS TINYINT), CAST(DECIMALNUM AS TINYINT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_TINYINT_CAST_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS TINYINT)");
        project.addStmtProcedure("ORDER_TINYINT_CAST_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS TINYINT)");
        project.addStmtProcedure("ORDER_TINYINT_CAST_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS TINYINT)");
        project.addStmtProcedure("ORDER_TINYINT_CAST_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS TINYINT)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_TINYINT_CAST_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS TINYINT), INTEGERNUM");
        project.addStmtProcedure("ORDER_TINYINT_CAST_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS TINYINT), INTEGERNUM");

        project.addStmtProcedure("WHERE_TINYINT_CAST_INTEGER",  "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_TINYINT",  "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_SMALLINT", "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_BIGINT",   "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_FLOAT",    "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_DECIMAL",  "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS TINYINT) = ?");

        project.addStmtProcedure("DISPLAY_SMALLINT", "select CAST(INTEGERNUM AS SMALLINT), CAST(TINYNUM AS SMALLINT), CAST(SMALLNUM AS SMALLINT), CAST(BIGNUM AS SMALLINT), CAST(FLOATNUM AS SMALLINT), CAST(DECIMALNUM AS SMALLINT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_SMALLINT_CAST_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS SMALLINT)");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS SMALLINT)");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS SMALLINT)");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS SMALLINT)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_SMALLINT_CAST_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS SMALLINT), INTEGERNUM");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS SMALLINT), INTEGERNUM");

        project.addStmtProcedure("WHERE_SMALLINT_CAST_INTEGER",  "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_TINYINT",  "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_SMALLINT", "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_BIGINT",   "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_FLOAT",    "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_DECIMAL",  "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS SMALLINT) = ?");

        project.addStmtProcedure("DISPLAY_BIGINT", "select CAST(INTEGERNUM AS BIGINT), CAST(TINYNUM AS BIGINT), CAST(SMALLNUM AS BIGINT), CAST(BIGNUM AS BIGINT), CAST(FLOATNUM AS BIGINT), CAST(DECIMALNUM AS BIGINT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_BIGINT_CAST_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS BIGINT)");
        project.addStmtProcedure("ORDER_BIGINT_CAST_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS BIGINT)");
        project.addStmtProcedure("ORDER_BIGINT_CAST_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS BIGINT)");
        project.addStmtProcedure("ORDER_BIGINT_CAST_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS BIGINT)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_BIGINT_CAST_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS BIGINT), INTEGERNUM");
        project.addStmtProcedure("ORDER_BIGINT_CAST_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS BIGINT), INTEGERNUM");

        project.addStmtProcedure("WHERE_BIGINT_CAST_INTEGER",  "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_TINYINT",  "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_SMALLINT", "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_BIGINT",   "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_FLOAT",    "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_DECIMAL",  "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS BIGINT) = ?");

        project.addStmtProcedure("DISPLAY_FLOAT", "select CAST(INTEGERNUM AS FLOAT), CAST(TINYNUM AS FLOAT), CAST(SMALLNUM AS FLOAT), CAST(BIGNUM AS FLOAT), CAST(FLOATNUM AS FLOAT), CAST(DECIMALNUM AS FLOAT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_FLOAT_CAST_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS FLOAT)");

        project.addStmtProcedure("WHERE_FLOAT_CAST_INTEGER",  "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_TINYINT",  "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_SMALLINT", "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_BIGINT",   "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_FLOAT",    "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_DECIMAL",  "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS FLOAT) = ?");

        project.addStmtProcedure("DISPLAY_DECIMAL", "select CAST(INTEGERNUM AS DECIMAL), CAST(TINYNUM AS DECIMAL), CAST(SMALLNUM AS DECIMAL), CAST(BIGNUM AS DECIMAL), CAST(FLOATNUM AS DECIMAL), CAST(DECIMALNUM AS DECIMAL) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_DECIMAL_CAST_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS DECIMAL)");

        project.addStmtProcedure("WHERE_DECIMAL_CAST_INTEGER",  "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_TINYINT",  "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_SMALLINT", "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_BIGINT",   "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_FLOAT",    "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_DECIMAL",  "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS DECIMAL) = ?");

        project.addStmtProcedure("DISPLAY_SUBSTRING", "select SUBSTRING (DESC FROM 2) from P1 where ID = -12");
        project.addStmtProcedure("DISPLAY_SUBSTRING2", "select SUBSTRING (DESC FROM 2 FOR 2) from P1 where ID = -12");


        project.addStmtProcedure("DISPLAY_VARCHAR", "select CAST(INTEGERNUM AS VARCHAR), CAST(TINYNUM AS VARCHAR), CAST(SMALLNUM AS VARCHAR), CAST(BIGNUM AS VARCHAR), CAST(FLOATNUM AS VARCHAR), CAST(DECIMALNUM AS VARCHAR) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_VARCHAR_CAST_INTEGER",  "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_TINYINT",  "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_SMALLINT", "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_BIGINT",   "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_FLOAT",    "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_DECIMAL",  "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS VARCHAR)");

        project.addStmtProcedure("WHERE_VARCHAR_CAST_INTEGER",  "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_TINYINT",  "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_SMALLINT", "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_BIGINT",   "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_FLOAT",    "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_DECIMAL",  "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS VARCHAR) = ?");

        project.addStmtProcedure("ORDER_SUBSTRING", "select ID+15 from P1 order by SUBSTRING (DESC FROM 2)");

        project.addStmtProcedure("WHERE_SUBSTRING2",
                                 "select count(*) from P1 " +
                                 "where SUBSTRING (DESC FROM 2) > '12"+paddedToNonInlineLength+"'");
        project.addStmtProcedure("WHERE_SUBSTRING3",
                                 "select count(*) from P1 " +
                                 "where not SUBSTRING (DESC FROM 2 FOR 1) > '13'");

        // Test GROUP BY by support
        project.addStmtProcedure("AGG_OF_SUBSTRING", "select MIN(SUBSTRING (DESC FROM 2)) from P1 where ID < -7");
        project.addStmtProcedure("AGG_OF_SUBSTRING2", "select MIN(SUBSTRING (DESC FROM 2 FOR 2)) from P1 where ID < -7");

        // Test parameterizing functions
        // next one disabled until ENG-3486
        //project.addStmtProcedure("PARAM_SUBSTRING", "select SUBSTRING(? FROM 2) from P1");
        project.addStmtProcedure("PARAM_ABS", "select ABS(? + NUM) from P1");

        project.addStmtProcedure("LEFT", "select id, LEFT(DESC,?) from P1 where id = ?");
        project.addStmtProcedure("RIGHT", "select id, RIGHT(DESC,?) from P1 where id = ?");
        project.addStmtProcedure("SPACE", "select id, SPACE(?) from P1 where id = ?");
        project.addStmtProcedure("REPEAT", "select id, REPEAT(DESC,?) from P1 where id = ?");
        project.addStmtProcedure("CONCAT", "select id, CONCAT(DESC,?) from P1 where id = ?");
        project.addStmtProcedure("ConcatOpt", "select id, DESC || ? from P1 where id = ?");

        project.addStmtProcedure("INSERT_NULL", "insert into P1 values (?, null, null, null, null)");
        // project.addStmtProcedure("UPS", "select count(*) from P1 where UPPER(DESC) > 'L'");

        // CONFIG #1: Local Site/Partitions running on JNI backend
        config = new LocalCluster("fixedsql-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // alternative to enable for debugging */ config = new LocalCluster("IPC-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL
        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // no clustering tests for functions

        return builder;
    }
}
