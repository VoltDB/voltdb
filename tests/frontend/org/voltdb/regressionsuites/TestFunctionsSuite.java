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
import java.sql.Timestamp;
import java.util.Date;
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
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID) );
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
        // */


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

    public void testCurrentTimestamp() throws Exception
    {
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
        ClientResponse cr = null;
        VoltTable vt = null;
        Date before = null;
        Date after = null;

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

    public void testTimestampConversions() throws Exception
    {
        Client client = getClient();
        ClientResponse cr = null;
        VoltTable r = null;
        long result;
        int columnIndex = 0;

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
            cr = client.callProcedure("@AdHoc", "select cast('2014-07-04 00:00:00.000000' as timestamp) from R2 where id = 1;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            r = cr.getResults()[0];
            r.advanceRow();
            assertEquals(r.getTimestampAsTimestamp(0).toString(), "2014-07-04 00:00:00.000000");
            cr = client.callProcedure("@AdHoc", "select cast('2014-07-05' as timestamp) from R2 where id = 1;");
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
//        for (int i=0; i< 8; i++) {
//            assertNull(r.getLong(i));
//        }
//        assertNull(r.getLong(8));


        // Normal test case 2001-9-9 01:46:40
        cr = client.callProcedure("P1.insert", 1, "X0", 10, 1.1, new Timestamp(1000000000789L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("EXTRACT_TIMESTAMP", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        columnIndex = 0;

        int EXPECTED_YEAR = 2001;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_YEAR, result);

        int EXPECTED_MONTH = 9;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MONTH, result);

        int EXPECTED_DAY = 9;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DAY, result);

        int EXPECTED_DOW = 1;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOW, result);

        int EXPECTED_DOM = EXPECTED_DAY;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOM, result);

        int EXPECTED_DOY = 252;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOY, result);

        int EXPECTED_QUARTER = 3;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_QUARTER, result);

        int EXPECTED_HOUR = 1;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_HOUR, result);

        int EXPECTED_MINUTE = 46;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MINUTE, result);

        BigDecimal EXPECTED_SECONDS = new BigDecimal("40.789000000000");
        BigDecimal decimalResult = r.getDecimalAsBigDecimal(columnIndex++);
        assertEquals(EXPECTED_SECONDS, decimalResult);

        // ISO 8601 regards Sunday as the last day of a week
        int EXPECTED_WEEK = 36;
        if (isHSQL()) {
            // hsql get answer 37, because it believes a week starts with Sunday
            EXPECTED_WEEK = 37;
        }
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEK, result);
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEK, result);

        // VoltDB has a special function to handle WEEKDAY, and it is not the same as DAY_OF_WEEK
        int EXPECTED_WEEKDAY = 6;
        if (isHSQL()) {
            // We map WEEKDAY keyword to DAY_OF_WEEK in hsql parser
            EXPECTED_WEEKDAY = EXPECTED_DOW;
        }
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEKDAY, result);

        // test timestamp before epoch, Human time (GMT): Thu, 18 Nov 1948 16:32:02 GMT
        // Leap year!
        // http://disc.gsfc.nasa.gov/julian_calendar.shtml
        cr = client.callProcedure("P1.insert", 2, "X0", 10, 1.1, new Timestamp(-666430077123L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("EXTRACT_TIMESTAMP", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        columnIndex = 0;

        EXPECTED_YEAR = 1948;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_YEAR, result);

        EXPECTED_MONTH = 11;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MONTH, result);

        EXPECTED_DAY = 18;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DAY, result);

        EXPECTED_DOW = 5;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOW, result);

        EXPECTED_DOM = EXPECTED_DAY;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOM, result);

        EXPECTED_DOY = 323;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOY, result);

        EXPECTED_QUARTER = 4;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_QUARTER, result);

        EXPECTED_HOUR = 16;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_HOUR, result);

        EXPECTED_MINUTE = 32;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MINUTE, result);

        EXPECTED_SECONDS = new BigDecimal("2.877000000000");
        decimalResult = r.getDecimalAsBigDecimal(columnIndex++);
        assertEquals(EXPECTED_SECONDS, decimalResult);

        EXPECTED_WEEK = 47;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEK, result);
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEK, result);

        // VoltDB has a special function to handle WEEKDAY, and it is not the same as DAY_OF_WEEK
        EXPECTED_WEEKDAY = 3;
        if (isHSQL()) {
            // We map WEEKDAY keyword to DAY_OF_WEEK in hsql parser
            EXPECTED_WEEKDAY = EXPECTED_DOW;
        }
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEKDAY, result);

        // test timestamp with a very old date, Human time (GMT): Fri, 05 Jul 1658 14:22:27 GMT
        cr = client.callProcedure("P1.insert", 3, "X0", 10, 1.1, new Timestamp(-9829676252456L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("EXTRACT_TIMESTAMP", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        columnIndex = 0;

        EXPECTED_YEAR = 1658;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_YEAR, result);

        EXPECTED_MONTH = 7;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MONTH, result);

        EXPECTED_DAY = 5;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DAY, result);

        EXPECTED_DOW = 6;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOW, result);

        EXPECTED_DOM = EXPECTED_DAY;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOM, result);

        EXPECTED_DOY = 186;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOY, result);

        EXPECTED_QUARTER = 3;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_QUARTER, result);

        EXPECTED_HOUR = 14;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_HOUR, result);

        EXPECTED_MINUTE = 22;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MINUTE, result);

        EXPECTED_SECONDS = new BigDecimal("27.544000000000");
        decimalResult = r.getDecimalAsBigDecimal(columnIndex++);
        assertEquals(EXPECTED_SECONDS, decimalResult);

        EXPECTED_WEEK = 27;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEK, result);
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEK, result);

        // VoltDB has a special function to handle WEEKDAY, and it is not the same as DAY_OF_WEEK
        EXPECTED_WEEKDAY = 4;
        if (isHSQL()) {
            // We map WEEKDAY keyword to DAY_OF_WEEK in hsql parser
            EXPECTED_WEEKDAY = EXPECTED_DOW;
        }
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEKDAY, result);

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
        assertEquals(EXPECTED_YEAR, result);

        EXPECTED_MONTH = 7;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MONTH, result);

        EXPECTED_DAY = 2;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DAY, result);

        EXPECTED_DOW = 2;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOW, result);

        EXPECTED_DOM = EXPECTED_DAY;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOM, result);

        EXPECTED_DOY = 184;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOY, result);

        EXPECTED_QUARTER = 3;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_QUARTER, result);

        EXPECTED_HOUR = 12;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_HOUR, result);

        EXPECTED_MINUTE = 53;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MINUTE, result);

        EXPECTED_SECONDS = new BigDecimal("37.123000000000");
        decimalResult = r.getDecimalAsBigDecimal(columnIndex++);
        assertEquals(EXPECTED_SECONDS, decimalResult);

        EXPECTED_WEEK = 27;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEK, result);
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEK, result);

        // VoltDB has a special function to handle WEEKDAY, and it is not the same as DAY_OF_WEEK
        EXPECTED_WEEKDAY = 0;
        if (isHSQL()) {
            // We map WEEKDAY keyword to DAY_OF_WEEK in hsql parser
            EXPECTED_WEEKDAY = EXPECTED_DOW;
        }
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_WEEKDAY, result);
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
        subtestInlineVarCharCast(client);
        System.out.println("ENDING test of TO VARCHAR CAST");
    }

    private void subtestInlineVarCharCast(Client client) throws Exception {
        // This is regression test coverage for ENG-6666.
        String sql = "INSERT INTO INLINED_VC_VB_TABLE (ID, VC1, VC2, VB1, VB2) " +
            "VALUES (22, 'FOO', 'BAR', 'DEADBEEF', 'CDCDCDCD');";
        client.callProcedure("@AdHoc", sql);
        sql = "SELECT CAST(VC1 AS VARCHAR) FROM INLINED_VC_VB_TABLE WHERE ID = 22;";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        vt.advanceRow();
        assertEquals("FOO", vt.getString(0));

        sql = "SELECT CAST(VB1 AS VARBINARY) FROM INLINED_VC_VB_TABLE WHERE ID = 22;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        vt.advanceRow();
        assertTrue(VoltType.varbinaryToPrintableString(vt.getVarbinary(0)).contains("DEADBEEF"));
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


    public void testLowerUpper() throws NoConnectionsException, IOException, ProcCallException {
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
        assertEquals(null, result.getString(1));
        assertEquals(null, result.getString(2));

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

    public void testTrim() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING test Trim");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "  VoltDB   ", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        result = client.callProcedure("@AdHoc", "select trim(LEADING null from desc) from P1").getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(null, result.getString(0));

        cr = client.callProcedure("TRIM_SPACE", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("VoltDB   ", result.getString(1));
        assertEquals("VoltDB   ", result.getString(2));
        assertEquals("  VoltDB",  result.getString(3));
        assertEquals("  VoltDB",  result.getString(4));
        assertEquals("VoltDB",  result.getString(5));
        assertEquals("VoltDB",  result.getString(6));


        cr = client.callProcedure("TRIM_ANY", " ", " ", " ", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("VoltDB   ", result.getString(1));
        assertEquals("  VoltDB",  result.getString(2));
        assertEquals("VoltDB",  result.getString(3));

        try {
            cr = client.callProcedure("TRIM_ANY", "", "", "", 1);
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("data exception"));
            assertTrue(ex.getMessage().contains("trim error"));
        }

        // Test TRIM with other character
        cr = client.callProcedure("P1.insert", 2, "vVoltDBBB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("TRIM_ANY", "v", "B", "B", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("VoltDBBB", result.getString(1));
        assertEquals("vVoltD", result.getString(2));
        assertEquals("vVoltD", result.getString(3));

        // Multiple character trim, Hsql does not support
        if (!isHSQL()) {
            cr = client.callProcedure("TRIM_ANY", "vV", "BB", "Vv", 2);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals("oltDBBB", result.getString(1));
            assertEquals("vVoltDB", result.getString(2));
            assertEquals("vVoltDBBB", result.getString(3));
        }

        // Test null trim character
        cr = client.callProcedure("TRIM_ANY", null, null, null, 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(null, result.getString(1));
        assertEquals(null, result.getString(2));
        assertEquals(null, result.getString(3));


        // Test non-ASCII trim_char
        cr = client.callProcedure("P1.insert", 3, "贾vVoltDBBB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("TRIM_ANY", "贾", "v", "贾", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("vVoltDBBB", result.getString(1));
        assertEquals("贾vVoltDBBB", result.getString(2));
        assertEquals("vVoltDBBB", result.getString(3));

        if (!isHSQL()) {
            // Complete match
            cr = client.callProcedure("TRIM_ANY", "贾vVoltDBBB", "贾vVoltDBBB", "贾vVoltDBBB", 3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals("", result.getString(1));
            assertEquals("", result.getString(2));
            assertEquals("", result.getString(3));

            cr = client.callProcedure("TRIM_ANY", "贾vVoltDBBB_TEST", "贾vVoltDBBB贾vVoltDBBB", "贾vVoltDBBBT", 3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals("贾vVoltDBBB", result.getString(1));
            assertEquals("贾vVoltDBBB", result.getString(2));
            assertEquals("贾vVoltDBBB", result.getString(3));
        }

        // Complicated test
        cr = client.callProcedure("P1.insert", 4, "贾贾vVoltDBBB贾贾贾", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // UTF-8 hex, 贾: 0xe8 0xb4 0xbe, 辴: 0xe8 0xbe 0xb4
        cr = client.callProcedure("TRIM_ANY", "辴", "辴", "辴", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("贾贾vVoltDBBB贾贾贾", result.getString(1));
        assertEquals("贾贾vVoltDBBB贾贾贾", result.getString(2));
        assertEquals("贾贾vVoltDBBB贾贾贾", result.getString(3));

        cr = client.callProcedure("TRIM_ANY", "贾", "贾", "贾", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("vVoltDBBB贾贾贾", result.getString(1));
        assertEquals("贾贾vVoltDBBB", result.getString(2));
        assertEquals("vVoltDBBB", result.getString(3));

        if (!isHSQL()) {
            cr = client.callProcedure("TRIM_ANY", "贾辴", "贾辴", "贾辴", 4);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals("贾贾vVoltDBBB贾贾贾", result.getString(1));
            assertEquals("贾贾vVoltDBBB贾贾贾", result.getString(2));
            assertEquals("贾贾vVoltDBBB贾贾贾", result.getString(3));

            cr = client.callProcedure("TRIM_ANY", "贾贾vV", "贾贾", "B贾贾贾", 4);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals("oltDBBB贾贾贾", result.getString(1));
            assertEquals("贾贾vVoltDBBB贾", result.getString(2));
            assertEquals("贾贾vVoltDBB", result.getString(3));
        }

        cr = client.callProcedure("P1.insert", 5, "vVoltADBDB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

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

    public void testReplace() throws NoConnectionsException, IOException, ProcCallException {
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
            assertEquals(null, result.getString(1));
        }

        result = client.callProcedure("REPLACE", null, "XX", 1).getResults()[0];
        assertTrue(result.advanceRow());
        if (isHSQL()) {
            // NULL means not change for the original string for Hsql
            assertEquals("foo", result.getString(1));
        } else {
            assertEquals(null, result.getString(1));
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

    public void testOverlay() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING test Overlay");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "Xin@VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        result = client.callProcedure("OVERLAY", "Jia", 4, 7, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("XinJia", result.getString(1));

        result = client.callProcedure("OVERLAY", "Jia_", 4, 1, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("XinJia_VoltDB", result.getString(1));

        result = client.callProcedure("OVERLAY", "Jia", 4.2, 7, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("XinJia", result.getString(1));

        result = client.callProcedure("OVERLAY", "Jia", 4.9, 7, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("XinJia", result.getString(1));

        // Test NULL results
        result = client.callProcedure("OVERLAY", null, 4, 7, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(null, result.getString(1));

        result = client.callProcedure("OVERLAY", "Jia", 4, null, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(null, result.getString(1));

        result = client.callProcedure("OVERLAY", "Jia", null, 7, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(null, result.getString(1));

        result = client.callProcedure("OVERLAY_FULL_LENGTH", "Jia", 4, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("XinJialtDB", result.getString(1));

        result = client.callProcedure("OVERLAY_FULL_LENGTH", "J", 4, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("XinJVoltDB", result.getString(1));


        // Test UTF-8 OVERLAY
        cr = client.callProcedure("P1.insert", 2, "贾鑫@VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        result = client.callProcedure("OVERLAY", "XinJia", 1, 2, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("XinJia@VoltDB", result.getString(1));

        result = client.callProcedure("OVERLAY", "XinJia", 8, 2, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾鑫@VoltXinJia", result.getString(1));

        result = client.callProcedure("OVERLAY", "XinJia", 1, 9, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("XinJia", result.getString(1));

        result = client.callProcedure("OVERLAY", "XinJia", 2, 7, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾XinJiaB", result.getString(1));

        result = client.callProcedure("OVERLAY", "XinJia", 2, 8, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾XinJia", result.getString(1));

        result = client.callProcedure("OVERLAY_FULL_LENGTH", "_", 3, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾鑫_VoltDB", result.getString(1));

        result = client.callProcedure("OVERLAY_FULL_LENGTH", " at ", 2, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾 at ltDB", result.getString(1));


        result = client.callProcedure("OVERLAY", "XinJia", 9, 1, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾鑫@VoltDXinJia", result.getString(1));

        result = client.callProcedure("OVERLAY", "石宁", 9, 1, 2).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾鑫@VoltD石宁", result.getString(1));

        // Hsql has bugs on string(substring) index
        if (!isHSQL()) {
            result = client.callProcedure("OVERLAY", "XinJia", 9, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals("贾鑫@VoltDXinJia", result.getString(1));

            result = client.callProcedure("OVERLAY", "石宁", 9, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals("贾鑫@VoltD石宁", result.getString(1));

            result = client.callProcedure("OVERLAY", "XinJia", 10, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals("贾鑫@VoltDBXinJia", result.getString(1));

            result = client.callProcedure("OVERLAY", "石宁", 10, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals("贾鑫@VoltDB石宁", result.getString(1));

            // various start argument tests
            // start from 0, not 1, but treat it at least 1
            result = client.callProcedure("OVERLAY", "XinJia", 100, 2, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals("贾鑫@VoltDBXinJia", result.getString(1));

            // various length argument
            result = client.callProcedure("OVERLAY", "XinJia", 2, 0, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals("贾XinJia鑫@VoltDB", result.getString(1));

            result = client.callProcedure("OVERLAY", "XinJia", 1, 10, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals("XinJia", result.getString(1));

            result = client.callProcedure("OVERLAY", "XinJia", 1, 100, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals("XinJia", result.getString(1));

            result = client.callProcedure("OVERLAY", "XinJia", 2, 100, 2).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals("贾XinJia", result.getString(1));


            // Negative tests
            try {
                result = client.callProcedure("OVERLAY", "XinJia", -10, 2, 2).getResults()[0];
                fail();
            } catch (Exception ex) {
                assertTrue(ex.getMessage().contains(
                        "data exception -- OVERLAY error, not positive start argument -10"));
            }

            try {
                result = client.callProcedure("OVERLAY", "XinJia", 0, 2, 2).getResults()[0];
                fail();
            } catch (Exception ex) {
                assertTrue(ex.getMessage().contains(
                        "data exception -- OVERLAY error, not positive start argument 0"));
            }

            try {
                result = client.callProcedure("OVERLAY", "XinJia", 1, -1, 2).getResults()[0];
                fail();
            } catch (Exception ex) {
                assertTrue(ex.getMessage().contains(
                        "data exception -- OVERLAY error, negative length argument -1"));
            }
        }
    }

    // Unicode character to UTF8 string character
    public void testChar() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING test CHAR");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        // Hsql has wrong answers.
        if (isHSQL()) return;

        cr = client.callProcedure("P1.insert", 1, "Xin@VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        result = client.callProcedure("CHAR", 36158, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾", result.getString(1));

        result = client.callProcedure("CHAR", 37995, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("鑫", result.getString(1));

        String voltDB = "VoltDB";

        for (int i = 0; i < voltDB.length(); i++) {
            char ch = voltDB.charAt(i);
            result = client.callProcedure("CHAR", (int)ch, 1).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(String.valueOf(ch), result.getString(1));
        }

        result = client.callProcedure("CHAR", null, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(null, result.getString(1));
    }

    public void testCaseWhen() throws Exception {
        System.out.println("STARTING test Case When...");
        Client cl = getClient();
        VoltTable vt;
        String sql;

        //                           ID, DESC,   NUM, FLOAT, TIMESTAMP
        cl.callProcedure("R1.insert", 1, "VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        cl.callProcedure("R1.insert", 2, "Memsql",  5, 5.0, new Timestamp(1000000000000L));

        sql = "SELECT ID, CASE WHEN num < 3 THEN 0 ELSE 8 END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 0},{2, 8}});

        sql = "SELECT ID, CASE WHEN num < 3 THEN num/2 ELSE num + 10 END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 0},{2, 15}});

        sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN num * 5 " +
                "WHEN num >=5 THEN num * 10  ELSE num END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 5},{2, 50}});


        // (2) Test case when Types.
        sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                "WHEN num >=5 THEN num * 10  ELSE num END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.BIGINT, vt.getColumnType(1));
        if (isHSQL()) {
            validateTableOfLongs(vt, new long[][] {{1, 0},{2, 50}});
        } else {
            validateTableOfLongs(vt, new long[][] {{1, Long.MIN_VALUE},{2, 50}});
        }

        sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                "WHEN num >=5 THEN NULL  ELSE num END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.INTEGER, vt.getColumnType(1));
        if (isHSQL()) {
            validateTableOfLongs(vt, new long[][] {{1, 0},{2, 0}});
        } else {
            validateTableOfLongs(vt, new long[][] {{1, Long.MIN_VALUE},{2, Long.MIN_VALUE}});
        }

        // Expected failed type cases:
        try {
            sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                    "WHEN num >=5 THEN NULL ELSE NULL END FROM R1 ORDER BY 1;";
            vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
            fail();
        } catch (Exception ex) {
            assertNotNull(ex);
            assertTrue(ex.getMessage().contains("data type cast needed for parameter or null literal"));
        }

        try {
            // Use String as the casted type
            sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                    "WHEN num >=5 THEN NULL ELSE 'NULL' END FROM R1 ORDER BY 1;";
            vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        } catch (Exception ex) {
            fail();
        }

        try {
            sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                    "WHEN num >=5 THEN 'I am null'  ELSE num END FROM R1 ORDER BY 1;";
            vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
            fail();
        } catch (Exception ex) {
            assertNotNull(ex);
            assertTrue(ex.getMessage().contains("incompatible data types in combination"));
        }

        // Test string types
        sql = "SELECT ID, CASE WHEN desc > 'Volt' THEN 'Good' ELSE 'Bad' END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(2, vt.getRowCount());
        vt.advanceRow();
        assertEquals(vt.getLong(0), 1);
        assertTrue(vt.getString(1).equals("Good"));
        vt.advanceRow();
        assertEquals(vt.getLong(0), 2);
        if (isHSQL()) {
            assertTrue(vt.getString(1).contains("Bad"));
        } else {
            assertTrue(vt.getString(1).equals("Bad"));
        }


        // Test string concatenation
        sql = "SELECT ID, desc || ':' ||  CASE WHEN desc > 'Volt' THEN 'Good' ELSE 'Bad' END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(2, vt.getRowCount());
        vt.advanceRow();
        assertEquals(vt.getLong(0), 1);
        assertTrue(vt.getString(1).equals("VoltDB:Good"));
        vt.advanceRow();
        assertEquals(vt.getLong(0), 2);
        if (isHSQL()) {
            assertTrue(vt.getString(1).contains("Memsql:Bad"));
        } else {
            assertTrue(vt.getString(1).equals("Memsql:Bad"));
        }

        // Test inlined varchar/varbinary value produced by CASE WHEN.
        // This is regression coverage for ENG-6666.
        sql = "INSERT INTO INLINED_VC_VB_TABLE (ID, VC1, VC2, VB1, VB2) " +
            "VALUES (72, 'FOO', 'BAR', 'DEADBEEF', 'CDCDCDCD');";
        cl.callProcedure("@AdHoc", sql);
        sql = "SELECT CASE WHEN ID > 11 THEN VC1 ELSE VC2 END FROM INLINED_VC_VB_TABLE WHERE ID = 72;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        vt.advanceRow();
        assertEquals("FOO", vt.getString(0));

        sql = "SELECT CASE WHEN ID > 11 THEN VB1 ELSE VB2 END FROM INLINED_VC_VB_TABLE WHERE ID = 72;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        vt.advanceRow();
        assertTrue(VoltType.varbinaryToPrintableString(vt.getVarbinary(0)).contains("DEADBEEF"));

        cl.callProcedure("R1.insert", 3, "ORACLE",  8, 8.0, new Timestamp(1000000000000L));
        // Test nested case when
        sql = "SELECT ID, CASE WHEN num < 5 THEN num * 5 " +
                "WHEN num < 10 THEN CASE WHEN num > 7 THEN num * 10 ELSE num * 8 END " +
                "END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 5},{2, 40}, {3, 80}});


        // Test case when without ELSE clause
        sql = "SELECT ID, CASE WHEN num > 3 AND num < 5 THEN 4 " +
                "WHEN num >=5 THEN num END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.INTEGER, vt.getColumnType(1));
        if (isHSQL()) {
            validateTableOfLongs(vt, new long[][] {{1, 0},{2,5}, {3, 8}});
        } else {
            validateTableOfLongs(vt, new long[][] {{1, Long.MIN_VALUE},{2,5}, {3, 8}});
        }

        sql = "SELECT ID, CASE WHEN num > 3 AND num < 5 THEN 4 " +
                "WHEN num >=5 THEN num*10 END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.BIGINT, vt.getColumnType(1));
        if (isHSQL()) {
            validateTableOfLongs(vt, new long[][] {{1, 0},{2,50}, {3, 80}});
        } else {
            validateTableOfLongs(vt, new long[][] {{1, Long.MIN_VALUE},{2,50}, {3, 80}});
        }

        // Test NULL
        cl.callProcedure("R1.insert", 4, "DB2",  null, null, new Timestamp(1000000000000L));
        sql = "SELECT ID, CASE WHEN num < 3 THEN num/2 ELSE num + 10 END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.INTEGER, vt.getColumnType(1));
        if (isHSQL()) {
            validateTableOfLongs(vt, new long[][] {{1, 0},{2, 15}, {3, 18}, {4, 0}});
        } else {
            validateTableOfLongs(vt, new long[][] {{1, 0},{2, 15}, {3, 18}, {4, Long.MIN_VALUE}});
        }

    }

    public void testCaseWhenLikeDecodeFunction() throws Exception {
        System.out.println("STARTING test Case When like decode function...");
        Client cl = getClient();
        String sql;

        //      ID, DESC,   NUM, FLOAT, TIMESTAMP
        cl.callProcedure("R1.insert", 1, "VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        cl.callProcedure("R1.insert", 2, "MySQL",  5, 5.0, new Timestamp(1000000000000L));

        sql = "SELECT ID, CASE num WHEN 3 THEN 3*2 WHEN 1 THEN 0 ELSE 10 END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 0},{2, 10}});

        // No ELSE clause
        sql = "SELECT ID, CASE num WHEN 1 THEN 10 WHEN 2 THEN 1 END FROM R1 ORDER BY 1;";
        if (isHSQL()) {
            validateTableOfLongs(cl, sql, new long[][] {{1, 10},{2, 0}});
        } else {
            validateTableOfLongs(cl, sql, new long[][] {{1, 10},{2, Long.MIN_VALUE}});
        }

        // Test NULL
        cl.callProcedure("R1.insert", 3, "Oracle",  null, null, new Timestamp(1000000000000L));
        sql = "SELECT ID, CASE num WHEN 5 THEN 50 ELSE num + 10 END FROM R1 ORDER BY 1;";
        if (isHSQL()) {
            validateTableOfLongs(cl, sql, new long[][] {{1, 11},{2, 50}, {3, 0}});
        } else {
            validateTableOfLongs(cl, sql, new long[][] {{1, 11},{2, 50}, {3, Long.MIN_VALUE}});
        }
    }

    private static StringBuilder joinStringArray(String[] params, String sep) {
        StringBuilder sb = new StringBuilder();
        for (String s : params) {
            sb.append(s).append(sep);
        }
        sb.delete(sb.length()-sep.length(), sb.length());
        return sb;
    }

    // concat params with a sql query string, and test the return value
    private void doTestCoalesceWithoutConst(Client cl, String[] params,
                                                   String expect, String id) throws Exception {
        String allPara = joinStringArray(params, ",").toString();
        String sql;
        if (expect=="NULL"){
            // sql = "SELECT CASE WHEN (COALESCE(para1, para2, ...) IS NULL)
            //               THEN 0 ELSE 1
            //               END FROM C_NULL WHERE ID=id";
            sql = "SELECT CASE WHEN(COALESCE(" + allPara + ") IS NULL)" +
                  " THEN 0 ELSE 1 END FROM C_NULL WHERE ID=" + id;
        }
        else {
            // sql = "SELECT CASE COALESCE(para1, para2, ...)
            //               WHEN expect
            //               THEN 0 ELSE 1
            //               END FROM C_NULL WHERE ID=id";
            sql = "SELECT CASE COALESCE(" + allPara + ") " +
                   "WHEN " + expect + " THEN 0 ELSE 1 END FROM C_NULL WHERE ID=" + id;
        }
        validateTableOfLongs(cl, sql, new long[][] {{0}});
    }

    private void doTestCoalesceWithConst(Client cl, String[] params,
                                                String cst ,String expect, String id) throws Exception {
        String allPara = joinStringArray(params, ",").toString();
        allPara += ","+cst;
        String sql;
        if (expect=="NULL"){
            // sql = "SELECT CASE WHEN (COALESCE(para1, para2, ..., cst) IS NULL)
            //               THEN 0 ELSE 1
            //               END FROM C_NULL WHERE ID=id";
            sql = "SELECT CASE WHEN(COALESCE(" + allPara + ") IS NULL)" +
                  " THEN 0 ELSE 1 END FROM C_NULL WHERE ID=" + id;
        }
        else {
            // sql = "SELECT CASE COALESCE(para1, para2, ..., cst)
            //               WHEN expect
            //               THEN 0 ELSE 1
            //               END FROM C_NULL WHERE ID=id";
            sql = "SELECT CASE COALESCE(" + allPara + ") " +
                   "WHEN " + expect + " THEN 0 ELSE 1 END FROM C_NULL WHERE ID=" + id;
        }
        validateTableOfLongs(cl, sql, new long[][] {{0}});
    }

    // col1 is not null while col2 is null
    private void doTestCoalescePairOneNull(Client cl, String col1, String col2) throws Exception {
        // coalesce(col1, col2) == coalesce(col2, col1) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2}, col1, "1");
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1}, col1, "1");
    }

    private void doTestCoalescePairBothNull(Client cl, String col1, String col2) throws Exception{
        // coalesce(col1, col2) == coalesce(col2, col1) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2}, "NULL", "0");
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1}, "NULL", "0");
    }

    // Both the columns are not null
    private void doTestCoalescePairNotNull(Client cl, String col1, String col2) throws Exception {
        // coalesce(col1, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2}, col1, "2");
        // coalesce(col2, col1) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1}, col2, "2");
    }

    // All the columns are not null
    private void doTestCoalesceTriNotNull(Client cl, String col1,
                                                 String col2, String col3, String cst) throws Exception {
        // coalesce(col1, col2, col3) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2, col3}, col1, "3");
        // coalesce(col1, col3, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col3, col2}, col1, "3");
        // coalesce(col2, col1, col3) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1, col3}, col2, "3");
        // coalesce(col2, col3, col1) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col3, col1}, col2, "3");
        // coalesce(col3, col1, col2) == col3
        doTestCoalesceWithoutConst(cl, new String[]{col3, col1, col2}, col3, "3");
        // coalesce(col3, col2, col1) == col3
        doTestCoalesceWithoutConst(cl, new String[]{col3, col2, col1}, col3, "3");
        // coalesce(col1, col2, col3, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col2, col3}, cst, col1, "3");
        // coalesce(col1, col3, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col3, col2}, cst, col1, "3");
        // coalesce(col2, col1, col3, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col2, col1, col3}, cst, col2, "3");
        // coalesce(col2, col3, col1, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col2, col3, col1}, cst, col2, "3");
        // coalesce(col3, col1, col2, cst) == col3
        doTestCoalesceWithConst(cl, new String[]{col3, col1, col2}, cst, col3, "3");
        // coalesce(col3, col2, col1, cst) == col3
        doTestCoalesceWithConst(cl, new String[]{col3, col2, col1}, cst, col3, "3");
    }

    // col3 is null
    private void doTestCoalesceTriOneNull(Client cl, String col1,
                                                 String col2, String col3, String cst) throws Exception {
        // coalesce(col1, col2, col3) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2, col3}, col1, "2");
        // coalesce(col1, col3, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col3, col2}, col1, "2");
        // coalesce(col2, col1, col3) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1, col3}, col2, "2");
        // coalesce(col2, col3, col1) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col3, col1}, col2, "2");
        // coalesce(col3, col1, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col3, col1, col2}, col1, "2");
        // coalesce(col3, col2, col2) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col3, col2, col1}, col2, "2");
        // coalesce(col1, col2, col3, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col2, col3}, cst, col1, "2");
        // coalesce(col1, col3, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col3, col2}, cst, col1, "2");
        // coalesce(col2, col1, col3, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col2, col1, col3}, cst, col2, "2");
        // coalesce(col2, col3, col1, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col2, col3, col1}, cst, col2, "2");
        // coalesce(col3, col1, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col3, col1, col2}, cst, col1, "2");
        // coalesce(col3, col1, col2, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col3, col2, col1}, cst, col2, "2");
    }

    // col2 and col3 are null
    private void doTestCoalesceTriTwoNull(Client cl, String col1,
                                                 String col2, String col3, String cst) throws Exception {
        // coalesce(col1, col2, col3) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2, col3}, col1, "1");
        // coalesce(col1, col3, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col3, col2}, col1, "1");
        // coalesce(col2, col1, col3) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1, col3}, col1, "1");
        // coalesce(col2, col3, col1) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col2, col3, col1}, col1, "1");
        // coalesce(col3, col1, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col3, col1, col2}, col1, "1");
        // coalesce(col3, col2, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col3, col2, col1}, col1, "1");
        // coalesce(col1, col2, col3, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col2, col3}, cst, col1, "1");
        // coalesce(col1, col3, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col3, col2}, cst, col1, "1");
        // coalesce(col2, col1, col3, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col2, col1, col3}, cst, col1, "1");
        // coalesce(col2, col3, col1, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col2, col3, col1}, cst, col1, "1");
        // coalesce(col3, col1, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col3, col1, col2}, cst, col1, "1");
        // coalesce(col3, col1, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col3, col2, col1}, cst, col1, "1");
    }

    // all columns are null
    private void doTestCoalesceTriAllNull(Client cl, String col1,
                                                 String col2, String col3, String cst) throws Exception{
        // coalesce(col1, col2, col3) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2, col3}, "NULL", "0");
        // coalesce(col1, col3, col2) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col1, col3, col2}, "NULL", "0");
        // coalesce(col2, col1, col3) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1, col3}, "NULL", "0");
        // coalesce(col2, col3, col1) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col2, col3, col1}, "NULL", "0");
        // coalesce(col3, col1, col2) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col3, col1, col2}, "NULL", "0");
        // coalesce(col3, col2, col2) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col3, col2, col1}, "NULL", "0");
        // coalesce(col1, col2, col3, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col1, col2, col3}, cst, cst, "0");
        // coalesce(col1, col3, col2, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col1, col3, col2}, cst, cst, "0");
        // coalesce(col2, col1, col3, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col2, col1, col3}, cst, cst, "0");
        // coalesce(col2, col3, col1, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col2, col3, col1}, cst, cst, "0");
        // coalesce(col3, col1, col2, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col3, col1, col2}, cst, cst, "0");
        // coalesce(col3, col1, col2, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col3, col2, col1}, cst, cst, "0");
    }

    private void doTestTwoColCoalesce(Client cl, String col1, String col2) throws Exception {
        doTestCoalescePairBothNull(cl, col1, col2);
        doTestCoalescePairOneNull(cl, col1, col2);
        doTestCoalescePairNotNull(cl, col1, col2);
    }

    private void doTestThreeColCoalesce(Client cl, String col1,
                                        String col2, String col3, String cst) throws Exception {
        doTestCoalesceTriAllNull(cl, col1, col2, col3, cst);
        doTestCoalesceTriTwoNull(cl, col1, col2, col3, cst);
        doTestCoalesceTriOneNull(cl, col1, col2, col3, cst);
        doTestCoalesceTriNotNull(cl, col1, col2, col3, cst);
    }

    public void testCoalesce() throws Exception {
        System.out.println("STARTING test COALESCE function...");
        Client cl = getClient();

        // one row with three sets of nulls
        cl.callProcedure("@AdHoc", "insert into C_NULL(ID) values (0);");
        // one row with one set of non-null columns and two sets of nulls
        cl.callProcedure("@AdHoc", "insert into C_NULL(ID,S1,I1,F1,D1,V1,T1) values (1,1,1,1,1,'1',100000)");
        // TODO: below is wrong, because the null timestamp will be regarded as an invalid input by hsql
        //cl.callProcedure("C_NULL.insert", 1,1,1,1,1,"1",new Timestamp(1000000000000L), null, null, null, null, null);
        // two sets of non-null columns and one set of null column
        cl.callProcedure("@AdHoc", "insert into C_NULL(ID,S1,I1,F1,D1,V1,T1,I2,F2,D2,V2,T2)"
                                + " values (2,1,1,1,1,'1',100000,2,2,2,'2',200000)");
        // three set non-nulls
        cl.callProcedure("C_NULL.insert", 3,1,1,1,1,"1",new Timestamp(1000000000000L),
                                              2,2,2,"2",new Timestamp(2000000000000L),
                                              3,3,3,"3",new Timestamp(3000000000000L));

        doTestTwoColCoalesce(cl, "I1", "I2");
        doTestTwoColCoalesce(cl, "F1", "F2");
        doTestTwoColCoalesce(cl, "D1", "D2");
        doTestTwoColCoalesce(cl, "V1", "V2");
        doTestTwoColCoalesce(cl, "T1", "T2");

        doTestThreeColCoalesce(cl, "I1", "I2", "I3", "100");
        doTestThreeColCoalesce(cl, "F1", "F2", "F3", "100.0");
        doTestThreeColCoalesce(cl, "D1", "D2", "D3", "100.0");
        doTestThreeColCoalesce(cl, "V1", "V2", "V3", "'hahaha'");
        doTestThreeColCoalesce(cl, "T1", "T2", "T3", "CAST ('2014-07-09 00:00:00.000000' as TIMESTAMP)");

        // test compatiable types
        doTestThreeColCoalesce(cl, "S1", "I2", "I3", "100");
        doTestThreeColCoalesce(cl, "S1", "F2", "D3", "100.0");
        doTestThreeColCoalesce(cl, "I1", "F2", "D3", "100.0");

        // test incompatiable types
        // TODO: Is the exception throwed by coalesce? Or by decode?
        try {
            doTestThreeColCoalesce(cl, "S1", "I2", "V3", "100");
            fail();
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("incompatible data types"));
        }
        try {
            doTestThreeColCoalesce(cl, "S1", "I2", "T3", "100");
            fail();
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("incompatible data types"));
        }
    }

    public void testManyExtractTimeFieldFunction() throws Exception {
        System.out.println("STARTING test functions extracting fields in timestamp ...");
        Client cl = getClient();
        VoltTable result;
        String sql;

        ClientResponse cr = cl.callProcedure("P1.insert", 0, null, null, null,
                Timestamp.valueOf("2014-07-15 01:02:03.456"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = cl.callProcedure("P1.insert", 1, null, null, null, Timestamp.valueOf("2012-02-29 12:20:30.123"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = cl.callProcedure("P1.insert", 2, null, null, null, Timestamp.valueOf("2012-12-31 12:59:30"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        sql = "select id, YEAR(past) from p1 order by id;";
        validateTableOfLongs(cl, sql, new long[][]{{0, 2014}, {1, 2012}, {2, 2012}});

        sql = "select id, MONTH(past) from p1 order by id;";
        validateTableOfLongs(cl, sql, new long[][]{{0, 7}, {1, 2}, {2, 12}});

        sql = "select id, DAY(past) from p1 order by id;";
        validateTableOfLongs(cl, sql, new long[][]{{0, 15}, {1, 29}, {2, 31}});

        sql = "select id, HOUR(past) from p1 order by id;";
        validateTableOfLongs(cl, sql, new long[][]{{0, 1}, {1, 12}, {2, 12}});

        sql = "select id, MINUTE(past) from p1 order by id;";
        validateTableOfLongs(cl, sql, new long[][]{{0, 2}, {1, 20}, {2, 59}});

        sql = "select id, cast(SECOND(past) as VARCHAR) from p1 order by id;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        if (isHSQL()) {
            validateTableColumnOfScalarVarchar(result, 1, new String[]{"3.456000", "30.123000", "30.000000"});
        }
        else {
            validateTableColumnOfScalarVarchar(result, 1, new String[]{"3.456000000000", "30.123000000000",
                    "30.000000000000"});
        }

        sql = "select id, QUARTER(past) from p1 order by id;";
        validateTableOfLongs(cl, sql, new long[][]{{0, 3}, {1, 1}, {2, 4}});

        sql = "select DAYOFWEEK(past) from p1 order by id;";
        validateTableOfLongs(cl, sql,new long[][]{{3}, {4}, {2}});

        sql = "select WEEKDAY(past) from p1 order by id;";
        if (isHSQL()) {
            // we modify the hsql parser, and so it maps to extract week_of_day
            validateTableOfLongs(cl, sql,new long[][]{{3}, {4}, {2}});
        }
        else {
            // call our ee function, and so return different value
            validateTableOfLongs(cl, sql,new long[][]{{1}, {2}, {0}});
        }

        sql = "select DAYOFMONTH(past) from p1 order by id;";
        validateTableOfLongs(cl, sql,new long[][]{{15}, {29}, {31}});

        sql = "select DAYOFYEAR(past) from p1 order by id;";
        validateTableOfLongs(cl, sql,new long[][]{{196}, {60}, {366}});

        // WEEK 1 is often the correct answer for the last day of the year.
        // See https://en.wikipedia.org/wiki/ISO_week_year#Last_week
        sql = "select WEEK(past) from p1 order by id;";
        validateTableOfLongs(cl, sql,new long[][]{{29}, {9}, {1}});
    }

    // ENG-3283
    public void testAliasesOfSomeStringFunctions() throws IOException, ProcCallException {
        String sql;
        VoltTable result;
        Client cl = getClient();
        ClientResponse cr = cl.callProcedure("P1.insert", 0, "abc123ABC", null, null,
                Timestamp.valueOf("2014-07-15 01:02:03.456"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // LTRIM and RTRIM has been implemented and tested

        // SUBSTR
        sql = "select SUBSTR(DESC, 1, 2) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ab"});

        sql = "select SUBSTR(DESC, 4, 3) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"123"});

        sql = "select SUBSTR(DESC, 3) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"c123ABC"});

        // Test spelled out SUBSTRING with comma delimiters vs. old-school FROM and FOR keywords.
        sql = "select SUBSTRING(DESC, 1, 2) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ab"});

        sql = "select SUBSTRING(DESC, 4, 3) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"123"});

        sql = "select SUBSTRING(DESC, 3) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"c123ABC"});

        // Some weird cases -- the SQL-2003 standard says that even START < 1
        // moves the end point (in this case, to the left) which is based on (LENGTH + START).
        sql = "select SUBSTR(DESC, 0, 2) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"a"}); // not "ab" !

        sql = "select SUBSTR(DESC, -1, 2) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not "ab" !

        sql = "select SUBSTR(DESC, -1, 1) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not "a" !

        sql = "select SUBSTR(DESC, -3, 1) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not an error !

        sql = "select SUBSTRING(DESC, 0, 2) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"a"}); // not "ab" !

        sql = "select SUBSTRING(DESC, -1, 2) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not "ab" !

        sql = "select SUBSTRING(DESC, -1, 1) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not "a" !

        sql = "select SUBSTRING(DESC, -3, 1) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not an error !

        // LCASE and UCASE
        sql = "select LCASE(DESC) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"abc123abc"});

        sql = "select UCASE(DESC) from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABC123ABC"});

        // INSERT
        sql = "select INSERT(DESC, 1, 3,'ABC') from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABC123ABC"});

        sql = "select INSERT(DESC, 1, 1,'ABC') from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABCbc123ABC"});

        sql = "select INSERT(DESC, 1, 4,'ABC') from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABC23ABC"});

        sql = "select INSERT(DESC, 1, 0,'ABC') from p1 where id = 0;";
        cr = cl.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABCabc123ABC"});
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
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

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

        project.addStmtProcedure("EXTRACT_TIMESTAMP", "select EXTRACT(YEAR FROM PAST), EXTRACT(MONTH FROM PAST), EXTRACT(DAY FROM PAST), " +
                "EXTRACT(DAY_OF_WEEK FROM PAST), EXTRACT(DAY_OF_MONTH FROM PAST), EXTRACT(DAY_OF_YEAR FROM PAST), EXTRACT(QUARTER FROM PAST), " +
                "EXTRACT(HOUR FROM PAST), EXTRACT(MINUTE FROM PAST), EXTRACT(SECOND FROM PAST), EXTRACT(WEEK_OF_YEAR FROM PAST), " +
                "EXTRACT(WEEK FROM PAST), EXTRACT(WEEKDAY FROM PAST) from P1 where ID = ?");


        project.addStmtProcedure("VERIFY_TIMESTAMP_STRING_EQ",
                "select PAST, CAST(DESC AS TIMESTAMP), DESC, CAST(PAST AS VARCHAR) from R2 " +
                "where PAST <> CAST(DESC AS TIMESTAMP)");

        project.addStmtProcedure("VERIFY_STRING_TIMESTAMP_EQ",
                "select PAST, CAST(DESC AS TIMESTAMP), DESC, CAST(PAST AS VARCHAR) from R2 " +
                "where DESC <> CAST(PAST AS VARCHAR)");

        project.addStmtProcedure("DUMP_TIMESTAMP_STRING_PATHS",
                "select PAST, CAST(DESC AS TIMESTAMP), DESC, CAST(PAST AS VARCHAR) from R2");



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
        project.addStmtProcedure("LOWER_UPPER", "select id, LOWER(DESC), UPPER(DESC) from P1 where id = ?");

        project.addStmtProcedure("TRIM_SPACE", "select id, LTRIM(DESC), TRIM(LEADING ' ' FROM DESC), " +
                "RTRIM(DESC), TRIM(TRAILING ' ' FROM DESC), TRIM(DESC), TRIM(BOTH ' ' FROM DESC) from P1 where id = ?");
        project.addStmtProcedure("TRIM_ANY", "select id, TRIM(LEADING ? FROM DESC), TRIM(TRAILING ? FROM DESC), " +
                "TRIM(BOTH ? FROM DESC) from P1 where id = ?");

        project.addStmtProcedure("REPEAT", "select id, REPEAT(DESC,?) from P1 where id = ?");
        project.addStmtProcedure("REPLACE", "select id, REPLACE(DESC,?, ?) from P1 where id = ?");
        project.addStmtProcedure("OVERLAY", "select id, OVERLAY(DESC PLACING ? FROM ? FOR ?) from P1 where id = ?");
        project.addStmtProcedure("OVERLAY_FULL_LENGTH", "select id, OVERLAY(DESC PLACING ? FROM ?) from P1 where id = ?");

        project.addStmtProcedure("CHAR", "select id, CHAR(?) from P1 where id = ?");

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
