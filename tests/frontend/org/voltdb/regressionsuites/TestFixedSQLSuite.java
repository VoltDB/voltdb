/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.apache.commons.lang3.StringUtils;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;
import org.voltdb_testprocs.regressionsuites.fixedsql.TestENG1232;
import org.voltdb_testprocs.regressionsuites.fixedsql.TestENG1232_2;
import org.voltdb_testprocs.regressionsuites.fixedsql.TestENG2423;

/**
 * Actual regression tests for SQL that I found that was broken and
 * have fixed.  Didn't like any of the other potential homes that already
 * existed for this for one reason or another.
 */

public class TestFixedSQLSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class, TestENG1232.class, TestENG1232_2.class,
        TestENG2423.InnerProc.class };

    static final int VARCHAR_VARBINARY_THRESHOLD = 100;

    public void testSmallFixedTests() throws IOException, ProcCallException
    {
        subTestInsertNullPartitionString();
        subTestAndExpressionComparingSameTableColumns();
        subTestSeqScanFailedPredicateDoesntCountAgainstLimit();
        subTestSelectExpression();
        subTestNestLoopJoinPredicates();
        subTestnestLoopJoinPredicatesWithExpressions();
        subTestNestLoopJoinPredicatesWithAliases();
        subTestGreaterThanOnOrderedIndex();
        subTestForHSQLDecimalFailures();
        subTestNumericExpressionConversion();
        subTestInsertWithCast();
    }

    private void subTestTicketEng2250_IsNull() throws IOException, ProcCallException, InterruptedException
    {
        System.out.println("STARTING testTicketEng2250_IsNull");
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
                PRIMARY KEY (ID)
                );
        */
        System.out.println("Eng2250: null entries.");
        for(int id=0; id < 5; id++) {
            client.callProcedure(callback, "P1.insert", id, null, 10, 1.1);
            client.drain();
        }
        System.out.println("Eng2250: not null entries.");
        for (int id=5; id < 8; id++) {
            client.callProcedure(callback, "P1.insert", id,"description", 10, 1.1);
            client.drain();
        }
        VoltTable r1 = client.callProcedure("@AdHoc", "select count(*) from P1 where desc is null").getResults()[0];
        //* enable for debugging */ System.out.println(r1);
        assertEquals(5, r1.asScalarLong());

        VoltTable r2 = client.callProcedure("@AdHoc", "select count(*) from P1 where not desc is null").getResults()[0];
        //* enable for debugging */ System.out.println(r2);
        assertEquals(3, r2.asScalarLong());

        VoltTable r3 = client.callProcedure("@AdHoc", "select count(*) from P1 where NOT (id=2 and desc is null)").getResults()[0];
        //* enable for debugging */ System.out.println(r3);
        assertEquals(7, r3.asScalarLong());

        VoltTable r4 = client.callProcedure("@AdHoc", "select count(*) from P1 where NOT (id=6 and desc is null)").getResults()[0];
        //* enable for debugging */ System.out.println(r4);
        assertEquals(8, r4.asScalarLong());

        VoltTable r5 = client.callProcedure("@AdHoc", "select count(*) from P1 where id < 6 and NOT desc is null;").getResults()[0];
        //* enable for debugging */ System.out.println(r5);
        assertEquals(1, r5.asScalarLong());

        truncateTable(client, "P1");

    }

    private void subTestTicketEng1850_WhereOrderBy() throws Exception
    {
        System.out.println("STARTING testTicketENG1850_WhereOrderBy");

        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException("Failed with response: " + clientResponse.getStatusString());
                }
            }
        };

        Client client = getClient();
        int cid=0;
        do {
            for (int aid = 0; aid < 5; aid++) {
                int pid = cid % 10;
                client.callProcedure(callback, "ENG1850.insert", cid++, aid, pid, (pid+aid));
            }
        } while (cid < 1000);

        client.drain();

        VoltTable r1 = client.callProcedure("@AdHoc", "select count(*) from ENG1850;").getResults()[0];
        assertEquals(1000, r1.asScalarLong());

        VoltTable r2 = client.callProcedure("@AdHoc", "select count(*) from ENG1850 where pid =2;").getResults()[0];
        assertEquals(100, r2.asScalarLong());

        VoltTable r3 = client.callProcedure("@AdHoc", "select * from ENG1850 where pid = 2 limit 1;").getResults()[0];
        //* enable for debugging */ System.out.println("r3\n" + r3);
        assertEquals(1, r3.getRowCount());

        // this failed, returning 0 rows.
        VoltTable r4 = client.callProcedure("@AdHoc", "select * from ENG1850 where pid = 2 order by pid, aid").getResults()[0];
        //* enable for debugging */ System.out.println("r4\n:" + r4);
        assertEquals(100, r4.getRowCount());

        // this is the failing condition reported in the defect report (as above but with the limit)
        VoltTable r5 = client.callProcedure("@AdHoc", "select * from ENG1850 where pid = 2 order by pid, aid limit 1").getResults()[0];
        //* enable for debugging */ System.out.println("r5\n" + r5);
        assertEquals(1, r5.getRowCount());

        truncateTable(client, "ENG1850");
    }

    private void subTestTicketEng1850_WhereOrderBy2() throws Exception
    {
        System.out.println("STARTING testTIcketEng1850_WhereOrderBy2");

        // verify that selecting * where pid = 2 order by pid, aid gets the right number
        // of tuples when <pid, null> exists in the relation (as this would be the first
        // key found by moveToKeyOrGreater - verify this key is added to the output if
        // it really exists

        Client client = getClient();
        // index is (pid, aid)
        // schema: insert (cid, aid, pid, attr)
        client.callProcedure("ENG1850.insert", 0, 1, 1, 0);
        if (!isHSQL()) {
            // unsure why HSQL throws out-of-range exception here.
            // there are sql coverage tests for this case. skip it here.
            client.callProcedure("ENG1850.insert", 1, null, 2, 0);
        }
        client.callProcedure("ENG1850.insert", 2, 1, 2, 0);
        client.callProcedure("ENG1850.insert", 3, 2, 2, 0);
        client.callProcedure("ENG1850.insert", 4, 3, 3, 0);

        VoltTable r1 = client.callProcedure("@AdHoc", "select * from ENG1850 where pid = 2 order by pid, aid").getResults()[0];
        //* enable for debugging */ System.out.println(r1);
        assertEquals(isHSQL() ? 2: 3, r1.getRowCount());

        VoltTable r2 = client.callProcedure("@AdHoc", "select * from ENG1850 where pid = 2 order by aid, pid").getResults()[0];
        //* enable for debugging */ System.out.println(r2);
        assertEquals(isHSQL() ? 2 : 3, r2.getRowCount());

        VoltTable r3 = client.callProcedure("@AdHoc", "select * from ENG1850 where pid > 1 order by pid, aid").getResults()[0];
        //* enable for debugging */ System.out.println(r3);
        assertEquals(isHSQL() ?  3 :  4, r3.getRowCount());

        VoltTable r4 = client.callProcedure("@AdHoc", "select * from ENG1850 where pid = 2").getResults()[0];
        //* enable for debugging */ System.out.println(r4);
        assertEquals(isHSQL() ? 2 : 3, r4.getRowCount());


        truncateTable(client, "ENG1850");
    }

    private void subTestTicketENG1232() throws Exception {
        Client client = getClient();

        client.callProcedure("@AdHoc", "insert into test_eng1232 VALUES(9);");

        VoltTable result[] = client.callProcedure("TestENG1232", 9).getResults();
        assertTrue(result[0].advanceRow());
        assertEquals(9, result[0].getLong(0));
        assertTrue(result[1].advanceRow());
        assertEquals(1, result[1].getLong(0));


        client.callProcedure("@AdHoc", "insert into test_eng1232 VALUES(9);");

        result = client.callProcedure("TestENG1232_2", 9).getResults();
        assertTrue(result[0].advanceRow());
        assertEquals(1, result[0].getLong(0));
        assertFalse(result[1].advanceRow());

        truncateTable(client, "test_eng1232");
    }

    private void subTestInsertNullPartitionString() throws IOException, ProcCallException
    {
        // This test is for issue ENG-697
        Client client = getClient();
        boolean caught = false;
        try
        {
            client.callProcedure("InsertNullString", null, 0, 1);
        }
        catch (final ProcCallException e)
        {
            if (e.getMessage().contains("CONSTRAINT VIOLATION"))
                caught = true;
            else {
                e.printStackTrace();
                fail();
            }
        }
        assertTrue(caught);
    }

    private void subTestTicket309() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 500, 14.5);

            String query =
                String.format("select count(*), %s.NUM from %s group by %s.NUM",
                              table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            while (results[0].advanceRow())
            {
                if (results[0].getLong(1) == 100)
                {
                    assertEquals(3, results[0].getLong(0));
                }
                else if (results[0].getLong(1) == 300)
                {
                    assertEquals(2, results[0].getLong(0));
                }
                else if (results[0].getLong(1) == 500)
                {
                    assertEquals(1, results[0].getLong(0));
                }
                else
                {
                    fail();
                }
            }
        }

        truncateTables(client, tables);
    }

    //
    // Regression test for broken SQL of the variety:
    //
    // select * from TABLE where (TABLE.ID = value) and
    //          (TABLE.col1 compared_to TABLE.col2)
    //
    // which would return results any time TABLE.ID = value was true,
    // regardless of whether the second expression was true.
    //
    private void subTestAndExpressionComparingSameTableColumns()
    throws IOException, ProcCallException
    {
        Client client = getClient();
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 5, "desc", 10, 14.5);
            client.callProcedure("Insert", table, 15, "desc2", 10, 14.5);
            // These queries should result in no rows, but the defect in
            // SubPlanAssembler resulted in only the NO_NULLS.PKEY = 5 expression
            // being used
            String query = "select * from " + table + " where (" +
                table + ".ID = 5) and (" + table + ".NUM < " + table +".ID)";
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = "select * from " + table + " where (" +
                table + ".ID = 5) and (" + table + ".NUM <= " + table +".ID)";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = "select * from " + table + " where (" +
                table + ".ID = 15) and (" + table + ".NUM > " + table +".ID)";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = "select * from " + table + " where (" +
                table + ".ID = 15) and (" + table + ".NUM >= " + table +".ID)";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
        }

        truncateTables(client, tables);
    }

    //
    // Regression test for broken SQL of the variety:
    //
    // select * from replicated_table where (predicate) LIMIT n
    //
    // For replicated tables, LIMIT is inlined in seqscan; the tuple count was
    // being incremented for each input tuple regardless of the predicate
    // result, which was resulting in the wrong number of rows returned in some
    // cases.
    // @throws IOException
    // @throws ProcCallException
    //
    private void subTestSeqScanFailedPredicateDoesntCountAgainstLimit()
    throws IOException, ProcCallException
    {
        Client client = getClient();
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            // our predicate is going to be ID < NUM.
            // Insert one row where this is false
            client.callProcedure("Insert", table, 1, "desc", -1, 14.5);
            // And two where it is true
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            String query = "select * from " + table + " where " +
                table + ".ID < " + table +".NUM limit 2";
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            // we should get 2 rows but this bug would result in only 1 returned
            assertEquals(2, results[0].getRowCount());
        }

        truncateTables(client, tables);
    }

    //
    // Regression test for broken SQL of the variety:
    //
    // select (non-aggregating expression) from table
    // e.g. select col1 + col2 from table
    //
    // PlanAssembler extracts the left side of the expression to discard
    // aggregation-type expressions from the parsed SQL, but was basically
    // assuming that anything not a VALUE_TUPLE was an aggregate.
    //
    // Note: Adding 5.5 in the third test here also tests a "fix" in
    // HSQL where we coerce the type of numeric literals from NUMERIC to DOUBLE
    //
    private void subTestSelectExpression() throws IOException, ProcCallException
    {
        Client client = getClient();
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 2, 14.5);
            String query = String.format("select %s.ID + 10 from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(11, results[0].getLong(0));
            query = String.format("select %s.NUM + 20 from %s", table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(22, results[0].getLong(0));
            query = String.format("select %s.RATIO + 5.5 from %s",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(20.0, results[0].getDouble(0));
            query = String.format("select %s.ID + %s.NUM from %s",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(3, results[0].getLong(0));
            // ENG-5035
            query = String.format("select '%s' from %s", table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(table, results[0].getString(0));
            query = String.format("select '%s' from %s", "qwertyuiop", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals("qwertyuiop", results[0].getString(0));
            query = String.format("select %s.RATIO, '%s' from %s", table, "qwertyuiop", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals("qwertyuiop", results[0].getString(1));
        }

        truncateTables(client, tables);
    }


    //
    // Regression test for broken SQL of the variety:
    //
    // trac #166
    //
    // When evaluating the nest loop join predicate, insufficient
    // information was available to tuplevalue expression nodes to
    // understand which column(s) needed to be evaluated by the TVE's
    // operators.
    //
    private void subTestNestLoopJoinPredicates() throws IOException, ProcCallException
    {
        Client client = getClient();
        for (int id=0; id < 5; id++) {
            // insert id, (5-id) in to P1
            client.callProcedure("Insert", "P1", id, "desc", (5-id), 2.5);
            // insert id, (id) in to R1
            client.callProcedure("Insert", "R1", id, "desc", (id), 2.5);
        }
        // join on the (5-id), (id) columns
        String query = "select * from P1, R1 where P1.NUM = R1.NUM";
        VoltTable vts[] = client.callProcedure("@AdHoc", query).getResults();
        nestLoopJoinPredicates_verify(vts);

        // same thing using inner join syntax
        query = "select * from P1 INNER JOIN R1 on P1.NUM = R1.NUM";
        vts = client.callProcedure("@AdHoc", query).getResults();
        nestLoopJoinPredicates_verify(vts);

        // join on ID and verify NUM. (ID is indexed)
        query = "select * from P1, R1 where P1.ID = R1.ID";
        vts = client.callProcedure("@AdHoc", query).getResults();
        nestLoopJoinPredicates_verifyid(vts);

        // as above with inner join syntax
        query = "select * from P1 INNER JOIN R1 on P1.ID = R1.ID";
        vts = client.callProcedure("@AdHoc", query).getResults();
        nestLoopJoinPredicates_verifyid(vts);

        truncateTables(client, new String[]{"P1", "R1"});
    }

    private void nestLoopJoinPredicates_verifyid(VoltTable[] vts) {
        assertEquals(1, vts.length);
        //* enable for debugging */ System.out.println("verifyid: " + vts[0]);
        assertEquals(5, vts[0].getRowCount());

        while(vts[0].advanceRow()) {
            int p_id = ((Integer)vts[0].get(0, VoltType.INTEGER)).intValue();
            int r_id = ((Integer)vts[0].get(4, VoltType.INTEGER)).intValue();
            int p_n =  ((Integer)vts[0].get(2, VoltType.INTEGER)).intValue();
            int r_n =  ((Integer)vts[0].get(6, VoltType.INTEGER)).intValue();

            assertEquals(p_id, r_id);
            assertEquals(5 - p_n, r_n);
        }
    }

    private void nestLoopJoinPredicates_verify(VoltTable[] vts)
    {
        assertEquals(1, vts.length);
        //* enable for debugging */ System.out.println(vts[0]);
        assertEquals(4, vts[0].getRowCount());

        // the id of the first should be (5-id) in the second
        // because of the insertion trickery done above
        // verifies trac #125
        while(vts[0].advanceRow()) {
            int id1 = ((Integer)vts[0].get(0, VoltType.INTEGER)).intValue();
            int id2 = ((Integer)vts[0].get(4, VoltType.INTEGER)).intValue();
            assertEquals(id1, (5 - id2));
        }
    }

    //
    // Regression test for broken SQL of the variety:
    //
    // trac #125.  (verification in addition to testNestLoopJoinPredicates).
    //
    // Select a complex expression (not just a TupleValueExpression)
    // to verify that non-root TVEs are correctly offset.
    //
    private void subTestnestLoopJoinPredicatesWithExpressions() throws IOException, ProcCallException
    {
        Client client = getClient();
        for (int id=0; id < 5; id++) {
            // insert id, (5-id) in to P1
            client.callProcedure("Insert", "P1", id, "desc", (5-id), 2.5);
            // insert id, (id) in to R1
            client.callProcedure("Insert", "R1", id, "desc", (id), 2.5);
        }
        // join on the (5-id), (id) columns and select a value modified by an expression
        String query = "select (P1.ID + 20), (R1.ID + 40) from P1, R1 where P1.NUM = R1.NUM";
        VoltTable vts[] = client.callProcedure("@AdHoc", query).getResults();
        nestLoopJoinPredicatesWithExpressions_verify(vts);

        // same thing using inner join syntax
        query = "select (P1.ID + 20), (R1.ID + 40) from P1 INNER JOIN R1 on P1.NUM = R1.NUM";
        vts = client.callProcedure("@AdHoc", query).getResults();
        nestLoopJoinPredicatesWithExpressions_verify(vts);

        truncateTables(client, new String[]{"P1", "R1"});
    }

    private void nestLoopJoinPredicatesWithExpressions_verify(
            VoltTable[] vts) {
        assertEquals(1, vts.length);
        //* enable for debugging */ System.out.println(vts[0]);
        assertEquals(4, vts[0].getRowCount());

        // the id of the first should be (5-id) in the second once the addition
        // done in the select expression is un-done.
        while(vts[0].advanceRow()) {
            int p1_id = ((Integer)vts[0].get(0, VoltType.INTEGER)).intValue();
            int r1_id = ((Integer)vts[0].get(1, VoltType.INTEGER)).intValue();
            assertEquals( (p1_id - 20), (5 - (r1_id - 40)) );
            // and verify that the addition actually happened.
            assertTrue(p1_id >= 20);
            assertTrue(p1_id <= 24);
            assertTrue(r1_id >= 40);
            assertTrue(r1_id <= 44);
        }
    }

    //
    // Regression test for broken SQL of the variety:
    //
    // trac #125. (additional verification).
    //
    // Select columns and expressions with aliases.
    //
    private void subTestNestLoopJoinPredicatesWithAliases() throws IOException, ProcCallException
    {
        Client client = getClient();
        for (int id=0; id < 5; id++) {
            // insert id, (5-id) in to P1
            client.callProcedure("Insert", "P1", id, "desc", (5-id), 2.5);
            // insert id, (id) in to R1
            client.callProcedure("Insert", "R1", id, "desc", (id), 2.5);
        }
        // join on the (5-id), (id) columns and select a value modified by an expression
        // use an alias that would select an invalid column. (be a jerk).
        String query = "select R1.ID AS DESC, (P1.ID + 20) AS THOMAS from P1, R1 where P1.NUM = R1.NUM";
        VoltTable vts[] = client.callProcedure("@AdHoc", query).getResults();
        nestLoopJoinPredicatesWithAliases_verify(vts);

        // same thing using inner join syntax
        query = "select R1.ID AS DESC, (P1.ID + 20) AS THOMAS from P1 INNER JOIN R1 on P1.NUM = R1.NUM";
        vts = client.callProcedure("@AdHoc", query).getResults();
        nestLoopJoinPredicatesWithAliases_verify(vts);

        truncateTables(client, new String[]{"P1", "R1"});
    }

    private void nestLoopJoinPredicatesWithAliases_verify(VoltTable[] vts) {
        assertEquals(1, vts.length);
        //* enable for debugging */ System.out.println(vts[0]);
        assertEquals(4, vts[0].getRowCount());

        // the id of the first should be (5-id) in the second once the addition
        // done in the select expression is un-done.
        while(vts[0].advanceRow()) {
            int p1_id = ((Integer)vts[0].get(1, VoltType.INTEGER)).intValue();
            int r1_id = ((Integer)vts[0].get(0, VoltType.INTEGER)).intValue();
            assertEquals( (p1_id - 20), (5 - r1_id) );
            // and verify that the addition actually happened.
            assertTrue(p1_id >= 20);
            assertTrue(p1_id <= 24);
            assertTrue(r1_id >= 0);
            assertTrue(r1_id <= 4);
        }
    }



    //
    // Regression test for broken SQL of the sort
    //
    // select * from TABLE where COL_WITH_ORDERED_INDEX > n
    //
    // The bug is that indexscanexecutor and indexes treat > as >=
    // @throws IOException
    // @throws ProcCallException
    //
    private void subTestGreaterThanOnOrderedIndex() throws IOException, ProcCallException
    {
        String[] tables = {"P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 100, 14.5);
            String query = "select * from " + table + " where " +
                table + ".ID > 1";
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            // we should get 5 rows but this bug would result in all 6 returned
            assertEquals(5, results[0].getRowCount());
            // make sure that we work if the value we want isn't present
            query = "select * from " + table + " where " +
                table + ".ID > 4";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            query = "select * from " + table + " where " +
                table + ".ID > 8";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
        }

        truncateTables(client, tables);
    }

    public void testFixedTickets() throws Exception
    {
        subTestTicketEng2250_IsNull();
        subTestTicketEng1850_WhereOrderBy();
        subTestTicketEng1850_WhereOrderBy2();
        subTestTicketENG1232();
        subTestTicket309();
        subTestTicket196();
        subTestTicket201();
        subTestTicket216();
        subTestTicket194();
        subTestTickets227And228();
        subTestTicket220();
        subTestTicket310();
        subTestTicket221();
        subTestTicket222();
        subTestTicket224();
        subTestTicket226();
        subTestTicket231();
        subTestTicket232();
        subTestTicket293();
        subTestTicketEng397();
        //subTestTicketEng490();
        subTestTicketEng993();
        subTestTicketEng1316();
        subTestTicket2423();
        subTestTicket5151_ColumnDefaultNull();
        subTestTicket5486_NULLcomparison();
        subTestENG4146();
        subTestENG5669();
        subTestENG5637_VarcharVarbinaryErrorMessage();
        subTestENG6870();
        subTestENG6926();
        subTestENG7041ViewAndExportTable();
        subTestENG7349_InnerJoinWithOverflow();
        subTestENG7724();
        subTestENG7480();
        subTestENG8120();
        subTestENG9032();
        subTestENG9389();
        subTestENG9533();
        subTestENG9796();
        subTestENG11256();
    }

    private void subTestENG11256() throws Exception {
        Client client = getClient();
        validateDMLTupleCount(client, "INSERT INTO R1 VALUES(1, 'A', 12, 0.6)", 1);
        validateDMLTupleCount(client, "INSERT INTO R1 VALUES(2, 'A', 12, 0.2)", 1);
        validateDMLTupleCount(client, "INSERT INTO R1 VALUES(3, 'B', 34, 0.0)", 1);
        validateDMLTupleCount(client, "INSERT INTO R1 VALUES(4, 'B', 34, 0.7)", 1);

        validateDMLTupleCount(client, "INSERT INTO R2 VALUES(5, 'C', 56, 0.8)", 1);
        validateDMLTupleCount(client, "INSERT INTO R2 VALUES(6, 'C', 56, 0.5)", 1);
        validateDMLTupleCount(client, "INSERT INTO R2 VALUES(7, 'D', 78, 0.9)", 1);
        validateDMLTupleCount(client, "INSERT INTO R2 VALUES(8, 'D', 78, 0.3)", 1);

        String[] filterOps = new String[] { " <> ", " IS DISTINCT FROM " };
        for (String filterOp : filterOps) {
            long expected = " <> ".equals(filterOp) ? 4 : 0;
            String query;
            String start = "SELECT count(*) FROM R1 PARENT WHERE DESC NOT IN (";
            String end = " ON LHS.ID = RHS.ID " +
                    "WHERE LHS.ID " + filterOp + " PARENT.NUM);";
            // Zero result rows because values from R1 always have matches in
            // R1 JOIN R1.

            query = start +
                    "SELECT LHS.DESC FROM R1 LHS FULL JOIN R1 RHS " + end;
            validateTableOfLongs(client, query, new long[][] {{0}});
            query = start +
                    "SELECT LHS.DESC FROM R1 LHS FULL JOIN R2 RHS " + end;
            validateTableOfLongs(client, query, new long[][] {{0}});
            // An IS DISTINCT FROM bug in the HSQL backend causes it
            // to always to return 0 rows,
            // which is only correct for <>.
            // Remove this condition when ENG-11256 is fixed.
            if (isHSQL() && expected != 0) {
                query = start +
                        "SELECT LHS.DESC FROM R2 LHS FULL JOIN R1 RHS " + end;
                validateTableOfLongs(client, query, new long[][] {{expected}});
            }
            query = start +
                    "SELECT LHS.DESC FROM R2 LHS FULL JOIN R2 RHS " + end;
            validateTableOfLongs(client, query, new long[][] {{4}});
        }
        truncateTables(client, new String[]{"R1", "R2"});
    }

    private void subTestTicket196() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 500, 14.5);
            String query = String.format("select count(*) from %s", table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            results[0].advanceRow();
            assertEquals(6, results[0].getLong(0));
            query = String.format("select %s.NUM, count(*) from %s group by %s.NUM",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            while (results[0].advanceRow())
            {
                if (results[0].getLong(0) == 100)
                {
                    assertEquals(3, results[0].getLong(1));
                }
                else if (results[0].getLong(0) == 300)
                {
                    assertEquals(2, results[0].getLong(1));
                }
                else if (results[0].getLong(0) == 500)
                {
                    assertEquals(1, results[0].getLong(1));
                }
                else
                {
                    fail();
                }
            }
        }

        // SO, given our current count(*) hack (replace * with the first column
        // in the input to the aggregator, this is a test that will
        // FAIL when we go and implement COUNT to do the right thing with null
        // values.  If this test breaks for you, don't blow it off.
        String query = "insert into COUNT_NULL values (10, 0, 100)";
        client.callProcedure("@AdHoc", query);
        query = "insert into COUNT_NULL values (NULL, 1, 200)";
        client.callProcedure("@AdHoc", query);
        query = "insert into COUNT_NULL values (10, 2, 300)";
        client.callProcedure("@AdHoc", query);
        query = "insert into COUNT_NULL values (NULL, 3, 400)";
        client.callProcedure("@AdHoc", query);
        query = "select count(*) from COUNT_NULL";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(1, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals(4, results[0].getLong(0));


        truncateTables(client, tables);
        truncateTable(client, "COUNT_NULL");
    }

    private void subTestTicket201() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 400, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 500, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 600, 14.5);
            String query = String.format("select * from %s where (%s.ID + 1) = 2",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            query = String.format("select * from %s where (%s.ID + 1) > 2",
                                         table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(5, results[0].getRowCount());
            query = String.format("select * from %s where (%s.ID + 1) >= 2",
                                         table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
        }

        truncateTables(client, tables);
    }

    //public void testTicket205() throws IOException, ProcCallException
    //{
    //    String[] tables = {"P1", "R1", "P2", "R2"};
    //    Client client = getClient();
    //    for (String table : tables)
    //    {
    //        client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
    //        client.callProcedure("Insert", table, 2, "desc", 200, 14.5);
    //        client.callProcedure("Insert", table, 3, "desc", 300, 14.5);
    //        client.callProcedure("Insert", table, 6, "desc", 400, 14.5);
    //        client.callProcedure("Insert", table, 7, "desc", 500, 14.5);
    //        client.callProcedure("Insert", table, 8, "desc", 600, 14.5);
    //        String query = String.format("select sum(%s.NUM + 1) from %s",
    //                                     table, table);
    //        VoltTable[] results = client.callProcedure("@AdHoc", query);
    //        assertEquals(1, results[0].getRowCount());
    //        query = String.format("select sum(%s.NUM + %s.ID) from %s",
    //                                     table, table);
    //        results = client.callProcedure("@AdHoc", query);
    //        assertEquals(1, results[0].getRowCount());
    //    }
    //}

    private void subTestTicket216() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 100.0);
            client.callProcedure("Insert", table, 2, "desc", 200, 200.0);
            client.callProcedure("Insert", table, 3, "desc", 300, 300.0);
            client.callProcedure("Insert", table, 6, "desc", 400, 400.0);
            client.callProcedure("Insert", table, 7, "desc", 500, 500.0);
            client.callProcedure("Insert", table, 8, "desc", 600, 600.0);
            String query = String.format("select %s.RATIO / 2.0 from %s order by ID",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
            for (double f=50.0; results[0].advanceRow(); f+=50.0) {
                double num = (results[0].getDouble(0));
                assertEquals(f, num);
            }
            query = String.format("select * from %s where %s.RATIO >= 400.0",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
        }

        truncateTables(client, tables);
    }


    private void subTestTicket194() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 400, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 500, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 600, 14.5);
            String query = String.format("select * from %s where %s.ID >= 2.1",
                                  table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 4.0",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
        }

        truncateTables(client, tables);
    }


    private void subTestTickets227And228() throws IOException, ProcCallException
    {
        String[] tables = {"P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 100, 14.5);
        }
        // test > on the join (ticket 227)
        String query = "select * from R2, P2 where R2.ID > 1";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(30, results[0].getRowCount());
        query = "select * from P2, R2 where R2.ID > 1";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(30, results[0].getRowCount());
        // test >= on the join (ticket 228)
        query = "select * from R2, P2 where R2.ID >= 3";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(24, results[0].getRowCount());
        query = "select * from P2, R2 where R2.ID >= 3";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(24, results[0].getRowCount());
        query = "select * from R2, P2 where R2.ID >= 4";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(18, results[0].getRowCount());
        query = "select * from P2, R2 where R2.ID >= 4";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(18, results[0].getRowCount());

        truncateTables(client, tables);
    }

    private void subTestTicket220() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
        }
        String query = "select R1.ID + 5 from R1, P1 order by R1.ID";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(9, results[0].getRowCount());
        for (int i = 0; i < 3; i++)
        {
            for (int j = 0; j < 3; j++)
            {
                results[0].advanceRow();
                assertEquals(i + 3 + 5, results[0].getLong(0));
            }
        }

        truncateTables(client, tables);
    }

    //
    // At first pass, HSQL barfed on decimal in sql-coverage. Debug/test that here.
    //
    private void subTestForHSQLDecimalFailures() throws IOException, ProcCallException
    {
        Client client = getClient();
        String sql =
            "INSERT INTO R1_DECIMAL VALUES (26, 307473.174514, 289429.605067, 9.71903320295135486617e-01)";
        client.callProcedure("@AdHoc", sql);
        sql = "select R1_DECIMAL.CASH + 2.0 from R1_DECIMAL";
        VoltTable[] results = client.callProcedure("@AdHoc", sql).getResults();
        assertEquals(1, results.length);

        truncateTable(client, "R1_DECIMAL");
    }

    private void subTestTicket310() throws IOException, ProcCallException
    {
        Client client = getClient();
        String sql =
            "INSERT INTO R1_DECIMAL VALUES (26, 307473.174514, 289429.605067, 9.71903320295135486617e-01)";
        client.callProcedure("@AdHoc", sql);

        boolean caught = false;
        // HSQL doesn't choke the same way Volt does at the moment.
        // Fake the test out.
        if (isHSQL())
        {
            caught = true;
        }
        try
        {
            sql = "SELECT * FROM R1_DECIMAL WHERE " +
            "(R1_DECIMAL.CASH <= 999999999999999999999999999999.0622493314185)" +
            " AND (R1_DECIMAL.ID > R1_DECIMAL.CASH)";
            client.callProcedure("@AdHoc", sql);
        }
        catch (ProcCallException e)
        {
            caught = true;
        }
        assertTrue(caught);

        truncateTable(client, "R1_DECIMAL");
    }

    private void subTestNumericExpressionConversion() throws IOException, ProcCallException
    {
        VoltTable[] results;
        Client client = getClient();

        String sql = "INSERT INTO R1_DECIMAL VALUES " +
           "(26, 307473.174514, 289429.605067, 9.71903320295135486617e-01)";
        results = client.callProcedure("@AdHoc", sql).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());

        sql = "UPDATE R1_DECIMAL SET CASH = CASH * 5 WHERE " +
            "R1_DECIMAL.CASH != 88687.224073";
        results = client.callProcedure("@AdHoc", sql).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());

        sql = "UPDATE R1_DECIMAL SET CASH = CASH + 5.5 WHERE " +
            "R1_DECIMAL.CASH != 88687.224073";
        results = client.callProcedure("@AdHoc", sql).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());


        truncateTable(client, "R1_DECIMAL");
    }

    private void subTestTicket221() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
        }
        String query = "select distinct P1.NUM from R1, P1 order by P1.NUM";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(3, results[0].getRowCount());
        for (int i = 100; results[0].advanceRow(); i+=100)
        {
            assertEquals(i, results[0].getLong(0));
            //* enable for debugging */ System.out.println("i: " + results[0].getLong(0));
        }

        truncateTables(client, tables);
    }

    private void subTestTicket222() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
        }
        String query = "select max(P1.ID) from R1, P1";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(1, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals(2, results[0].getLong(0));
        //* enable for debugging */ System.out.println("i: " + results[0].getLong(0));

        truncateTables(client, tables);
    }

    private void subTestTicket224() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
        }
        String query = "select P1.ID from R1, P1 group by P1.ID order by P1.ID";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(3, results[0].getRowCount());
        assertEquals(1, results[0].getColumnCount());

        //* enable for debugging */ System.out.println(results[0].toFormattedString());

        for (int i = 0; results[0].advanceRow(); i++)
        {
            assertEquals(i, results[0].getLong(0));
            //* enable for debugging */ System.out.println("i: " + results[0].getLong(0));
        }

        truncateTables(client, tables);
    }

    private void subTestTicket226() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
        }
        String query = "select P1.ID from P1, R1 order by P1.ID";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(9, results[0].getRowCount());
        assertEquals(1, results[0].getColumnCount());
        for (int i = 0; i < 3; i++)
        {
            for (int j = 0; j < 3; j++)
            {
                results[0].advanceRow();
                assertEquals(i, results[0].getLong(0));
                //* enable for debugging */ System.out.println("i: " + results[0].getLong(0));
            }
        }

        truncateTables(client, tables);
    }

    private void subTestTicket231() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 300, 14.5);

            // This statement is a test case for one of the ticket 231
            // work-arounds
            String query =
                String.format("select (%s.NUM + %s.NUM) as NUMSUM from %s where (%s.NUM + %s.NUM) > 400",
                              table, table, table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
// This failing statement is the current ticket 231 failing behavior.
//            query =
//                String.format("select (%s.NUM + %s.NUM) as NUMSUM from %s order by (%s.NUM + %s.NUM)",
//                              table, table, table, table, table);
//            results = client.callProcedure("@AdHoc", query);
//            assertEquals(6, results[0].getRowCount());
        }

        truncateTables(client, tables);
    }



    private void subTestTicket232() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 300, 14.5);
            String query =
                String.format("select %s.NUM from %s group by %s.NUM order by %s.NUM",
                              table, table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
        }

        truncateTables(client, tables);
    }


    private void subTestTicket293() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 17.5);
            client.callProcedure("Insert", table, id++, "desc", 400, 18.5);
            String query = String.format("select distinct %s.NUM from %s order by %s.NUM",
                                         table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
        }
        String query = "select distinct P1.NUM from R1, P1 order by P1.NUM";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(4, results[0].getRowCount());

        truncateTables(client, tables);
    }

    private void subTestTicketEng397() throws IOException, ProcCallException
    {
        Client client = getClient();
        for (int i=0; i < 20; i++) {
            client.callProcedure("Insert", "P1", i, "desc", 100 + i, 4.5);
        }
        // base case
        VoltTable[] results = client.callProcedure("Eng397Limit1", new Integer(10)).getResults();
        assertEquals(10, results[0].getRowCount());

        // negative limit rollsback
        boolean caught = false;
        try {
            results = client.callProcedure("Eng397Limit1", new Integer(-1)).getResults();
        }
        catch (ProcCallException ignored) {
            caught = true;
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        assertTrue(caught);

        truncateTable(client, "P1");
    }

    // RE-ENABLE ONCE ENG-490 IS FIXED
//    private void subTestTicketEng490() throws IOException, ProcCallException {
//        Client client = getClient();
//
//        VoltTable[] results = client.callProcedure("Eng490Select").getResults();
//        assertEquals(1, results.length);
//
//        String query = "SELECT  A.ASSET_ID,  A.OBJECT_DETAIL_ID,  OD.OBJECT_DETAIL_ID " +
//                "FROM   ASSET A,  OBJECT_DETAIL OD WHERE   A.OBJECT_DETAIL_ID = OD.OBJECT_DETAIL_ID;";
//        results = client.callProcedure("@AdHoc", query).getResults();
//        assertEquals(1, results.length);
//    }

    private void subTestTicketEng993() throws IOException, ProcCallException
    {
        Client client = getClient();
        // this tests some other mumbo jumbo as well like ENG-999 and ENG-1001
        ClientResponse response = client.callProcedure("Eng993Insert", 5, 5.5);
        assertTrue(response.getStatus() == ClientResponse.SUCCESS);
        // Verify ENG-999 (Literal string 'NULL' round-trips as literal string
        // and doesn't transform into a SQL NULL value)
        response = client.callProcedure("@AdHoc", "select DESC from P1 where ID = 6");
        VoltTable result = response.getResults()[0];
        assertEquals("NULL", result.fetchRow(0).get(0, VoltType.STRING));

        // Additional verification that inserts are not bothered by math that used to
        // generate unexpectedly formatted temp tuples and garbled persistent tuples.
        // ENG-5926
        response = client.callProcedure("@AdHoc", "select * from P1");
        result = response.getResults()[0];
        result.advanceRow();
        assertEquals(6, result.getLong(0));
        assertEquals("NULL", result.getString(1));
        result.getLong(2);
        // Not sure what's up with HSQL failing to find null here.
        if ( ! isHSQL()) {
            assertTrue(result.wasNull());
        }
        assertEquals(6.5, result.getDouble(3));

        // Further verify that inline varchar columns still properly handle potentially larger values
        // even after the temp tuple formatting fix for ENG-5926.
        response = client.callProcedure("Eng5926Insert", 5, "", 5.5);
        assertTrue(response.getStatus() == ClientResponse.SUCCESS);
        try {
            response = client.callProcedure("Eng5926Insert", 7, "HOO", 7.5);
            fail("Failed to throw ProcCallException for runtime varchar length exceeded.");
        } catch(ProcCallException pce) {
        }
        response = client.callProcedure("@AdHoc", "select * from PWEE ORDER BY ID DESC");
        result = response.getResults()[0];
        result.advanceRow();
        assertEquals(6, result.getLong(0));
        assertEquals("WEE", result.getString(1));
        result.getLong(2);
        // Not sure what's up with HSQL failing to find null here.
        if ( ! isHSQL()) {
            assertTrue(result.wasNull());
        }
        assertEquals(6.5, result.getDouble(3));

        // this is the actual bug
        try {
            client.callProcedure("@AdHoc", "insert into P1 (ID,DESC,NUM,RATIO) VALUES('?',?,?,?);");
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Incorrect number of parameters passed: expected 3, passed 0"));
        }
        // test that missing parameters don't work (ENG-1000)
        try {
            client.callProcedure("@AdHoc", "insert into P1 (ID,DESC,NUM,RATIO) VALUES(?,?,?,?);");
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Incorrect number of parameters passed: expected 4, passed 0"));
        }
        //VoltTable results = client.callProcedure("@AdHoc", "select * from P1;").getResults()[0];
        //System.out.println(results.toJSONString());


        truncateTables(client, new String[]{"P1", "PWEE"});
    }

    /**
     * Verify that DML returns correctly named "modified_tuple" column name
     * @throws IOException
     * @throws ProcCallException
     */
    private void subTestTicketEng1316() throws IOException, ProcCallException
    {
        // Fake HSQL. Only care about Volt column naming code.
        if (isHSQL())
        {
            assertTrue(true);
            return;
        }

        Client client = getClient();
        ClientResponse rsp = null;

        // Test partitioned tables (multipartition query)
        rsp = client.callProcedure("Eng1316Insert_P", 100, "varcharvalue", 120, 1.0);
        assertEquals(1, rsp.getResults()[0].asScalarLong());
        assertEquals("modified_tuples", rsp.getResults()[0].getColumnName(0));
        rsp = client.callProcedure("Eng1316Insert_P", 101, "varcharvalue2", 121, 1.1);
        rsp = client.callProcedure("Eng1316Insert_P", 102, "varcharvalue2", 122, 1.2);
        rsp = client.callProcedure("Eng1316Insert_P", 103, "varcharvalue2", 123, 1.3);
        rsp = client.callProcedure("Eng1316Insert_P", 104, "varcharvalue2", 124, 1.4);
        rsp = client.callProcedure("Eng1316Update_P"); // update where id < 124
        assertEquals(4, rsp.getResults()[0].asScalarLong());
        assertEquals("modified_tuples", rsp.getResults()[0].getColumnName(0));

        // Test partitioned tables (single partition query)
        rsp = client.callProcedure("Eng1316Insert_P1", 200, "varcharvalue", 120, 1.0);
        assertEquals(1, rsp.getResults()[0].asScalarLong());
        assertEquals("modified_tuples", rsp.getResults()[0].getColumnName(0));
        rsp = client.callProcedure("Eng1316Insert_P1", 201, "varcharvalue2", 121, 1.1);
        rsp = client.callProcedure("Eng1316Insert_P1", 202, "varcharvalue2", 122, 1.2);
        rsp = client.callProcedure("Eng1316Insert_P1", 203, "varcharvalue2", 123, 1.3);
        rsp = client.callProcedure("Eng1316Insert_P1", 204, "varcharvalue2", 124, 1.4);
        rsp = client.callProcedure("Eng1316Update_P1", 201); // update where id == ?
        assertEquals(1, rsp.getResults()[0].asScalarLong());
        assertEquals("modified_tuples", rsp.getResults()[0].getColumnName(0));

        // Test replicated tables.
        rsp = client.callProcedure("Eng1316Insert_R", 100, "varcharvalue", 120, 1.0);
        assertEquals(1, rsp.getResults()[0].asScalarLong());
        assertEquals("modified_tuples", rsp.getResults()[0].getColumnName(0));
        rsp = client.callProcedure("Eng1316Insert_R", 101, "varcharvalue2", 121, 1.1);
        rsp = client.callProcedure("Eng1316Insert_R", 102, "varcharvalue2", 122, 1.2);
        rsp = client.callProcedure("Eng1316Insert_R", 103, "varcharvalue2", 123, 1.3);
        rsp = client.callProcedure("Eng1316Insert_R", 104, "varcharvalue2", 124, 1.4);
        rsp = client.callProcedure("Eng1316Update_R"); // update where id < 104
        assertEquals(4, rsp.getResults()[0].asScalarLong());
        assertEquals("modified_tuples", rsp.getResults()[0].getColumnName(0));

        truncateTables(client, new String[]{"P1", "R1"});
    }

    // make sure we can call an inner proc
    private void subTestTicket2423() throws NoConnectionsException, IOException, ProcCallException, InterruptedException {
        Client client = getClient();
        client.callProcedure("TestENG2423$InnerProc");
        releaseClient(client);
        // get it again to make sure the server is all good
        client = getClient();
        client.callProcedure("TestENG2423$InnerProc");
    }

    // Ticket: ENG-5151
    private void subTestTicket5151_ColumnDefaultNull() throws IOException, ProcCallException {
        System.out.println("STARTING default null test...");
        Client client = getClient();
        VoltTable result = null;
        // It used to throw errors from EE when inserting without giving explicit values for default null columns.
        result = client.callProcedure("@AdHoc",
                " INSERT INTO DEFAULT_NULL(id) VALUES (1);").getResults()[0];

        result = client.callProcedure("@AdHoc",
                " select id, num1, num2, ratio from DEFAULT_NULL;").getResults()[0];

        assertTrue(result.advanceRow());
        assertEquals(1, result.getLong(0));

        if (!isHSQL()) {
            result.getLong(1);
            assertTrue(result.wasNull());

            result.getLong(2);
            assertTrue(result.wasNull());

            result.getDouble(3);
            assertTrue(result.wasNull());
        }

        truncateTable(client, "DEFAULT_NULL");
    }

    // Ticket: ENG-5486
    private void subTestTicket5486_NULLcomparison() throws IOException, ProcCallException {
        System.out.println("STARTING default null test...");
        Client client = getClient();
        VoltTable result = null;
/**
            CREATE TABLE DEFAULT_NULL (
              ID INTEGER NOT NULL,
              num1 INTEGER DEFAULT NULL,
              num2 INTEGER ,
              ratio FLOAT DEFAULT NULL,
              num3 INTEGER DEFAULT NULL,
              desc VARCHAR(300) DEFAULT NULL,
              PRIMARY KEY (ID)
            );
            create index idx_num3 on DEFAULT_NULL (num3);
 */
        result = client.callProcedure("@AdHoc",
                " INSERT INTO DEFAULT_NULL(id) VALUES (1);").getResults()[0];
        validateTableOfScalarLongs(result, new long[]{1});

        // Test null column comparison
        result = client.callProcedure("@AdHoc",
                " select count(*), count(num1) from DEFAULT_NULL where num1 < 3;").getResults()[0];
        validateTableOfLongs(result, new long[][]{{0, 0}});

        result = client.callProcedure("@AdHoc",
                " select count(*), count(num1) from DEFAULT_NULL where num1 <= 3;").getResults()[0];
        validateTableOfLongs(result, new long[][]{{0, 0}});

        result = client.callProcedure("@AdHoc",
                " select count(*), count(num1) from DEFAULT_NULL where num1 > 3;").getResults()[0];
        validateTableOfLongs(result, new long[][]{{0, 0}});

        // Test null column comparison with index
        result = client.callProcedure("@AdHoc",
                " select count(*), count(num3) from DEFAULT_NULL where num3 > 3;").getResults()[0];
        validateTableOfLongs(result, new long[][]{{0, 0}});

        result = client.callProcedure("@AdHoc",
                " select count(*), count(num3) from DEFAULT_NULL where num3 < 3;").getResults()[0];
        validateTableOfLongs(result, new long[][]{{0, 0}});

        result = client.callProcedure("@AdHoc",
                " select count(*), count(num3) from DEFAULT_NULL where num3 <= 3;").getResults()[0];
        validateTableOfLongs(result, new long[][]{{0, 0}});

        result = client.callProcedure("@Explain",
                "select count(*) from DEFAULT_NULL where num3 < 3;").getResults()[0];
        //* enable for debugging */ System.out.println(result);

        // Reverse scan, count(*)
        result = client.callProcedure("@AdHoc",
                " select count(*) from DEFAULT_NULL where num3 < 3;").getResults()[0];
        validateTableOfScalarLongs(result, new long[]{0});

        truncateTable(client, "DEFAULT_NULL");
    }


    private void subTestENG4146() throws IOException, ProcCallException {
        System.out.println("STARTING insert no json string...");
        if (isHSQL()) {
            return;
        }
        Client client = getClient();
        VoltTable result = null;

        // it used to throw EE exception
        // when inserting a non-json encoded var char into a column that has a field() index;
        client.callProcedure("NO_JSON.insert",  1, "jpiekos1", "foo", "no json");

        result = client.callProcedure("@AdHoc","select id, var1, var2, var3 from no_json;").getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(1, result.getLong(0));

        assertEquals("jpiekos1", result.getString(1));
        assertEquals("foo", result.getString(2));
        assertEquals("no json", result.getString(3));

        client.callProcedure("NO_JSON.insert",  2, "jpiekos2", "foo2", "no json2");

        result = client.callProcedure("@AdHoc","select id from no_json " +
                "order by var2, field(var3,'color');").getResults()[0];
        validateTableOfLongs(result, new long[][] {{1},{2}});

        result = client.callProcedure("@AdHoc","select id from no_json " +
                "where var2 = 'foo' and field(var3,'color') = 'red';").getResults()[0];
        assertEquals(0, result.getRowCount());

        truncateTable(client, "NO_JSON");
    }

    // SQL HAVING bug on partitioned materialized table
    private void subTestENG5669() throws IOException, ProcCallException {
        System.out.println("STARTING testing HAVING......");
        Client client = getClient();
        VoltTable vt = null;

        String sqlArray =
                "INSERT INTO P3 VALUES (0, -5377, 837, -21764, 18749);" +
                "INSERT INTO P3 VALUES (1, -5377, 837, -21764, 26060);" +
                "INSERT INTO P3 VALUES (2, -5377, 837, -10291, 30855);" +
                "INSERT INTO P3 VALUES (3, -5377, 837, -10291, 10718);" +
                "INSERT INTO P3 VALUES (4, -5377, 24139, -12116, -26619);" +
                "INSERT INTO P3 VALUES (5, -5377, 24139, -12116, -28421);" +
                "INSERT INTO P3 VALUES (6, -5377, 24139, 26580, 21384);" +
                "INSERT INTO P3 VALUES (7, -5377, 24139, 26580, 16131);" +
                "INSERT INTO P3 VALUES (8, 24862, -32179, 17651, 15165);" +
                "INSERT INTO P3 VALUES (9, 24862, -32179, 17651, -27633);" +
                "INSERT INTO P3 VALUES (10, 24862, -32179, 12941, 12036);" +
                "INSERT INTO P3 VALUES (11, 24862, -32179, 12941, 18363);" +
                "INSERT INTO P3 VALUES (12, 24862, -25522, 7979, 3903);" +
                "INSERT INTO P3 VALUES (13, 24862, -25522, 7979, 19380);" +
                "INSERT INTO P3 VALUES (14, 24862, -25522, 29263, 2730);" +
                "INSERT INTO P3 VALUES (15, 24862, -25522, 29263, -19078);" +

                "INSERT INTO P3 VALUES (32, 1010, 1010, 1010, 1010);" +
                "INSERT INTO P3 VALUES (34, 1020, 1020, 1020, 1020);" +
                "INSERT INTO P3 VALUES (36, -1010, 1010, 1010, 1010);" +
                "INSERT INTO P3 VALUES (38, -1020, 1020, 1020, 1020);" +
                "INSERT INTO P3 VALUES (40, 3620, 5836, 10467, 31123);" +
                "INSERT INTO P3 VALUES (41, 3620, 5836, 10467, -28088);" +
                "INSERT INTO P3 VALUES (42, 3620, 5836, -29791, -8520);" +
                "INSERT INTO P3 VALUES (43, 3620, 5836, -29791, 24495);" +
                "INSERT INTO P3 VALUES (44, 3620, 4927, 18147, -27779);" +
                "INSERT INTO P3 VALUES (45, 3620, 4927, 18147, -30914);" +
                "INSERT INTO P3 VALUES (46, 3620, 4927, 8494, -30592);" +
                "INSERT INTO P3 VALUES (47, 3620, 4927, 8494, 20340);" +
                "INSERT INTO P3 VALUES (48, -670, 26179, -25323, -23185);" +
                "INSERT INTO P3 VALUES (49, -670, 26179, -25323, 22429);" +
                "INSERT INTO P3 VALUES (50, -670, 26179, -17828, 24248);" +
                "INSERT INTO P3 VALUES (51, -670, 26179, -17828, 4962);" +
                "INSERT INTO P3 VALUES (52, -670, -14477, -14488, 13599);" +
                "INSERT INTO P3 VALUES (53, -670, -14477, -14488, -14801);" +
                "INSERT INTO P3 VALUES (54, -670, -14477, 16827, -12008);" +
                "INSERT INTO P3 VALUES (55, -670, -14477, 16827, 27722);";

        // Test Default
        String []sqls = sqlArray.split(";");
        //* enable for debugging */ System.out.println(sqls);
        for (String sql: sqls) {
            sql = sql.trim();
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        }
        vt = client.callProcedure("@AdHoc", "SELECT SUM(V_SUM_RENT), SUM(V_G2) FROM V_P3;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {90814,-6200}});

        vt = client.callProcedure("@AdHoc", "SELECT SUM(V_SUM_RENT) FROM V_P3 HAVING SUM(V_G2) < 42").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {90814}});
        //* enable for debugging */ System.out.println(vt);

        truncateTable(client, "P3");
    }

    public void testVarchar() throws IOException, ProcCallException {
        subTestVarcharByBytes();
        subTestVarcharByCharacter();
        subTestInlineVarcharAggregation();
    }

    private void subTestVarcharByBytes() throws IOException, ProcCallException {
        System.out.println("STARTING testing varchar by BYTES ......");

        Client client = getClient();
        VoltTable vt = null;
        String var;

        var = "VO";
        client.callProcedure("@AdHoc", "Insert into VarcharBYTES (id, var2) VALUES (0,'" + var + "')");
        vt = client.callProcedure("@AdHoc", "select var2 from VarcharBYTES where id = 0").getResults()[0];
        validateTableColumnOfScalarVarchar(vt, new String[] {var});


        if (isHSQL()) return;
        var = "VOLT";
        try {
            client.callProcedure("@AdHoc", "Insert into VarcharBYTES (id, var2) VALUES (1,'" + var + "')");
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value '%s' exceeds the size of the VARCHAR(%d BYTES) column.",
                            var.length(), var, 2)));
        }

        var = "贾鑫";
        try {
            // assert here that this two-character string decodes via UTF8 to a bytebuffer longer than 2 bytes.
            assertEquals(2, var.length());
            assertEquals(6, var.getBytes("UTF-8").length);
            client.callProcedure("@AdHoc", "Insert into VarcharBYTES (id, var2) VALUES (1,'" + var + "')");
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value '%s' exceeds the size of the VARCHAR(%d BYTES) column.",
                            6, var, 2)));
        }

        var = "Voltdb is great | Voltdb is great " +
                "| Voltdb is great | Voltdb is great| Voltdb is great | Voltdb is great" +
                "| Voltdb is great | Voltdb is great| Voltdb is great | Voltdb is great";
        try {
            client.callProcedure("VARCHARBYTES.insert", 2, null, var);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value '%s...' exceeds the size of the VARCHAR(%d BYTES) column.",
                            var.length(), var.substring(0, VARCHAR_VARBINARY_THRESHOLD), 80)));
        }

        var = var.substring(0, 70);
        client.callProcedure("VARCHARBYTES.insert", 2, null, var);
        vt = client.callProcedure("@AdHoc", "select var80 from VarcharBYTES where id = 2").getResults()[0];
        validateTableColumnOfScalarVarchar(vt, new String[] {var});

        truncateTable(client, "VarcharBYTES");
    }

    private void subTestVarcharByCharacter() throws IOException, ProcCallException {
        System.out.println("STARTING testing varchar by character ......");

        Client client = getClient();
        VoltTable vt = null;
        String var;

        var = "VO";
        client.callProcedure("@AdHoc", "Insert into VarcharTB (id, var2) VALUES (0,'" + var + "')");
        vt = client.callProcedure("@AdHoc", "select var2 from VarcharTB where id = 0").getResults()[0];
        validateTableColumnOfScalarVarchar(vt, new String[] {var});

        var = "V贾";
        client.callProcedure("@AdHoc", "Insert into VarcharTB (id, var2) VALUES (1,'" + var + "')");
        vt = client.callProcedure("@AdHoc", "select var2 from VarcharTB where id = 1").getResults()[0];
        validateTableColumnOfScalarVarchar(vt, new String[] {var});

        // It used to fail to insert if VARCHAR column is calculated by BYTEs.
        var = "贾鑫";
        client.callProcedure("@AdHoc", "Insert into VarcharTB (id, var2) VALUES (2,'" + var + "')");
        vt = client.callProcedure("@AdHoc", "select var2 from VarcharTB where id = 2").getResults()[0];
        validateTableColumnOfScalarVarchar(vt, new String[] {var});

        var = "VoltDB是一个以内存数据库为主要产品的创业公司.";
        try {
            client.callProcedure("VARCHARTB.insert", 3, var, null);
            fail();
        } catch(Exception ex) {
            System.err.println(ex.getMessage());
            if (isHSQL()) {
                assertTrue(ex.getMessage().contains("HSQL Backend DML Error (data exception: string data, right truncation)"));
            } else {
                assertTrue(ex.getMessage().contains(
                        String.format("The size %d of the value '%s' exceeds the size of the VARCHAR(%d) column.",
                                var.length(), var, 2)));
                // var.length is 26;
            }
        }

        // insert into
        client.callProcedure("VARCHARTB.insert", 3, null, var);
        vt = client.callProcedure("@AdHoc", "select var80 from VarcharTB where id = 3").getResults()[0];
        validateTableColumnOfScalarVarchar(vt, new String[] {var});

        // Test threshold
        var += "它是Postgres和Ingres联合创始人Mike Stonebraker领导开发的下一代开源数据库管理系统。它能在现有的廉价服务器集群上实现每秒数百万次数据处理。" +
                "VoltDB大幅降低了服务器资源 开销，单节点每秒数据处理远远高于其它数据库管理系统。";
        try {
            client.callProcedure("VARCHARTB.insert", 4, null, var);
            fail();
        } catch(Exception ex) {
            System.err.println(ex.getMessage());
            if (isHSQL()) {
                assertTrue(ex.getMessage().contains("HSQL Backend DML Error (data exception: string data, right truncation)"));
            } else {
                assertTrue(ex.getMessage().contains(
                        String.format("The size %d of the value '%s...' exceeds the size of the VARCHAR(%d) column.",
                                var.length(), var.substring(0, 100), 80)));
            }
        }

        truncateTable(client, "VarcharTB");
    }

    private void subTestENG5637_VarcharVarbinaryErrorMessage() throws IOException, ProcCallException {
        System.out.println("STARTING testing error message......");

        if (isHSQL()) {
            return;
        }
        Client client = getClient();
        // Test Varchar

        // Test AdHoc
        String var1 = "Voltdb is a great database product";
        try {
            client.callProcedure("@AdHoc", "Insert into VARLENGTH (id, var1) VALUES (2,'" + var1 + "')");
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("Value ("+var1+") is too wide for a constant varchar value of size 10"));
        }

        try {
            client.callProcedure("@AdHoc", "Insert into VARLENGTH (id, var1) VALUES (2,'" + var1 + "' || 'abc')");
            fail();
        } catch(Exception ex) {
            //* enable for debugging */ System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains("Value ("+var1+"abc) is too wide for a constant varchar value of size 10"));
        }

        // Test inlined varchar with stored procedure
        try {
            client.callProcedure("VARLENGTH.insert", 1, var1, null, null, null);
            fail();
        } catch(Exception ex) {
            //* enable for debugging */ System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value '%s' exceeds the size of the VARCHAR(%d) column.",
                            var1.length(), var1, 10)));
        }

        // Test non-inlined varchar with stored procedure and threshold
        String var2 = "Voltdb is great | Voltdb is great " +
                "| Voltdb is great | Voltdb is great| Voltdb is great | Voltdb is great" +
                "| Voltdb is great | Voltdb is great| Voltdb is great | Voltdb is great";
        try {
            client.callProcedure("VARLENGTH.insert", 2, null, var2, null, null);
            fail();
        } catch(Exception ex) {
            //* enable for debugging */ System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value '%s...' exceeds the size of the VARCHAR(%d) column.",
                            174, var2.substring(0, VARCHAR_VARBINARY_THRESHOLD), 80)));
        }

        // Test non-inlined varchar with stored procedure
        var2 = "Voltdb is great | Voltdb is great " +
                "| Voltdb is great | Voltdb is great| Voltdb is great";
        try {
            client.callProcedure("VARLENGTH.insert", 21, null, var2, null, null);
            fail();
        } catch(Exception ex) {
            //* enable for debugging */ System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value '%s' exceeds the size of the VARCHAR(%d) column.",
                            86, var2, 80)));
        }

        // Test update
        client.callProcedure("VARLENGTH.insert", 1, "voltdb", null, null, null);
        try {
            client.callProcedure("VARLENGTH.update", 1, var1, null, null, null, 1);
            fail();
        } catch(Exception ex) {
            //* enable for debugging */ System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value '%s' exceeds the size of the VARCHAR(%d) column.",
                            var1.length(), var1, 10)));
        }


        // Test varbinary
        // Test AdHoc
        String bin1 = "1111111111111111111111000000";
        try {
            client.callProcedure("@AdHoc", "Insert into VARLENGTH (id, bin1) VALUES (6,'" + bin1 + "')");
            fail();
        } catch(Exception ex) {
            //* enable for debugging */ System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains("Value ("+bin1+") is too wide for a constant varbinary value of size 10"));
        }

        // Test inlined varchar with stored procedure
        try {
            client.callProcedure("VARLENGTH.insert", 7, null, null, bin1, null);
            fail();
        } catch(Exception ex) {
            //* enable for debugging */ System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value exceeds the size of the VARBINARY(%d) column.",
                            bin1.length()/2, 10)));
        }

        // Test non-inlined varchar with stored procedure
        String bin2 = "111111111111111111111100000011111111111111111111110000001111111111111111111111000000" +
                "111111111111111111111100000011111111111111111111110000001111111111111111111111000000" +
                "111111111111111111111100000011111111111111111111110000001111111111111111111111000000";
        try {
            client.callProcedure("VARLENGTH.insert", 2, null, null, null, bin2);
            fail();
        } catch(Exception ex) {
            //* enable for debugging */ System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value exceeds the size of the VARBINARY(%d) column.",
                            bin2.length() / 2, 80)));
        }

        // Test update
        client.callProcedure("VARLENGTH.insert", 7, null, null, "1010", null);
        try {
            client.callProcedure("VARLENGTH.update", 7, null, null, bin1, null, 7);
            fail();
        } catch(Exception ex) {
            //* enable for debugging */ System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains(
                    String.format("The size %d of the value exceeds the size of the VARBINARY(%d) column.",
                            bin1.length()/2, 10)));
        }


        truncateTable(client, "VARLENGTH");
    }

    // This is a regression test for ENG-6792
    private void subTestInlineVarcharAggregation() throws IOException, ProcCallException {
        Client client = getClient();
        ClientResponse cr;

        cr = client.callProcedure("VARCHARTB.insert",  1, "zz", "panda");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("VARCHARTB.insert", 6, "a", "panda");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("VARCHARTB.insert", 7, "mm", "panda");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("VARCHARTB.insert",  8, "z", "orangutan");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("VARCHARTB.insert", 9, "aa", "orangutan");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("VARCHARTB.insert", 10, "n", "orangutan");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "select max(var2), min(var2) from VarcharTB");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals("zz", vt.getString(0));
        assertEquals("a", vt.getString(1));

        // Hash aggregation may have the same problem, so let's
        // test it here as well.
        String sql = "select var80, max(var2) as maxvar2, min(var2) as minvar2 " +
                "from VarcharTB " +
                "group by var80 " +
                "order by maxvar2, minvar2";
                cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());

        // row 1: panda, zz, a
        // row 2: orangutan, z, aa
        assertEquals("orangutan", vt.getString(0));
        assertEquals("z", vt.getString(1));
        assertEquals("aa", vt.getString(2));

        assertTrue(vt.advanceRow());
        assertEquals("panda", vt.getString(0));
        assertEquals("zz", vt.getString(1));
        assertEquals("a", vt.getString(2));

        cr = client.callProcedure("PWEE_WITH_INDEX.insert", 0, "MM", 88);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("PWEE_WITH_INDEX.insert", 1, "ZZ", 88);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("PWEE_WITH_INDEX.insert", 2, "AA", 88);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("PWEE_WITH_INDEX.insert", 3, "NN", 88);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "select num, max(wee), min(wee) " +
                "from pwee_with_index group by num order by num");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals("ZZ", vt.getString(1));
        assertEquals("AA", vt.getString(2));


        truncateTables(client, new String[]{"VARCHARTB", "PWEE_WITH_INDEX"});
    }

    // Bug: parser drops extra predicates over certain numbers e.g. 10.
    private void subTestENG6870() throws IOException, ProcCallException {
        System.out.println("test ENG6870...");

        Client client = this.getClient();
        VoltTable vt;
        String sql;

        client.callProcedure("ENG6870.insert",
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, null, 1, 1);

        client.callProcedure("ENG6870.insert",
                2, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1);

        client.callProcedure("ENG6870.insert",
                3, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1);

        sql = "SELECT COUNT(*) FROM ENG6870 "
                + "WHERE C14 = 1 AND C1 IS NOT NULL AND C2 IS NOT NULL "
                + "AND C5  = 3 AND C7 IS NOT NULL AND C8 IS NOT NULL "
                + "AND C0 IS NOT NULL AND C10 IS NOT NULL "
                + "AND C11 IS NOT NULL AND C13 IS NOT NULL  "
                + "AND C12 IS NOT NULL;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{0});

        truncateTable(client, "ENG6870");
    }

    private void subTestInsertWithCast() throws IOException, ProcCallException {
        Client client = getClient();
        client.callProcedure("@AdHoc", "delete from p1");

        // in ENG-5929, this would cause a null pointer exception,
        // because OperatorException.refineValueType was not robust to casts.
        String stmt = "insert into p1 (id, num) values (1, cast(1 + ? as integer))";
        VoltTable vt = client.callProcedure("@AdHoc", stmt, 100).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // This should even work when assigning the expression to the partitioning column:
        // Previously this would fail with a mispartitioned tuple error.
        stmt = "insert into p1 (id, num) values (cast(1 + ? as integer), 1)";
        vt = client.callProcedure("@AdHoc", stmt, 100).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        stmt = "select id, num from p1 order by id";
        vt = client.callProcedure("@AdHoc", stmt).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1, 101}, {101, 1}});

        truncateTable(client, "P1");
    }

    private void subTestENG6926() throws Exception {
        // Aggregation of a joined table was not ordered
        // according to ORDER BY clause when the OB column
        // was not first in the select list.

        Client client = getClient();

        String insStmt = "insert into eng6926_ipuser(ip, countrycode, province) values (?, ?, ?)";
        client.callProcedure("@AdHoc", insStmt, "23.101.135.101", "US", "District of Columbia");
        client.callProcedure("@AdHoc", insStmt, "23.101.142.5", "US", "District of Columbia");
        client.callProcedure("@AdHoc", insStmt, "23.101.143.89", "US", "District of Columbia");
        client.callProcedure("@AdHoc", insStmt, "23.101.138.62", "US", "District of Columbia");
        client.callProcedure("@AdHoc", insStmt, "69.67.23.26", "US", "Minnesota");
        client.callProcedure("@AdHoc", insStmt, "198.179.137.202", "US", "Minnesota");
        client.callProcedure("@AdHoc", insStmt, "23.99.35.61", "US", "Washington");

        insStmt = "insert into eng6926_hits(ip, week) values (?, ?)";
        client.callProcedure("@AdHoc", insStmt, "23.101.135.101", 20140914);
        client.callProcedure("@AdHoc", insStmt, "23.101.142.5", 20140914);
        client.callProcedure("@AdHoc", insStmt, "23.101.143.89", 20140914);
        client.callProcedure("@AdHoc", insStmt, "23.101.138.62", 20140914);
        client.callProcedure("@AdHoc", insStmt, "69.67.23.26", 20140914);
        client.callProcedure("@AdHoc", insStmt, "198.179.137.202", 20140914);
        client.callProcedure("@AdHoc", insStmt, "23.99.35.61", 20140914);

        String query = "select count(ip.ip), ip.province as state " +
                "from eng6926_hits as h, eng6926_ipuser as ip " +
                "where ip.ip=h.ip and ip.countrycode='US' " +
                    "group by ip.province " + "order by count(ip.ip) desc";

        VoltTable vt = client.callProcedure("@AdHoc", query).getResults()[0];
        long[] col0Expected = new long[] {4, 2, 1};
        String[] col1Expected = new String[] {"District of Columbia", "Minnesota", "Washington"};
        int i = 0;
        while (vt.advanceRow()) {
            assertEquals(col0Expected[i], vt.getLong(0));
            assertEquals(col1Expected[i], vt.getString(1));
            ++i;
        }

        truncateTable(client, "eng6926_ipuser");
    }

    private void subTestENG7041ViewAndExportTable() throws Exception {
        Client client = getClient();

        // Materialized view wasn't being updated, because the
        // connection with its source table wasn't getting created
        // when there was a (completely unrelated) export table in the
        // database.
        //
        // When loading the catalog in the EE, we were erroneously
        // aborting view processing when encountering an export table.
        client.callProcedure("TRANSACTION.insert", 1, 99, 100.0, "NH", "Manchester", new TimestampType(), 20);

        validateTableOfLongs(client, "select count(*) from transaction",
                new long[][] {{1}});

        // The buggy behavior would show zero rows in the view.
        validateTableOfLongs(client, "select count(*) from acct_vendor_totals",
                new long[][] {{1}});

        truncateTable(client, "TRANSACTION");
    }

    private void subTestENG7349_InnerJoinWithOverflow() throws IOException, ProcCallException {
        // In this bug, ENG-7349, we would fail an erroneous assertion
        // in the EE that we must have more than one active index key when
        // joining with a multi-component index.

        Client client = getClient();

        VoltTable vt = client.callProcedure("SM_IDX_TBL.insert", 1, 1, 1000)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        validateTableOfLongs(client,
                "select * "
                + "from sm_idx_tbl as t1 inner join sm_idx_tbl as t2 "
                + "on t1.ti1 = t2.bi",
                new long[][] {});

        truncateTable(client, "SM_IDX_TBL");
    }

    private void insertForInParamsTests(Client client) throws IOException, ProcCallException {
        for (int i = 0; i < 10; ++i) {
            VoltTable vt = client.callProcedure("P1.insert",
                    i, Integer.toString(i), i * 10, i * 100.0)
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});
        }
    }

    public void testInWithString() throws IOException, ProcCallException, InterruptedException {
        subTestInWithIntParams();
        subTestInWithStringParams();
        subTestInWithStringParamsAdHoc();
        subTestInWithStringParamsAsync();
    }


    // Note: the following tests for IN with parameters should at some point
    // be moved into their own suite along with existing tests for IN
    // that now live in TestIndexesSuite.  This is ENG-7607.
    private void subTestInWithIntParams() throws IOException, ProcCallException {
        // HSQL does not support WHERE f IN ?
        if (isHSQL())
            return;

        Client client = getClient();
        insertForInParamsTests(client);

        VoltTable vt = client.callProcedure("one_list_param", new int[] {1, 2})
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1, 2});

        // The following error message characterizes what happens if the
        // users passes long array to an IN parameter on an INTEGER column.
        // VoltDB requires that the data types match exactly here.
        //
        // This error message isn't that friendly (ENG-7606).
        verifyProcFails(client,
                "tryScalarMakeCompatible: "
                + "Unable to match parameter array:int to provided long",
                "one_list_param", new long[] {1, 2});

        // scalar param where list should be provided fails
        verifyProcFails(client,
                "Array / Scalar parameter mismatch",
                "one_list_param", 1);

        vt = client.callProcedure("one_scalar_param", 5)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {5});

        // passing a list to a scalar int parameter fails
        verifyProcFails(client , "Array / Scalar parameter mismatch",
                "one_scalar_param", new long[] {1, 2});


        truncateTable(client, "P1");
    }

    private void subTestInWithStringParams() throws IOException, ProcCallException {
        if (isHSQL())
            return;

        Client client = getClient();
        insertForInParamsTests(client);

        String[] stringArgs = {"7", "8"};

        // For vararg methods like callProcedure, when there is an array of objects
        // (not an array of native types) passed as the only vararg argument, the
        // compile-time type affects how the compiler presents the arguments to the
        // callee:
        //    cast to Object   - callProcedure sees just one param (which is an array)
        //    cast to Object[] - (or a subclass of Object[]) callee sees each array
        //                       element as its own parameter value

        // where desc in ?
        // Cast parameter value as an object and it's treated as a single parameter in the callee.
        VoltTable vt = client.callProcedure("one_string_list_param", (Object)stringArgs)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {7, 8});

        // where desc in ?
        // Casting the argument to object array means it's treated
        // as two arguments in the callee.
        verifyProcFails(client, "EXPECTS 1 PARAMS, BUT RECEIVED 2",
                "one_string_list_param", (Object[])stringArgs);

        // where desc in ?
        // scalar parameter fails
        verifyProcFails(client, "Array / Scalar parameter mismatch",
                "one_string_list_param", "scalar param");

        // where desc in (?)
        // Caller treats this as a single list parameter.
        verifyProcFails(client, "Array / Scalar parameter mismatch",
                "one_string_scalar_param", (Object)stringArgs);

        // where desc in (?)
        // Cast to an array type makes caller treat this as two arguments.
        verifyProcFails(client, "EXPECTS 1 PARAMS, BUT RECEIVED 2",
                 "one_string_scalar_param", (Object[])stringArgs);

        // where desc in (?)
        // This succeeds as it should
        vt = client.callProcedure("one_string_scalar_param", "9")
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {9});


        truncateTable(client, "P1");
    }

    private void subTestInWithStringParamsAdHoc() throws IOException, ProcCallException {
        if (isHSQL())
            return;

        Client client = getClient();
        insertForInParamsTests(client);

        String[] stringArgs = {"7", "8"};

        String adHocQueryWithListParam = "select id from P1 where desc in ?";
        String adHocQueryWithScalarParam = "select id from P1 where desc in (?)";

        VoltTable vt;

        verifyProcFails(client, "Array / Scalar parameter mismatch",
                "@AdHoc", adHocQueryWithListParam, stringArgs);

        // where desc in ?
        // scalar parameter fails
        verifyProcFails(client, "rhs of IN expression is of a non-list type varchar",
                "@AdHoc", adHocQueryWithListParam, "scalar param");

        // where desc in (?)
        // Caller treats this as a single list parameter.
        verifyProcFails(client, "Array / Scalar parameter mismatch",
                "@AdHoc", adHocQueryWithScalarParam, stringArgs);

        // where desc in (?)
        // This succeeds as it should
        vt = client.callProcedure("@AdHoc", adHocQueryWithScalarParam, "9")
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {9});

        truncateTable(client, "P1");
    }

    static private final class SimpleCallback implements ProcedureCallback {

        private ClientResponse m_clientResponse = null;

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            m_clientResponse = clientResponse;
        }

        public ClientResponse getClientResponse() {
            return m_clientResponse;
        }
    }

    private void subTestInWithStringParamsAsync() throws IOException, ProcCallException, InterruptedException {
        if (isHSQL())
            return;

        // There is nothing particularly special about asynchronous procedure calls
        // with IN and parameters.  I wrote these test cases to try and
        // reproduce ENG-7354, which was closed as "not a bug."
        //
        // There doesn't seem to be a lot of tests for async call error recovery,
        // so these tests are preserved here (hopefully they can find a better
        // home someday).

        Client client = getClient();
        insertForInParamsTests(client);
        String[] stringArgs = {"7", "8"};

        // Try with the async version of callProcedure.
        boolean b;
        SimpleCallback callback = new SimpleCallback();
        b = client.callProcedure(callback,
                "one_string_scalar_param", (Object)stringArgs);
        // This is queued, but execution fails as it should.
        assertTrue(b);
        client.drain();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, callback.getClientResponse().getStatus());
        assertTrue(callback.getClientResponse().getStatusString().contains(
                "Array / Scalar parameter mismatch"));

        b = client.callProcedure(callback,
                "one_string_scalar_param", (Object[])stringArgs);
        // This is queued, but execution fails as it should.
        assertTrue(b);
        client.drain();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, callback.getClientResponse().getStatus());
        assertTrue(callback.getClientResponse().getStatusString().contains(
                "EXPECTS 1 PARAMS, BUT RECEIVED 2"));

        // This should succeed
        b = client.callProcedure(callback,
                "one_string_list_param", (Object)stringArgs);
        assertTrue(b);
        client.drain();
        assertEquals(ClientResponse.SUCCESS, callback.getClientResponse().getStatus());
        VoltTable vt = callback.getClientResponse().getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {7, 8});

        // Try some ad hoc queries as well.
        String adHocQueryWithListParam = "select id from P1 where desc in ?";
        String adHocQueryWithScalarParam = "select id from P1 where desc in (?)";

        // Here's what happens with too many parameters
        b = client.callProcedure(callback,
                "one_string_scalar_param", "dog", "cat");
        // This is queued, but execution fails as it should.
        assertTrue(b);
        client.drain();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, callback.getClientResponse().getStatus());
        assertTrue(callback.getClientResponse().getStatusString().contains(
                "EXPECTS 1 PARAMS, BUT RECEIVED 2"));

        b = client.callProcedure(callback, "@AdHoc",
                adHocQueryWithScalarParam, stringArgs);
        assertTrue(b);
        client.drain();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, callback.getClientResponse().getStatus());
        assertTrue(callback.getClientResponse().getStatusString().contains(
                "Array / Scalar parameter mismatch"));

        // This should succeed, but doesn't (ENG-7604 again)
        b = client.callProcedure(callback, "@AdHoc",
                adHocQueryWithListParam, stringArgs);
        assertTrue(b);
        client.drain();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, callback.getClientResponse().getStatus());
        assertTrue(callback.getClientResponse().getStatusString().contains(
                "Array / Scalar parameter mismatch"));

        truncateTable(client, "P1");
    }

    private void subTestENG7724() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = client.callProcedure("voltdbSelectProductChanges", 1, 1).getResults()[0];
        assertEquals(13, vt.getColumnCount());
    }

    private void runQueryGetDecimal(Client client, String sql, double value) throws IOException, ProcCallException {
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(value, vt.getDecimalAsBigDecimal(0).doubleValue(), 0.0001);
    }

    private void runQueryGetDouble(Client client, String sql, double value) throws IOException, ProcCallException {
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(value, vt.getDouble(0), 0.0001);
    }

    private void subTestENG7480() throws IOException, ProcCallException {
        Client client = getClient();

        String sql;
        sql = "insert into R1 Values(1, 'MA', 2, 2.2);";
        client.callProcedure("@AdHoc", sql);
        // query constants interpreted as DECIMAL

        //
        // operation between float and decimal
        //
        sql = "SELECT 0.1 + (1-0.1) + ratio FROM R1";
        runQueryGetDouble(client, sql, 3.2);

        sql = "SELECT 0.1 + (1-0.1) - ratio FROM R1";
        runQueryGetDouble(client, sql, -1.2);

        sql = "SELECT 0.1 + (1-0.1) / ratio FROM R1";
        runQueryGetDouble(client, sql, 0.509090909091);

        sql = "SELECT 0.1 + (1-0.1) * ratio FROM R1";
        runQueryGetDouble(client, sql, 2.08);

        // reverse order
        sql = "SELECT 0.1 + ratio + (1-0.1) FROM R1";
        runQueryGetDouble(client, sql, 3.2);

        sql = "SELECT 0.1 + ratio - (1-0.1) FROM R1";
        runQueryGetDouble(client, sql, 1.4);

        sql = "SELECT 0.1 + ratio / (1-0.1) FROM R1";
        runQueryGetDouble(client, sql, 2.544444444444);

        sql = "SELECT 0.1 + ratio * (1-0.1) FROM R1";
        runQueryGetDouble(client, sql, 2.08);


        //
        // operation between decimal and integer
        //
        sql = "SELECT 0.1 + (1-0.1) + NUM FROM R1";
        runQueryGetDecimal(client, sql, 3.0);

        sql = "SELECT 0.1 + (1-0.1) - NUM FROM R1";
        runQueryGetDecimal(client, sql, -1.0);

        sql = "SELECT 0.1 + (1-0.1) / NUM FROM R1";
        runQueryGetDecimal(client, sql, 0.55);

        sql = "SELECT 0.1 + (1-0.1) * NUM FROM R1";
        runQueryGetDecimal(client, sql, 1.9);

        // reverse order
        sql = "SELECT 0.1 + NUM + (1-0.1) FROM R1";
        runQueryGetDecimal(client, sql, 3.0);

        sql = "SELECT 0.1 + NUM - (1-0.1) FROM R1";
        runQueryGetDecimal(client, sql, 1.2);

        sql = "SELECT 0.1 + NUM / (1-0.1) FROM R1";
        runQueryGetDecimal(client, sql, 2.322222222222);

        sql = "SELECT 0.1 + NUM * (1-0.1) FROM R1";
        runQueryGetDecimal(client, sql, 1.9);


        //
        // test Out of range decimal and float
        //

        // test overflow and any underflow decimal are rounded
        sql = "SELECT NUM + 111111111111111111111111111111111111111.1111 FROM R1";
        if (isHSQL()) {
            verifyStmtFails(client, sql, "HSQL-BACKEND ERROR");
            verifyStmtFails(client, sql, "to the left of the decimal point is 39 and the max is 26");
        } else {
            verifyStmtFails(client, sql, "Maximum precision exceeded. "
                    + "Maximum of 26 digits to the left of the decimal point");
        }

        sql = "SELECT NUM + 111111.1111111111111111111111111111111111111 FROM R1";
        runQueryGetDecimal(client, sql, 111113.1111111111111111111111111111111111111);

        sql = "SELECT NUM + " + StringUtils.repeat("1", 256) + ".1111E1 FROM R1";
        runQueryGetDouble(client, sql, Double.parseDouble(StringUtils.repeat("1", 255) + "3.1111E1"));

        sql = "SELECT NUM + " + StringUtils.repeat("1", 368) + ".1111E1 FROM R1";
        verifyStmtFails(client, sql, "java.lang.NumberFormatException");


        // test stored procedure
        VoltTable vt = null;
        vt = client.callProcedure("R1_PROC1").getResults()[0];
        validateTableColumnOfScalarDecimal(vt, 0, new BigDecimal[]{new BigDecimal(2.1)});

        vt = client.callProcedure("R1_PROC2").getResults()[0];
        validateTableColumnOfScalarFloat(vt, 0, new double[]{2.1});

        truncateTable(client, "R1");
    }


    private void nullIndexSearchKeyChecker(Client client, String sql, String tbleName, String columnName) throws IOException, ProcCallException {
        VoltTable vt;
        vt = client.callProcedure("@AdHoc", sql, null).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});


        String sql1;
        // We replace a select list element with its count.
        // if tbleName == null,
        //   Replace "SELECT $columnName" with "SELECT COUNT($columnName)"
        // else
        //   Replace "SELECT $tbleName.$columnName" with "SELECT COUNT($tableName.$columnName)"
        //
        // Of course, we can't use $tbleName and $columnName, so we need to
        // do some hacking with strings.
        //
        String pattern = ((tbleName == null) ? "" : (tbleName + ".")) + columnName;
        String selectListElement = "SELECT " + pattern;
        String repl = "SELECT COUNT(" + pattern + ")";
        sql1 = sql.replace(selectListElement, repl);
        assertTrue(sql1.contains(repl + " FROM"));
        vt = client.callProcedure("@AdHoc", sql1, null).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{0});
        String sql2 = sql.replace(selectListElement, "SELECT COUNT(*)");
        assertTrue(sql2.contains("SELECT COUNT(*) FROM"));
        vt = client.callProcedure("@AdHoc", sql2, null).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{0});
    }

    private void subTestENG8120() throws IOException, ProcCallException {
        // hsqldb does not handle null
        if (isHSQL()) {
            return;
        }

        Client client = getClient();
        VoltTable vt;
        String sql;

        String[] tables = {"R1", "R3", "R4"};
        for (String tb : tables)
        {
            sql = "insert into " + tb + "  (id, num) Values(?, ?);";
            client.callProcedure("@AdHoc", sql, 1, null);
            client.callProcedure("@AdHoc", sql, 2, null);
            client.callProcedure("@AdHoc", sql, 3, 3);
            client.callProcedure("@AdHoc", sql, 4, 4);

            sql = "select count(*) from " + tb;
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfScalarLongs(vt, new long[]{4});

            // activate # of searchkey is 1
            sql = "SELECT ID FROM " + tb + " B WHERE B.ID > ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID >= ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID = ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID < ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID <= ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            // activate # of searchkey is 2
            sql = "SELECT ID FROM " + tb + " B WHERE B.ID = 3 and num > ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID = 3 and num >= ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID = 3 and num = ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID = 3 and num < ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID = 3 and num <= ?;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            // post predicate
            sql = "SELECT ID FROM " + tb + " B WHERE B.ID > ? and num > 1;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID = ? and num > 1;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            sql = "SELECT ID FROM " + tb + " B WHERE B.ID < ? and num > 1;";
            nullIndexSearchKeyChecker(client, sql, null, "ID");

            // nest loop index join
            sql = "SELECT A.ID FROM R4 A, " + tb + " B WHERE B.ID = A.ID and B.num > ?;";

            if (tb != "R4") {
                vt = client.callProcedure("@Explain", sql, null).getResults()[0];
                assertTrue(vt.toString().contains("inline INDEX SCAN of \"" + tb));
                assertTrue(vt.toString().contains("SEQUENTIAL SCAN of \"R4"));
            }
            nullIndexSearchKeyChecker(client, sql, "A", "ID");

            sql = "SELECT A.ID FROM R4 A, " + tb + " B WHERE B.ID = A.ID and B.num >= ?;";
            nullIndexSearchKeyChecker(client, sql, "A", "ID");

            sql = "SELECT A.ID FROM R4 A, " + tb + " B WHERE B.ID = A.ID and B.num = ?;";
            nullIndexSearchKeyChecker(client, sql, "A", "ID");

            sql = "SELECT A.ID FROM R4 A, " + tb + " B WHERE B.ID = A.ID and B.num < ?;";
            nullIndexSearchKeyChecker(client, sql, "A", "ID");

            sql = "SELECT A.ID FROM R4 A, " + tb + " B WHERE B.ID = A.ID and B.num <= ?;";
            nullIndexSearchKeyChecker(client, sql, "A", "ID");
        }

        truncateTables(client, tables);
    }

    private void subTestENG9032() throws IOException, ProcCallException {
        System.out.println("test subTestENG9032...");
        Client client = getClient();
        String sql;

        sql = "INSERT INTO t1 VALUES (NULL, 1);";
        client.callProcedure("@AdHoc", sql);
        sql = "INSERT INTO t1 VALUES (10, 2);";
        client.callProcedure("@AdHoc", sql);
        sql = "INSERT INTO t1 VALUES (20, 3);";
        client.callProcedure("@AdHoc", sql);

        sql = "SELECT * from t1 where a < 15 order by a;";
        validateTableOfLongs(client, sql, new long[][]{{10, 2}});

        truncateTable(client, "T1");
    }

    private void subTestENG9389() throws IOException, ProcCallException {
        System.out.println("test subTestENG9389 outerjoin is null...");
        Client client = getClient();
        String sql;

        sql = "INSERT INTO t1 VALUES (1, 2);";
        client.callProcedure("@AdHoc", sql);
        sql = "INSERT INTO t1 VALUES (2, 2);";
        client.callProcedure("@AdHoc", sql);
        sql = "INSERT INTO t1 VALUES (3, 2);";

        client.callProcedure("@AdHoc", sql);
        sql = "INSERT INTO t2 VALUES (2, NULL);";
        client.callProcedure("@AdHoc", sql);

        sql = "INSERT INTO t3 VALUES (2, 2, NULL);";
        client.callProcedure("@AdHoc", sql);
        sql = "INSERT INTO t3 VALUES (3, 3, 10);";
        client.callProcedure("@AdHoc", sql);

        sql = "INSERT INTO t3_no_index VALUES (2, 2, NULL);";
        client.callProcedure("@AdHoc", sql);
        sql = "INSERT INTO t3_no_index VALUES (3, 3, 10);";
        client.callProcedure("@AdHoc", sql);

        // NULL padded row in T3 will trigger the bug ENG-9389
        // Test with both indexed and unindexed inner table to exercise both
        // nested-loop and nested-loop-index joins
        for (String innerTable : new String[] {"t3", "t3_no_index"}) {

            sql = "select t1.A "
                    + "from t1 left join " + innerTable + " as t3 "
                    + "on t3.A = t1.A "
                    + "where t3.D is null and t1.B = 2 "
                    + "order by t1.A;";
            validateTableOfScalarLongs(client, sql, new long[]{1, 2});

            sql = " select t1.A "
                    + "from T1 left join " + innerTable + " as t3 "
                    + "on t3.A = t1.A "
                    + "where t3.D is null and t1.B = 2 "
                    + "and exists(select 1 from t2 where t2.B = t1.B and t2.D is null) "
                    + "order by t1.a;";
            validateTableOfScalarLongs(client, sql, new long[]{1, 2});

            sql = "select t1.A "
                    + "from t1 inner join t2 on t2.B = t1.B "
                    + "left join " + innerTable + " as t3 "
                    + "on t3.A = t1.A "
                    + "where t2.D is null and t3.D is null and t2.B = 2 "
                    + "order by t1.a;";
            validateTableOfScalarLongs(client, sql, new long[]{1, 2});

            sql = "select t1.b + t3.d as thesum "
                    + "from t1 "
                    + "left outer join " + innerTable + " as t3 "
                    + "on t1.a = t3.a "
                    + "where t1.b > 1 "
                    + "order by thesum;";
            System.out.println(client.callProcedure("@Explain", sql).getResults()[0]);
            validateTableOfScalarLongs(client, sql, new long[]{Long.MIN_VALUE, Long.MIN_VALUE, 12});
        }

        truncateTables(client, new String[]{"T1", "T2", "T3", "T3_NO_INDEX"});
    }

    private void subTestENG9533() throws IOException, ProcCallException {
        System.out.println("test subTestENG9533 outerjoin with OR pred...");
        Client client = getClient();
        String insStmts[] = {
                "insert into test1_eng_9533 values (0);",
                "insert into test1_eng_9533 values (1);",
                "insert into test1_eng_9533 values (2);",
                "insert into test1_eng_9533 values (3);",
                "insert into test2_eng_9533 values (1, 'athing', 'one', 5);",
                "insert into test2_eng_9533 values (2, 'otherthing', 'two', 10);",
                "insert into test2_eng_9533 values (3, 'yetotherthing', 'three', 3);"
        };

        for (String stmt : insStmts) {
            validateTableOfScalarLongs(client, stmt, new long[] {1});
        }

        String sqlStmt =
                "select "
                + "  id, t_int "
                + "from test1_eng_9533 "
                + "  left join test2_eng_9533 "
                + "  on t_id = id "
                + "where "
                + "  id <= 1 or t_int > 4 "
                + "order by id * 2"; // this order by is so that we don't force an index scan on the outer table.

        validateTableOfLongs(client, sqlStmt, new long[][] {
                {0, Long.MIN_VALUE},
                {1, 5},
                {2, 10}
        });
    }

    private void subTestENG9796() throws IOException, ProcCallException {
        Client client = getClient();

        // In this bug, result tables that had duplicate column names
        // (not possible for a persistent DB table, but is possible
        // for the output of a join or a subquery), produced wrong
        // answers.

        //                                id, desc,  num, ratio
        client.callProcedure("p1.Insert", 10, "foo", 20,  40.0);
        client.callProcedure("r1.Insert", 11, "bar", 20,  99.0);
        client.callProcedure("r2.Insert", 12, "baz", 20,  111.0);

        VoltTable vt;
        vt = client.callProcedure("@AdHoc",
                "select * from (select id as zzz, num as zzz from p1) as derived")
                .getResults()[0];
        assertContentOfTable(new Object[][] {{10, 20}}, vt);

        vt = client.callProcedure("@AdHoc",
                "select * from (select id  * 5 as zzz, num * 10 as zzz from p1) as derived")
                .getResults()[0];
        assertContentOfTable(new Object[][] {{50, 200}}, vt);

        vt = client.callProcedure("@AdHoc",
                "select S1.* "
                + "from (R1 join R2 using(num)) as S1,"
                + "     (R1 join R2 using(num)) as S2")
                .getResults()[0];
        System.out.println(vt);
        assertContentOfTable(new Object[][] {{20, 11, "bar", 99.0, 12, "baz", 111.0}}, vt);

        vt = client.callProcedure("@AdHoc",
                "select * "
                + "from (R1 join R2 using(num)) as S1,"
                + "     (R1 join R2 using(num)) as S2")
                .getResults()[0];
        System.out.println(vt);
        assertContentOfTable(new Object[][] {{
            20, 11, "bar", 99.0, 12, "baz", 111.0,
            20, 11, "bar", 99.0, 12, "baz", 111.0
            }}, vt);
        truncateTables(client, new String[] {"p1", "r1", "r2"});
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestFixedSQLSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestFixedSQLSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(Insert.class.getResource("fixed-sql-ddl.sql"));
        project.addProcedures(PROCEDURES);

        // Now that this fails to compile with an overflow error, it should be migrated to a
        // Failures suite.
        //project.addStmtProcedure("Crap", "insert into COUNT_NULL values (" + Long.MIN_VALUE + ", 1, 200)");

        project.addStmtProcedure("Eng397Limit1", "Select P1.NUM from P1 order by P1.NUM limit ?;");
//        project.addStmtProcedure("Eng490Select", "SELECT A.ASSET_ID, A.OBJECT_DETAIL_ID,  OD.OBJECT_DETAIL_ID FROM ASSET A, OBJECT_DETAIL OD WHERE A.OBJECT_DETAIL_ID = OD.OBJECT_DETAIL_ID;");
        project.addStmtProcedure("InsertNullString", "Insert into STRINGPART values (?, ?, ?);",
                                 "STRINGPART.NAME: 0");
        project.addStmtProcedure("Eng993Insert", "insert into P1 (ID,DESC,NUM,RATIO) VALUES(1+?,'NULL',NULL,1+?);");
        project.addStmtProcedure("Eng5926Insert", "insert into PWEE (ID,WEE,NUM,RATIO) VALUES(1+?,?||'WEE',NULL,1+?);");

        project.addStmtProcedure("Eng1316Insert_R", "insert into R1 values (?, ?, ?, ?);");
        project.addStmtProcedure("Eng1316Update_R", "update R1 set num = num + 1 where id < 104");
        project.addStmtProcedure("Eng1316Insert_P", "insert into P1 values (?, ?, ?, ?);");
        project.addStmtProcedure("Eng1316Update_P", "update P1 set num = num + 1 where id < 104");
        project.addStmtProcedure("Eng1316Insert_P1", "insert into P1 values (?, ?, ?, ?);", "P1.ID: 0");
        project.addStmtProcedure("Eng1316Update_P1", "update P1 set num = num + 1 where id = ?", "P1.ID: 0");

        //* CONFIG #1: JNI -- keep this enabled by default with / / vs. / *
        config = new LocalCluster("fixedsql-threesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        /*/ // CONFIG #1b: IPC -- keep this normally disabled with / * vs. //
        config = new LocalCluster("fixedsql-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // end of normally disabled section */

        // CONFIG #2: HSQL
        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
