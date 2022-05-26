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

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.planner.TestPlansInExistsSubQueries;

public class TestSubQueriesSuite extends RegressionSuite {
    //ENG-15265
    public TestSubQueriesSuite(String name) {
        super(name);
    }

    static protected void validateTableOfLongs(Client c, String sql, long[][] expected)
            throws IOException, ProcCallException {
        if (! USING_CALCITE) {
            RegressionSuite.validateTableOfLongs(c, sql, expected);
        }
    }

    private static final String [] tbs = {"R1", "R2", "P1", "P2", "P3"};
    private static final String [] replicated_tbs = {"R1", "R2"};
    private static final long[][] EMPTY_TABLE = new long[][] {};

    private void loadData(boolean extra) throws Exception {
        Client client = this.getClient();
        ClientResponse cr;

        // Empty data from table.
        for (String tb: tbs) {
            cr = client.callProcedure("@AdHoc", "delete from " + tb);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            // Insert records into the table.
            String proc = tb + ".insert";
            //                             id,wage,dept,tm
            cr = client.callProcedure(proc, 1, 10, 1, "2013-06-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(proc, 2, 20, 1, "2013-07-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(proc, 3, 30, 1, "2013-07-18 10:40:01.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(proc, 4, 40, 2, "2013-08-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(proc, 5, 50, 2, "2013-09-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            if (extra) {
                //                             id,wage,dept,tm
                cr = client.callProcedure(proc, 6, 10, 2, "2013-07-18 02:00:00.123457");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                cr = client.callProcedure(proc, 7, 40, 2, "2013-09-18 02:00:00.123457");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            }
        }
    }

    /**
     * Simple sub-query
     * @throws Exception
     */
    @Test
    public void testSimpleFromClause() throws Exception {
        final Client client = getClient();
        loadData(false);
        for (String tb: tbs) {
            // baseline
            validateTableOfLongs(client,
                    "select ID, DEPT from (select ID, DEPT from " + tb + ") T1 order by ID;",
                    new long[][] {{1, 1}, {2, 1}, {3, 1}, {4, 2}, {5, 2}});

            // WHERE clause has same effect inside and outside subquery.
            validateTableOfLongs(client,
                    "select ID, DEPT from (select ID, DEPT from " + tb + ") T1 where T1.ID > 4;",
                    new long[][] {{5, 2}});

            validateTableOfLongs(client,
                    "select ID, DEPT from (select ID, DEPT from " + tb + " where ID > 4) T1;",
                    new long[][] {{5, 2}});

            validateTableOfLongs(client,
                    "select ID, DEPT from (select ID, DEPT from " + tb + ") T1 where ID < 3 order by ID;",
                    new long[][] {{1, 1}, {2, 1}});

            validateTableOfLongs(client,
                    "select ID, DEPT from (select ID, DEPT from " + tb + " where ID < 3) T1 order by ID DESC;",
                    new long[][] {{2, 1}, {1, 1}});

            // Nested
            validateTableOfLongs(client,
                    "select A2 from (select A1 AS A2 from (select ID AS A1 from " + tb +
                            ") T1 where T1.A1 - 2 > 0) T2 where T2.A2 < 6 order by A2",
                    new long[][] {{3}, {4}, {5}});

            validateTableOfLongs(client,
                    "select A2 + 10 from (select A1 AS A2 from (select ID AS A1 from " + tb +
                            " where ID > 3) T1) T2 where T2.A2 < 6 order by A2",
                    new long[][] {{14}, {15}});
        }
    }

    /**
     * SELECT FROM SELECT FROM GROUP BY
     * @throws Exception
     */
    @Test
    public void testFromClauseAggregation() throws Exception {
        Client client = getClient();
        loadData(true);

        // Test group by queries, order by, limit
        for (String tb: tbs) {
            String sql;
            validateTableOfLongs(client,
                    "select * from (select dept, sum(wage) as sw, sum(wage)/count(wage) as avg_wage from " + tb +
                            "  group by dept) T1 order by dept DESC;",
                    new long[][] {{2, 140, 35}, {1, 60, 20}});

            validateTableOfScalarLongs(client,
                    "select sw from (select dept, sum(wage) as sw, sum(wage)/count(wage) as avg_wage from " + tb +
                            "  group by dept) T1 order by dept DESC;",
                    new long[] {140, 60});

            validateTableOfScalarLongs(client,
                    "select avg_wage from (select dept, sum(wage) as sw, sum(wage)/count(wage) as avg_wage from " + tb +
                            "      group by dept) T1 order by dept DESC;",
                    new long[] {35, 20});

            // derived aggregated table + aggregation on subselect
            validateTableOfLongs(client,
                    "select a4, sum(wage) from (select wage, sum(id)+1 as a1, sum(id+1) as a2, " +
                            "sum(dept+3)/count(distinct dept) as a4 from " + tb +
                            " group by wage order by wage ASC limit 4) T1 " +
                            "group by a4 order by a4;",
                    new long[][] {{4, 60}, {10, 40}});

            // group by agg function from group by
            validateTableOfLongs(client,
                    "select dept_count, count(*) from (select dept, count(*) as dept_count from R1 group by dept) T1 " +
                            "group by dept_count order by dept_count;",
                    new long[][] {{3, 1}, {4, 1}});

            // groupby from groupby + limit

            // The limit drops the final raw row to turn the group of 4 to another group of 3.
            validateTableOfLongs(client,
                    "select dept_count, count(*) from (select dept, count(*) as dept_count from (" +
                            "select dept, id from " + tb + " order by dept, id limit 6) T1 group by dept) T2 " +
                            "group by dept_count order by dept_count;",
                    new long[][] {{3, 2}});

            // The limit and offset drop the first and last groups,
            // leaving 2 groups of 1 and 1 group of 2.
            validateTableOfLongs(client,
                    "select wage_count, count(*) from (select wage, count(*) as wage_count from (select wage, id from " + tb +
                            ") T1 group by wage order by wage limit 3 offset 1) T2 group by wage_count order by wage_count;",
                    new long[][] {{1, 2}, {2, 1}});
        }
    }

    /**
     * Join two sub queries
     * @throws Exception
     */
    @Test
    public void testJoinsOfSubselects() throws Exception {
        Client client = getClient();
        loadData(false);
        String sql;

        for (String tb: tbs) {
            // Join parallel subqueries.
            validateTableOfLongs(client,
                    "select newid, id from (select id, wage from R1) T1, (select id as newid, dept from " + tb +
                            "      where dept > 1) T2 where T1.id = T2.dept order by newid;",
                    new long[][] {{4, 2}, {5, 2}});

            // Join replicated table with subquery.
            validateTableOfLongs(client,
                    "select id, wage, dept_count from R1, (select dept, count(*) as dept_count from (" +
                            "select dept, id from " + tb + " order by dept limit 5) T1 group by dept) T2 " +
                            "where R1.wage / T2.dept_count > 10 order by wage, dept_count;",
                    new long[][] {{3, 30, 2}, {4, 40, 2}, {4, 40, 3},{5, 50, 2},{5, 50, 3}});

            // Join parallel subqueries, fancier case.
            validateTableOfLongs(client,
                    "select id, newid  from (select id, wage from R1) T1 LEFT OUTER JOIN (select id as newid, dept from " + tb +
                            "      where dept > 1) T2 ON T1.id = T2.dept order by id, newid;",
                    new long[][] {{1, Long.MIN_VALUE}, {2, 4}, {2, 5}, {3, Long.MIN_VALUE},
                            {4, Long.MIN_VALUE}, {5, Long.MIN_VALUE}});

            // Join table with subquery on replicated data.
            validateTableOfLongs(client,
                    "select T2.id from (select id, wage from R1) T1, " + tb + " T2 order by T2.id;",
                    new long[][] {
                            {1}, {1}, {1}, {1}, {1},
                            {2}, {2}, {2}, {2}, {2},
                            {3}, {3}, {3}, {3}, {3},
                            {4}, {4}, {4}, {4}, {4},
                            {5}, {5}, {5}, {5}, {5}});
        }
    }

    @Test
    public void testFromReplicated() throws Exception {
        Client client = getClient();
        loadData(false);
        validateTableOfLongs(client,
                "select P1.ID, P1.WAGE from (select ID, DEPT from R1) T1, P1 where T1.ID = P1.ID and T1.ID < 4 order by P1.ID;",
                new long[][] {{1,10}, {2, 20}, {3, 30}});

        validateTableOfLongs(client,
                "select P1.ID, P1.WAGE from (select ID, DEPT from R1) T1, P1 where T1.ID = P1.ID and T1.ID = 3 order by P1.ID;",
                new long[][] {{3, 30}});

        validateTableOfLongs(client,
                "select P1.ID, P1.WAGE from (select ID, DEPT from R1) T1, P1 where T1.ID = P1.ID and P1.ID = 3 order by P1.ID;",
                new long[][] {{3, 30}});

        validateTableOfLongs(client,
                "select T1.ID, P1.WAGE from (select ID, DEPT from R1) T1, P1 where T1.ID = P1.WAGE / 10 order by P1.ID;",
                new long[][] {{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}});
    }

    // This got a wrong answer when partitioned GROUP in subquery is joined with replicated parent table
    @Test
    public void testENG6276() throws Exception {
        Client client = getClient();
        String sql;

        String[] sqlArray = {
                "INSERT INTO P4 VALUES (0, 'EPOJbVcUPlDghTEMs', NULL, 2.90574307197424275273e-01);",
                "INSERT INTO P4 VALUES (1, 'EPOJbVcUPlDghTEMs', NULL, 6.95147507397556374542e-01);",
                "INSERT INTO P4 VALUES (2, 'EPOJbVcUPlDghTEMs', -27645, 9.49225716086843585018e-01);",
                "INSERT INTO P4 VALUES (3, 'EPOJbVcUPlDghTEMs', -27645, 3.41233435850314625881e-01);",
                "INSERT INTO P4 VALUES (4, 'baYqQXVHBZHVlDRlu', 8130, 7.10103786492815025611e-01);",
                "INSERT INTO P4 VALUES (5, 'baYqQXVHBZHVlDRlu', 8130, 7.24543183451542227580e-01);",
                "INSERT INTO P4 VALUES (6, 'baYqQXVHBZHVlDRlu', 23815, 4.49837414257097889525e-01);",
                "INSERT INTO P4 VALUES (7, 'baYqQXVHBZHVlDRlu', 23815, 4.91748197919483431839e-01);",

                "INSERT INTO R4 VALUES (0, 'EPOJbVcUPlDghTEMs', NULL, 2.90574307197424275273e-01);",
                "INSERT INTO R4 VALUES (1, 'EPOJbVcUPlDghTEMs', NULL, 6.95147507397556374542e-01);",
                "INSERT INTO R4 VALUES (2, 'EPOJbVcUPlDghTEMs', -27645, 9.49225716086843585018e-01);",
                "INSERT INTO R4 VALUES (3, 'EPOJbVcUPlDghTEMs', -27645, 3.41233435850314625881e-01);",
                "INSERT INTO R4 VALUES (4, 'baYqQXVHBZHVlDRlu', 8130, 7.10103786492815025611e-01);",
                "INSERT INTO R4 VALUES (5, 'baYqQXVHBZHVlDRlu', 8130, 7.24543183451542227580e-01);",
                "INSERT INTO R4 VALUES (6, 'baYqQXVHBZHVlDRlu', 23815, 4.49837414257097889525e-01);",
                "INSERT INTO R4 VALUES (7, 'baYqQXVHBZHVlDRlu', 23815, 4.91748197919483431839e-01);"
        };
        sql = StringUtils.join(sqlArray);
        ClientResponse cr = client.callProcedure("@AdHoc", sql);
        assertEquals("Failed data initialization.", ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt[] = cr.getResults();
        assertEquals("Failed data initialization.", sqlArray.length, vt.length);
        long[] row = new long[] {-8, 63890};
        validateTableOfLongs(client,
                "select -8, A.NUM from R4 B, (select max(RATIO) RATIO, sum(NUM) NUM, DESC from P4 group by DESC) A where (A.NUM + 5) > 44",
                new long[][] {row, row, row, row, row, row, row, row});
    }

    /**
     * Simple sub-query expression
     * @throws Exception
     */
    @Test
    public void testInExistsSimple() throws Exception {
        Client client = getClient();
        loadData(false);

        for (String tb: replicated_tbs) {
            validateTableOfLongs(client,
                    "select ID, DEPT from " + tb +  " where ID IN (select ID from " + tb + " where ID > 3) order by ID;",
                    new long[][] {{4, 2}, {5, 2}});

            // correlate by parent expression
            validateTableOfLongs(client,
                    "select ID, DEPT from " + tb + " where abs(ID) IN (select ID from " + tb +
                            " where DEPT = 2 order by 1 limit 1 offset 1);",
                    new long[][] {{5, 2}});

            // limit offset in subquery
            validateTableOfLongs(client,
                    "select ID, DEPT from " + tb +  " where ID IN (select ID from " + tb +
                            " where ID > 2 order by ID limit 3 offset 1) order by ID;",
                    new long[][] {{4, 2}, {5, 2}});

            // AND of in/exists
            validateTableOfLongs(client,
                    "select ID, DEPT from " + tb + " T1 where abs(ID) IN (select ID from " + tb +
                            " where ID > 4) and exists (select 1 from " + tb + " where ID * T1.DEPT = 10) order by ID;",
                    new long[][] {{5, 2}});

            // not exists
            validateTableOfLongs(client,
                    "select ID, DEPT from " + tb + " T1 where not exists (select 1 from " + tb +
                            " where ID * T1.DEPT = 10) and T1.ID < 3 order by ID;",
                    new long[][] {{1, 1}, {2, 1}});

            // Subquery with user parameter
            validateTableOfLongs(
                    client.callProcedure("@AdHoc",
                            "select ID from " + tb + " T1 where exists (select 1 from R2 T2 where T1.ID * T2.ID = ?);",
                            9).getResults()[0],
                    new long[][] {{3}});

            // Subquery with parent column correlation
            validateTableOfLongs(client,
                    "select ID from " + tb + " T1 where exists (select 1 from R2 T2 where T1.ID * T2.ID = 9);",
                    new long[][] {{3}});

            // Subquery with a grand-parent column correlation
            validateTableOfLongs(client,
                    "select ID from " + tb + " T1 where exists (select 1 from R1 T2 where exists (" +
                            "select ID from R2 T3 where T1.ID > T3.ID and T1.ID * T3.ID = 12));",
                    new long[][] {{4}});
        }
    }

    @Test
    public void testLhsScalarInSubquery() throws Exception {
        Client client = getClient();
        loadData(false);
        String sql;
        // Non-correlated IN with a non-correlated select on the left side.
        validateTableOfLongs(client,
                "select ID from R1 T1 where (select ID from R2 T2 where ID = 5) IN (select ID from R2 T3 where T3.ID = 5) order by ID;",
                new long[][] {{1}, {2}, {3}, {4}, {5}});

        // Correlated IN with a non-correlated select on the left side.
        validateTableOfLongs(client,
                "select ID from R1 T1 where (select ID from R2 T2 where ID = 5) IN (select ID from R2 T3 where T3.ID > T1.ID) order by ID;",
                new long[][] {{1}, {2}, {3}, {4}});

        // Correlated IN with a correlated select on the left side.
        validateTableOfLongs(client,
                "select ID from R1 T1 where (select ID from R2 T2 where T2.ID = T1.ID) IN (" +
                        "select ID from R2 T3 where T3.ID <> 5 and T3.ID >= T1.ID) order by ID;",
                new long[][] {{1}, {2}, {3}, {4}});

        // Cardinality error
        try {
            client.callProcedure("@AdHoc",
                    "select ID from R1 T1 where (select ID from R2 T2 where T2.ID <= T1.ID) IN (" +
                            "select ID from R2 T2 where T2.ID <= T1.ID);");
            fail("Did not get the expected scalar subquery cardinality error");
        } catch (ProcCallException ex) {
            String errMsg = isHSQL() ? "cardinality violation" :
                "More than one row returned by a scalar/row subquery";
            assertTrue(ex.getMessage().contains(errMsg));
        }
    }

    /**
     * Join two sub queries
     * @throws Exception
     */
    @Test
    public void testJoinsOfInExists() throws Exception {
        final Client client = getClient();
        loadData(false);

        for (String tb: replicated_tbs) {
            validateTableOfLongs(client,
                    "select T1.id from R1 T1, " + tb + " T2 where T1.id = T2.id and exists (" +
                            "select 1 from R1 where R1.dept * 2 = T2.dept) order by id;",
                    new long[][] {{4}, {5}});

            validateTableOfLongs(client,
                    "select t1.id, t2.id from r1 t1, " + tb + " t2 where t1.id IN (" +
                            "select id from r2 where t2.id = r2.id * 2) order by t1.id;",
                    new long[][] {{1,2}, {2,4}});

            // Advanced mix of FROM clause subselects with exists clause subselects
            // Core dump
            if (!isHSQL()) {
                validateTableOfLongs(client,
                        "select id, newid from (select id, wage from R1) T1 LEFT OUTER JOIN (" +
                                "select id as newid, dept from " + tb + " where dept > 1) T2 ON T1.id = T2.dept " +
                                "     and exists (select 1 from R1 where R1.ID = T2.newid) order by id, newid;",
                        new long[][] {{1, Long.MIN_VALUE}, {2, 4}, {2, 5},
                                {3, Long.MIN_VALUE}, {4, Long.MIN_VALUE}, {5, Long.MIN_VALUE}});
            }
        }
    }


    /**
     * SELECT WHERE IN/EXISTS SELECT GROUP BY
     * @throws Exception
     */
    @Test
    public void testInExistsGroupBy() throws Exception {
        Client client = getClient();
        loadData(true);
        for (String tb: replicated_tbs) {
            // row value IN grouped result set
            validateTableOfLongs(client,
                    "select dept, sum(wage) as sw1 from " + tb + " where (id, dept + 2) IN (" +
                            "select dept, count(*) from " + tb + " group by dept) group by dept;",
                    new long[][] {{1,10}});

            // trivial variant with inconsequential ORDER BY
            // ORDER BY here is meaningless,
            // but it used to cause serious problems, so keep the test.
            validateTableOfLongs(client,
                    "select dept, sum(wage) as sw1 from " + tb + " where (id, dept + 2) IN (" +
                            "select dept, count(*) from " + tb + " group by dept order by dept DESC) group by dept;",
                    new long[][] {{1,10}});

            // Exists AGG with GROUP BY but with no having.
            // It's not clear what (if any) optimizations this
            // obscure query might be testing.
            // Is it some kind of base case that shows that we do not
            // over-simplify when optimizing?
            // EXISTS (SELECT MAX(x) GROUP BY FROM ...)
            // Is this or is this not just a perverse way to express
            // EXISTS (SELECT 1 FROM ...)
            // even if all x values are null?
            validateTableOfLongs(client,
                    "select id from " + tb + " TBA where exists (" +
                            "select max(dept) from R1 where TBA.id = R1.id group by dept) order by id;",
                    new long[][] {{1}, {2}, {3}, {4}, {5}, {6}, {7}});

            // subquery with having
            validateTableOfLongs(client,
                    "select id from " + tb + " where wage IN (select max(wage) from R1 group by dept having max(wage) > 30);",
                    new long[][] {{5}});

            // subquery with having that uses a user parameter
            validateTableOfLongs(
                    client.callProcedure("@AdHoc",
                            "select id from " + tb +
                                    " TBA where exists (select dept from R1 group by dept having max(wage) = ?);",
                            3).getResults()[0], EMPTY_TABLE);

            // subquery with having that uses a correlation column
            validateTableOfLongs(client,
                    "select id from " + tb +" TBA where exists (" +
                            "select dept from R1 group by dept having max(wage) = TBA.wage or min(wage) = TBA.wage) order by id;"
                    , new long[][] {{1}, {3}, {5}, {6}});

            // subquery with having that uses a grandparent correlation column
            validateTableOfLongs(client,
                    "select id from " + tb + " TBA where exists (select 1 from R2 where exists (" +
                            "select dept from R1 group by dept having max(wage) = TBA.wage)) order by id;",
                    new long[][] {{3}, {5}});
        }
    }


    /**
     * SELECT ... HAVING ... SELECT
     * @throws Exception
     */
    @Test
    public void testHavingSubselect() throws Exception {
        Client client = getClient();
        loadData(true);

        for (String tb: replicated_tbs) {

            /*/ Uncomment these tests when ENG-8306 "HAVING with subquery" is fixed
            validateTableOfLongs(client, sql, new long[][] {{2}, {1}});
            /*/
            verifyStmtFails(client,
                    "select dept from " + tb + " group by dept having max(wage) IN (select wage from R1) order by dept desc",
                    TestPlansInExistsSubQueries.HavingErrorMsg); // for now
            //*/

            final String sql =
                    "select dept from " + tb + " group by dept having max(wage) + 1 - 1 IN (select wage from R1) order by dept desc";
            /*/ Uncomment these tests when ENG-8306 "HAVING with subquery" is fixed
            validateTableOfLongs(client, sql, new long[][] {{2}, {1}});
            /*/
            verifyStmtFails(client, sql, TestPlansInExistsSubQueries.HavingErrorMsg); // for now
            //*/
        }
    }


    /**
     * SELECT ... WHERE ... SELECT UNION SELECT
     * @throws Exception
     */
    @Test
    public void testUnions() throws Exception {
        final Client client = getClient();
        loadData(false);

        for (String tb: replicated_tbs) {
            validateTableOfLongs(client,
                    "select ID from " + tb + " where ID IN ((select ID from R1 where ID > 2 limit 3 offset 1) UNION " +
                            "select ID from R2 where ID <= 2 INTERSECT select ID from R1 where ID = 1) order by ID;"
                    , new long[][] {{1}, {4}, {5}});

            //* enable for debug */ dumpQueryPlans(client, sql);
            validateTableOfLongs(client,
                    "select ID from " + tb + " where ID IN (select ID from R1 where ID >= 2 EXCEPT " +
                            "       select ID from R2 where ID <= 2) order by ID;",
                    new long[][] {{3}, {4}, {5}});

            // Now let's try a correlated subquery.
            validateTableOfLongs(client,
                    "select ID from " + tb + " as outer_tbl where ID = ALL (select id from r1 where id = outer_tbl.id UNION " +
                            "       select id from r2 where id = outer_tbl.id  + 2) order by ID;",
                    new long[][] {{4}, {5}});
        }
    }

    @Test
    public void testRowInOrOpAnyNonNull() throws Exception {
        final Client client = getClient();
        loadData(false);

        // PURPOSELY repeat each query using
        // ORDER, LIMIT, and OFFSET
        // instead of a filter to skip the first and last row
        // to prevent IN-to-EXISTS transformations.
        //* enable for debug */ dumpQueryPlans(client, sql);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) IN (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{2},{3},{4}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) IN (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{2},{3},{4}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) IN (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{2},{3},{4}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) IN (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{2},{3},{4}});

        //* enable for debug */ dumpQueryPlans(client, sql);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ANY (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{2},{3},{4}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) = ANY (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{2},{3},{4}});
        //* enable for debug */ dumpQueryPlans(client, sql);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{2},{3},{4}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) = ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{2},{3},{4}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) >= ANY (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{2},{3},{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ANY (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{2},{3},{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) >= ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{2},{3},{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{2},{3},{4},{5}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ANY (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{2},{3},{4}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ANY (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{2},{3},{4}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{2},{3},{4}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{2},{3},{4}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) > ANY (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{3},{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ANY (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{3},{4},{5}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) > ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{3},{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{3},{4},{5}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ANY (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{2},{3}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ANY (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{2},{3}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{2},{3}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{2},{3}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <> ANY (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{2},{3},{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <> ANY (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{2},{3},{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <> ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{2},{3},{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <> ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{2},{3},{4},{5}});

        for (String tb: replicated_tbs) {
            validateTableOfLongs(client,
                    "select ID, DEPT from " + tb +
                            " T1 where (abs(ID) + 1 - 1, DEPT) IN (select DEPT, WAGE/10 from " + tb + ");",
                    new long[][] {{1, 1}});
        }
    }

    /**
     * SELECT FROM WHERE OUTER OP INNER inner.
     * If there is a match, IN evaluates to TRUE
     * If there is no match, IN evaluates to FALSE if the INNER result set is empty
     * If there is no match, IN evaluates to NULL if the INNER result set is not empty
     *       and there are inner NULLs
     * Need to keep OFFSET for the IN expressions
     * to prevent IN-to-EXISTS optimization
     *
     * @throws Exception
     */
    @Test
    public void testRowEqualityIsNull() throws Exception {
        Client client = getClient();
        //                                 id, wage, dept, tm
        client.callProcedure("R1.insert", 100, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 101, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 102, 1001, 2, "2013-07-18 02:00:00.123457");

        client.callProcedure("R2.insert", 100, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102, 1001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103, 1003, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105, 1000, 2, "2013-07-18 02:00:00.123457");

        // When inner result has a NULL. The equality expression is NULL
        // HSQL-BACKEND gets mysterious
        // java.lang.ClassCastException: java.lang.Integer cannot be cast to [Ljava.lang.Object;
        if (!isHSQL()) {
            validateTableOfLongs(client,
                    "select ID from R1 where ((WAGE, DEPT) = (select WAGE, DEPT from R2 where ID = R1.ID)) IS NULL order by ID;",
                    new long[][] {{100}, {101}});
        }

        // Inner result is empty. The equality expression is always NULL
        validateTableOfLongs(client,
                "select ID from R1 where ((WAGE, DEPT) = (select WAGE, DEPT from R2 where ID = 107)) IS NULL order by ID;",
                new long[][] {{100}, {101}, {102}});

        // When outer result has a NULL, the expression is NULL
        // HSQL-BACKEND gets mysterious
        // java.lang.ClassCastException: java.lang.Integer cannot be cast to [Ljava.lang.Object;
        if (!isHSQL()) {
            validateTableOfLongs(client,
                    "select ID from R1 where ((WAGE, DEPT) = (select WAGE, DEPT from R2 where ID = 102)) IS NULL;",
                    new long[][] {{101}});
        }

        // Outer result is empty. The expression is NULL
        validateTableOfLongs(client,
                "select ID from R1 where ((select WAGE, DEPT from R2 where ID = 107) = (" +
                        "select WAGE, DEPT from R2 where ID = R1.ID)) IS NULL order by ID;",
                new long[][] {{100}, {101}, {102}});

        // Both outer and inner are empty. The expression is NULL
        validateTableOfLongs(client,
                "select ID from R1 where ((select WAGE, DEPT from R2 where ID = 107) = (" +
                        "select WAGE, DEPT from R2 where ID = 107)) IS NULL order by ID;",
                new long[][] {{100}, {101}, {102}});
    }

    /**
     * SELECT FROM WHERE OUTER IN(=ANY) (SELECT INNER ...) returning inner NULL.
     * If there is a match, IN evaluates to TRUE
     * If there is no match, IN evaluates to FALSE if the INNER result set is empty
     * If there is no match, IN evaluates to NULL if the INNER result set is not empty
     *       and there are "near misses" involving NULLs
     * @throws Exception
     */
    @Test
    public void testRowInOrOpAnyWithInnerNull() throws Exception {
        Client client = getClient();
        //                                 id, wage, dept, tm
        client.callProcedure("R1.insert",  10,  100, 1, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 100, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 300, 3000, 3, "2013-07-18 02:00:00.123457");

        client.callProcedure("R2.insert", 100, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102, 1001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103, 1003, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105, 1000, 2, "2013-07-18 02:00:00.123457");

        // Repeat each query with the null in a different position.

        // There is an exact match, IN/ANY expression evaluates to TRUE
        validateTableOfLongs(client, "select ID from R1 where (WAGE, DEPT) IN (select WAGE, DEPT from R2);",
                new long[][] {{100}});
        validateTableOfLongs(client, "select ID from R1 where (DEPT, WAGE) IN (select DEPT, WAGE from R2);",
                new long[][] {{100}});

        validateTableOfLongs(client, "select ID from R1 where (WAGE, DEPT) = ANY (select WAGE, DEPT from R2);",
                new long[][] {{100}});
        validateTableOfLongs(client, "select ID from R1 where (DEPT, WAGE) = ANY (select DEPT, WAGE from R2);",
                new long[][] {{100}});

        // Run <> ANY for a case with one exact match
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE+3, DEPT) <> ANY (select WAGE, DEPT from R2 where ID = 103) order by ID;",
                new long[][] {{10},{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE+3) <> ANY (select DEPT, WAGE from R2 where ID = 103) order by ID;",
                new long[][] {{10},{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE+3, DEPT) <> ANY (select WAGE, DEPT from R2 where ID = 103 order by ID limit 3) order by ID;",
                new long[][] {{10},{300}});

        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE+3) <> ANY (select DEPT, WAGE from R2 where ID = 103 order by ID limit 3) order by ID;",
                new long[][] {{10},{300}});

        // When there is no match, the "IN" or "OP ANY" expression evaluates
        // to NULL when the non-empty inner result set has a null in a critical
        // column.
        // Repeat each query with a different placement of the null value and
        // with a re-expression of the subquery filter
        // using ORDER, LIMIT, and OFFSET that skips one of the two nulls
        // but is otherwise identical to prevent IN-to-EXISTS transformations.

        //* enable for debug */ dumpQueryPlans(client, sql);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) IN (select WAGE, DEPT from R2 where ID < 104);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) IN (select DEPT, WAGE from R2 where ID < 104);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) IN (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) IN (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);

        //* enable for debug */ dumpQueryPlans(client, sql);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ANY (select WAGE, DEPT from R2 where ID < 104);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) = ANY (select DEPT, WAGE from R2 where ID < 104);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) = ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) >= ANY (select WAGE, DEPT from R2 where ID < 104);",
                new long[][] {{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ANY (select DEPT, WAGE from R2 where ID < 104);",
                new long[][] {{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) >= ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                new long[][] {{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                new long[][] {{300}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ANY (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                new long[][] {{10},{100}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ANY (select DEPT, WAGE from R2 where ID < 104) order by ID;",
                new long[][] {{10},{100}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by ID;",
                new long[][] {{10},{100}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by ID;",
                new long[][] {{10},{100}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) > ANY (select WAGE, DEPT from R2 where ID < 104);",
                new long[][] {{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ANY (select DEPT, WAGE from R2 where ID < 104);",
                new long[][] {{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) > ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                new long[][] {{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                new long[][] {{300}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ANY (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                new long[][] {{10},{100}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ANY (select DEPT, WAGE from R2 where ID < 104) order by ID;",
                new long[][] {{10},{100}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by ID;",
                new long[][] {{10},{100}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by ID;",
                new long[][] {{10},{100}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <> ANY (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                new long[][] {{10},{100},{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <> ANY (select DEPT, WAGE from R2 where ID < 104) order by ID;",
                new long[][] {{10},{100},{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <> ANY (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by ID;",
                new long[][] {{10},{100},{300}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <> ANY (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by ID;",
                new long[][] {{10},{100},{300}});
    }


    /**
     * SELECT FROM WHERE OUTER IN(=ANY) (SELECT INNER ...) returning inner NULL.
     * If there is a match, IN evalueates to TRUE
     * If there is no match, IN evaluates to FASLE if the INNER result set is empty
     * If there is no match, IN evaluates to NULL if the INNER result set is not empty
     *       and there are inner NULLs
     * Need to keep OFFSET for the IN expressions
     * to prevent IN-to-EXISTS optimization
     *
     * @throws Exception
     */
    @Test
    public void testRowNotOrIsNullInOrOpAnyWithInnerNull() throws Exception {
        final Client client = getClient();
        //                                 id, wage, dept, tm
        client.callProcedure("R1.insert",  10,  100, 1, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 100, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 300, 3000, 3, "2013-07-18 02:00:00.123457");

        client.callProcedure("R2.insert", 100, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102, 1001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103, 1003, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105, 1000, 2, "2013-07-18 02:00:00.123457");

        // The inner result set is empty, IN/ANY expression evaluates to FALSE
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) IN (select WAGE, DEPT from R2 where ID = 0) IS FALSE order by ID;",
                new long[][] {{10}, {100}, {300}});
        // That's specifically FALSE vs. NULL
        validateTableOfLongs(client,
                "select ID from R1 where ((WAGE, DEPT) IN (select WAGE, DEPT from R2 where ID = 0)) IS NULL;", EMPTY_TABLE);

        // When there is no match, the "IN" or "OP ANY" expression evaluates
        // to NULL when the non-empty inner result set has a null in a critical
        // column.
        if ( ! isHSQL()) { // wrong (0 rows) even in HSQL.
            validateTableOfLongs(client,
                    "select ID from R1 where ((WAGE, DEPT) IN (select WAGE, DEPT from R2 where ID < 104)) IS NULL;",
                    new long[][] {{100}});

            validateTableOfLongs(client,
                    "select ID from R1 where ((WAGE, DEPT) = ANY (select WAGE, DEPT from R2 where ID < 104)) IS NULL;",
                    new long[][] {{100}});

            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) NOT IN (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                    new long[][] {{10}, {300}});

            validateTableOfLongs(client,
                    "select ID from R1 where NOT (WAGE, DEPT) = ANY (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                    new long[][] {{10}, {300}});

            // Try single-column-based expressions as row columns.
            validateTableOfLongs(client,
                    "select ID from R1 where (abs(ID), 2*DEPT-DEPT) IN (select ID, DEPT from R2 where ID < 104) order by ID;",
                    new long[][] {{100}});

            // Try a hard-coded constant as a row column.
            // This currently works only in the cases like this where the
            // IN rewrites as an EXISTS.
            validateTableOfLongs(client,
                    "select ID from R1 where (ID, 2) IN (select ID, DEPT from R2 where ID < 104) order by ID;",
                    new long[][] {{100}});

            // Try a multi-column expression as a row column.
            // This currently works only in the cases like this where the
            // IN rewrites as an EXISTS.
            validateTableOfLongs(client,
                    "select ID from R1 where (ID, ID+DEPT-ID) IN (select ID, DEPT from R2 where ID < 104) order by ID;",
                    new long[][] {{100}});

        }

        // IN should evaluate to NULL
        // when there is a null-nonmatch but no match.
        if (! isHSQL()) { // wrong even in HSQL.
            validateTableOfLongs(client,
                    "select ID from R1 where NOT ((WAGE, DEPT) IN (select WAGE, DEPT from R2 where ID < 104)) order by ID;",
                    new long[][] {{10}, {300}});

            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) IN (select WAGE, DEPT from R2 where ID < 104) IS FALSE order by ID;",
                    new long[][] {{10}, {300}});

            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) NOT IN (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                    new long[][] {{10}, {300}});

            // There is no match, inner result set is non empty, IN evaluates to NULL, NOT IN is also NULL
            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) NOT IN (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                    new long[][] {{10}, {300}});

            // Try single-column-based expressions as row columns in a
            // NOT IN query that will not get rewritten as an EXISTS query.
            validateTableOfLongs(client,
                    "select ID from R1 where (abs(WAGE), 2+DEPT-2) NOT IN (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                    new long[][] {{10}, {300}});

            // Try non-working cases of a constant-valued row column.
            // NOT IN does not rewrite as EXISTS, so the constant row column is rejected.
            try {
                validateTableOfLongs(client,
                        "select ID from R1 where (WAGE, 2) NOT IN (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                        new long[][] {{10}, {300}});
                if (! USING_CALCITE) {
                    fail("Was not expecting constant row column to survive planning");
                }
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("use of a constant value"));
            }
            // A subquery with a limit does not rewrite as EXISTS,
            // so the constant row column is rejected.
            try {
                validateTableOfLongs(client,
                        "select ID from R1 where (WAGE, 2) IN (select WAGE, DEPT from R2 where ID < 104 limit 1) order by ID;",
                        new long[][] {{10}, {300}});
                if (! USING_CALCITE) {
                    fail("Was not expecting constant row column to survive planning");
                }
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("use of a constant value"));
            }

            // Try non-working cases of a multi-column-expression in a row column.
            // NOT IN does not rewrite as EXISTS, so the multi-column-based
            // row column expression is rejected.
            try {
                validateTableOfLongs(client,
                        "select ID from R1 where (WAGE, ID+DEPT-ID) NOT IN (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                        new long[][] {{10}, {300}});
                if (! USING_CALCITE) {
                    fail("Was not expecting multi-column expression to survive planning");
                }
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("combination of column values"));
            }
            // A subquery with a limit does not rewrite as EXISTS,
            // so the multi-column-based row column expression
            // is rejected.
            try {
                validateTableOfLongs(client,
                        "select ID from R1 where (WAGE, ID+DEPT-ID) IN (select WAGE, DEPT from R2 where ID < 104 limit 1) order by ID;",
                        new long[][] {{10}, {300}});
                if (! USING_CALCITE) {
                    fail("Was not expecting multi-column expression to survive planning");
                }
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("combination of column values"));
            }
        }
    }


    /**
     * SELECT FROM WHERE OUTER op ALL (SELECT INNER ...) returning inner NULL.
     * If there is a match, IN evalueates to TRUE
     * If there is no match, IN evaluates to FASLE if the INNER result set is empty
     * If there is no match, IN evaluates to NULL if the INNER result set is not empty
     *       and there are inner NULLs
     * Need to keep OFFSET for the IN expressions
     * to prevent IN-to-EXISTS optimization
     *
     * @throws Exception
     */
    @Test
    public void testRowOpAllWithInnerNull() throws Exception {
        final Client client = getClient();
        //                                 id, wage, dept, tm
        client.callProcedure("R1.insert",  10,  100, 1, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 100, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 300, 3000, 3, "2013-07-18 02:00:00.123457");

        client.callProcedure("R2.insert", 100, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102, 1001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103, 1003, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105, 1000, 2, "2013-07-18 02:00:00.123457");

        // The inner result set is empty, ALL expression evaluates to FALSE
        // specifically vs. NULL
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID = 0) IS FALSE;",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID = 0 order by ID limit 6 offset 1) IS FALSE;",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where ((WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID = 0)) IS NULL;",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where ((WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID = 0 order by ID limit 6 offset 1)) IS NULL;",
                EMPTY_TABLE);

        // There is no match, the "IN" or "OP ALL" expression evaluates to NULL
        // (non-empty inner result set has a null in one of its columns).
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL order by ID limit 6 offset 1);",
                EMPTY_TABLE);
        if (!isHSQL()) { // HSQL erroneously matches 0 rows by returning FALSE vs. NULL
            //* enable to debug */ dumpQueryPlans(client, sql);
            validateTableOfLongs(client,
                    "select ID from R1 where ((WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL)) IS NULL;",
                    new long[][] {{100}});
            validateTableOfLongs(client,
                    "select ID from R1 where ((WAGE, DEPT) = ALL (select WAGE, DEPT from R2 " +
                            "where ID = 0 or WAGE is NULL order by ID limit 6 offset 1)) IS NULL;",
                    new long[][] {{100}});
        }

        // Focus a set of queries on the data filtered down to a NULL row.
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) >= ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) >= ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL order by ID limit 4 offset 1);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL order by ID limit 4 offset 1);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL order by ID limit 4 offset 1);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL order by ID limit 4 offset 1);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) > ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) > ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL order by ID limit 4 offset 1);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL order by ID limit 4 offset 1);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL order by ID limit 4 offset 1);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ALL (select WAGE, DEPT from R2 where ID = 0 or WAGE is NULL order by ID limit 4 offset 1);",
                EMPTY_TABLE);

        // "<> ALL" and "NOT IN"
        // should only evaluate to TRUE when there is no definite match
        // AND no null match OR a definite non-match.
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <> ALL (select WAGE, DEPT from R2 order by WAGE, DEPT limit 4 offset 1) order by ID;",
                new long[][] {{10},{300}});

        // Just run the same patterns here as the IN/ANY test...
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID < 104);", EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 order by ID limit 3 offset 1);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID < 104);", EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) = ALL (select DEPT, WAGE from R2 where ID < 104);", EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) = ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);

        if (!isHSQL()) { // HSQL erroneously matches an extra row?
            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) >= ALL (select WAGE, DEPT from R2 where ID < 104);",
                    EMPTY_TABLE);
        }
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ALL (select DEPT, WAGE from R2 where ID < 104);",
                new long[][] {{300}});
        if (!isHSQL()) { // HSQL erroneously matches an extra row?
            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) >= ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                    EMPTY_TABLE);
        }
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                new long[][] {{300}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ALL (select WAGE, DEPT from R2 where ID < 104);", EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ALL (select DEPT, WAGE from R2 where ID < 104);",
                new long[][] {{10}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                new long[][] {{10}});

        if (!isHSQL()) { // HSQL erroneously matches an extra row?
            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) > ALL (select WAGE, DEPT from R2 where ID < 104);",
                    EMPTY_TABLE);
        }
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ALL (select DEPT, WAGE from R2 where ID < 104);",
                new long[][] {{300}});
        if (!isHSQL()) { // HSQL erroneously matches an extra row?
            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) > ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                    EMPTY_TABLE);
        }
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                new long[][] {{300}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ALL (select WAGE, DEPT from R2 where ID < 104);", EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ALL (select DEPT, WAGE from R2 where ID < 104);",
                new long[][] {{10}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                new long[][] {{10}});

        if (!isHSQL()) { // HSQL erroneously matches all rows
            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) <> ALL (select WAGE, DEPT from R2 where ID < 104) order by ID;",
                    new long[][] {{10}, {300}});
            validateTableOfLongs(client,
                    "select ID from R1 where (DEPT, WAGE) <> ALL (select DEPT, WAGE from R2 where ID < 104) order by ID;",
                    new long[][] {{10}, {300}});
            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE, DEPT) <> ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by ID;",
                    new long[][] {{10}, {300}});
            validateTableOfLongs(client,
                    "select ID from R1 where (DEPT, WAGE) <> ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by ID;",
                    new long[][] {{10}, {300}});
        }
    }


    /**
     * SELECT FROM WHERE OUTER  OP ALL (SELECT INNER ...) with no NULLs.
    *
     * @throws Exception
     */
    @Test
    public void testRowOpAllNoNull() throws Exception {
        final Client client = getClient();
        loadData(false);

        // Run the same basic query forms as testRowInOrOpAnyNonNull
        // where we PURPOSELY repeat each query using
        // ORDER, LIMIT, and OFFSET
        // instead of a filter to skip the first and last row
        // to prevent to-EXISTS transformations (are these even possible?).

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 where ID > 1 and ID < 5);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) = ALL (select DEPT, WAGE from R2 where ID > 1 and ID < 5);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) = ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) = ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) >= ALL (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ALL (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) >= ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{4},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) >= ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{4},{5}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ALL (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{2}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ALL (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{2}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <= ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{2}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <= ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{2}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) > ALL (select WAGE, DEPT from R2 where ID > 1 and ID < 5);",
                new long[][] {{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ALL (select DEPT, WAGE from R2 where ID > 1 and ID < 5);",
                new long[][] {{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) > ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                new long[][] {{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) > ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                new long[][] {{5}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ALL (select WAGE, DEPT from R2 where ID > 1 and ID < 5);",
                new long[][] {{1}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ALL (select DEPT, WAGE from R2 where ID > 1 and ID < 5);",
                new long[][] {{1}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) < ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3);",
                new long[][] {{1}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) < ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3);",
                new long[][] {{1}});

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <> ALL (select WAGE, DEPT from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <> ALL (select DEPT, WAGE from R2 where ID > 1 and ID < 5) order by 1;",
                new long[][] {{1},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (WAGE, DEPT) <> ALL (select WAGE, DEPT from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{5}});
        validateTableOfLongs(client,
                "select ID from R1 where (DEPT, WAGE) <> ALL (select DEPT, WAGE from R2 order by ID offset 1 limit 3) order by 1;",
                new long[][] {{1},{5}});
    }

    @Test
    public void testInExistsOrOpAnyWithInnerNull() throws Exception {
        final Client client = getClient();
        //                                 id, wage, dept, tm
        client.callProcedure("R1.insert", 100, 1000, 2, "2013-07-18 02:00:00.123457");

        client.callProcedure("R2.insert", 100, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102, 1001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103, 1003, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105, 1000, 2, "2013-07-18 02:00:00.123457");

        // There is a match, other than the NULLs
        validateTableOfLongs(client, "select ID from R1 where WAGE IN (select WAGE from R2);", new long[][] {{100}});

        // There is no match, other than the NULLs
        validateTableOfLongs(client,
                "select ID from R1 where WAGE IN (select WAGE from R2 where WAGE <> 1000 or WAGE is NULL);", EMPTY_TABLE);

        // Subtle bug in HSQL
        // the IN expression evaluates to FALSE rather than NULL, here
        if (!isHSQL()) {
            validateTableOfLongs(client,
                    "select ID from R1 where (WAGE IN (select WAGE from R2 where WAGE <> 1000 or WAGE is NULL)) IS NULL;",
                    new long[][] {{100}});

            validateTableOfLongs(client,
                    "select ID from R1 where WAGE = ANY (select WAGE from R2 where WAGE <> 1000 or WAGE is NULL) IS FALSE;",
                    EMPTY_TABLE);
        }

        // NULL row exists
        validateTableOfLongs(client, "select ID from R1 where EXISTS (select WAGE from R2 where WAGE is NULL);",
                new long[][] {{100}});

        // Rows exist
        validateTableOfLongs(client, "select ID from R1 where NOT EXISTS (select WAGE from R2);", EMPTY_TABLE);

        if (!isHSQL()) {
            validateTableOfLongs(client,
                    "select ID from R1 where WAGE NOT IN (select WAGE from R2 where ID IN (100, 102, 103));",
                    EMPTY_TABLE);

            validateTableOfLongs(client,
                    "select ID from R1 where NOT WAGE IN (select WAGE from R2 where ID IN (100, 102, 103));",
                    EMPTY_TABLE);
        }
    }

    /**
     * SELECT FROM WHERE OUTER IN (SELECT INNER ...). The OUTER is NULL.
     * If there is a match, IN evalueates to TRUE
     * If OUTER is NULL and INNER result set is empty, the IN expression evaluates to FASLE
     * If OUTER is NULL and INNER result set is not empty, the IN expression evaluates to NULL
     * Need to keep OFFSET for the IN expressions
     * to prevent IN-to-EXISTS optimization
     * @throws Exception
     */
    @Test
    public void testOuterNullInOpAny() throws Exception {
        final Client client = getClient();
        //                                 id, wage, dept, tm
        client.callProcedure("R1.insert", 100, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 101, 1001, 2, "2013-07-18 02:00:00.123457");

        client.callProcedure("R2.insert", 200, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 201, 2001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 202, 1001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 203, null, 2, "2013-07-18 02:00:00.123457");

        // R2.200 - the inner result set is not empty, the IN/ANY  expression is NULL
        validateTableOfLongs(client,
                "select ID from R2 where WAGE IN (select WAGE from R1 order by WAGE limit 4 offset 1) is false;",
                new long[][] {{201}});
        validateTableOfLongs(client,
                "select ID from R2 where WAGE = ANY (select WAGE from R1 order by WAGE limit 4 offset 1) is false;",
                new long[][] {{201}});

        // R2.200 - the inner result set is not empty, the IN  expression is NULL
        validateTableOfLongs(client,
                "select ID from R2 where WAGE IN (select WAGE from R1 order by WAGE limit 4 offset 1) is true;",
                new long[][] {{202}});
        validateTableOfLongs(client,
                "select ID from R2 where WAGE = ANY (select WAGE from R1 order by WAGE limit 4 offset 1) is true;",
                new long[][] {{202}});

        // R2.200 - the inner result set is not empty, the IN  expression is NULL
        validateTableOfLongs(client,
                "select ID from R2 where WAGE IN (select WAGE from R1 order by WAGE limit 4 offset 1);",
                new long[][] {{202}});

        // R2.200 - the inner result set is not empty, the IN and not IN  expressions are NULL
        validateTableOfLongs(client,
                "select ID from R2 where WAGE not IN (select WAGE from R1 order by WAGE limit 4 offset 1);",
                new long[][] {{201}});

        // R2.200 - the inner result set is empty, the IN expression is TRUE
        validateTableOfLongs(client,
                "select ID from R2 where WAGE IN (select WAGE from R1 where ID > 1000 order by WAGE limit 4 offset 1) is false order by ID;",
                new long[][] {{200}, {201}, {202}, {203}});
        validateTableOfLongs(client,
                "select ID from R2 where WAGE = ANY (select WAGE from R1 where ID > 1000 order by WAGE limit 4 offset 1) is false order by ID;",
                new long[][] {{200}, {201}, {202}, {203}});

        // R2.202 and R1.101 have the same WAGE
        validateTableOfLongs(client,
                "select ID from R2 where exists (select WAGE from R1 where R1.WAGE = R2.WAGE) order by id;",
                new long[][] {{202}});

        // R2.202 and R1.101 have the same WAGE
        validateTableOfLongs(client,
                "select ID from R2 where not exists (select WAGE from R1 where R1.WAGE = R2.WAGE) order by id;",
                new long[][] {{200}, {201}, {203}});

        // NULL not equal NULL, R2.200 and R2.203 have NULL WAGE
        validateTableOfLongs(client,
                "select ID from R2 RR2 where exists (select 1 from R2 where RR2.WAGE = R2.WAGE) order by id;",
                new long[][] {{201}, {202}});

        // NULL not equal NULL, R2.200 and R2.203 have NULL WAGE
        validateTableOfLongs(client,
                "select ID from R2 RR2 where RR2.WAGE IN (select WAGE from R2 order by WAGE limit 4 offset 1) order by id;",
                new long[][] {{201}, {202}});
        validateTableOfLongs(client,
                "select ID from R2 RR2 where RR2.WAGE = ANY (select WAGE from R2 order by WAGE limit 4 offset 1) order by id;",
                new long[][] {{201}, {202}});

        validateTableOfLongs(client,
                "select ID from R2 where (WAGE IN (select WAGE from R1 order by WAGE limit 4 offset 1)) IS NULL order by id;",
                new long[][] {{200}, {203}});
        validateTableOfLongs(client,
                "select ID from R2 where (WAGE = ANY (select WAGE from R1 order by WAGE limit 4 offset 1)) IS NULL order by id;",
                new long[][] {{200}, {203}});

        // The outer expression is empty. The inner expression is not empty. The =ANY is NULL
        validateTableOfLongs(client,
                "select ID from R2 where ((select WAGE from R1 where ID = 0) = ANY (" +
                        "select WAGE from R2 order by WAGE limit 4 offset 1)) IS NULL order by id;",
                new long[][] {{200}, {201}, {202}, {203}});

        // The outer expression is empty. The inner expression is empty. The =ANY is FALSE
        validateTableOfLongs(client,
                "select ID from R2 where not (select WAGE from R1 where ID = 0) = ANY " +
                        " (select WAGE from R1 where ID = 0 order by WAGE limit 4 offset 1) order by id;",
                new long[][] {{200}, {201}, {202}, {203}});
    }

    /**
     * SELECT FROM WHERE OUTER = ALL (SELECT INNER ...) returning inner NULL.
     * If inner_expr is empty => TRUE
     * If inner_expr contains NULL and outer_expr OP inner_expr is TRUE for all other inner values => NULL
     * If inner_expr contains NULL and outer_expr OP inner_expr is FALSE for some other inner values => FALSE
     *
     * @throws Exception
     */
    @Test
    public void testOpAllWithInnerNull() throws Exception {
        final Client client = getClient();
        //                                 id, wage, dept, tm
        client.callProcedure("R1.insert", 100, 1000, 2, "2013-07-18 02:00:00.123457");

        client.callProcedure("R2.insert", 100, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102, 1001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103, 1003, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105, 1000, 2, "2013-07-18 02:00:00.123457");

        // The inner_expr is empty => TRUE
        validateTableOfLongs(client,
                "select ID from R1 where WAGE = ALL (select WAGE from R2 where ID > 107);", new long[][] {{100}});

        validateTableOfLongs(client,
                "select ID from R1 where (select WAGE from R1) = ALL (select WAGE from R2 where ID > 107);",
                new long[][] {{100}});

        // The inner set consists only of NULLs
        validateTableOfLongs(client,
                "select ID from R1 where WAGE = ALL (select WAGE from R2 where ID in (100, 101));", EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE = ALL (select WAGE from R2 where ID in (100, 101))) IS NULL;",
                new long[][] {{100}});

        // If inner_expr contains NULL and outer_expr OP inner_expr is TRUE
        // for all other inner values
        validateTableOfLongs(client,
                "select ID from R1 where WAGE = ALL (select WAGE from R2 where ID in (100, 104, 105));",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select ID from R1 where (WAGE = ALL (select WAGE from R2 where ID in (100, 104, 105))) IS NULL;",
                new long[][] {{100}});

        // If inner_expr contains NULL and
        // outer_expr OP inner_expr is FALSE for some other inner values,
        // the result is FALSE
        if ( ! isHSQL()) { // HSQL gets this one wrong
            validateTableOfLongs(client, "select ID from R1 where not (WAGE = ALL (select WAGE from R2));", new long[][] {{100}});
        }
    }

    /**
     * SELECT FROM WHERE OUTER = ALL (SELECT INNER ...). The OUTER is NULL.
     * If outer_expr is NULL and inner_expr is empty => TRUE
     * If outer_expr is NULL and inner_expr produces any row => NULL
     * @throws Exception
     */
    @Test
    public void testOpAllWithOuterNull() throws Exception {
        Client client = getClient();
        //                                 id, wage, dept, tm
        client.callProcedure("R1.insert", 100, 1000, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 101, 1001, 2, "2013-07-18 02:00:00.123457");

        client.callProcedure("R2.insert", 200, null, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 201, 2001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 202, 1001, 2, "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 203, null, 2, "2013-07-18 02:00:00.123457");

        // the inner result set is empty, the =ALL  expression is TRUE
        validateTableOfLongs(client,
                "select ID from R2 where WAGE = ALL (select WAGE from R1 where ID = 107) order by ID;",
                new long[][] {{200}, {201}, {202}, {203}});

        validateTableOfLongs(client,
                "select ID from R2 where (ID, WAGE) = ALL (select ID, WAGE from R1 where ID = 1000) order by ID;",
                new long[][] {{200}, {201}, {202}, {203}});

        // the inner result set is empty, the =ALL  expression is TRUE
        validateTableOfLongs(client,
                "select ID from R2 where (select WAGE from R1 where ID = 1000) = ALL (select WAGE from R1 where ID = 1000) order by ID;",
                new long[][] {{200}, {201}, {202}, {203}});

        //  the outer_expr is NULL and inner_expr is not empty => NULL
        validateTableOfLongs(client,
                "select ID from R2 where (WAGE = ALL (select WAGE from R1)) IS NULL order by ID;",
                new long[][] {{200}, {203}});

        if (!isHSQL()) {
            // I think HSQL gets this one wrong evaluating the =ALL to FALSE instead of NULL.
            // PostgreSQL agrees with us
            validateTableOfLongs(client,
                    "select ID from R2 where ID = 200 and ((ID,WAGE) = ALL (select ID, WAGE from R1)) IS NULL;",
                    new long[][] {{200}});
        }
    }

    // Test subqueries on partitioned table cases
    @Test
    public void testSubSelects_from_partitioned() throws Exception {
        final Client client = getClient();
        loadData(false);

        validateTableOfLongs(client,
                "select T1.ID, T1.DEPT from (select ID, DEPT from P1) T1, P2 where T1.ID = P2.DEPT order by T1.ID;",
                new long[][] {{1, 1}, {1, 1}, {1, 1}, {2, 1}, {2, 1}});

        validateTableOfLongs(client,
                "select T1.ID, T1.DEPT from (select ID, DEPT from P1 where ID = 2) T1, P2 where T1.ID = P2.DEPT order by T1.ID;",
                new long[][] {{2, 1}, {2, 1}});

        validateTableOfLongs(client,
                "select T1.ID, T1.DEPT from (select ID, DEPT from P1 where ID = 2) T1, (select DEPT from P2) T2, " +
                        "(select ID from P3) T3 where T1.ID = T2.DEPT and T2.DEPT = T3.ID order by T1.ID;",
                new long[][] {{2, 1}, {2, 1}});

        validateTableOfLongs(client,
                "select T1.ID, T1.DEPT from (select P1.ID, P1.DEPT from P1, P2 where P1.ID = P2.DEPT) T1, " +
                        "P2 where T1.ID = P2.DEPT and P2.DEPT = 2 order by T1.ID;",
                new long[][] {{2, 1}, {2, 1}, {2, 1}, {2, 1}});


        // Outer joins
        validateTableOfLongs(client,
                "select T1.ID, T1.DEPT from (select ID, DEPT from P1) T1 LEFT OUTER JOIN P2 ON T1.ID = P2.DEPT order by T1.ID;",
                new long[][] {{1, 1}, {1, 1}, {1, 1}, {2, 1}, {2, 1}, {3, 1}, {4, 2}, {5, 2}});

        validateTableOfLongs(client,
                "select T1.ID, T1.DEPT from (select ID, DEPT from P1) T1 LEFT OUTER JOIN P2 ON T1.ID = P2.DEPT where T1.ID = 3 order by T1.ID;",
                new long[][] {{3, 1}});

        validateTableOfLongs(client,
                "select T1.ID, T1.DEPT, P2.WAGE from (select ID, DEPT from P1) T1 LEFT OUTER JOIN P2 ON " +
                        "T1.ID = P2.DEPT AND P2.DEPT = 2 order by 1, 2, 3;",
                new long[][] {
                        {1, 1, Long.MIN_VALUE}, {2, 1, 40}, {2, 1, 50},
                        {3, 1, Long.MIN_VALUE}, {4, 2, Long.MIN_VALUE}, {5,2, Long.MIN_VALUE}});

        // ENG-16932 NPE when subquery on partitioned table
        try {
            client.callProcedure("@AdHoc", "select * from P1 where exists (select count(*) from P1 where ID = ID);");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unsupported statement, subquery expressions are only supported for "
                    + "single partition procedures and AdHoc replicated tables"));
        }
    }

    // Test scalar subqueries
    @Test
    public void testSelectScalar() throws Exception {
        Client client = getClient();
        loadData(true);

        for (String tb : new String[] { "R1", "P1"} ) {
            subtestSelectScalarwithParentTable(tb, client);
            subTestGroupByScalarSubqueryWithParentTable(tb, client);
        }

        // ENG-8145
        subTestScalarSubqueryWithParentOrderByOrGroupBy(client);

        // ENG-8159, ENG-8160
        // test Scalar sub-query with non-integer type
        subTestScalarSubqueryWithNonIntegerType(client);
    }

    private void subtestSelectScalarwithParentTable(String tb, Client client) throws Exception {
        // Non-correlated
        validateTableOfLongs(client,
                "select T1.ID, T1.DEPT, (select ID from R2 where ID = 2) from " + tb + " T1 " +
                        "where T1.ID < 3 order by T1.ID desc;",
                new long[][] {{2, 1, 2}, {1, 1, 2}});

        // User-parameter-correlated
        validateTableOfLongs(client.callProcedure("@AdHoc",
                "select T1.ID, T1.DEPT, (select ID from R2 where ID = ?) from " + tb + " T1 " +
                        "where T1.ID < 3 order by T1.ID desc;", 2).getResults()[0],
                new long[][] {{2, 1, 2}, {1, 1, 2}});

        // Correlated
        validateTableOfLongs(client,
                "select T1.ID, T1.DEPT, (select ID from R2 where R2.ID = T1.ID and R2.WAGE = 50) from " +
                        tb + " T1 where T1.ID > 3 order by T1.ID desc;",
                new long[][] {{7, 2, Long.MIN_VALUE}, {6, 2, Long.MIN_VALUE}, {5, 2, 5}, {4, 2, Long.MIN_VALUE}});

        // Uncorreleted on simple seq scan
        validateTableOfLongs(client,
                "select T1.DEPT, (select ID from R2 where R2.ID = 1) from " + tb + " T1 where T1.DEPT = 2;",
                new long[][] {{2, 1}, {2, 1}, {2, 1}, {2, 1}});

        // check for cardinality error
        try {
            client.callProcedure("@AdHoc",
                    "select T1.ID, T1.DEPT, (select ID from R2 where R2.ID < T1.ID) from " +
                            tb + " T1 where T1.ID > 3 order by T1.ID desc;");
            fail("Did not get expected cardinality error");
        } catch (ProcCallException ex) {
            String errMsg = (isHSQL()) ? "cardinality violation" :
                "More than one row returned by a scalar/row subquery";
            assertTrue(ex.getMessage().contains(errMsg));
        }

        // scalar value expression correlated by group by column
        // Hsqldb back end bug: ENG-8273 NPE
        if (!isHSQL()) {
            validateTableOfLongs(client,
                    "select T1.DEPT, count(*), (select max(dept) from R2 where R2.wage = T1.wage) from " + tb +
                            " T1 group by dept, wage order by dept, wage;",
                    new long[][] {{1, 1, 2}, {1, 1, 1}, {1, 1, 1}, {2, 1, 2}, {2, 2, 2}, {2,1,2}});

            validateTableOfLongs(client,
                    "select T1.DEPT, count(*), (select sum(dept) from R2 where R2.wage > T1.dept * 10) from " + tb +
                            " T1 group by dept order by dept;",
                    new long[][] {{1,3,8}, {2, 4, 7}});
        }
    }

    private void subTestGroupByScalarSubqueryWithParentTable(String tb, Client client) throws Exception {
        // group by scalar value expression
        validateTableOfLongs(client,
                "select T1.DEPT, count(*) as ct from " + tb + " T1 group by dept, (" +
                        "select count(dept) from R2 where R2.wage = T1.wage) order by dept, ct;",
                new long[][] {{1, 1}, {1, 2}, {2, 1}, {2, 3}});

        // dumb edge case -- non-correlated so constant group by expression
        validateTableOfLongs(client,
                "select T1.DEPT, count(*) as ct from " + tb + " T1 group by dept, (" +
                        "select count(dept) from R2 where R2.wage > 15) order by dept, ct;",
                new long[][] {{1,3}, {2, 4}});

        // group by scalar in a complex expression all referenced by tag
        validateTableOfLongs(client,
                "select T1.DEPT, abs((select count(dept) from R2 where R2.wage > T1.wage) / 2 - 3) as tag, count(*) as ct from " +
                        tb + " T1 group by dept, tag order by dept, ct;",
                new long[][] {{1,2,1}, {1,1,2}, {2,1,1}, {2,3,3}});

        // duplicates the subquery expression
        validateTableOfLongs(client,
                "select T1.DEPT, count(*) as ct from " + tb + " T1 group by dept, (" +
                        "select count(dept) from R2 where R2.wage > 15), (select count(dept) from R2 where R2.wage > 15) order by dept, ct;",
                new long[][] {{1,3}, {2, 4}});

        // changes a little bit on the subquery
        validateTableOfLongs(client,
                "select T1.DEPT, count(*) as ct from " + tb + " T1 group by dept, (" +
                        "select count(dept) from R2 where R2.wage > 15), (select count(dept) from R2 where R2.wage > 14) order by dept, ct;",
                new long[][] {{1,3}, {2, 4}});

        // expression with subquery
        validateTableOfLongs(client,
                "select T1.DEPT, count(*) as ct from " + tb + " T1 group by dept, (" +
                        "select count(dept) from R2 where R2.wage > 15), (1 + (select count(dept) from R2 where R2.wage > 14)) order by dept, ct;",
                new long[][] {{1,3}, {2, 4}});

        // duplicates the subquery expression
        validateTableOfLongs(client,
                "select T1.DEPT, abs((select count(dept) from R2 where R2.wage > T1.wage) / 2 - 3) as tag1, " +
                        "       abs((select count(dept) from R2 where R2.wage > T1.wage) / 2 - 3) as tag2, count(*) as ct from " +
                        tb + " T1 group by dept, tag1 order by dept, ct;",
                new long[][] {{1,2,2,1}, {1,1,1,2}, {2,1,1,1}, {2,3,3,3}});

        // expression with subquery
        validateTableOfLongs(client,
                "select T1.DEPT, abs((select count(dept) from R2 where R2.wage > T1.wage) / 2 - 3) as tag1, (5 + " +
                        "abs((select count(dept) from R2 where R2.wage > T1.wage) / 2 - 3)) as tag2, " +
                        "count(*) as ct from " + tb + " T1 group by dept, tag1 order by dept, ct;",
                new long[][] {{1,2,7,1}, {1,1,6,2}, {2,1,6,1}, {2,3,8,3}});

        // check for cardinality error from grouped by scalar
        try {
            client.callProcedure("@AdHoc",
                    "select max(T1.ID), T1.DEPT from " + tb + " T1 where T1.ID > 3 group by DEPT, (" +
                            "select ID from R2 where R2.ID < T1.ID) order by T1.DEPT desc;");
            fail("Did not get expected cardinality error");
        } catch (ProcCallException ex) {
            String errMsg = (isHSQL()) ? "cardinality violation" :
                "More than one row returned by a scalar/row subquery";
            assertTrue(ex.getMessage().contains(errMsg));
        }
    }

    private void subTestScalarSubqueryWithParentOrderByOrGroupBy(Client client) throws Exception {
        String sql;
        int len = 100;

        if (isValgrind()) {
            // valgrind is too slow with 100 rows, use a small number
            len = 10;
        }

        long[][] expected = new long[len][1];
        for (int i = 0; i < len; ++i) {
            client.callProcedure("@AdHoc", "insert into R_ENG8145_1 values (?, ?);", i, i * 2);
            client.callProcedure("@AdHoc", "insert into R_ENG8145_2 values (?, ?);", i, i * 2);
            long val = len - ((i * 2) + 1);
            if (val < 0)
                val = 0;
            expected[i][0] = val;
        }

        validateTableOfLongs(client,
                "select (select count(*) from R_ENG8145_1 where ID > parent.num) from R_ENG8145_2 parent order by id;",
                expected);
        // has to have order by ID to be deterministic
        validateTableOfLongs(client,
                "select (select count(*) from R_ENG8145_1 where ID > parent.num) from R_ENG8145_2 parent group by id order by id;",
                expected);

        // ENG-8173
        client.callProcedure("@AdHoc", "insert into R_ENG8173_1 values (0, 'foo', 50);");
        client.callProcedure("@AdHoc", "insert into R_ENG8173_1 values (1, 'goo', 25);");

        // These queries were failing because we weren't calling "resolveColumnIndexes"
        // for subqueries that appeared on the select list (as part of a projection node).
        VoltTable vt = client.callProcedure("@AdHoc",
                "select *, (select SUM(NUM) from R_ENG8173_1) from R_ENG8173_1 A1 order by DESC;")
                .getResults()[0];

        assertTrue (vt.advanceRow());
        assertEquals(0, vt.getLong(0));
        assertEquals("foo", vt.getString(1));
        assertEquals(50, vt.getLong(2));
        assertEquals(75, vt.getLong(3));

        assertTrue (vt.advanceRow());
        assertEquals(1, vt.getLong(0));
        assertEquals("goo", vt.getString(1));
        assertEquals(25, vt.getLong(2));
        assertEquals(75, vt.getLong(3));

        assertFalse(vt.advanceRow());

        validateTableOfLongs(client,
                "select (select SUM(NUM) + SUM(ID) from R_ENG8173_1) from R_ENG8173_1 A1 order by DESC;",
                new long[][] {{76}, {76}});

        // Similar queries from ENG-8174
        client.callProcedure("@AdHoc", "truncate table R4");
        client.callProcedure("@AdHoc", "insert into R4 values (0,null,null,null);");
        client.callProcedure("@AdHoc", "insert into R4 values (1,'foo1',-1,1.1);");

        vt = client.callProcedure("@AdHoc",
                "select NUM V, (select SUM(RATIO) from R4) from R4 order by V;").getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getLong(0); assertTrue(vt.wasNull());
        assertEquals(1.1, vt.getDouble(1));

        assertTrue(vt.advanceRow());
        assertEquals(-1, vt.getLong(0));
        assertEquals(1.1, vt.getDouble(1));
        assertFalse(vt.advanceRow());


        vt = client.callProcedure("@AdHoc",
                "select RATIO V, (select SUM(NUM) from R4) from R4 order by V;").getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getDouble(0); assertTrue(vt.wasNull());
        assertEquals(-1, vt.getLong(1));

        assertTrue(vt.advanceRow());
        assertEquals(1.1, vt.getDouble(0));
        assertEquals(-1, vt.getLong(1));
        assertFalse(vt.advanceRow());


        vt = client.callProcedure("@AdHoc",
                "select NUM V, (select MAX(DESC) from R4) from R4 order by V;").getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getLong(0); assertTrue(vt.wasNull());
        assertEquals("foo1", vt.getString(1));

        assertTrue(vt.advanceRow());
        assertEquals(-1, vt.getLong(0));
        assertEquals("foo1", vt.getString(1));
        assertFalse(vt.advanceRow());
    }

    private void subTestScalarSubqueryWithNonIntegerType(Client client) throws Exception {

        client.callProcedure("@AdHoc", "truncate table R4");
        client.callProcedure("R4.insert", 1, "foo1", -1, 1.1);
        client.callProcedure("R4.insert", 2, "foo2", -1, 2.2);
        VoltTable vt;

        // test FLOAT
        vt = client.callProcedure("@AdHoc", "select ID, (select SUM(RATIO) from R4) from R4 order by ID;").getResults()[0];
        assertEquals(2, vt.getRowCount());
        assertTrue(vt.advanceRow());
        assertEquals(1, vt.getLong(0)); assertEquals(3.3, vt.getDouble(1), 0.0001);
        assertTrue(vt.advanceRow());
        assertEquals(2, vt.getLong(0)); assertEquals(3.3, vt.getDouble(1), 0.0001);

        // test VARCHAR
        vt = client.callProcedure("@AdHoc",
                "select ID, (select MIN(DESC) from R4) from R4 order by ID;").getResults()[0];
        assertEquals(2, vt.getRowCount());
        assertTrue(vt.advanceRow());
        assertEquals(1, vt.getLong(0)); assertEquals("foo1", vt.getString(1));
        assertTrue(vt.advanceRow());
        assertEquals(2, vt.getLong(0)); assertEquals("foo1", vt.getString(1));
    }

    @Test
    public void testWhereScalarSubSelects() throws Exception {
        Client client = getClient();
        loadData(false);

        for (String tb : new String[] { "R1", "P1"} ) {
            subtestWhereScalarForParentTable(tb, client);
        }
    }

    private void subtestWhereScalarForParentTable(String tb, Client client) throws Exception {
        // Index Scan
        validateTableOfLongs(client.callProcedure("@AdHoc",
                "select T1.ID from " + tb + " T1 where T1.ID = (select ID from R2 where ID = ?);",
                2).getResults()[0], new long[][] {{2}});

        // Index Scan correlated
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where T1.ID = (select ID/2 from R2 where ID = T1.ID * 2) order by id;",
                new long[][] {{1}, {2}});

        // Seq Scan
        validateTableOfLongs(client.callProcedure("@AdHoc",
                "select T1.ID from " + tb + " T1 where T1.DEPT = (select DEPT from R2 where ID = ?) order by id;",
                1).getResults()[0], new long[][] {{1}, {2}, {3}});

        // Seq Scan correlated
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where T1.DEPT = (select DEPT from R2 where ID = T1.ID * 2);",
                new long[][] {{1}});

        // Different comparison operators
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where T1.DEPT > (select DEPT from R2 where ID = 3) order by id;",
                new long[][] {{4}, {5}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (select DEPT from R2 where ID = 3) != T1.DEPT order by id;",
                new long[][] {{4}, {5}});

        // NLIJ
        validateTableOfLongs(client.callProcedure("@AdHoc",
                "select T1.ID, R2.ID from " + tb + " T1, R2 where T1.DEPT = R2.DEPT + " +
                        "(select DEPT from R2 where ID = ?) order by T1.ID, R2.ID limit 2;",
                1).getResults()[0], new long[][] {{4, 1}, {4, 2}});

        // @TODO NLIJ correlated
        validateTableOfLongs(client,
                "select T1.ID, R2.ID from " + tb + " T1, R2 where R2.ID = (select ID from R2 where ID = T1.ID) order by T1.ID;",
                new long[][] {{1, 1}, {2,2}, {3,3}, {4,4}, {5,5}});

        // NLJ correlated
        validateTableOfLongs(client,
                "select T1.ID, R2.ID from " + tb + " T1, R2 where R2.DEPT = (select DEPT from R2 where ID = T1.ID + 4) order by T1.ID, R2.ID;",
                new long[][] {{1, 4}, {1,5}});

        // Having
        // Uncomment these tests when ENG-8306 is finished
        //        vt = client.callProcedure("@AdHoc", sql, 2).getResults()[0];
        //        validateTableOfLongs(vt, new long[][] {{5}});
        verifyAdHocFails(client, TestPlansInExistsSubQueries.HavingErrorMsg,
                "select max(T1.ID) from " + tb + " T1 group by T1.DEPT having count(*) = (select R2.ID from R2 where R2.ID = ?);",
                2);

        // Having correlated -- parent TVE in the aggregated child expression
        // Uncomment these tests when ENG-8306 is finished
        //        validateTableOfScalarLongs(vt, new long[] {5});
        verifyStmtFails(client,
                "select max(T1.ID) from " + tb + " T1 group by T1.DEPT having count(*) = (select R2.ID from R2 where R2.ID = T1.DEPT);",
                TestPlansInExistsSubQueries.HavingErrorMsg);

        // Uncomment these tests when ENG-8306 is finished
        //        validateTableOfLongs(client, sql, new long[][] {{2,5}});
        verifyStmtFails(client,
                "select DEPT, max(T1.ID) from " + tb + " T1 group by T1.DEPT having count(*) = (select R2.ID from R2 where R2.ID = T1.DEPT);",
                TestPlansInExistsSubQueries.HavingErrorMsg);

        try {
            client.callProcedure("@AdHoc",
                    "select T1.ID from " + tb + " T1 where T1.ID = (select ID from R2);");
            fail("Did not get expected cardinality violation");
        } catch (ProcCallException ex) {
            String errMsg = (isHSQL()) ? "cardinality violation" :
                "More than one row returned by a scalar/row subquery";
            assertTrue(ex.getMessage().contains(errMsg));
        }
    }

    @Test
    public void testSingleColumnOpAll() throws Exception {
        if (! USING_CALCITE) {
            Client client = getClient();
            loadData(false);

            for (String tb : new String[]{"R1", "P1"}) {
                subtestSingleColumnOpAllForParentTable(tb, client);
            }
        }
    }

    private void subtestSingleColumnOpAllForParentTable(String tb, Client client) throws Exception {
        // Subquery with limit/offset parameter
        validateTableOfLongs(client.callProcedure("@AdHoc",
                "select T1.ID from " + tb + " T1 where T1.ID > ALL (select ID from R2 order by ID limit ? offset ?);",
                2, 2).getResults()[0], new long[][] {{5}});

        validateTableOfLongs(client.callProcedure("@AdHoc",
                "select T1.ID from " + tb + " T1 where T1.ID > ALL (select ID from R2 order by ID limit ? offset ?) order by 1;",
                2, 1).getResults()[0], new long[][] {{4}, {5}});

        validateTableOfLongs(client.callProcedure("@AdHoc",
                "select T1.ID from " + tb + " T1 where T1.ID > ALL (select ID from R2 order by ID limit ? offset ?) order by 1;",
                1, 2).getResults()[0], new long[][] {{4}, {5}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where T1.DEPT >= ALL (select DEPT from R2) order by id;",
                new long[][] {{4}, {5}});

        // Index scan
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where T1.ID > ALL (select ID from R2 where R2.ID < 4) order by id;",
                new long[][] {{4}, {5}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where T1.ID >= ALL (select ID from R2);",
                new long[][] {{5}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where T1.ID <= ALL (select ID from R2);",
                new long[][] {{1}});
    }

    @Test
    public void testWhereRowSubSelects() throws Exception {
        if (isHSQL()) {
            // hsqldb has back end error for these cases
            return;
        }

        Client client = getClient();
        //                               id,wage,dept,tm
        client.callProcedure("R2.insert", 3,  5, 1, "2013-07-18 10:40:01.123457");
        client.callProcedure("R2.insert", 4, 10, 1, "2013-08-18 02:00:00.123457");
        client.callProcedure("R2.insert", 5, 10, 1, "2013-08-18 02:00:00.123457");
        client.callProcedure("R2.insert", 6, 10, 2, "2013-08-18 02:00:00.123457");
        client.callProcedure("R2.insert", 7, 50, 2, "2013-09-18 02:00:00.123457");

        for (String tb : new String[] { "R1", "P1" }) {
            subtestWhereRowSubSelectsForParentTable(tb, client);
        }
    }

    private void subtestWhereRowSubSelectsForParentTable(String tb, Client client) throws Exception {
        //                               id,wage,dept,tm
        client.callProcedure(tb + ".insert", 1,  5, 1, "2013-06-18 02:00:00.123457");
        client.callProcedure(tb + ".insert", 2, 10, 1, "2013-07-18 10:40:01.123457");
        client.callProcedure(tb + ".insert", 3, 10, 2, "2013-08-18 02:00:00.123457");

        // T1 2, 10, 1 = R2 4, 10, 1
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.WAGE, T1.DEPT) = (select WAGE, DEPT from R2 where ID = 4);",
                new long[][] {{2}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.WAGE, T1.DEPT) != (select WAGE, DEPT from R2 where ID = 4) order by ID;",
                new long[][] {{1}, {3}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.WAGE, T1.DEPT) > (select WAGE, DEPT from R2 where ID = 4);",
                new long[][] {{3}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.WAGE, T1.DEPT) < (select WAGE, DEPT from R2 where ID = 4);",
                new long[][] {{1}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.WAGE, T1.DEPT) >= (select WAGE, DEPT from R2 where ID = 4) order by ID;",
                new long[][] {{2}, {3}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.WAGE, T1.DEPT) <= (select WAGE, DEPT from R2 where ID = 4) order by ID;",
                new long[][] {{1}, {2}});

        // T1 2, 10, 1 = R2 4, 10, 1 and 5, 10, 1
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.WAGE, T1.DEPT) = ALL (select WAGE, DEPT from R2 where ID in (4,5));",
                new long[][] {{2}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.WAGE, T1.DEPT) = ALL (select WAGE, DEPT from R2);",
                EMPTY_TABLE);

        // T1 3, 10, 2 >= ALL R2 except R2.7
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where ID = 3 and (T1.WAGE, T1.DEPT) >= ALL " +
                        " (select WAGE, DEPT from R2 where ID < 7 order by WAGE, DEPT DESC);",
                new long[][] {{3}});

        // T1 3, 10, 2 < R2 except R2.7 50 2
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.WAGE, T1.DEPT) >= ALL (select WAGE, DEPT from R2);",
                EMPTY_TABLE);

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.DEPT, T1.TM) < ALL (select DEPT, TM from R2);",
                new long[][] {{1}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.DEPT, T1.TM) <= ALL (select DEPT, TM from R2) order by ID;",
                new long[][] {{1}, {2}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.DEPT, T1.TM) <= ALL (select DEPT, TM from R2 order by DEPT, TM ASC) order by ID;",
                new long[][] {{1}, {2}});

        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where (T1.DEPT, T1.TM) <= ALL (" +
                        "select DEPT, TM from R2 order by DEPT, TM DESC) order by ID;",
                new long[][] {{1}, {2}});
    }

    @Test
    public void testRepeatedQueriesDifferentData() throws Exception {
        Client client = getClient();
        //                               id,wage,dept,tm
        client.callProcedure("R1.insert", 1,  5, 1, "2013-06-18 02:00:00.123457");
        client.callProcedure("R1.insert", 2, 10, 1, "2013-07-18 10:40:01.123457");
        client.callProcedure("R1.insert", 3, 15, 2, "2013-08-18 02:00:00.123457");

        client.callProcedure("R2.insert", 1,  5, 1, "2013-08-18 02:00:00.123457");

        validateTableOfScalarLongs(client, "select (select max(wage) from r1) from r2;", new long[] {15});

        client.callProcedure("@AdHoc", "update r1 set wage = 35 where id = 2");

        // Make sure that the second query reflects the current data.
        validateTableOfScalarLongs(client, "select (select max(wage) from r1) from r2;", new long[] {35});
    }

    @Test
    public void testSubqueryWithExceptions() throws Exception {
        Client client = getClient();
        //                               id,wage,dept,tm
        client.callProcedure("R1.insert", 1,  5, 1, "2013-06-18 02:00:00.123457");
        client.callProcedure("R1.insert", 2, 10, 1, "2013-07-18 10:40:01.123457");
        client.callProcedure("R1.insert", 3, 15, 2, "2013-08-18 02:00:00.123457");
        client.callProcedure("R1.insert", 4,  0, 2, "2013-08-18 02:00:00.123457");

        // A divide by zero exception in the top-level query!
        // Debug assertions in the EE will make this test fail
        // if we don't clean up temp tables for both inner and outer queries.
        String expectedMsg = isHSQL() ? "division by zero" : "Attempted to divide 30 by 0";
        verifyStmtFails(client, "select (select max(30 / wage) from r1 where wage <> 0) from r1 where id = 30 / wage;",
                expectedMsg);
        verifyStmtFails(client, "select (select max(30 / wage) from r1 where wage <> 0) from r1 where id = 30 / wage;",
                expectedMsg);

        // As above, but this time the execption occurs in the inner query.
        verifyStmtFails(client, "select (select max(30 / wage) from r1) from r1;", expectedMsg);
        verifyStmtFails(client, "select (select max(30 / wage) from r1) from r1;", expectedMsg);
    }

    @Test
    public void testSubqueriesWithArithmetic() throws Exception {
        Client client = getClient();

        //                               id,wage,dept,tm
        client.callProcedure("R1.insert", 1, 300, 1, "2013-06-18 02:00:00.123457");
        client.callProcedure("R1.insert", 2, 200, 1, "2013-06-18 02:00:00.123457");

        // These test cases exercise the fix for ENG-8226, in which a missing ScalarValueExpression
        // caused the result of a subquery to be seen as the subquery ID, rather than the contents
        // of subquery's result table.

        validateTableOfScalarLongs(client, "select (select max(wage) from r1) from r1",
                new long[] {300, 300});
        validateTableOfScalarLongs(client, "select (select max(wage) from r1) + 0 as subq from r1",
                new long[] {300, 300});

        validateTableOfScalarLongs(client, "select wage from r1 where wage = (select max(wage) from r1)",
                new long[] {300});
        validateTableOfScalarLongs(client, "select wage from r1 where wage = (select max(wage) - 30 from r1) + 30",
                new long[] {300});

        // The IN operator takes a VectorExpression on its RHS, which uses the "args" field.
        // Make sure that we can handle subqueries in there too.
        validateTableOfScalarLongs(client,
                "select wage from r1 where wage in (7, 8, (select max(wage) from r1), 9, 10, 200) order by wage",
                new long[] {200, 300});
    }

    @Test
    public void testExistsSimplification() throws Exception {
        Client client = getClient();
        for (String tb : new String[] { "R1", "P1" }) {
            subtestExistsSimplificationForParentTable(tb, client);
        }

        client.callProcedure("R2.insert", 1,  5, 1, "2013-06-18 02:00:00.123457");
        client.callProcedure("R2.insert", 2, 10, 1, "2013-07-18 10:40:01.123457");
        client.callProcedure("R2.insert", 3, 10, 2, "2013-08-18 02:00:00.123457");

        for (String tb : new String[] { "R1", "P1" }) {
            subtestExistsSimplificationWithMoreDataForParentTable(tb, client);
        }
    }

    private void subtestExistsSimplificationForParentTable(String tb, Client client) throws Exception {
        VoltTable vt;
        //                               id,wage,dept,tm
        client.callProcedure(tb + ".insert", 1,  5, 1, "2013-06-18 02:00:00.123457");
        client.callProcedure(tb + ".insert", 2, 10, 1, "2013-07-18 10:40:01.123457");
        client.callProcedure(tb + ".insert", 3, 10, 2, "2013-08-18 02:00:00.123457");

        // EXISTS(table-agg-without-having-groupby) => EXISTS(TRUE)
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where exists (select max(ID) from R2) order by ID;",
                new long[][] {{1}, {2}, {3}});

        // EXISTS(SELECT...LIMIT 0) => EXISTS(FALSE)
        if (!isHSQL()) {
            validateTableOfLongs(client,
                    "select T1.ID from " + tb + " T1 where exists (select max(id) from R2 limit 0)",
                    EMPTY_TABLE);

            // count(*) limit 0
            validateTableOfLongs(client,
                    "select T1.ID from " + tb + " T1 where exists (select count(*) from R2 limit 0)",
                    EMPTY_TABLE);

            // EXISTS(SELECT...limit ?) => EXISTS(TRUE/FALSE)
            vt = client.callProcedure("@AdHoc",
                    "select T1.ID from " + tb + " T1 where exists (select count(id) from R2 limit ?)",
                    0).getResults()[0];
            validateTableOfLongs(vt, EMPTY_TABLE);
        }

        vt = client.callProcedure("@AdHoc",
                "select T1.ID from " + tb + " T1 where exists (select count(*) from R2 limit ?) order by id;",
                1).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1}, {2}, {3}});

        // EXISTS(able-agg-without-having-groupby offset 1) => EXISTS(FALSE)
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where exists (select max(ID) from R2 offset 1);",
                EMPTY_TABLE);

        // count(*) offset 1
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where exists (select count(*) from R2 offset 1);",
                EMPTY_TABLE);

        // join on EXISTS(FALSE)
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 join R1 T2 ON exists (select max(ID) from R2 offset 1) and T1.ID = 1;",
                EMPTY_TABLE);

        // join on EXISTS(TRUE)
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 join R1 T2 ON exists (select max(ID) from R2) or T1.ID = 25 order by 1;",
                new long[][] {{1}, {1}, {1}, {2}, {2}, {2}, {3}, {3}, {3}});

        // having TRUE
        validateTableOfLongs(client,
                "select max(ID), WAGE from " + tb + " T1 group by WAGE having exists (" +
                        "select max(ID) from R2) or max(ID) = 25 order by max(ID) asc",
                new long[][] {{1}, {3}});

        // having FALSE
        validateTableOfLongs(client,
                "select max(ID), WAGE from " + tb + " T1 group by WAGE having exists (" +
                        "select max(ID) from R2 offset 1) and max(ID) > 0 order by max(ID) asc",
                EMPTY_TABLE);

    }

    private void subtestExistsSimplificationWithMoreDataForParentTable(String tb, Client client) throws Exception {
        // EXISTS(SELECT ... OFFSET ?)
        validateTableOfLongs(client.callProcedure("@AdHoc",
                "select T1.ID from " + tb + " T1 where exists (select ID from R2 offset ?)",
                4).getResults()[0], EMPTY_TABLE);

        validateTableOfLongs(client.callProcedure("@AdHoc",
                "select T1.ID from " + tb + " T1 where exists (select ID from R2 offset ?) order by id;",
                1).getResults()[0], new long[][] {{1}, {2}, {3}});

        // Subquery subquery-without-having with group by and no limit => select .. from r2 limit 1
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where exists (select WAGE from R2 group by WAGE) order by id;",
                new long[][] {{1}, {2}, {3}});

        // Subquery subquery-without-having with group by and offset => select .. from r2 group by offset
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where exists (select WAGE from R2 group by WAGE offset 2)",
                EMPTY_TABLE);

        // Subquery subquery-without-having with group by => select .. from r2 limit 1
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where exists (select ID, MAX(WAGE) from R2 group by ID) order by id;",
                new long[][] {{1}, {2}, {3}});

        // Subquery subquery-with-having with group by => select .. from r2 group by having agg
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where exists (select ID, MAX(WAGE) from R2 group by ID having MAX(WAGE) > 20)",
                EMPTY_TABLE);

        // Subquery subquery-with-having with group by => select .. from r2 group by having limit 1
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where exists (select ID, MAX(WAGE) from R2 group by ID having MAX(WAGE) > 9) order by id;",
                new long[][] {{1}, {2}, {3}});

        // Subquery subquery-with-having with group by offset => select .. from r2 group by having limit 1 offset
        validateTableOfLongs(client,
                "select T1.ID from " + tb + " T1 where exists (select ID, MAX(WAGE) from R2 group by ID having MAX(WAGE) > 9 offset 2);",
                EMPTY_TABLE);
    }

    @Test
    public void testAmbiguousColumns() throws Exception {
        Client client = getClient();
        Object [][] R1Contents = {
                { 101, 100, 10, "2013-07-18 02:00:00.123457" },
                { 102, 101, 10, "2013-07-18 02:00:00.123457" },
                { 103, 104, 10, "2013-07-18 02:00:00.123457" }
        };
        Object [][] R2Contents = {
                { 201, 100 + 101, 21, "2013-07-18 02:00:00.123457"},
                { 202, 102 + 101, 22, "2013-07-18 02:00:00.123457"},
                { 203, 103 + 104, 23, "2013-07-18 02:00:00.123457"}
        };
        for (Object[] row : R1Contents) {
            client.callProcedure("R1.insert", row);
        }
        for (Object[] row : R2Contents) {
            client.callProcedure("R2.insert", row);
        }
        // DEPT should be from R2.  WAGE should be from S1 and R2 both.
        String sql = "select DEPT, WAGE from (select ID + WAGE as WAGE from R1) AS S1 join R2 using(WAGE) order by DEPT;";
        long[][] expected = {
                {21, 100 + 101},
                {22, 102 + 101},
                {23, 103 + 104}
        };
        validateTableOfLongs(client, sql, expected);
    }

    @Test
    public void testEng8394SubqueryWithUnionAndCorrelation() throws Exception {
        if (USING_CALCITE) {
            return;
        }
        Client client = getClient();

        Object[][] paramsArray = {
            {8, "MkqCtZgvOHdpeG", -25010, 6.94485579315452628002e-01 },
            {9, "MkqCtZgvOHdpeG", -25010, 5.09864294045922816778e-01},
            {10, "MkqCtZgvOHdpeG", -18299, 7.41008138128985693882e-02},
            {11, "MkqCtZgvOHdpeG", -18299, 1.60503696919861771342e-01},
            {12, "BQIdkCDzTcGaTW", -17683, 3.32297930030505339616e-01},
            {13, "BQIdkCDzTcGaTW", -17683, 7.72335099708186811895e-01},
            {14, "BQIdkCDzTcGaTW", null, 2.89585585895251185207e-02},
            {15, "BQIdkCDzTcGaTW", null, 6.75424182636293113369e-01}
        };

        for (Object[] params : paramsArray) {
            client.callProcedure("R4.Insert", params);
        }

        // In this bug, we were getting an invalid cast here, because
        // we were peeking at the stale VARCHAR parameter from the insert statement
        // when trying to evaluate the outer reference in the subquery.
        // The correct answer is zero rows.
        String subqueryWithUnionAndCorrelation =
                "SELECT ID, RATIO FROM R4 Z WHERE RATIO > (SELECT RATIO FROM R4 WHERE RATIO = Z.RATIO UNION " +
                "    SELECT RATIO FROM R4 WHERE RATIO = Z.RATIO); ";
        VoltTable vt = client.callProcedure("@AdHoc",
                subqueryWithUnionAndCorrelation)
                .getResults()[0];
        assertFalse(vt.advanceRow());

        vt = client.callProcedure("@AdHoc",
                "SELECT RATIO FROM R4 WHERE RATIO = 6.944855793154526e-1 UNION SELECT RATIO FROM R4 WHERE RATIO = 6.944855793154526e-1;")
                .getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(0.6944855793154526, vt.getDouble(0), 0.000001);
        assertFalse(vt.advanceRow());

        // Before the bug was fixed we saw a wrong answer here when
        // we picked up the stale double parameter from the previous query.
        vt = client.callProcedure("@AdHoc",
                subqueryWithUnionAndCorrelation)
                .getResults()[0];
        assertFalse(vt.advanceRow());

        // The following test cases are modified from bugs
        // found by sqlcoverage:
        //   ENG-8391
        //   ENG-8393
        //   ENG-8395

        client.callProcedure("R4.Insert", new Object[]
                {16, "IYMzTgzZjBNgji", null, 3.03873080947161366971e-01});

        vt = client.callProcedure("@AdHoc",
                "SELECT ID, DESC FROM R4 Z WHERE DESC > ANY (SELECT DESC FROM R4 WHERE NUM > -20000 INTERSECT ALL " +
                "    SELECT DESC FROM R4 WHERE NUM < 10000 AND Z.NUM IS NOT NULL) ORDER BY ID")
                .getResults()[0];
        int i = 8;
        while (vt.advanceRow()) {
            assertEquals(i, vt.getLong(0));
            assertEquals("MkqCtZgvOHdpeG", vt.getString(1));
            ++i;
        }
        assertEquals(12, i);

        client.callProcedure("R4.Insert", new Object[]
                {17, "MkqCtZgvOHdpeG", -25010, 6.94485579315452628002e-01});

        vt = client.callProcedure("@AdHoc",
                "SELECT ID, NUM FROM R4 Z WHERE NUM = ALL (SELECT NUM FROM R4 WHERE NUM = Z.NUM UNION " +
                "    SELECT CAST(NUM + 1 AS INTEGER) FROM R4 WHERE NUM = Z.NUM AND Z.ID >= 10) " +
                "AND NUM IS NOT NULL ORDER BY ID")
                .getResults()[0];
        i = 8;
        while (vt.advanceRow()) {
            assertEquals(i, vt.getLong(0));
            assertEquals(-25010, vt.getLong(1));
            ++i;
        }
        assertEquals(10, i);

        // ENG-8396.  In this one the "more than one row" error is expected.
        paramsArray = new Object[][] {
                {19, "MkqCtZgvOHdpeG", -25010, 6.94485579315452628002e-01},
                {20, "MkqCtZgvOHdpeG", -25010, 5.09864294045922816778e-01},
                {21, "MkqCtZgvOHdpeG", -18299, 7.41008138128985693882e-02},
                {22, "MkqCtZgvOHdpeG", -18299, 1.60503696919861771342e-01},
                {23, "BQIdkCDzTcGaTW", -17683, 3.32297930030505339616e-01},
                {24, "BQIdkCDzTcGaTW", -17683, 7.72335099708186811895e-01},
                {25, "BQIdkCDzTcGaTW", null, 2.89585585895251185207e-02},
                {26, "BQIdkCDzTcGaTW", null, 6.75424182636293113369e-01}
        };

        for (Object[] params : paramsArray) {
            client.callProcedure("R4.Insert", params);
        }

        String expectedError = isHSQL() ?
                "cardinality violation" : "More than one row returned by a scalar/row subquery";
        verifyStmtFails(client,
                "SELECT ID ID7, ID FROM R4 Z WHERE ID > (SELECT ID FROM R4 WHERE ID = Z.ID  UNION ALL " +
                        "SELECT ID FROM R4 WHERE ID = Z.ID);",
                expectedError);
    }

    @Test
    public void testNPEbug() throws Exception {
        Client client = getClient();

        for (String tb : new String[] { "R1", "P1" }) {

            //                                  id,wage,dept,tm
            client.callProcedure(tb + ".insert", 1,  5, 1, "2013-06-18 02:00:00.123457");
            client.callProcedure(tb + ".insert", 2, 10, 1, "2013-07-18 10:40:01.123457");
            client.callProcedure(tb + ".insert", 3, 10, 2, "2013-08-18 02:00:00.123457");

            // The simplest case that repros a lingering NPE bug found just before
            // release of universal support for subqueries on replicated tables
            // involved grouping by a scalar subquery and specifically calculating
            // an average on a partitioned parent table column -- the bug was in the
            // feature interaction with the code that considers pushing down avg
            // calculations to the partitions.
            validateTableOfLongs(client,
                    "select (select ID from R2 WHERE DEPT = 7) C0, AVG(WAGE) from " + tb + " T1 group by C0;",
                    new long[][] {{Long.MIN_VALUE, 8}});
        }
    }

    @Test
    public void testSubquerySimplification() throws Exception {
        final Client client = getClient();

        client.callProcedure("@AdHoc", "insert into R5 values (1,2,3)");
        client.callProcedure("@AdHoc", "insert into R5 values (4,5,6)");

        validateTableOfLongs(client,
                "select * from (select C as D, D from R5) T ORDER BY 1;",
                new long[][] {{2, 3}, {5,6}});

        validateTableOfLongs(client,
                "select * from (select A as C, C as D, D from R5) T where C = 1;",
                new long[][] {{1, 2, 3}});

        validateTableOfLongs(client,
                "select a from (select * from (select d as a, c, a as d from R5) T1) T2 ORDER BY a;",
                new long[][] {{3}, {6}});

        validateTableOfLongs(client,
                "select * from (select A + C + D ACD from R5) T where ACD = 6;",
                new long[][] {{6}});

        validateTableOfLongs(client,
                "select * from (select A + C + D ACD, A*C*D ACD from R5) T ORDER BY 1;",
                new long[][] {{6, 6}, {15, 120}});

        validateTableOfLongs(client,
                "select * from (select * from (select * from R5) T1) T2 ORDER BY 1;",
                new long[][] {{1,2,3}, {4,5,6}});

        validateTableOfLongs(client,
                "select MAX(C), D from (select A C, C D from R5) T1 GROUP BY D HAVING MAX(C) > 1;",
                new long[][] {{4,5}});
    }

    // ENG-16616, duplicated plannode id causing insert to crash
    @Test
    public void testSelectSubQueryDuplicatePlanId() throws Exception {
        final Client client = getClient();

        client.callProcedure("@AdHoc", "insert into R_ENG16616 values (1)");

        validateTableOfLongs(client,
                "select * from R_ENG16616;",
                new long[][] {{1}});

        validateTableOfLongs(client,
                "select t0.id from R_ENG16616 t0 order by t0.id limit 1;",
                new long[][] {{1}});

        validateTableOfLongs(client,
                "select * from R_ENG16616 t0 where (select t0.id from R_ENG16616 t0 order by t0.id limit 1) = t0.id;",
                new long[][] {{1}});

        validateTableOfLongs(client,
                "insert into R_ENG16616 select * from R_ENG16616 t0 where (select t0.id from R_ENG16616 t0 order by t0.id limit 1) = t0.id;",
                new long[][] {{1}});

        validateTableOfLongs(client,
                "select * from R_ENG16616;",
                new long[][] {{1}, {1}});
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSubQueriesSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) );" +

                "CREATE TABLE R2 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) );" +

                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) );" +
                "PARTITION TABLE P1 ON COLUMN ID;" +

                "CREATE TABLE P2 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL ASSUMEUNIQUE, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID, DEPT) );" +
                "PARTITION TABLE P2 ON COLUMN DEPT;" +

                "CREATE TABLE P3 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID, WAGE) );" +
                "PARTITION TABLE P3 ON COLUMN ID;" +

                "CREATE TABLE R4 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "DESC VARCHAR(200), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PRIMARY KEY (ID) );" +

                "CREATE TABLE P4 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "DESC VARCHAR(200), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PRIMARY KEY (ID) );" +
                "PARTITION TABLE P4 ON COLUMN ID;" +

                "CREATE TABLE R5 ( " +
                "A INTEGER, " +
                "C INTEGER, " +
                "D INTEGER ); " +

                "CREATE TABLE R_ENG8145_1 (" +
                "ID integer, NUM integer);" +

                "CREATE TABLE R_ENG8145_2 (" +
                "ID integer, NUM integer);" +

                "CREATE TABLE R_ENG8173_1 (" +
                "ID integer, DESC VARCHAR(300), NUM integer);" +

                "CREATE TABLE R_ENG16616 (ID integer);"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            fail();
        }
        boolean success;

        config = new LocalCluster("subselect-onesite.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        /*/ disable for now -- doesn't add much but runtime, anyway. // Cluster
        config = new LocalCluster("subselect-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // */
        return builder;
    }

}
