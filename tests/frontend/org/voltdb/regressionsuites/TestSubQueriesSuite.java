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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.planner.TestPlansInExistsSubQueries;

public class TestSubQueriesSuite extends RegressionSuite {
    public TestSubQueriesSuite(String name) {
        super(name);
    }

    private static final String [] tbs =  {"R1","R2","P1","P2","P3"};
    private static final String [] replicated_tbs =  {"R1", "R2"};
    private void loadData(boolean extra) throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        ClientResponse cr = null;

        // Empty data from table.
        for (String tb: tbs) {
            cr = client.callProcedure("@AdHoc", "delete from " + tb);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            // Insert records into the table.
            String proc = tb + ".insert";
            // id, wage, dept, tm
            cr = client.callProcedure(proc, 1,  10,  1 , "2013-06-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(proc, 2,  20,  1 , "2013-07-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(proc, 3,  30,  1 , "2013-07-18 10:40:01.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(proc, 4,  40,  2 , "2013-08-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(proc, 5,  50,  2 , "2013-09-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            if (extra) {
                client.callProcedure(proc, 6,  10,  2 , "2013-07-18 02:00:00.123457");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                client.callProcedure(proc, 7,  40,  2 , "2013-09-18 02:00:00.123457");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            }
        }
    }

    /**
     * Simple sub-query
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubSelects_Simple() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(false);
        VoltTable vt;
        String sql;

        for (String tb: tbs) {
            sql = "select ID, DEPT FROM (SELECT ID, DEPT FROM "+ tb +") T1 " +
                    "WHERE T1.ID > 4;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {5, 2}});

            sql = "select ID, DEPT FROM (SELECT ID, DEPT FROM "+ tb +") T1 " +
                    "WHERE ID < 3 ORDER BY ID;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 1}, {2, 1}});

            // Nested
            sql = "select A2 FROM (SELECT A1 AS A2 FROM (SELECT ID AS A1 FROM "+ tb +") T1 WHERE T1.A1 - 2 > 0) T2 " +
                    "WHERE T2.A2 < 6 ORDER BY A2";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3}, {4}, {5}});

            sql = "select A2 + 10 FROM (SELECT A1 AS A2 FROM (SELECT ID AS A1 FROM "+ tb +" WHERE ID > 3) T1 ) T2 " +
                    "WHERE T2.A2 < 6 ORDER BY A2";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{14}, {15}});
        }
    }

    /**
     * SELECT FROM SELECT FROM SELECT
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubSelects_Aggregations() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(true);
        VoltTable vt;

        // Test group by queries, order by, limit
        for (String tb: tbs) {
            String sql;
            sql = "select * from ( SELECT dept, sum(wage) as sw, sum(wage)/count(wage) as avg_wage " +
                    "from " + tb + " GROUP BY dept) T1 ORDER BY dept DESC;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{2, 140, 35}, {1, 60, 20} });

            sql = "select sw from ( SELECT dept, sum(wage) as sw, sum(wage)/count(wage) as avg_wage " +
                    "from " + tb + " GROUP BY dept) T1 ORDER BY dept DESC;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfScalarLongs(vt, new long[]{140, 60});

            sql = "select avg_wage from ( SELECT dept, sum(wage) as sw, sum(wage)/count(wage) as avg_wage " +
                    "from " + tb + " GROUP BY dept) T1 ORDER BY dept DESC;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfScalarLongs(vt, new long[]{35, 20});

            // derived aggregated table + aggregation on subselect
            sql =  " select a4, sum(wage) " +
                    " from (select wage, sum(id)+1 as a1, sum(id+1) as a2, sum(dept+3)/count(distinct dept) as a4 " +
                    "       from " + tb +
                    "       GROUP BY wage ORDER BY wage ASC LIMIT 4) T1" +
                    " Group by a4 order by a4;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{4, 60}, {10, 40}});

            // groupby from groupby
            sql = "select dept_count, count(*) from (select dept, count(*) as dept_count from R1 group by dept) T1 " +
                    "group by dept_count order by dept_count";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3, 1}, {4, 1}});

            // groupby from groupby + limit
            sql = "select dept_count, count(*) " +
                    "from (select dept, count(*) as dept_count " +
                    "       from (select dept, id from " + tb + " order by dept limit 6) T1 group by dept) T2 " +
                    "group by dept_count order by dept_count";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3, 2}});
        }
    }

    /**
     * Join two sub queries
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubSelects_Joins() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(false);
        VoltTable vt;
        String sql;

        for (String tb: tbs) {
            sql = "select newid, id  " +
                    "FROM (SELECT id, wage FROM R1) T1, (SELECT id as newid, dept FROM "+ tb +" where dept > 1) T2 " +
                    "WHERE T1.id = T2.dept ORDER BY newid";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{4, 2}, {5, 2}});

            sql = "select id, wage, dept_count " +
                    "FROM R1, (select dept, count(*) as dept_count " +
                    "          from (select dept, id " +
                    "                from "+tb+" order by dept limit 5) T1 " +
                    "          group by dept) T2 " +
                    "WHERE R1.wage / T2.dept_count > 10 ORDER BY wage,dept_count";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3, 30, 2}, {4, 40, 2}, {4, 40, 3},{5, 50, 2},{5, 50, 3}});

            sql = "select id, newid  " +
                    "FROM (SELECT id, wage FROM R1) T1 " +
                    "   LEFT OUTER JOIN " +
                    "   (SELECT id as newid, dept FROM "+ tb +" where dept > 1) T2 " +
                    "   ON T1.id = T2.dept " +
                    "ORDER BY id, newid";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, Long.MIN_VALUE}, {2, 4}, {2, 5},
                    {3, Long.MIN_VALUE}, {4, Long.MIN_VALUE}, {5, Long.MIN_VALUE}});
        }

        sql = "select T2.id " +
                "FROM (SELECT id, wage FROM R1) T1, R1 T2 " +
                "ORDER BY T2.id";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {1}, {1}, {1}, {1}, {2}, {2}, {2}, {2}, {2},
                {3}, {3}, {3}, {3}, {3}, {4}, {4}, {4}, {4}, {4}, {5}, {5}, {5}, {5}, {5}});
    }

    public void testSubSelects_from_replicated() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(false);
        VoltTable vt;
        String sql;

        sql = "select P1.ID, P1.WAGE FROM (SELECT ID, DEPT FROM R1) T1, P1 " +
                "where T1.ID = P1.ID and T1.ID < 4 order by P1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1,10}, {2, 20}, {3, 30}});

        sql = "select P1.ID, P1.WAGE FROM (SELECT ID, DEPT FROM R1) T1, P1 " +
                "where T1.ID = P1.ID and T1.ID = 3 order by P1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {3, 30}});

        sql = "select P1.ID, P1.WAGE FROM (SELECT ID, DEPT FROM R1) T1, P1 " +
                "where T1.ID = P1.ID and P1.ID = 3 order by P1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {3, 30}});

        sql = "select T1.ID, P1.WAGE FROM (SELECT ID, DEPT FROM R1) T1, P1 " +
                "where T1.ID = P1.WAGE / 10 order by P1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}});
    }

    public void testENG6276() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt = null;

        String sqlArray =
                "INSERT INTO P4 VALUES (0, 'EPOJbVcUPlDghTEMs', NULL, 2.90574307197424275273e-01);" +
                        "INSERT INTO P4 VALUES (1, 'EPOJbVcUPlDghTEMs', NULL, 6.95147507397556374542e-01);" +
                        "INSERT INTO P4 VALUES (2, 'EPOJbVcUPlDghTEMs', -27645, 9.49225716086843585018e-01);" +
                        "INSERT INTO P4 VALUES (3, 'EPOJbVcUPlDghTEMs', -27645, 3.41233435850314625881e-01);" +
                        "INSERT INTO P4 VALUES (4, 'baYqQXVHBZHVlDRlu', 8130, 7.10103786492815025611e-01);" +
                        "INSERT INTO P4 VALUES (5, 'baYqQXVHBZHVlDRlu', 8130, 7.24543183451542227580e-01);" +
                        "INSERT INTO P4 VALUES (6, 'baYqQXVHBZHVlDRlu', 23815, 4.49837414257097889525e-01);" +
                        "INSERT INTO P4 VALUES (7, 'baYqQXVHBZHVlDRlu', 23815, 4.91748197919483431839e-01);" +

            "INSERT INTO R4 VALUES (0, 'EPOJbVcUPlDghTEMs', NULL, 2.90574307197424275273e-01);" +
            "INSERT INTO R4 VALUES (1, 'EPOJbVcUPlDghTEMs', NULL, 6.95147507397556374542e-01);" +
            "INSERT INTO R4 VALUES (2, 'EPOJbVcUPlDghTEMs', -27645, 9.49225716086843585018e-01);" +
            "INSERT INTO R4 VALUES (3, 'EPOJbVcUPlDghTEMs', -27645, 3.41233435850314625881e-01);" +
            "INSERT INTO R4 VALUES (4, 'baYqQXVHBZHVlDRlu', 8130, 7.10103786492815025611e-01);" +
            "INSERT INTO R4 VALUES (5, 'baYqQXVHBZHVlDRlu', 8130, 7.24543183451542227580e-01);" +
            "INSERT INTO R4 VALUES (6, 'baYqQXVHBZHVlDRlu', 23815, 4.49837414257097889525e-01);" +
            "INSERT INTO R4 VALUES (7, 'baYqQXVHBZHVlDRlu', 23815, 4.91748197919483431839e-01);" ;

        // Test Default
        String []sqls = sqlArray.split(";");
        for (String sql: sqls) {
            sql = sql.trim();
            if (!sql.isEmpty()) {
                vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            }
        }

        String sql =
                "SELECT -8, A.NUM " +
                        "FROM R4 B, (select max(RATIO) RATIO, sum(NUM) NUM, DESC from P4 group by DESC) A " +
                        "WHERE (A.NUM + 5 ) > 44";

        //* enable for debug*/ vt = client.callProcedure("@Explain", sql).getResults()[0];
        //* enable for debug*/ System.err.println(vt);

        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        long[] row = new long[] {-8, 63890};
        validateTableOfLongs(vt, new long[][] {row, row, row, row,
                row, row, row, row});
    }

    /**
     * Simple sub-query expression
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubExpressions_Simple() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(false);
        VoltTable vt;

        for (String tb: replicated_tbs) {
            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" where ID in " +
                    " (select ID from " + tb + " WHERE ID > 3) order by ID;").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {4,2}, {5,2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" where abs(ID) in " +
                    " (select ID from " + tb + " WHERE DEPT = 2 limit 1 offset 1);").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {5,2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" where ID in " +
                    " (select ID from " + tb + " WHERE ID > 2 order by ID limit 3 offset 1) order by ID;").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {4,2}, {5,2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" T1 where abs(ID) in " +
                    " (select ID from " + tb + " WHERE ID > 4) " +
                    "and exists (select 1 from " + tb + " where ID * T1.DEPT  = 10) order by ID;").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {5, 2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" T1 where " +
                    "not exists (select 1 from " + tb + " where ID * T1.DEPT  = 10) " +
                    "and T1.ID < 3 order by ID;").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 1}, {2, 1} });

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" T1 where " +
                    "(abs(ID) + 1 - 1, DEPT) IN (select DEPT, WAGE/10 from " + tb + ") ").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 1}});
        }

        vt = client.callProcedure("@AdHoc",
                "select ID from R1 T1 where exists " +
                        "(SELECT 1 FROM R2 T2 where T1.ID * T2.ID  = ?) "
                        , 9).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{3}});

        // Subquery with a parent parameter TVE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 T1 where exists " +
                "(SELECT 1 FROM R2 T2 where T1.ID * T2.ID  = 9) ").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{3}});

        // Subquery with a grand-parent parameter TVE
        vt = client.callProcedure("@AdHoc",
                "select ID from " + tbs[0] + " T1 where exists " +
                        "(SELECT 1 FROM " + tbs[1] + " T2 where exists " +
                        "(SELECT ID FROM "+ tbs[1] +" T3 WHERE T1.ID * T3.ID  = 9))").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{3}});

        //IN with the select on the left side.
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 T1 where (select ID from R2 T2 where ID = 3) IN " +
                "(SELECT ID FROM R2 T3 where T3.ID  = 3) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1}, {2}, {3}, {4}, {5}});

        // Cardinality error
        try{
            vt = client.callProcedure("@AdHoc",
                    "select ID from R1 T1 where (select ID from R2 T2) IN " +
                            "(SELECT 1 FROM R2 T3 where T1.ID * T3.ID  = ? order by ID limit 1 offset 1) "
                            , 9).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3}});
        } catch (ProcCallException ex) {
            String errMsg = (isHSQL()) ? "cardinality violation" :
                "More than one row returned by a scalar/row subquery";
            assertTrue(ex.getMessage().contains(errMsg));
        }

    }

    /**
     * Join two sub queries
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testExists_Joins() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(false);

        VoltTable vt;

        for (String tb: replicated_tbs) {
            vt = client.callProcedure("@AdHoc",
                    "select T1.id from R1 T1, " + tb +" T2 where " +
                            "T1.id = T2.id and exists ( " +
                    " select 1 from R1 where R1.dept * 2 = T2.dept) order by id").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{4}, {5}});

            vt = client.callProcedure("@AdHoc",
                    "select t1.id, t2.id  from r1 t1, " + tb + " t2 where " +
                            "t1.id IN (select id from r2 where t2.id = r2.id * 2) order by t1.id"
                    ).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,2}, {2,4}});

            // Core dump
            if (!isHSQL()) {
                vt = client.callProcedure("@AdHoc",
                        "select id, newid  " +
                                "FROM (SELECT id, wage FROM R1) T1 " +
                                "   LEFT OUTER JOIN " +
                                "   (SELECT id as newid, dept FROM "+ tb +" where dept > 1) T2 " +
                                "   ON T1.id = T2.dept and EXISTS( " +
                                "      select 1 from R1 where R1.ID =  T2.newid ) " +
                        "ORDER BY id, newid").getResults()[0];
                validateTableOfLongs(vt, new long[][] { {1, Long.MIN_VALUE}, {2, 4}, {2, 5},
                        {3, Long.MIN_VALUE}, {4, Long.MIN_VALUE}, {5, Long.MIN_VALUE}});
            }
        }

    }


    /**
     * SELECT FROM SELECT FROM SELECT
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubExpressions_Aggregations() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(false);
        VoltTable vt;
        String sql;
        ClientResponse cr;

        for (String tb: tbs) {
            cr = client.callProcedure(tb+".insert", 6,  10,  2 , "2013-07-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(tb+".insert", 7,  40,  2 , "2013-07-18 02:00:00.123457");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

        for (String tb: replicated_tbs) {
            vt = client.callProcedure("@AdHoc",
                    "select dept, sum(wage) as sw1 from " + tb +
                    " where (id, dept + 2) in " +
                    "        ( select dept, count(dept) from " + tb +
                    "          group by dept " +
//// Leaving out ORDER BY -- which really should be getting ignored/dropped
//// from an "in expression" subquery but instead was getting serialized with
//// a bad column index.
////                    "          order by dept DESC " +
                    "        ) " +
                    "group by dept;").getResults()[0];
            //* enable for debug */ System.out.println(vt);
            validateTableOfLongs(vt, new long[][] {{1,10}});
            assertFalse(vt.toString().toLowerCase().contains("subquery: null"));

            sql = "select dept from " + tb +
                    " group by dept " +
                    " having max(wage) in (select wage from R1) order by dept desc";
            // Uncomment these tests when ENG-8306 is finished
            //            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            //            /* enable for debug */ System.out.println(vt);
            //            validateTableOfLongs(vt, new long[][] {{2} ,{1}});
            verifyStmtFails(client, sql, TestPlansInExistsSubQueries.HavingErrorMsg);


            sql = "select dept from " + tb + " group by dept " +
                    " having max(wage) + 1 - 1 in (select wage from R1) order by dept desc";
            // Uncomment these tests when ENG-8306 is finished
            //            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            //            validateTableOfLongs(vt, new long[][] {{2}, {1} });
            verifyStmtFails(client, sql, TestPlansInExistsSubQueries.HavingErrorMsg);

            // subquery with having
            sql = "select id from " + tb + " TBA where exists " +
                    " (select dept from R1 " +
                    "  group by dept " +
                    "  having max(wage) = TBA.wage or min(wage) = TBA.wage) order by id;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1}, {3}, {5}, {6}});

            // subquery with having and grand parent parameter TVE
            sql =  "select id from " + tb + " TBA where exists " +
                    " (select 1 from R2 where exists " +
                    "         (select dept from R1 " +
                    "          group by dept " +
                    "          having max(wage) = TBA.wage) ) order by id;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3}, {5}});

            vt = client.callProcedure("@AdHoc",
                    "select id from " + tb + " TBA where exists " +
                            " (select dept from R1 " +
                            "  group by dept " +
                            "  having max(wage) = ?)", 3).getResults()[0];
            validateTableOfLongs(vt, new long[][] {});

            // having with subquery with having
            vt = client.callProcedure("@AdHoc",
                    "select id from " + tb + " where wage " +
                            " in (select max(wage) from R1 " +
                            "     group by dept " +
                            "     having max(wage) > 30) ").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{5}});

            // subquery with group by but no having
            vt = client.callProcedure("@AdHoc",
                    "select id from " + tb + " TBA where exists " +
                    " (select max(dept) from R1 where TBA.id = R1.id group by dept) order by id;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1}, {2}, {3}, {4}, {5}, {6}, {7}});

        }

    }

    /**
     * SELECT FROM SELECT UNION SELECT
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubExpressions_Unions() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(false);
        VoltTable vt;

        for (String tb: replicated_tbs) {
            vt = client.callProcedure("@AdHoc",
                    "select ID from " + tb + " where ID in " +
                            "( (SELECT ID from R1 WHERE ID > 2 LIMIT 3 OFFSET 1) " +
                            " UNION SELECT ID from R2 WHERE ID <= 2"
                            + " INTERSECT SELECT ID from R1 WHERE ID =1) order by ID;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1}, {4}, {5}});

            vt = client.callProcedure("@AdHoc",
                    "select ID from " + tb + " where ID in " +
                            "( SELECT ID from R1 WHERE ID > 2 " +
                            " EXCEPT SELECT ID from R2 WHERE ID <= 2) order by ID;").getResults()[0];
            //* enable for debug */ System.out.println(vt.toString());
            validateTableOfLongs(vt, new long[][] {{3}, {4}, {5}});
        }
    }

    /**
     * SELECT FROM WHERE OUTER OP INNER inner.
     * If there is a match, IN evalueates to TRUE
     * If there is no match, IN evaluates to FASLE if the INNER result set is empty
     * If there is no match, IN evaluates to NULL if the INNER result set is not empty
     *       and there are inner NULLs
     * Need to keep OFFSET for the IN expressions
     * to prevent IN-to-EXISTS optimization
     *
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubExpressions_InnerNull() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt;
        client.callProcedure("R1.insert", 100,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 101,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 100,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103,  1003,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105,  1000,  2 , "2013-07-18 02:00:00.123457");

        // Inner result is NULL. The expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where ((WAGE, DEPT) = " +
                "( select WAGE, DEPT from R2 where ID = 100)) is NULL order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}, {101}});

        // Inner result is empty. The expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where ((WAGE, DEPT) = " +
                "( select WAGE, DEPT from R2 where ID = 1000)) is NULL order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}, {101}});

        // Outer result is NULL. The expression is NULL
        if (!isHSQL()) {
            vt = client.callProcedure("@AdHoc",
                    "select ID from R1 where ((WAGE, DEPT) = " +
                    "( select WAGE, DEPT from R2 where ID = 102)) is NULL;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{101}});
        }

        // Outer result is empty. The expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where ((select WAGE, DEPT from R2 where ID = 1000) = " +
                "( select WAGE, DEPT from R2 where ID = 102)) is NULL order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}, {101}});

        // Outer result is NULL. Inner is empty The expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where  ID =101 and ((WAGE, DEPT) = " +
                "( select WAGE, DEPT from R2 where ID = 1000)) is NULL;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{101}});

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
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testANYSubExpressions_InnerNull() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt;
        client.callProcedure("R1.insert", 100,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 100,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103,  1003,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105,  1000,  2 , "2013-07-18 02:00:00.123457");

        // There is an exact match, IN/ANY extression evaluates to TRUE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) in " +
                "( select WAGE, DEPT from R2 ORDER BY WAGE, DEPT limit 6 offset 1) is true;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) =ANY " +
                "( select WAGE, DEPT from R2 ORDER BY WAGE, DEPT limit 6 offset 1) is true;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});

        // There inner result set is empty, IN/ANY expression evaluates to FALSE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) in " +
                "( select WAGE, DEPT from R2 where ID = 0 ORDER BY WAGE, DEPT limit 6 offset 1) is false;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) =ANY " +
                "( select WAGE, DEPT from R2 where ID = 0 ORDER BY WAGE, DEPT limit 6 offset 1) is false;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});

        // There is no match, IN extression evaluates to NULL (non-empty inner result set)
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) in " +
                "( select WAGE, DEPT from R2 where WAGE != 1000 or WAGE is NULL ORDER BY WAGE, DEPT limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {});
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) = ANY " +
                "( select WAGE, DEPT from R2 where WAGE != 1000 or WAGE is NULL ORDER BY WAGE, DEPT limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {});

        // There is an exact match, NOT IN evaluates to FALSE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) not in " +
                "( select WAGE, DEPT from R2 ORDER BY WAGE, DEPT limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {});

        // There is no match, inner result set is non empty, IN evaluates to NULL, NOT IN is also NULL
        // HSQL gets it wrong
        if (!isHSQL()) {
            vt = client.callProcedure("@AdHoc",
                    "select ID from R1 where (WAGE, DEPT) not in " +
                    "( select WAGE, DEPT from R2 where WAGE != 1000 or WAGE is NULL ORDER BY WAGE, DEPT limit 4 offset 1);").getResults()[0];
            validateTableOfLongs(vt, new long[][] {});
        }

        // There is no match, the inner result set doesn't have NULLs
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where WAGE in " +
                "( select WAGE from R2 where WAGE != 1000 ORDER BY WAGE limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {});
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where WAGE =ANY " +
                "( select WAGE from R2 where WAGE != 1000 ORDER BY WAGE limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {});

        // There is a match, the inner result set doesn't have NULLs, The IN expression evaluates to FALSE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where WAGE in " +
                "( select WAGE from R2 where WAGE != 1000 ORDER BY WAGE limit 6 offset 1) is false;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where WAGE =ANY " +
                "( select WAGE from R2 where WAGE != 1000 ORDER BY WAGE limit 6 offset 1) is false;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});

        // NULL row exists
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where exists " +
                "( select WAGE from R2 where WAGE is NULL);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});

        // Rows exist
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where not exists " +
                "( select WAGE, DEPT from R2 );").getResults()[0];
        validateTableOfLongs(vt, new long[][] {});

        // The NULL from R2 is eliminated by the offset
        // HSQL gets it wrong
        if (!isHSQL()) {
            vt = client.callProcedure("@AdHoc",
                    "select ID from R1 where R1.WAGE NOT IN " +
                    "(select WAGE from R2 where ID < 104 order by WAGE desc limit 1 offset 1);").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{100}});
        }

    }

    /**
     * SELECT FROM WHERE OUTER IN (SELECT INNER ...). The OUTER is NULL.
     * If there is a match, IN evalueates to TRUE
     * If OUTER is NULL and INNER result set is empty, the IN expression evaluates to FASLE
     * If OUTER is NULL and INNER result set is not empty, the IN expression evaluates to NULL
     * Need to keep OFFSET for the IN expressions
     * to prevent IN-to-EXISTS optimization
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testANYSubExpressions_OuterNull() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt;
        client.callProcedure("R1.insert", 100,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 101,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 200,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 201,  2001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 202,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 203,  null,  2 , "2013-07-18 02:00:00.123457");

        // R2.200 - the inner result set is not empty, the IN/ANY  expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE in " +
                "( select WAGE from R1 order by WAGE limit 4 offset 1) is false;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{201}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE =ANY " +
                "( select WAGE from R1 order by WAGE limit 4 offset 1) is false;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{201}});

        // R2.200 - the inner result set is not empty, the IN  expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE in " +
                "( select WAGE from R1 order by WAGE limit 4 offset 1) is true;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{202}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE =ANY " +
                "( select WAGE from R1 order by WAGE limit 4 offset 1) is true;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{202}});

        // R2.200 - the inner result set is not empty, the IN  expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE in " +
                "( select WAGE from R1 order by WAGE limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{202}});

        // R2.200 - the inner result set is not empty, the IN and not IN  expressions are NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE not in " +
                "( select WAGE from R1 order by WAGE limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{201}});

        // R2.200 - the inner result set is empty, the IN expression is TRUE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE in " +
                "( select WAGE from R1 where ID > 1000 order by WAGE limit 4 offset 1) is false order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {202}, {203}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE =ANY " +
                "( select WAGE from R1 where ID > 1000 order by WAGE limit 4 offset 1) is false order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {202}, {203}});

        // R2.202 and R1.101 have the same WAGE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where exists " +
                "( select WAGE from R1 where R1.WAGE = R2.WAGE) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{202}});

        // R2.202 and R1.101 have the same WAGE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where not exists " +
                "( select WAGE from R1 where R1.WAGE = R2.WAGE) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {203}});

        // NULL not equal NULL, R2.200 and R2.203 have NULL WAGE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 RR2 where exists " +
                "( select 1 from R2 where RR2.WAGE = R2.WAGE) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{201}, {202}});

        // NULL not equal NULL, R2.200 and R2.203 have NULL WAGE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 RR2 where RR2.WAGE in " +
                "( select WAGE from R2 order by WAGE limit 4 offset 1) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{201}, {202}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 RR2 where RR2.WAGE = ANY " +
                "( select WAGE from R2 order by WAGE limit 4 offset 1) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{201}, {202}});

        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where (WAGE in " +
                "( select WAGE from R1 order by WAGE limit 4 offset 1)) is null order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {203}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where (WAGE = ANY " +
                "( select WAGE from R1 order by WAGE limit 4 offset 1)) is null order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {203}});

        // The outer expression is empty. The inner expression is not empty. The =ANY is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where ((select WAGE from R1 where ID = 0) = ANY " +
                "( select WAGE from R2 order by WAGE limit 4 offset 1)) is null order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {202}, {203}});

        // The outer expression is empty. The inner expression is empty. The =ANY is FALSE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where not (select WAGE from R1 where ID = 0) = ANY " +
                "( select WAGE from R1 where ID = 0 order by WAGE limit 4 offset 1) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {202}, {203}});
    }

    /**
     * SELECT FROM WHERE OUTER = ALL (SELECT INNER ...) returning inner NULL.
     * If inner_expr is empty => TRUE
     * If inner_expr contains NULL and outer_expr OP inner_expr is TRUE for all other inner values => NULL
     * If inner_expr contains NULL and outer_expr OP inner_expr is FALSE for some other inner values => FALSE
     *
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testALLSubExpressions_InnerNull() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt;
        client.callProcedure("R1.insert", 100,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 100,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103,  1003,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105,  1000,  2 , "2013-07-18 02:00:00.123457");

        // The subquery select WAGE from R1 limit 4 offset 1)) returns the empty set
        // The expression WAGE in ( select WAGE from R1 limit 4 offset 1))
        // Evaluates to FALSE
        //        vt = client.callProcedure("@AdHoc",
        //                "select ID from R2 where (WAGE in " +
        //                "( select WAGE from R1 limit 4 offset 1)) is null").getResults()[0];
        //        System.out.println(vt.toString());
        //        validateTableOfLongs(vt, new long[][] {{200}, {203}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where not (WAGE in " +
                "( select WAGE from R1 ORDER BY WAGE limit 4 offset 1)) order by ID").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}, {101}, {102}, {103}, {104}, {105}});

        // The inner_expr is empty => TRUE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) = ALL " +
                "( select WAGE, DEPT from R2 where ID = 1000);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (select WAGE from R1) = ALL " +
                "( select WAGE from R2 where ID = 1000);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});

        // The inner set consists only of NULLs
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where WAGE = ALL " +
                "( select WAGE from R2 where ID in (104, 105));").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE,DEPT) = ALL " +
                "( select WAGE, DEPT from R2 where ID in (104, 105));").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});

        // If inner_expr contains NULL and outer_expr OP inner_expr is TRUE
        // for all other inner values => NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE = ALL " +
                "( select WAGE from R2 where ID in (100, 104, 105))) is NULL;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});

        if (!isHSQL()) {
            // I think HSQL gets this one wrong evaluating the =ALL to FALSE instead of NULL.
            // PostgreSQL agrees with us
            vt = client.callProcedure("@AdHoc",
                    "select ID from R1 where ((WAGE, DEPT) = ALL " +
                    "( select WAGE, DEPT from R2 where ID in (100, 104, 105))) is NULL;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{100}});
        }

        // If inner_expr contains NULL and outer_expr OP inner_expr is FALSE
        // for some other inner values => FALSE
        if (!isHSQL()) {
            // I think HSQL gets this one wrong evaluating the =ALL to NULL instead of FALSE.
            // PostgreSQL agrees with us
            vt = client.callProcedure("@AdHoc",
                    "select ID from R1 where not (WAGE = ALL ( select WAGE from R2));").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{100}});
        }
    }

    /**
     * SELECT FROM WHERE OUTER = ALL (SELECT INNER ...). The OUTER is NULL.
     * If outer_expr is NULL and inner_expr is empty => TRUE
     * If outer_expr is NULL and inner_expr produces any row => NULL
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testALLSubExpressions_OuterNull() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt;
        client.callProcedure("R1.insert", 100,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 101,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 200,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 201,  2001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 202,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 203,  null,  2 , "2013-07-18 02:00:00.123457");

        // the inner result set is empty, the =ALL  expression is TRUE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE =ALL " +
                "( select WAGE from R1 where ID = 1000) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {202}, {203}});

        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where (ID,WAGE) =ALL " +
                "( select ID,WAGE from R1 where ID = 1000) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {202}, {203}});

        // the inner result set is empty, the =ALL  expression is TRUE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where (select WAGE from R1 where ID = 1000) =ALL " +
                "( select WAGE from R1 where ID = 1000) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {202}, {203}});

        //  the outer_expr is NULL and inner_expr is not empty => NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where ID = 200 and (WAGE =ALL " +
                "( select WAGE from R1)) is  null ;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}});
        if (!isHSQL()) {
            // I think HSQL gets this one wrong evaluating the =ALL to FALSE instead of NULL.
            // PostgreSQL agrees with us
            vt = client.callProcedure("@AdHoc",
                    "select ID from R2 where ID = 200 and ((ID,WAGE) =ALL " +
                    "( select ID, WAGE from R1)) is  null ;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{200}});
        }
    }

    // Test subqueries on partitioned table cases
    public void notestSubSelects_from_partitioned() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(false);
        VoltTable vt;
        String sql;

        sql = "select T1.ID, T1.DEPT FROM (SELECT ID, DEPT FROM P1) T1, P2 " +
                "where T1.ID = P2.DEPT order by T1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1,1}, {1, 1}, {1, 1}, {2, 1}, {2, 1}});

        sql = "select T1.ID, T1.DEPT FROM (SELECT ID, DEPT FROM P1 where ID = 2) T1, P2 " +
                "where T1.ID = P2.DEPT order by T1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {2, 1}, {2, 1}});

        sql = "select T1.ID, T1.DEPT " +
                "FROM (SELECT ID, DEPT FROM P1 where ID = 2) T1, " +
                "       (SELECT DEPT FROM P2 ) T2,  " +
                "       (SELECT ID FROM P3 ) T3  " +
                "where T1.ID = T2.DEPT and T2.DEPT = T3.ID order by T1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {2, 1}, {2, 1}});

        sql = "select T1.ID, T1.DEPT " +
                "FROM (SELECT P1.ID, P1.DEPT FROM P1, P2 where P1.ID = P2.DEPT) T1, P2 " +
                "where T1.ID = P2.DEPT and P2.DEPT = 2 order by T1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {2, 1}, {2, 1}, {2, 1}, {2, 1}});


        // Outer joins
        sql = "select T1.ID, T1.DEPT FROM (SELECT ID, DEPT FROM P1) T1 LEFT OUTER JOIN P2 " +
                "ON T1.ID = P2.DEPT order by T1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1,1}, {1, 1}, {1, 1},
                {2, 1}, {2, 1}, {3, 1}, {4, 2}, {5, 2}});

        sql = "select T1.ID, T1.DEPT FROM (SELECT ID, DEPT FROM P1) T1 LEFT OUTER JOIN P2 " +
                "ON T1.ID = P2.DEPT WHERE T1.ID = 3 order by T1.ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{3, 1}});

        sql = "select T1.ID, T1.DEPT, P2.WAGE FROM (SELECT ID, DEPT FROM P1) T1 LEFT OUTER JOIN P2 " +
                "ON T1.ID = P2.DEPT AND P2.DEPT = 2 order by 1, 2, 3;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1, 1, Long.MIN_VALUE}, {2, 1, 40}, {2, 1, 50},
                {3, 1, Long.MIN_VALUE},{4,2, Long.MIN_VALUE}, {5,2, Long.MIN_VALUE}});

    }

    // Test scalar subqueries
    public void testSelectScalarSubSelects() throws Exception
    {
        Client client = getClient();
        loadData(true);
        VoltTable vt;

        vt = client.callProcedure("@AdHoc",
                "select R1.ID, R1.DEPT, (SELECT ID FROM R2 where ID = 2) FROM R1 where R1.ID < 3 order by R1.ID desc;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {2, 1, 2}, {1, 1, 2} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID, R1.DEPT, (SELECT ID FROM R2 where ID = ?) FROM R1 where R1.ID < 3 order by R1.ID desc;", 2).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {2, 1, 2}, {1, 1, 2} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID, R1.DEPT, (SELECT ID FROM R2 where R2.ID = R1.ID and R2.WAGE = 50) FROM R1 where R1.ID > 3 order by R1.ID desc;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {  {7, 2, Long.MIN_VALUE}, {6, 2, Long.MIN_VALUE}, {5, 2, 5}, {4, 2, Long.MIN_VALUE} });

        // Seq scan
        vt = client.callProcedure("@AdHoc",
                "select R1.DEPT, (SELECT ID FROM R2 where R2.ID = 1) FROM R1 where R1.DEPT = 2;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {  {2, 1}, {2, 1}, {2, 1}, {2, 1} });

        // with group by correlated
        // Hsqldb back end bug: ENG-8273
        if (!isHSQL()) {
            vt = client.callProcedure("@AdHoc",
                    "select R1.DEPT, count(*), (SELECT max(dept) FROM R2 where R2.wage = R1.wage) FROM R1 "
                            + " GROUP BY dept, wage order by dept, wage;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {  {1,1,2}, {1,1,1}, {1,1,1}, {2, 1, 2}, {2, 2, 2}, {2,1,2} });

            vt = client.callProcedure("@AdHoc",
                    "select R1.DEPT, count(*), (SELECT sum(dept) FROM R2 where R2.wage > r1.dept * 10) FROM R1 "
                            + " GROUP BY dept order by dept;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {  {1,3,8}, {2, 4, 7} });
        }

        // ENG-8263: group by scalar value expression
        String sql = "select R1.DEPT, count(*) as tag FROM R1 "
                + "GROUP BY dept, (SELECT count(dept) FROM R2 where R2.wage = R1.wage) order by dept, tag;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] {  {1, 1}, {1, 2}, {2, 1}, {2, 3} });

        vt = client.callProcedure("@AdHoc", "select R1.DEPT, count(*) as tag FROM R1 "
                + "GROUP BY dept, (SELECT count(dept) FROM R2 where R2.wage > 15) order by dept, tag;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {  {1,3}, {2, 4} });

        vt = client.callProcedure("@AdHoc", "select R1.DEPT, abs((SELECT count(dept) FROM R2 where R2.wage > R1.wage) / 2 - 3) as ct, count(*) as tag FROM R1 "
                + "GROUP BY dept, ct order by dept, tag;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1,2,1}, {1,1,2}, {2,1,1}, {2,3,3} });

        // duplicates the subquery expression
        vt = client.callProcedure("@AdHoc", "select R1.DEPT, count(*) as tag FROM R1 "
                + "GROUP BY dept, (SELECT count(dept) FROM R2 where R2.wage > 15), "
                + "(SELECT count(dept) FROM R2 where R2.wage > 15) order by dept, tag;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {  {1,3}, {2, 4} });

        // changes a little bit on the subquery
        vt = client.callProcedure("@AdHoc", "select R1.DEPT, count(*) as tag FROM R1 "
                + "GROUP BY dept, (SELECT count(dept) FROM R2 where R2.wage > 15), "
                + "(SELECT count(dept) FROM R2 where R2.wage > 14) order by dept, tag;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {  {1,3}, {2, 4} });

        // expression with subuqery
        vt = client.callProcedure("@AdHoc", "select R1.DEPT, count(*) as tag FROM R1 "
                + "GROUP BY dept, (SELECT count(dept) FROM R2 where R2.wage > 15), "
                + "(1 + (SELECT count(dept) FROM R2 where R2.wage > 14) ) order by dept, tag;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {  {1,3}, {2, 4} });

        // duplicates the subquery expression
        vt = client.callProcedure("@AdHoc", "select R1.DEPT, "
                + "abs((SELECT count(dept) FROM R2 where R2.wage > R1.wage) / 2 - 3) as ct1, "
                + "abs((SELECT count(dept) FROM R2 where R2.wage > R1.wage) / 2 - 3) as ct2, "
                + "count(*) as tag FROM R1 "
                + "GROUP BY dept, ct1 order by dept, tag;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1,2,2,1}, {1,1,1,2}, {2,1,1,1}, {2,3,3,3} });

        // expression with subuqery
        vt = client.callProcedure("@AdHoc", "select R1.DEPT, "
                + "abs((SELECT count(dept) FROM R2 where R2.wage > R1.wage) / 2 - 3) as ct1, "
                + "(5 + abs((SELECT count(dept) FROM R2 where R2.wage > R1.wage) / 2 - 3)) as ct2, "
                + "count(*) as tag FROM R1 "
                + "GROUP BY dept, ct1 order by dept, tag;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1,2,7,1}, {1,1,6,2}, {2,1,6,1}, {2,3,8,3} });
        try {
            vt = client.callProcedure("@AdHoc",
                    "select R1.ID, R1.DEPT, (SELECT ID FROM R2) FROM R1 where R1.ID > 3 order by R1.ID desc;").getResults()[0];
        } catch (ProcCallException ex) {
            String errMsg = (isHSQL()) ? "cardinality violation" :
                "More than one row returned by a scalar/row subquery";
            assertTrue(ex.getMessage().contains(errMsg));
        }

        // ENG-8145
        subTestScalarSubqueryWithOrderByOrGroupBy();

        //
        // ENG-8159, ENG-8160
        // test Scalar sub-query with non-integer type
        //
        subTestScalarSubqueryWithNonIntegerType();
    }

    private void subTestScalarSubqueryWithOrderByOrGroupBy() throws Exception {
        Client client = getClient();
        int len = 100;

        if (isValgrind()) {
            // valgrind is too slow with 100 rows, use a small number
            len = 10;
        }

        long[][] expected = new long[len][1];
        for (int i = 0; i < len; ++i) {
            client.callProcedure("@AdHoc",  "insert into R_ENG8145_1 values (?, ?);", i, i * 2);
            client.callProcedure("@AdHoc",  "insert into R_ENG8145_2 values (?, ?);", i, i * 2);
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
                "SELECT *, (SELECT SUM(NUM) FROM R_ENG8173_1) FROM R_ENG8173_1 A1 ORDER BY DESC;")
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
                "SELECT  "
                + "(SELECT "
                + "  SUM(NUM) + SUM(ID) "
                + " FROM R_ENG8173_1) "
                + "FROM R_ENG8173_1 A1 ORDER BY DESC;",
                new long[][] {{76}, {76}});

        // Similar queries from ENG-8174
        client.callProcedure("@AdHoc", "truncate table R4");
        client.callProcedure("@AdHoc", "insert into R4 values (0,null,null,null);");
        client.callProcedure("@AdHoc", "insert into R4 values (1,'foo1',-1,1.1);");

        vt = client.callProcedure("@AdHoc", "select NUM V, (select SUM(RATIO) from R4) from R4 order by V;")
                .getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getLong(0); assertTrue(vt.wasNull());
        assertEquals(1.1, vt.getDouble(1));

        assertTrue(vt.advanceRow());
        assertEquals(-1, vt.getLong(0));
        assertEquals(1.1, vt.getDouble(1));
        assertFalse(vt.advanceRow());


        vt = client.callProcedure("@AdHoc", "select RATIO V, (select SUM(NUM) from R4) from R4 order by V;")
                .getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getDouble(0); assertTrue(vt.wasNull());
        assertEquals(-1, vt.getLong(1));

        assertTrue(vt.advanceRow());
        assertEquals(1.1, vt.getDouble(0));
        assertEquals(-1, vt.getLong(1));
        assertFalse(vt.advanceRow());


        vt = client.callProcedure("@AdHoc", "select NUM V, (select MAX(DESC) from R4) from R4 order by V;")
                .getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getLong(0); assertTrue(vt.wasNull());
        assertEquals("foo1", vt.getString(1));

        assertTrue(vt.advanceRow());
        assertEquals(-1, vt.getLong(0));
        assertEquals("foo1", vt.getString(1));
        assertFalse(vt.advanceRow());
    }

    private void subTestScalarSubqueryWithNonIntegerType() throws NoConnectionsException, IOException, ProcCallException {
        Client client = getClient();
        client.callProcedure("@AdHoc", "truncate table R4");

        VoltTable vt;
        String sql;
        client.callProcedure("R4.insert", 1, "foo1", -1, 1.1);
        client.callProcedure("R4.insert", 2, "foo2", -1, 2.2);

        // test FLOAT
        sql = "select ID, (select SUM(RATIO) from R4) from R4 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(2, vt.getRowCount());
        assertTrue(vt.advanceRow());
        assertEquals(1, vt.getLong(0)); assertEquals(3.3, vt.getDouble(1), 0.0001);
        assertTrue(vt.advanceRow());
        assertEquals(2, vt.getLong(0)); assertEquals(3.3, vt.getDouble(1), 0.0001);

        // test VARCHAR
        sql = "select ID, (select MIN(DESC) from R4) from R4 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(2, vt.getRowCount());
        assertTrue(vt.advanceRow());
        assertEquals(1, vt.getLong(0)); assertEquals("foo1", vt.getString(1));
        assertTrue(vt.advanceRow());
        assertEquals(2, vt.getLong(0)); assertEquals("foo1", vt.getString(1));
    }

    public void testWhereScalarSubSelects() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(false);
        VoltTable vt;

        // Index Scan
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.ID = (SELECT ID FROM R2 where ID = ?);", 2).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {2} });

        // Subquery with limit/offset parameter
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.ID > ALL (SELECT ID FROM R2 order by ID limit ? offset ?);", 2, 2).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {5} });

        // Index Scan correlated
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.ID = (SELECT ID/2 FROM R2 where ID = R1.ID * 2) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2} });

        // Seq Scan
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.DEPT = (SELECT DEPT FROM R2 where ID = ?) order by id;", 1).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2}, {3} });

        // Seq Scan correlated
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.DEPT = (SELECT DEPT FROM R2 where ID = R1.ID * 2);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1} });

        // Different comparison operators
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.DEPT > (SELECT DEPT FROM R2 where ID = 3) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {4}, {5} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (SELECT DEPT FROM R2 where ID = 3) != R1.DEPT order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {4}, {5} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.DEPT >= ALL (SELECT DEPT FROM R2) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {4}, {5} });

        // Index scan
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.ID > ALL (SELECT ID FROM R2 WHERE R2.ID < 4) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {4}, {5} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.ID >= ALL (SELECT ID FROM R2 order by ID asc);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {5} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.ID >= ALL (SELECT ID FROM R2 order by ID desc);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {5} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where R1.ID <= ALL (SELECT ID FROM R2 order by ID desc);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1} });

        // NLIJ
        vt = client.callProcedure("@AdHoc",
                "select R1.ID, R2.ID FROM R1, R2 where R1.DEPT = R2.DEPT + (SELECT DEPT FROM R2 where ID = ?) order by R1.ID, R2.ID limit 2;", 1).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {4, 1}, {4, 2} });

        // @TODO NLIJ correlated
        vt = client.callProcedure("@AdHoc",
                "select R2.ID, R2.ID FROM R1, R2 where R2.ID = (SELECT id FROM R2 where ID = R1.ID) order by R1.ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1, 1}, {2,2}, {3,3}, {4,4}, {5,5} });

        // NLJ
        vt = client.callProcedure("@AdHoc",
                "select R1.ID, R2.ID FROM R1, R2 where R1.DEPT = R2.DEPT + (SELECT DEPT FROM R2 where ID = ?) order by R1.ID, R2.ID limit 1;", 1).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {4, 1} });

        // NLJ correlated
        vt = client.callProcedure("@AdHoc",
                "select R1.ID, R2.ID FROM R1, R2 where R2.DEPT = (SELECT DEPT FROM R2 where ID = R1.ID + 4) order by R1.ID, R2.ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1, 4}, {1,5} });

        // Having
        String sql;
        sql = "select max(R1.ID) FROM R1 group by R1.DEPT having count(*) = " +
                "(select R2.ID from R2 where R2.ID = ?);";
        // Uncomment these tests when ENG-8306 is finished
        //        vt = client.callProcedure("@AdHoc", sql, 2).getResults()[0];
        //        validateTableOfLongs(vt, new long[][] { {5} });
        verifyAdHocFails(client, TestPlansInExistsSubQueries.HavingErrorMsg, sql, 2);

        // Having correlated -- parent TVE in the aggregated child expression
        sql = "select max(R1.ID) FROM R1 group by R1.DEPT having count(*) = "
                + "(select R2.ID from R2 where R2.ID = R1.DEPT);";
        // Uncomment these tests when ENG-8306 is finished
        //        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        //        validateTableOfScalarLongs(vt, new long[]{5});
        verifyStmtFails(client, sql, TestPlansInExistsSubQueries.HavingErrorMsg);

        sql = "select DEPT, max(R1.ID) FROM R1 group by R1.DEPT having count(*) = " +
                "(select R2.ID from R2 where R2.ID = R1.DEPT);";
        // Uncomment these tests when ENG-8306 is finished
        //        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        //        validateTableOfLongs(vt, new long[][] { {2,5} });
        verifyStmtFails(client, sql, TestPlansInExistsSubQueries.HavingErrorMsg);

        try {
            vt = client.callProcedure("@AdHoc",
                    "select R1.ID FROM R1 where R1.ID = (SELECT ID FROM R2);").getResults()[0];
            fail();
        } catch (ProcCallException ex) {
            String errMsg = (isHSQL()) ? "cardinality violation" :
                "More than one row returned by a scalar/row subquery";
            assertTrue(ex.getMessage().contains(errMsg));
        }
    }

    public void testWhereRowSubSelects() throws NoConnectionsException, IOException, ProcCallException
    {
        if (isHSQL()) {
            // hsqldb has back end error for these cases
            return;
        }

        Client client = getClient();
        client.callProcedure("R1.insert", 1,   5,  1 , "2013-06-18 02:00:00.123457");
        client.callProcedure("R1.insert", 2,  10,  1 , "2013-07-18 10:40:01.123457");
        client.callProcedure("R1.insert", 3,  10,  2 , "2013-08-18 02:00:00.123457");

        client.callProcedure("R2.insert", 3,   5,  1 , "2013-07-18 10:40:01.123457");
        client.callProcedure("R2.insert", 4,  10,  1 , "2013-08-18 02:00:00.123457");
        client.callProcedure("R2.insert", 5,  10,  1 , "2013-08-18 02:00:00.123457");
        client.callProcedure("R2.insert", 6,  10,  2 , "2013-08-18 02:00:00.123457");
        client.callProcedure("R2.insert", 7,  50,  2 , "2013-09-18 02:00:00.123457");
        VoltTable vt;

        // R1 2,  10,  1 = R2 4,  10,  1
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.WAGE, R1.DEPT) = (SELECT WAGE, DEPT FROM R2 where ID = 4);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {2} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.WAGE, R1.DEPT) != (SELECT WAGE, DEPT FROM R2 where ID = 4) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {3} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.WAGE, R1.DEPT) > (SELECT WAGE, DEPT FROM R2 where ID = 4);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {3} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.WAGE, R1.DEPT) < (SELECT WAGE, DEPT FROM R2 where ID = 4);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.WAGE, R1.DEPT) >= (SELECT WAGE, DEPT FROM R2 where ID = 4) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {2}, {3} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.WAGE, R1.DEPT) <= (SELECT WAGE, DEPT FROM R2 where ID = 4) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2} });

        // R1 2,  10,  1 = R2 4,  10,  1 and 5,  10,  1
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.WAGE, R1.DEPT) =ALL (SELECT WAGE, DEPT FROM R2 where ID in (4,5));").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {2} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.WAGE, R1.DEPT) =ALL (SELECT WAGE, DEPT FROM R2);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { });

        // R1 3,  10,  2 >= ALL R2 except R2.7
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where ID = 3 and (R1.WAGE, R1.DEPT) >= ALL (SELECT WAGE, DEPT FROM R2 where ID < 7 ORDER BY WAGE, DEPT DESC);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {3} });

        // R1 3,  10,  2 < R2 except R2.7 50 2
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.WAGE, R1.DEPT) >= ALL (SELECT WAGE, DEPT FROM R2);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.DEPT, R1.TM) < ALL (SELECT DEPT, TM FROM R2);").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.DEPT, R1.TM) <= ALL (SELECT DEPT, TM FROM R2) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.DEPT, R1.TM) <= ALL (SELECT DEPT, TM FROM R2 ORDER BY DEPT, TM ASC) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2} });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where (R1.DEPT, R1.TM) <= ALL (SELECT DEPT, TM FROM R2 ORDER BY DEPT, TM DESC) order by ID;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2} });

    }

    public void testRepeatedQueriesDifferentData() throws Exception
    {
        Client client = getClient();
        client.callProcedure("R1.insert", 1,   5,  1 , "2013-06-18 02:00:00.123457");
        client.callProcedure("R1.insert", 2,  10,  1 , "2013-07-18 10:40:01.123457");
        client.callProcedure("R1.insert", 3,  15,  2 , "2013-08-18 02:00:00.123457");

        client.callProcedure("R2.insert", 1,  5,  1 , "2013-08-18 02:00:00.123457");

        validateTableOfScalarLongs(client, "select (select max(wage) from r1) from r2;",
                new long[] {15});

        client.callProcedure("@AdHoc", "update r1 set wage = 35 where id = 2");

        // Make sure that the second query reflects the current data.
        validateTableOfScalarLongs(client, "select (select max(wage) from r1) from r2;",
                new long[] {35});
    }

    public void testSubqueryWithExceptions() throws Exception
    {
        Client client = getClient();
        client.callProcedure("R1.insert", 1,   5,  1 , "2013-06-18 02:00:00.123457");
        client.callProcedure("R1.insert", 2,  10,  1 , "2013-07-18 10:40:01.123457");
        client.callProcedure("R1.insert", 3,  15,  2 , "2013-08-18 02:00:00.123457");
        client.callProcedure("R1.insert", 4,  0,  2 , "2013-08-18 02:00:00.123457");

        // A divide by zero execption in the top-level query!
        // Debug assertions in the EE will make this test fail
        // if we don't to clean up temp tables for both inner and outer queries.
        String expectedMsg = isHSQL() ? "division by zero" : "Attempted to divide 30 by 0";
        verifyStmtFails(client, "select (select max(30 / wage) from r1 where wage != 0) from r1 where id = 30 / wage;", expectedMsg);
        verifyStmtFails(client, "select (select max(30 / wage) from r1 where wage != 0) from r1 where id = 30 / wage;", expectedMsg);

        // As above, but this time the execption occurs in the inner query.
        verifyStmtFails(client, "select (select max(30 / wage) from r1) from r1;", expectedMsg);
        verifyStmtFails(client, "select (select max(30 / wage) from r1) from r1;", expectedMsg);
    }

    public void testSubqueriesWithArithmetic() throws Exception {
        Client client = getClient();

        client.callProcedure("R1.insert", 1, 300,  1 , "2013-06-18 02:00:00.123457");
        client.callProcedure("R1.insert", 2, 200,  1 , "2013-06-18 02:00:00.123457");

        // These test cases exercise the fix for ENG-8226, in which a missing ScalarValueExpression
        // caused the result of a subquery to be seen as the subquery ID, rather than the contents
        // of subquery's result table.

        validateTableOfScalarLongs(client, "select (select max(wage) from r1) from r1",
                new long[] {300, 300});
        validateTableOfScalarLongs(client, "select (select max(wage) from r1) + 0 as subq from r1",
                new long[] {300, 300});

        validateTableOfScalarLongs(client, "select wage from r1 where wage = (select max(wage) from r1)", new long[] {300});
        validateTableOfScalarLongs(client, "select wage from r1 where wage = (select max(wage) - 30 from r1) + 30", new long[] {300});

        // The IN operator takes a VectorExpression on its RHS, which uses the "args" field.
        // Make sure that we can handle subqueries in there too.
        validateTableOfScalarLongs(client,
                "select wage from r1 "
                        + "where wage in (7, 8, (select max(wage) from r1), 9, 10, 200) "
                        + "order by wage",
                        new long[] {200, 300});
    }

    public void testExistsSimplification() throws NoConnectionsException, IOException, ProcCallException
    {

        Client client = getClient();
        client.callProcedure("R1.insert", 1,   5,  1 , "2013-06-18 02:00:00.123457");
        client.callProcedure("R1.insert", 2,  10,  1 , "2013-07-18 10:40:01.123457");
        client.callProcedure("R1.insert", 3,  10,  2 , "2013-08-18 02:00:00.123457");

        VoltTable vt;

        // EXISTS(table-agg-without-having-groupby) => EXISTS(TRUE)
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where exists( select max(ID) from R2 order by ID;)").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2}, {3} });

        // EXISTS(SELECT...LIMIT 0) => EXISTS(FALSE)
        if (!isHSQL()) {
            vt = client.callProcedure("@AdHoc",
                    "select R1.ID FROM R1 where exists( select max(id) from R2 limit 0)").getResults()[0];
            validateTableOfLongs(vt, new long[][] { });

            // count(*) limit 0
            vt = client.callProcedure("@AdHoc",
                    "select R1.ID FROM R1 where exists( select count(*) from R2 limit 0)").getResults()[0];
            validateTableOfLongs(vt, new long[][] { });

            // EXISTS(SELECT...LIMIT ?) => EXISTS(TRUE/FALSE)
            vt = client.callProcedure("@AdHoc",
                    "select R1.ID FROM R1 where exists( select count(id) from R2 limit ?)", 0).getResults()[0];
            validateTableOfLongs(vt, new long[][] { });
        }

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where exists( select count(*) from R2 limit ?) order by id;", 1).getResults()[0];
        validateTableOfLongs(vt, new long[][] {  {1}, {2}, {3} });

        // EXISTS(able-agg-without-having-groupby OFFSET 1) => EXISTS(FALSE)
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where exists( select max(ID) from R2 offset 1;)").getResults()[0];
        validateTableOfLongs(vt, new long[][] {  });

        // count(*) offset 1
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where exists( select count(*) from R2 offset 1)").getResults()[0];
        validateTableOfLongs(vt, new long[][] {  });

        // join on EXISTS(FALSE)
        vt = client.callProcedure("@AdHoc",
                "select T1.ID FROM R1 T1 join R1 T2 on exists(select max(ID) from R2 offset 1) and T1.ID = 1").getResults()[0];
        validateTableOfLongs(vt, new long[][] { });

        // join on EXISTS(TRUE)
        vt = client.callProcedure("@AdHoc",
                "select T1.ID FROM R1 T1 join R1 T2 on exists(select max(ID) from R2) or T1.ID = 25").getResults()[0];
        assertEquals(9, vt.getRowCount());

        // having TRUE
        vt = client.callProcedure("@AdHoc",
                "select max(ID), WAGE  FROM R1 group by WAGE having exists(select max(ID) from R2) or max(ID) = 25 order by max(ID) asc").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {3} });

        // having FALSE
        vt = client.callProcedure("@AdHoc",
                "select max(ID), WAGE  FROM R1 group by WAGE having exists(select max(ID) from R2 offset 1) and max(ID) > 0 order by max(ID) asc").getResults()[0];
        validateTableOfLongs(vt, new long[][] { });

        client.callProcedure("R2.insert", 1,   5,  1 , "2013-06-18 02:00:00.123457");
        client.callProcedure("R2.insert", 2,  10,  1 , "2013-07-18 10:40:01.123457");
        client.callProcedure("R2.insert", 3,  10,  2 , "2013-08-18 02:00:00.123457");

        // EXISTS(SELECT ... OFFSET ?)
        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where exists( select ID from R2 offset ?)", 4).getResults()[0];
        validateTableOfLongs(vt, new long[][] {  });

        vt = client.callProcedure("@AdHoc",
                "select R1.ID FROM R1 where exists( select ID from R2 offset ?) order by id;", 1).getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2}, {3} });

        // Subquery subquery-without-having with group by and no limit => select .. from r2 limit 1
        vt = client.callProcedure("@AdHoc",
                "select R1.ID from R1 where exists (select WAGE from R2 group by WAGE ) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2}, {3} });

        // Subquery subquery-without-having with group by and offset => select .. from r2 group by offset
        vt = client.callProcedure("@AdHoc",
                "select R1.ID from R1 where exists (select WAGE from R2 group by WAGE offset 2)").getResults()[0];
        validateTableOfLongs(vt, new long[][] {  });

        // Subquery subquery-without-having with group by => select .. from r2 limit 1
        vt = client.callProcedure("@AdHoc",
                "select R1.ID from R1 where exists (select ID, MAX(WAGE) from R2 group by ID) order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2}, {3} });

        // Subquery subquery-with-having with group by => select .. from r2 group by having limit 1
        vt = client.callProcedure("@AdHoc",
                "select R1.ID from R1 where exists (select ID, MAX(WAGE) from R2 group by ID having MAX(WAGE) > 20)").getResults()[0];
        validateTableOfLongs(vt, new long[][] { });

        // Subquery subquery-with-having with group by => select .. from r2 group by having limit 1
        vt = client.callProcedure("@AdHoc",
                "select R1.ID from R1 where exists (select ID, MAX(WAGE) from R2 group by ID having MAX(WAGE) > 9) "
                + "order by id;").getResults()[0];
        validateTableOfLongs(vt, new long[][] { {1}, {2}, {3} });

        // Subquery subquery-with-having with group by offset => select .. from r2 group by having limit 1 offset
        vt = client.callProcedure("@AdHoc",
                "select R1.ID from R1 where exists (select ID, MAX(WAGE) from R2 group by ID having MAX(WAGE) > 9 offset 2)").getResults()[0];
        validateTableOfLongs(vt, new long[][] { });

    }

    static public junit.framework.Test suite()
    {
        VoltServerConfig config = null;
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
                "PARTITION TABLE P3 ON COLUMN ID;"

                +

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

                "CREATE TABLE R_ENG8145_1 (" +
                "ID integer, NUM integer);" +

                "CREATE TABLE R_ENG8145_2 (" +
                "ID integer, NUM integer);" +

                "CREATE TABLE R_ENG8173_1 (" +
                "ID integer, DESC VARCHAR(300), NUM integer);"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        boolean success;

        config = new LocalCluster("subselect-onesite.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        config = new LocalCluster("subselect-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("subselect-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }

}
