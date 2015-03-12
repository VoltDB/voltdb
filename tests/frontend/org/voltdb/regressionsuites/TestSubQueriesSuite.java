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

public class TestSubQueriesSuite extends RegressionSuite {
    public TestSubQueriesSuite(String name) {
        super(name);
    }

    private static final String [] tbs =  {"R1","R2","P1","P2","P3"};
    private static final String [] replicated_tbs =  {"R1","R2"};
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
            cr = client.callProcedure(proc, 2,  20,  1 , "2013-07-18 02:00:00.123457");
            cr = client.callProcedure(proc, 3,  30,  1 , "2013-07-18 10:40:01.123457");
            cr = client.callProcedure(proc, 4,  40,  2 , "2013-08-18 02:00:00.123457");
            cr = client.callProcedure(proc, 5,  50,  2 , "2013-09-18 02:00:00.123457");

            if (extra) {
                client.callProcedure(proc, 6,  10,  2 , "2013-07-18 02:00:00.123457");
                client.callProcedure(proc, 7,  40,  2 , "2013-09-18 02:00:00.123457");
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
                    " (select ID from " + tb + " WHERE ID > 3);").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {4,2}, {5,2}});


            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" where ID in " +
                    " (select ID from " + tb + " WHERE DEPT = 2 limit 1 offset 1);").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {5,2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" where ID in " +
                    " (select ID from " + tb + " WHERE ID > 2 limit 3 offset 1);").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {4,2}, {5,2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" T1 where ID in " +
                    " (select ID from " + tb + " WHERE ID > 4) " +
                    "and exists (select 1 from " + tb + " where ID * T1.DEPT  = 10);").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {5, 2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" T1 where " +
                    "not exists (select 1 from " + tb + " where ID * T1.DEPT  = 10) " +
                    "and T1.ID < 3 order by ID;").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 1}, {2, 1} });

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" T1 where " +
                    "(ID + 1 - 1, DEPT) IN (select DEPT, WAGE/10 from " + tb + ") ").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 1}});

        }

        // Subquery with a user parameter - HSQL fails to parse
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
                    " select 1 from R1 where R1.dept * 2 = T2.dept)").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{4}, {5}});

            vt = client.callProcedure("@AdHoc",
                    "select t1.id, t2.id  from r1 t1, " + tb + " t2 where " +
                            "t1.id IN (select id from r2 where t2.id = r2.id * 2)"
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

        for (String tb: tbs) {
            client.callProcedure(tb+".insert", 6,  10,  2 , "2013-07-18 02:00:00.123457");
            client.callProcedure(tb+".insert", 7,  40,  2 , "2013-07-18 02:00:00.123457");
        }

        for (String tb: replicated_tbs) {
            vt = client.callProcedure("@AdHoc",
                    "select dept, sum(wage) as sw1 from " + tb + " where (id, dept + 2) in " +
                            "( SELECT dept, count(dept) " +
                            "from " + tb + " GROUP BY dept ORDER BY dept DESC) GROUP BY dept;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,10}});


            sql = "select dept from " + tb + " group by dept " +
                    " having max(wage) in (select wage from R1) order by dept desc";
            vt = client.callProcedure("@Explain", sql).getResults()[0];
            assertFalse(vt.toString().toLowerCase().contains("subquery: null"));

          sql = "select dept from " + tb + " group by dept " +
                  " having max(wage) in (select wage from R1) order by dept desc";
          vt = client.callProcedure("@AdHoc", sql).getResults()[0];
          System.out.println(vt.toString());
          validateTableOfLongs(vt, new long[][] {{2} ,{1}});

            // having with subquery
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{2}, {1}});

            // subquery with having
            vt = client.callProcedure("@AdHoc",
                    "select id from " + tb + " TBA where exists " +
                            " (select dept from R1  group by dept having max(wage) = TBA.wage or " +
                    " min(wage) = TBA.wage)").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1}, {3}, {5}, {6}});

            // subquery with having and grand parent parameter TVE
            vt = client.callProcedure("@AdHoc",
                    "select id from " + tb + " TBA where exists " +
                            " (select 1 from R2 where exists " +
                            " (select dept from R1  group by dept having max(wage) = TBA.wage))").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3}, {5}});

          vt = client.callProcedure("@AdHoc",
                  "select id from " + tb + " TBA where exists " +
                          " (select dept from R1  group by dept having max(wage) = ?)", 3).getResults()[0];
          validateTableOfLongs(vt, new long[][] {});

            // having with subquery with having
            vt = client.callProcedure("@AdHoc",
                    "select id from " + tb + " where wage " +
                            " in (select max(wage) from R1 group by dept " +
                    " having max(wage) > 30) ").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{5}});

            // subquery with group by but no having
            vt = client.callProcedure("@AdHoc",
                    "select id from " + tb + " TBA where exists " +
                    " (select max(dept) from R1 where TBA.id = R1.id group by dept )").getResults()[0];
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
                            + " INTERSECT SELECT ID from R1 WHERE ID =1);").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1}, {4}, {5}});
        }
    }

    /**
     * SELECT FROM WHERE OUTER IN (SELECT INNER ...) returning inner NULL.
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
        client.callProcedure("R2.insert", 100,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 101,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 102,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 103,  1003,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 104,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 105,  1000,  2 , "2013-07-18 02:00:00.123457");

        // There is an exact match, IN extression evaluates to TRUE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) in " +
                "( select WAGE, DEPT from R2 limit 6 offset 1) is true;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});

        // There is no match and inner result set is empty, , IN extression evaluates to FALSE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) in " +
                "( select WAGE, DEPT from R2 where ID = 0 limit 6 offset 1) is false;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{100}});

        // There is no match, IN extression evaluates to NULL (non-empty inner result set)
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) in " +
                "( select WAGE, DEPT from R2 where WAGE != 1000 or WAGE is NULL limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {});

        // There is an exact match, NOT IN evaluates to FALSE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where (WAGE, DEPT) not in " +
                "( select WAGE, DEPT from R2 limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {});

        // There is no match, inner result set is non empty, IN evaluates to NULL, NOT IN is also NULL
        // HSQL gets it wrong
        if (!isHSQL()) {
            vt = client.callProcedure("@AdHoc",
                    "select ID from R1 where (WAGE, DEPT) not in " +
                    "( select WAGE, DEPT from R2 where WAGE != 1000 or WAGE is NULL limit 4 offset 1);").getResults()[0];
            validateTableOfLongs(vt, new long[][] {});
        }

        // There is no match, the inner result set doesn't have NULLs
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where WAGE in " +
                "( select WAGE from R2 where WAGE != 1000 limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {});

        // There is a match, the inner result set doesn't have NULLs, The IN expression evaluates to FALSE
        vt = client.callProcedure("@AdHoc",
                "select ID from R1 where WAGE in " +
                "( select WAGE from R2 where WAGE != 1000 limit 6 offset 1) is false;").getResults()[0];
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
     * If OUTRE is NULL and INNER result set is empty, the IN expression evaluates to FASLE
     * If OUTRE is NULL and INNER result set is not empty, the IN expression evaluates to NULL
     * Need to keep OFFSET for the IN expressions
     * to prevent IN-to-EXISTS optimization
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubExpressions_OuterNull() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt;
        client.callProcedure("R1.insert", 100,  1000,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R1.insert", 101,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 200,  null,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 201,  2001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 202,  1001,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure("R2.insert", 203,  null,  2 , "2013-07-18 02:00:00.123457");

        // R2.200 - the inner result set is not empty, the IN  expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE in " +
                "( select WAGE from R1 limit 4 offset 1) is false;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{201}});

        // R2.200 - the inner result set is not empty, the IN  expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE in " +
                "( select WAGE from R1 limit 4 offset 1) is true;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{202}});

        // R2.200 - the inner result set is not empty, the IN  expression is NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE in " +
                "( select WAGE from R1 limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{202}});

        // R2.200 - the inner result set is not empty, the IN and not IN  expressions are NULL
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE not in " +
                "( select WAGE from R1 limit 4 offset 1);").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{201}});

        // R2.200 - the inner result set is empty, the IN expression is TRUE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where WAGE in " +
                "( select WAGE from R1 where ID > 1000 limit 4 offset 1) is false;").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {202}, {203}});

        // R2.202 and R1.101 have the same WAGE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where exists " +
                "( select WAGE from R1 where R1.WAGE = R2.WAGE);").getResults()[0];
        System.out.println(vt.toString());
        validateTableOfLongs(vt, new long[][] {{202}});

        // R2.202 and R1.101 have the same WAGE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 where not exists " +
                "( select WAGE from R1 where R1.WAGE = R2.WAGE);").getResults()[0];
        System.out.println(vt.toString());
        validateTableOfLongs(vt, new long[][] {{200}, {201}, {203}});

        // NULL not equal NULL, R2.200 and R2.203 have NULL WAGE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 RR2 where exists " +
                "( select 1 from R2 where RR2.WAGE = R2.WAGE);").getResults()[0];
        System.out.println(vt.toString());
        validateTableOfLongs(vt, new long[][] {{201}, {202}});

        // NULL not equal NULL, R2.200 and R2.203 have NULL WAGE
        vt = client.callProcedure("@AdHoc",
                "select ID from R2 RR2 where RR2.WAGE in " +
                "( select WAGE from R2 limit 4 offset 1);").getResults()[0];
        System.out.println(vt.toString());
        validateTableOfLongs(vt, new long[][] {{201}, {202}});

         vt = client.callProcedure("@AdHoc",
                 "select ID from R2 where (WAGE in " +
                 "( select WAGE from R1 limit 4 offset 1)) is null;").getResults()[0];
         System.out.println(vt.toString());
         validateTableOfLongs(vt, new long[][] {{200}, {203}});

    }

    // Test subqueries on partitioned table cases
    public void testSubSelects_from_partitioned() throws NoConnectionsException, IOException, ProcCallException
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

                ""
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
