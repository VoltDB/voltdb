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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

/*
 * Functional tests of the statements compiled in the test suite
 * org.voltdb.planner.TestComplexGroupBySuite.
 */

public class TestGroupByComplexSuite extends RegressionSuite {

    private final static String [] tbs = {"R1","P1","P2","P3"};

    private void loadData(boolean extra) throws IOException, ProcCallException {
        Client client = this.getClient();
        ClientResponse cr;

        // Empty data from table.
        for (String tb: tbs) {
            cr = client.callProcedure("@AdHoc", "delete from " + tb);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

        // Insert records into the table.
        // id, wage, dept, rate
        for (String tb: tbs) {
            String proc = tb + ".insert";
            client.callProcedure(proc, 1,  10,  1 , "2013-06-18 02:00:00.123457");
            client.callProcedure(proc, 2,  20,  1 , "2013-07-18 02:00:00.123457");
            client.callProcedure(proc, 3,  30,  1 , "2013-07-18 10:40:01.123457");
            client.callProcedure(proc, 4,  40,  2 , "2013-08-18 02:00:00.123457");
            client.callProcedure(proc, 5,  50,  2 , "2013-09-18 02:00:00.123457");

            if (extra) {
                client.callProcedure(proc, 6,  10,  2 , "2013-07-18 02:00:00.123457");
                client.callProcedure(proc, 7,  40,  2 , "2013-09-18 02:00:00.123457");
            }
        }
    }

    @Test
    public void testStrangeCasesAndOrderby() throws IOException, ProcCallException {
        loadData(false);

        final Client client = this.getClient();
        ClientResponse cr;
        // TODO: ENG-15256
        if (! USING_CALCITE) {
            for (String tb : tbs) {
                // Test group by PRIMARY KEY
                // Test pass-through columns, group by primary key
                cr = client.callProcedure("@AdHoc", "SELECT dept, count(wage) from " +
                        tb + " GROUP BY id ORDER BY dept DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                validateTableOfLongs(cr.getResults()[0], new long[][]{{2, 1}, {2, 1}, {1, 1}, {1, 1}, {1, 1}});

                // Test duplicates, operator expression, group by primary key
                cr = client.callProcedure("@AdHoc", "SELECT id, id, dept, dept+5 from " +
                        tb + " GROUP BY id ORDER BY id");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                validateTableOfLongs(cr.getResults()[0],
                        new long[][]{{1, 1, 1, 6}, {2, 2, 1, 6}, {3, 3, 1, 6}, {4, 4, 2, 7}, {5, 5, 2, 7}});

                // Test function expression with group by primary key
                cr = client.callProcedure("@AdHoc", "SELECT id, id + 1, sum(wage)/2, abs(dept-3) from " +
                        tb + " GROUP BY id ORDER BY id");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                validateTableOfLongs(cr.getResults()[0],
                        new long[][]{{1, 2, 5, 2}, {2, 3, 10, 2}, {3, 4, 15, 2}, {4, 5, 20, 1}, {5, 6, 25, 1}});

                // Test order by alias from display list
                cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) - 1 from " +
                        tb + " GROUP BY dept ORDER BY tag DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                validateTableOfLongs(cr.getResults()[0], new long[][]{{1, 3, 59}, {2, 2, 89}});
            }
        }
    }

    @Test
    public void testComplexAggs() throws IOException, ProcCallException {
        if (isHSQL()) {
            return;
        }
        loadData(false);
        Client client = getClient();
        ClientResponse cr;

        for (String tb: tbs) {
            // Test normal group by with expressions, addition, division for avg.
            cr = client.callProcedure("@AdHoc",
                    "SELECT dept, sum(wage), count(wage)+5, sum(wage)/count(wage) FROM " +
                            tb + " GROUP BY dept ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] {{2, 90, 7, 45}, {1, 60, 8, 20}});

            // Test different group by column order, non-grouped TVE, sum for column, division
            cr = client.callProcedure("@AdHoc",
                    "SELECT sum(wage)/count(wage) + 1, dept, SUM(wage+1), SUM(wage)/2 FROM " +
                            tb + " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] {{21 ,1, 63, 30}, {46, 2, 92, 45}});

            // Test Complex Agg with functions
            cr = client.callProcedure("@AdHoc",
                    "SELECT dept, SUM(ABS(wage) - 1) as tag, (count(*)+sum(dept*2))/2 FROM " +
                            tb + " GROUP BY dept ORDER BY ABS(dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] {{1, 57, 4}, {2, 88, 5}});

            // Test sum()/count(), Addition
            cr = client.callProcedure("@AdHoc",
                    "SELECT dept, SUM(wage), COUNT(wage), AVG(wage), MAX(wage), MIN(wage), " +
                            "SUM(wage)/COUNT(wage), MAX(wage)+MIN(wage)+1 FROM " + tb + " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] {
                            {1, 60, 3, 20, 30, 10, 20, 41},
                            {2, 90, 2, 45, 50, 40, 45, 91}
                    });
        }
    }

    @Test
    public void testComplexAggsOrderbySuite() throws IOException, ProcCallException {
        loadData(false);

        final Client client = this.getClient();
        ClientResponse cr;
        for (String tb: tbs) {
            // (0) Test no group by cases
            cr = client.callProcedure("@AdHoc", "SELECT id+dept from " + tb + " ORDER BY ABS(id+dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2}, {3}, {4}, {6}, {7} });

            cr = client.callProcedure("@AdHoc", "SELECT id+dept from " + tb + " ORDER BY id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2}, {3}, {4}, {6}, {7} });

            cr = client.callProcedure("@AdHoc", "SELECT id+dept from " + tb + " ORDER BY id+dept, wage");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2}, {3}, {4}, {6}, {7} });

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, wage from " + tb + " ORDER BY id+dept, wage");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2, 10}, {3,20}, {4,30}, {6,40}, {7,50} });

            // (1) Test Order by COUNT(*) without complex expression
            // Test order by agg with tag
            cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) from " +
                    tb + " GROUP BY dept ORDER BY tag DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3, 60} , {2, 2, 90}});

            // (2) Test Order by COUNT(*) with complex expression
            // Test order by agg with tag
            cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) - 1 from " +
                    tb + " GROUP BY dept ORDER BY tag DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3, 59} , {2, 2, 89}});


            // (3) Test Order by with FUNCTION expression, no group by column in display columns
            // Test Order by with unambiguous alias.
            cr = client.callProcedure("@AdHoc", "SELECT ABS(dept) as tag1, SUM(ABS(wage) - 1) as tag2, " +
                    "(count(*)+sum(dept*2))/2 from " + tb + " GROUP BY dept ORDER BY tag1");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 57, 4} , {2, 88, 5}});

            // Test Order by without any alias.
            cr = client.callProcedure("@AdHoc", "SELECT ABS(dept), SUM(ABS(wage) - 1) as tag, " +
                    "(count(*)+sum(dept*2))/2 from " + tb + " GROUP BY dept ORDER BY ABS(dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 57, 4} , {2, 88, 5}});
        }
    }

    @Test
    public void testComplexAggsDistinctLimit() throws IOException, ProcCallException {
        loadData(true);

        final Client client = this.getClient();
        ClientResponse cr;

        for (String tb: tbs) {
            // Test distinct with complex aggregations.
            cr = client.callProcedure("@AdHoc", "SELECT dept, count(wage), sum(distinct wage), sum(wage), " +
                    "count(distinct wage)+5, sum(wage)/(count(wage)+1) from " + tb + " GROUP BY dept ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] {{2, 4, 100, 140, 8, 28}, {1, 3, 60, 60, 8, 15} });

            // Test limit with complex aggregation.
            cr = client.callProcedure("@AdHoc",
                    "SELECT wage, sum(id)+1, sum(id+1),  sum(dept+3)/count(dept) from " + tb +
                    " GROUP BY wage ORDER BY wage ASC LIMIT 4 ;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] {{10, 8, 9, 4}, {20, 3, 3, 4}, {30, 4, 4, 4}, {40, 12, 13, 5}});

            // Test distinct limit together with complex aggregation.
            cr = client.callProcedure("@AdHoc", "SELECT wage, sum(id)+1, sum(id+1),  sum(dept+3)/count(distinct dept) from " + tb +
                    " GROUP BY wage ORDER BY wage ASC LIMIT 4 ;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] {{10, 8, 9, 4}, {20, 3, 3, 4}, {30, 4, 4, 4}, {40, 12, 13, 10}});
        }
    }

    @Test
    public void testComplexGroupby() throws IOException, ProcCallException, ParseException{
        loadData(true);

        final Client client = this.getClient();
        ClientResponse cr;
        for (String tb: tbs) {
            // (1) Without extra aggregation expression
            // Test complex group-by (Function expression) without complex aggregation.
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept) as tag, count(wage) from " + tb +
                    " GROUP BY abs(dept) ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3}, {2, 4} });

            // repeat above test with GROUP BY ALIAS feature
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept) as tag, count(wage) from " + tb +
                    " GROUP BY tag ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3}, {2, 4} });


            // Test complex group-by (normal expression) without complex aggregation.
            // Actually this AdHoc query has an extra projection node because of the pass-by column dept in order by columns.
            // ParameterValueExpression equal function return false. AggResultColumns contains: dept+1, count(wage) and dept.
            // If it is a stored procedure, there is no extra projection node. AggResultColumns: dept+1 and count(wage).
            cr = client.callProcedure("@AdHoc", "SELECT (dept+1) as tag, count(wage) from " + tb +
                    " GROUP BY dept+1 ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2, 3}, {3, 4} });

            // repeat above test with GROUP BY ALIAS feature
            cr = client.callProcedure("@AdHoc",
                    "SELECT (dept+1) as tag, count(wage) from " + tb + " GROUP BY tag ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2, 3}, {3, 4} });

            // test group by alias with constants in expression for stored procedure
            cr = client.callProcedure(tb +"_GroupbyAlias1", 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2, 3}, {3, 4} });

            // (2) With extra aggregation expression
            // Test complex group-by with with complex aggregation.
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept) as tag, count(wage)+1 from " + tb +
                    " GROUP BY abs(dept) ORDER BY tag DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2, 5}, {1, 4} });

            // repeat above test with GROUP BY ALIAS feature
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept) as tag, count(wage)+1 from " + tb +
                    " GROUP BY tag ORDER BY tag DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2, 5}, {1, 4} });


            // Test more complex group-by with with complex aggregation.
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, count(wage)+1 from " + tb +
                    " GROUP BY abs(dept-2) ORDER BY tag;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {0, 5}, {1, 4} });

            // repeat above test with GROUP BY ALIAS feature
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, count(wage)+1 from " + tb +
                    " GROUP BY tag ORDER BY tag;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {0, 5}, {1, 4} });

            // test group by alias with constants in expression for stored procedure
            cr = client.callProcedure(tb +"_GroupbyAlias2", -2);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {0, 5}, {1, 4} });


            // More hard general test case with multi group by columns and complex aggs
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(*)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {0,10,5,2,7}, {0,40,20,4,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3}, {1,30,15,2,4} });

            // repeat above test with GROUP BY ALIAS feature
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(*)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY tag, wage ORDER BY tag, wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {0,10,5,2,7}, {0,40,20,4,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3}, {1,30,15,2,4} });


            if (!isHSQL()) {
                // Timestamp function for complex group by
                cr = client.callProcedure("@AdHoc", "SELECT truncate(day, tm) as tag, count(id)+1, " +
                        "sum(wage)/count(wage) from " + tb + " GROUP BY truncate(day, tm) ORDER BY tag;");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                Date time1 = dateFormat.parse("2013-06-18 00:00:00.000");
                Date time2 = dateFormat.parse("2013-07-18 00:00:00.000");
                Date time3 = dateFormat.parse("2013-08-18 00:00:00.000");
                Date time4 = dateFormat.parse("2013-09-18 00:00:00.000");
                validateTableOfLongs(cr.getResults()[0],
                        new long[][] { {time1.getTime()*1000, 2, 10}, {time2.getTime()*1000, 4, 20},
                                {time3.getTime()*1000, 2, 40},{time4.getTime()*1000, 3, 45},});
            }
        }
    }

    @Test
    public void testComplexGroupbyDistinctLimit() throws IOException, ProcCallException {
        loadData(true);

        final Client client = this.getClient();
        ClientResponse cr;

        for (String tb: tbs) {
            // (1) Without extra aggregation expression
            // Test complex group-by (Function expression) without complex aggregation. (Depulicates: two 2s for dept)
            // Test distinct
            cr = client.callProcedure("@AdHoc",
                    "SELECT abs(wage) as tag, count(distinct dept) from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {10, 2}, {20, 1}, {30, 1}, {40, 1}, {50, 1} });

            // Test limit
            cr = client.callProcedure("@AdHoc", "SELECT abs(wage) as tag, count(dept) from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag limit 4");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {10, 2}, {20, 1}, {30, 1}, {40, 2} });

            // Test distinct and limit
            cr = client.callProcedure("@AdHoc",
                    "SELECT abs(wage) as tag, count(distinct dept) from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag limit 4");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {10, 2}, {20, 1}, {30, 1}, {40, 1} });

            // (2) With extra aggregation expression
            // Test complex group-by with with complex aggregation. (Depulicates: two 2s for dept)
            // Test distinct
            cr = client.callProcedure("@AdHoc",
                    "SELECT abs(wage) as tag, count(distinct dept)+1 from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {10, 3}, {20, 2}, {30, 2}, {40, 2}, {50, 2} });

            // Test limit
            cr = client.callProcedure("@AdHoc", "SELECT abs(wage) as tag, count(dept)+1 from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag limit 4");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {10, 3}, {20, 2}, {30, 2}, {40, 3}});

            // Test distinct and limit
            cr = client.callProcedure("@AdHoc",
                    "SELECT abs(wage) as tag, count(distinct dept)+1 from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag limit 4 ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {10, 3}, {20, 2}, {30, 2}, {40, 2} });

            // (3) More hard general test case with multi group by columns and complex aggs (Depulicates: two 40s for wage)
            // Test distinct
            cr = client.callProcedure("@AdHoc",
                    "SELECT abs(dept-2) as tag, wage, wage/2, count(distinct wage)*2, sum(id)/count(id)+1 from " +
                            tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {0,10,5,2,7}, {0,40,20,2,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3}, {1,30,15,2,4} });

            // Test Limit
            cr = client.callProcedure("@AdHoc",
                    "SELECT abs(dept-2) as tag, wage, wage/2, count(wage)*2, sum(id)/count(id)+1 from " +
                            tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage LIMIT 5;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {0,10,5,2,7}, {0,40,20,4,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3} });

            // Test distinct and limit
            cr = client.callProcedure("@AdHoc",
                    "SELECT abs(dept-2) as tag, wage, wage/2, count(distinct wage)*2, sum(id)/count(id)+1 from " +
                            tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage LIMIT 5;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {0,10,5,2,7}, {0,40,20,2,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3} });
        }
    }

    @Test
    public void testComplexGroupbyOrderbySuite() throws IOException, ProcCallException {
        loadData(true);

        final Client client = this.getClient();
        ClientResponse cr;

        for (String tb: tbs) {
            //(1) Test complex group-by with no extra aggregation expressions.
            // Test order by with tag
            cr = client.callProcedure("@AdHoc",
                    "SELECT abs(dept) as tag, count(wage), sum(id), avg(wage)  from " + tb +
                    " GROUP BY abs(dept) ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3, 6, 20}, {2, 4, 22, 35} });

            // Test order by without tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept) as tag, count(wage), sum(id), avg(wage) from " + tb +
                    " GROUP BY abs(dept) ORDER BY abs(dept) ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3, 6, 20}, {2, 4, 22, 35} });

            //(2) Test complex group-by with complex aggregation.
            // Test order by with tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, count(wage)+1, avg(wage)/2 from " + tb +
                    " GROUP BY abs(dept-2) ORDER BY tag;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {0, 5, 17}, {1, 4, 10} });

            // Test order by without tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, count(wage)+1, avg(wage)/2 from " + tb +
                    " GROUP BY abs(dept-2) ORDER BY abs(dept-2);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {0, 5, 17}, {1, 4, 10} });

            //(3) More hard general test cases with multi group by columns and complex aggs
            // Test order by with tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(*)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {0,10,5,2,7}, {0,40,20,4,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3}, {1,30,15,2,4} });

            // Test order by without tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(*)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY abs(dept-2), wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {0,10,5,2,7}, {0,40,20,4,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3}, {1,30,15,2,4} });

            // Test order by without tag and not in display columns
            cr = client.callProcedure("@AdHoc", "SELECT wage, wage/2, count(*)*2, sum(id)/count(id)+1 from " + tb +
                    " GROUP BY abs(dept-2), wage ORDER BY abs(dept-2), wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {10,5,2,7}, {40,20,4,6}, {50,25,2,6}, {10,5,2,2}, {20,10,2,3}, {30,15,2,4} });


            //(4) Other order by expressions (id+dept), expressions on that.
            cr = client.callProcedure("@AdHoc", "SELECT id+dept, sum(wage)+1 from " + tb +
                    " GROUP BY id+dept ORDER BY ABS(id+dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {2, 11}, {3,21}, {4,31}, {6,41}, {7,51}, {8,11}, {9,41} });

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage) from " + tb +
                    " GROUP BY id+dept ORDER BY ABS(id+dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {2, 10}, {3,20}, {4,30}, {6,40}, {7,50}, {8,10}, {9,40} });

            cr = client.callProcedure("@AdHoc", "SELECT id+dept from " + tb +
                    " GROUP BY id+dept ORDER BY ABS(id+dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {2}, {3}, {4}, {6}, {7}, {8}, {9} });

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, wage from " + tb +
                    " GROUP BY id+dept, wage ORDER BY ABS(id+dept), abs(wage)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {2, 10}, {3,20}, {4,30}, {6,40}, {7,50}, {8,10}, {9,40} });

            // Expressions on the columns from selected list
            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage) as tag from " + tb +
                    " GROUP BY id+dept ORDER BY ABS(tag), id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {2, 10}, {8,10}, {3,20}, {4,30}, {6,40}, {9,40}, {7,50}});

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage) as tag from " + tb +
                    " GROUP BY id+dept ORDER BY ABS(avg(wage)), id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {2, 10}, {8,10}, {3,20}, {4,30}, {6,40}, {9,40}, {7,50}});

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage) as tag from " + tb +
                    " GROUP BY id+dept ORDER BY ABS(avg(wage)) + 1, id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {2, 10}, {8,10}, {3,20}, {4,30}, {6,40}, {9,40}, {7,50}});


            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage)+1 as tag from " + tb +
                    " GROUP BY id+dept ORDER BY ABS(tag), id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {2, 11}, {8,11}, {3,21}, {4,31}, {6,41}, {9,41}, {7,51}});

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage)+1 as tag from " + tb +
                    " GROUP BY id+dept ORDER BY ABS(avg(wage)+1), id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {2, 11}, {8,11}, {3,21}, {4,31}, {6,41}, {9,41}, {7,51}});

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage)+1 as tag from " + tb +
                    " GROUP BY id+dept ORDER BY ABS(avg(wage)+1) + 1, id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0],
                    new long[][] { {2, 11}, {8,11}, {3,21}, {4,31}, {6,41}, {9,41}, {7,51}});
        }
    }

    @Test
    public void testENG4285() throws IOException, ProcCallException {
        loadData(false);

        final Client client = this.getClient();
        ClientResponse cr;

        for (String tb: tbs) {
            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage-id) from " + tb +
                    " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 54} , {2, 81}});

            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage-id), avg(wage-id), " +
                    "count(*) from " + tb + " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 54, 18, 3} , {2, 81, 40, 2}});

            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage-id) + 1, " +
                    "avg(wage-id), count(*) from " + tb + " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 55, 18, 3} , {2, 82, 40, 2}});

            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage-extract(month from tm)), " +
                    "avg(wage-extract(month from tm)), count(dept) from " + tb +
                    " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 40, 13, 3} , {2, 73, 36, 2}});
        }
    }

    // Test group by columns do not have to be in display columns.
    @Test
    public void testENG5016() throws IOException, ProcCallException {
        loadData(false);

        final Client client = this.getClient();
        ClientResponse cr;
        VoltTable vt;

        for (String tb: tbs) {
            cr = client.callProcedure("@AdHoc", "SELECT count(*), sum(wage) from " + tb +
                    " GROUP BY dept ORDER BY sum(wage)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {3, 60} , {2, 90}});

            cr = client.callProcedure("@AdHoc", "SELECT count(*) as tag, sum(wage), sum(wage) from " + tb +
                    " GROUP BY dept ORDER BY tag");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            validateTableOfLongs(vt, new long[][] { {2, 90, 90}, {3, 60, 60} });

            // Demo bug, ENG-5149
            // Check column alias for the identical aggregation
            assertEquals("C2", vt.getColumnName(1));
            assertEquals("C3", vt.getColumnName(2));
            assertEquals(1, vt.getColumnIndex("C2"));
            assertEquals(2, vt.getColumnIndex("C3"));

            cr = client.callProcedure("@AdHoc", "SELECT count(*) as tag, " +
                    "sum(wage)+1 as NO_BUG, sum(wage)+1 from " + tb +
                    " GROUP BY dept ORDER BY tag");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            validateTableOfLongs(vt, new long[][] { {2, 91, 91}, {3, 61, 61} });

            assertEquals("NO_BUG", vt.getColumnName(1));
            assertEquals("C3", vt.getColumnName(2));
            assertEquals(1, vt.getColumnIndex("NO_BUG"));
            assertEquals(2, vt.getColumnIndex("C3"));
        }
    }

    @Test
    public void testSupportedCases() throws IOException, ProcCallException {
        loadData(false);

        final Client client = this.getClient();
        ClientResponse cr;

        for (String tb: tbs) {
            // Test order by agg without tag
            cr = client.callProcedure("@AdHoc", "SELECT dept, count(*), sum(wage) from " + tb +
                    " GROUP BY dept ORDER BY sum(wage)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3, 60} , {2, 2, 90}});

            cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) from " + tb +
                    " GROUP BY dept ORDER BY COUNT(*) DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3, 60} , {2, 2, 90}});

            cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*), sum(wage) from " + tb +
                    " GROUP BY dept ORDER BY COUNT(*) DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3, 60} , {2, 2, 90}});

            cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) - 1 from " + tb +
                    " GROUP BY dept ORDER BY COUNT(*) DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] { {1, 3, 59} , {2, 2, 89}});
        }
    }

    @Test
    public void testENG7046() throws IOException, ProcCallException {
        final Client client = this.getClient();
        VoltTable vt;
        client.callProcedure("TB_STRING.insert", 1,  "MA");

        if (!isHSQL()) {
            // Hsql does not support DECODE function
            vt = client.callProcedure("@AdHoc",
                    "select min(decode(state, upper(state), state, "
                    + "state || ' with this kind of rambling string added to it may not be inlinable')) "
                    + "from tb_string").getResults()[0];
            validateTableColumnOfScalarVarchar(vt, new String[] {"MA"});
        }
    }

    // Test unsupported order by column not in display columns
    @Test
    public void testUnsupportedCases() throws IOException, ProcCallException {
        loadData(false);

        final Client client = this.getClient();
        for (String tb: tbs) {
            try {
                // Test order by agg not in display columns
                client.callProcedure("@AdHoc", "SELECT dept, avg(wage), sum(wage) from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                fail();
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            try {
                // Test order by agg not in display columns
                client.callProcedure("@AdHoc", "SELECT dept, count(*) from " + tb +
                        " GROUP BY dept ORDER BY sum(wage) DESC");
                fail();
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            try {
                // Test group by column not in display columns
                client.callProcedure("@AdHoc", "SELECT avg(wage), sum(wage) from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                fail();
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            try {
                // Test order by agg not in display columns
                client.callProcedure("@AdHoc", "SELECT dept, avg(wage), sum(wage) - 1 from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                fail();
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            try {
                // Test group by column not in display columns
                client.callProcedure("@AdHoc", "SELECT avg(wage), sum(wage) - 1 from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                fail();
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            try {
                // Test order by without tag and not in display columns, and not equal to group by columns
                client.callProcedure("@AdHoc", "SELECT count(wage), sum(id), avg(wage)  from " + tb +
                        " GROUP BY abs(dept) ORDER BY count(*) ");
                fail();
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }
        }
    }

    @Test
    public void testAggregateOnJoin() throws IOException, ProcCallException {
        System.out.println("Test aggs on joins...");
        loadData(false);

        Client client = this.getClient();
        VoltTable vt;
        String sql;

        sql = "SELECT r1.id, count(*) from r1, p2 where r1.id = p2.dept GROUP BY r1.id ORDER BY 1;";

        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().toLowerCase().contains("inline hash"));

        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1,3}, {2,2}});
    }

    @Test
    public void testHavingClause() throws IOException, ProcCallException {
        System.out.println("test Having clause...");
        loadData(false);

        Client client = this.getClient();
        ClientResponse cr;

        for (String tb: tbs) {
            // Test normal group by with expressions, addition, division for avg.
            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage), count(wage)+5, " +
                    "sum(wage)/count(wage) from " + tb + " GROUP BY dept ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] {{2, 90, 7, 45}, {1, 60, 8, 20} });

            // Test having
            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage) from " + tb +
                    " GROUP BY dept HAVING sum(wage) > 60 ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] {{2, 90}});

            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage), count(wage)+5 from " + tb +
                    " GROUP BY dept HAVING count(wage)+5 <> 7 ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] {{1, 60, 8}});

            // Test having clause not in display list
            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage) from " + tb +
                    " GROUP BY dept HAVING count(wage)+5 <> 7 ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] {{1, 60}});


            // Test normal group by with expressions, addition, division for avg.
            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage), count(wage)+5, " +
                    "sum(wage)/count(wage) from " + tb + " GROUP BY dept HAVING  sum(wage) < 80 ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], new long[][] {{1, 60, 8, 20} });

            // Test Having with COUNT(*)
            cr = client.callProcedure("@AdHoc", "SELECT count(*) from " + tb +
                    " HAVING count(*) > 60 " +
                    " ORDER BY 1 DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            assertEquals(0, cr.getResults()[0].getRowCount());

            // Test Having with AVG
            cr = client.callProcedure("@AdHoc", "SELECT AVG(wage) from " + tb +
                    " HAVING SUM(id) > 20 " +
                    " ORDER BY 1 DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            assertEquals(0, cr.getResults()[0].getRowCount());
        }
    }

    // This test case will trigger temp table "delete as we go" feature on join node
    // Turn off this test cases because of valgrind timeout.
    @Test
    public void testTurnOfftestAggregateOnJoinForMemoryIssue() throws IOException, ProcCallException, InterruptedException {
        final Client client = this.getClient();
        ClientResponse cr;
        // Empty data from table.
        for (String tb: tbs) {
            cr = client.callProcedure("@AdHoc", "delete from " + tb);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        int scale = 10;
        int numOfRecords = scale * 1000;
        // Insert records into the table.
        // id, wage, dept, rate
        String timeStamp = "2013-06-18 02:00:00.123457";

        String[] myProcs = {"R1.insert", "P1.insert"};
        for (String insertProc: myProcs) {
            for (int ii = 1; ii <= numOfRecords; ii++) {
                client.callProcedure(new NullCallback(), insertProc, ii,  ii % 1000,  ii % 2 , timeStamp);
            }
        }

        client.drain();

        // Serial aggregation because of no group by
        cr = client.callProcedure("@AdHoc", "SELECT sum(R1.wage) " +
                " from R1, P1 WHERE R1.id = P1.id ;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], new long[][] {{499500 * scale}});


        // hash aggregation because of no index on group by key
        cr = client.callProcedure("@AdHoc", "SELECT R1.dept, sum(R1.wage) " +
                " from R1, P1 WHERE R1.id = P1.id Group by R1.dept order by R1.dept;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], new long[][] {{0, 249500 * scale}, {1, 250000 * scale}});
    }

    @Test
    public void testDistinctWithGroupby() throws IOException, ProcCallException {
        System.out.println("Test Distinct...");
        loadData(true);
        final Client client = this.getClient();
        VoltTable vt;

        for (String tb: tbs) {
            vt = client.callProcedure("@AdHoc",
                    "SELECT DISTINCT ID, COUNT(DEPT) FROM " + tb + " GROUP BY ID, WAGE ORDER BY ID, 2  ")
                    .getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1}, {2,1}, {3,1}, {4,1}, {5,1}, {6,1}, {7,1}});

            // test LIMIT/OFFSET
            vt = client.callProcedure("@AdHoc",
                    "SELECT DISTINCT ID, COUNT(DEPT) FROM " + tb + " GROUP BY ID, WAGE ORDER BY 1, 2 LIMIT 2 ")
                    .getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1}, {2,1}});

            // query with one column distinct
            vt = client.callProcedure("@AdHoc",
                    "SELECT DISTINCT count(*) from " + tb + " GROUP BY abs(dept-2), wage ORDER BY 1;")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[]{1, 2});

            // (1) base query without distinct
            vt = client.callProcedure("@AdHoc",
                    "SELECT wage, count(*) from " + tb + " GROUP BY abs(dept-2), wage ORDER BY 1, 2;")
                    .getResults()[0];
            validateTableOfLongs(vt, new long[][] {{10,1}, {10,1}, {20,1}, {30,1}, {40,2}, {50,1}});

            // query with multiple columns distinct
            vt = client.callProcedure("@AdHoc",
                    "SELECT DISTINCT wage, count(*) from " + tb + " GROUP BY abs(dept-2), wage ORDER BY 1, 2;")
                    .getResults()[0];
            validateTableOfLongs(vt, new long[][] {{10,1}, {20,1}, {30,1}, {40,2}, {50,1}});

            // test LIMIT/OFFSET
            vt = client.callProcedure("@AdHoc",
                    "SELECT DISTINCT wage, count(*) from " + tb + " GROUP BY abs(dept-2), wage ORDER BY 1, 2 LIMIT 2;")
                    .getResults()[0];
            validateTableOfLongs(vt, new long[][] {{10,1}, {20,1}});

            // query with multiple expressions distinct
            vt = client.callProcedure("@AdHoc",
                    "SELECT DISTINCT ID, COUNT(DEPT) + 1 FROM " + tb + " GROUP BY ID, WAGE order by 1, 2")
                    .getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,2}, {2,2}, {3,2}, {4,2}, {5,2}, {6,2}, {7,2}});

            vt = client.callProcedure("@AdHoc",
                    "SELECT DISTINCT wage, count(*)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY 1, 2;")
                    .getResults()[0];
            validateTableOfLongs(vt, new long[][] {{10,2}, {20,2}, {30,2}, {40,3}, {50,2}});

            // test LIMIT/OFFSET
            vt = client.callProcedure("@AdHoc",
                    "SELECT DISTINCT wage, count(*)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY 1, 2 LIMIT 2;")
                    .getResults()[0];
            validateTableOfLongs(vt, new long[][] {{10,2}, {20,2}});
        }
    }

    @Test
    public void testOrderbyColumnsNotInDisplayList() throws IOException, ProcCallException {
        System.out.println("Test testOrderbyColumnsNotInDisplayList...");
        loadData(true);
        Client client = this.getClient();

        VoltTable vt;
        for (String tb: tbs) {
            // Test Order by column not in Display columns
            vt = client.callProcedure("@AdHoc",
                    "SELECT SUM(ABS(wage) - 1) as tag, (count(*)+sum(dept*2))/2 from " + tb +
                            " GROUP BY dept ORDER BY ABS(dept)").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {57, 4} , {136, 10}});

            vt = client.callProcedure("@AdHoc",
                    "SELECT count(wage), sum(id)  from " + tb +
                            " GROUP BY abs(dept) ORDER BY abs(dept) ").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {3, 6}, {4, 22} });

            vt = client.callProcedure("@AdHoc",
                    "SELECT count(wage), sum(id), avg(wage)  from " + tb +
                            " GROUP BY abs(dept) ORDER BY abs(dept) ").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {3, 6, 20}, {4, 22, 35} });

            vt = client.callProcedure("@AdHoc",
                    "SELECT count(wage)+1, avg(wage)/2 from " + tb +
                            " GROUP BY abs(dept-2) ORDER BY abs(dept-2);").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {5, 17}, {4, 10} });

            vt = client.callProcedure("@AdHoc",
                    "SELECT COUNT(*) as tag, sum(wage) from " + tb +
                            " GROUP BY dept ORDER BY abs(dept) DESC").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {4, 140}, {3, 60}});
        }
    }

    //
    // Suite builder boilerplate
    //
    public TestGroupByComplexSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestGroupByComplexSuite.class);

        StringBuilder addProcs = new StringBuilder();
        for (String tb: tbs) {
            addProcs.append("CREATE PROCEDURE ")
                    .append(tb)
                    .append("_GroupbyAlias1 AS ")
                    .append(" SELECT (dept+?) as tag, count(wage) from ")
                    .append(tb)
                    .append(" GROUP BY tag ORDER BY tag;");

            addProcs.append("CREATE PROCEDURE ")
                    .append(tb)
                    .append("_GroupbyAlias2 AS ")
                    .append(" SELECT abs(dept+?) as tag, count(wage)+1 from ")
                    .append(tb)
                    .append(" GROUP BY tag ORDER BY tag;");
        }

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
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
                "ID INTEGER DEFAULT 0 NOT NULL ASSUMEUNIQUE, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID, WAGE) );" +
                "PARTITION TABLE P3 ON COLUMN WAGE;" +

                "CREATE TABLE TB_STRING ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "STATE VARCHAR(2), " +
                "PRIMARY KEY (ID) );" +

                "create table t ( a INTEGER NOT NULL, b INTEGER NOT NULL, c INTEGER NOT NULL); " + addProcs;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        config = new LocalCluster("groupByComplex-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);
        // NOTE: remove test for HSQL_BACKEND
        // Cluster
        config = new LocalCluster("groupByComplex-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        return builder;
    }
}
