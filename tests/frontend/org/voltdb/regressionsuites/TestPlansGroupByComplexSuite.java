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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

/*
 * Functional tests of the statements compiled in the test suite
 * org.voltdb.planner.TestComplexGroupBySuite.
 */

public class TestPlansGroupByComplexSuite extends RegressionSuite {

    private void compareTable(VoltTable vt, long[][] expected) {
        int len = expected.length;
        for (int i=0; i < len; i++) {
            compareRow(vt, expected[i]);
        }
    }

    private void compareRow(VoltTable vt, long [] expected) {
        int len = expected.length;
        assertTrue(vt.advanceRow());
        for (int i=0; i < len; i++) {
            long actual = -10000000;
            // ENG-4295: hsql bug: HSQLBackend sometimes returns wrong column type.
            try {
                actual = vt.getLong(i);
            } catch (IllegalArgumentException ex) {
                try {
                    actual = (long) vt.getDouble(i);
                } catch (IllegalArgumentException newEx) {
                    actual = vt.getTimestampAsLong(i);
                }
            }
            assertEquals(expected[i], actual);
        }
    }

    private void loadData() throws IOException, ProcCallException {
        Client client = this.getClient();
        ClientResponse cr = null;

        // id, wage, dept, rate
        String[] procs = {"R1.insert", "P1.insert"};
        for (String tb: procs) {
            cr = client.callProcedure(tb, 1,  10,  1 , "2013-06-18 02:00:00.123457");
            cr = client.callProcedure(tb, 2,  20,  1 , "2013-07-18 02:00:00.123457");
            cr = client.callProcedure(tb, 3,  30,  1 , "2013-07-18 10:40:01.123457");
            cr = client.callProcedure(tb, 4,  40,  2 , "2013-08-18 02:00:00.123457");
            cr = client.callProcedure(tb, 5,  50,  2 , "2013-09-18 02:00:00.123457");
        }
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }


    public void testStrangeCasesAndOrderby() throws IOException, ProcCallException {
        loadData();

        Client client = this.getClient();
        ClientResponse cr;
        VoltTable vt;
        long[][] expected;

        String [] tbs = {"R1", "P1"};
        for (String tb: tbs) {
            // Test pass-through columns, group by primary key
            cr = client.callProcedure("@AdHoc", "SELECT dept, count(wage) from " + tb +
                    " GROUP BY id ORDER BY dept DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{2,1}, {2,1}, {1,1}, {1,1}, {1,1} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test duplicates, operator expression, group by primary key
            cr = client.callProcedure("@AdHoc", "SELECT id, id, dept, dept+5 from " + tb +
                    " GROUP BY id ORDER BY id");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{1,1,1,6}, {2,2,1,6}, {3,3,1,6}, {4,4,2,7}, {5,5,2,7} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test function expression with group by primary key
            cr = client.callProcedure("@AdHoc", "SELECT id, id + 1, sum(wage)/2, abs(dept-3) from " + tb +
                    " GROUP BY id ORDER BY id");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{1,2,5,2}, {2,3,10,2}, {3,4,15,2}, {4,5,20,1}, {5,6,25,1} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test order by expression column which is not in display columns
            cr = client.callProcedure("@AdHoc", "SELECT COUNT(*) as tag, sum(wage) from " + tb +
                    " GROUP BY dept ORDER BY abs(dept) DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 90}, {3, 60}};
            compareTable(vt, expected);

            // Test order by expression column which is not in display columns, with complex aggregations
            cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) - 1 from " + tb +
                    " GROUP BY dept ORDER BY tag DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 3, 59} , {2, 2, 89}};
            System.out.println(vt.toString());
            compareTable(vt, expected);
        }
    }

    public void testComplexAggsOrderbySuite() throws IOException, ProcCallException {
        loadData();

        Client client = this.getClient();
        ClientResponse cr;
        VoltTable vt;
        long[][] expected;

        String [] tbs = {"R1", "P1"};
        for (String tb: tbs) {
            // (0) Test no group by cases
            cr = client.callProcedure("@AdHoc", "SELECT id+dept from " + tb + " ORDER BY ABS(id+dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2}, {3}, {4}, {6}, {7} };
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept from " + tb + " ORDER BY id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2}, {3}, {4}, {6}, {7} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept from " + tb + " ORDER BY id+dept, wage");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2}, {3}, {4}, {6}, {7} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, wage from " + tb + " ORDER BY id+dept, wage");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 10}, {3,20}, {4,30}, {6,40}, {7,50} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // (1) Test Order by COUNT(*) without complex expression
            // Test order by agg with tag
            cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) from " + tb +
                    " GROUP BY dept ORDER BY tag DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 3, 60} , {2, 2, 90}};
            System.out.println(vt.toString());
            compareTable(vt, expected);


            // (2) Test Order by COUNT(*) with complex expression
            // Test order by agg with tag
            cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) - 1 from " + tb +
                    " GROUP BY dept ORDER BY tag DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 3, 59} , {2, 2, 89}};
            System.out.println(vt.toString());
            compareTable(vt, expected);


            // (3) Test Order by with FUNCTION expression, no group by column in display columns
            // Test Order by without tag
            cr = client.callProcedure("@AdHoc", "SELECT ABS(dept) as tag, SUM(ABS(wage) - 1) as tag, " +
                    "(count(*)+sum(dept*2))/2 from " + tb + " GROUP BY dept ORDER BY tag");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 57, 4} , {2, 88, 5}};
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test Order by without tag
            cr = client.callProcedure("@AdHoc", "SELECT ABS(dept), SUM(ABS(wage) - 1) as tag, " +
                    "(count(*)+sum(dept*2))/2 from " + tb + " GROUP BY dept ORDER BY ABS(dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 57, 4} , {2, 88, 5}};
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test Order by column not in Display columns
            cr = client.callProcedure("@AdHoc", "SELECT SUM(ABS(wage) - 1) as tag, " +
                    "(count(*)+sum(dept*2))/2 from " + tb + " GROUP BY dept ORDER BY ABS(dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {57, 4} , {88, 5}};
            System.out.println(vt.toString());
            compareTable(vt, expected);
        }

    }

    public void testComplexAggs() throws IOException, ProcCallException {
        loadData();

        Client client = this.getClient();
        ClientResponse cr;
        VoltTable vt;
        long[][] expected;

        String [] tbs = {"R1", "P1"};
        for (String tb: tbs) {
            // Test normal group by with expressions, addition, division for avg.
            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage), count(wage)+5, " +
                    "sum(wage)/count(wage) from " + tb + " GROUP BY dept ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{2, 90, 7, 45}, {1, 60, 8, 20} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test different group by column order, non-grouped TVE, sum for column, division
            cr = client.callProcedure("@AdHoc", "SELECT sum(wage)/count(wage) + 1, dept, " +
                    "SUM(wage+1), SUM(wage)/2 from " + tb + " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{21 ,1, 63, 30}, {46, 2, 92, 45}};
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test Complex Agg with functions
            cr = client.callProcedure("@AdHoc", "SELECT dept, SUM(ABS(wage) - 1) as tag, " +
                    "(count(*)+sum(dept*2))/2 from " + tb + " GROUP BY dept ORDER BY ABS(dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 57, 4} , {2, 88, 5}};
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test sum()/count(), Addition
            cr = client.callProcedure("@AdHoc", "SELECT dept, SUM(wage), COUNT(wage), AVG(wage), " +
                    "MAX(wage), MIN(wage), SUM(wage)/COUNT(wage),  " +
                    "MAX(wage)+MIN(wage)+1 from " + tb + " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{1, 60, 3, 20, 30, 10, 20, 41}, {2, 90, 2, 45, 50, 40, 45, 91}};
            compareTable(vt, expected);
        }
    }

    public void testComplexAggsDistinctLimit() throws IOException, ProcCallException {
        loadData();

        Client client = this.getClient();
        ClientResponse cr = null;
        VoltTable vt;
        long[][] expected;

        // id, wage, dept, rate
        String[] procs = {"R1.insert", "P1.insert"};
        for (String tb: procs) {
            cr = client.callProcedure(tb, 6,  10,  2 , "2013-07-18 02:00:00.123457");
            cr = client.callProcedure(tb, 7,  40,  2 , "2013-07-18 02:00:00.123457");
        }
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        String [] tbs = {"R1","P1"};
        for (String tb: tbs) {
            // Test distinct with complex aggregations.
            cr = client.callProcedure("@AdHoc", "SELECT dept, count(wage), sum(distinct wage), sum(wage), " +
                    "count(distinct wage)+5, sum(wage)/(count(wage)+1) from " + tb + " GROUP BY dept ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{2, 4, 100, 140, 8, 28}, {1, 3, 60, 60, 8, 15} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test limit with complex aggregation.
            cr = client.callProcedure("@AdHoc", "SELECT wage, sum(id)+1, sum(id+1),  sum(dept+3)/count(dept) from " + tb +
                    " GROUP BY wage ORDER BY wage ASC LIMIT 4 ;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{10, 8, 9, 4}, {20, 3, 3, 4}, {30, 4, 4, 4}, {40, 12, 13, 5}};
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test distinct limit together with complex aggregation.
            cr = client.callProcedure("@AdHoc", "SELECT wage, sum(id)+1, sum(id+1),  sum(dept+3)/count(distinct dept) from " + tb +
                    " GROUP BY wage ORDER BY wage ASC LIMIT 4 ;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{10, 8, 9, 4}, {20, 3, 3, 4}, {30, 4, 4, 4}, {40, 12, 13, 10}};
            System.out.println(vt.toString());
            compareTable(vt, expected);
        }
    }

    public void testComplexGroupby() throws IOException, ProcCallException, ParseException{
        loadData();

        Client client = this.getClient();
        ClientResponse cr = null;
        VoltTable vt;
        long[][] expected;

        // id, wage, dept, rate
        String[] procs = {"R1.insert", "P1.insert"};
        for (String tb: procs) {
            cr = client.callProcedure(tb, 6,  10,  2 , "2013-07-18 02:00:00.123457");
            cr = client.callProcedure(tb, 7,  40,  2 , "2013-09-18 02:00:00.123457");
        }

        String [] tbs = {"R1","P1"};
        for (String tb: tbs) {
            // (1) Without extra aggregation expression
            // Test complex group-by (Function expression) without complex aggregation.
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept) as tag, count(wage) from " + tb +
                    " GROUP BY abs(dept) ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 3}, {2, 4} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test complex group-by (normal expression) without complex aggregation.
            // Actually this AdHoc query has an extra projection node because of the pass-by column dept in order by columns.
            // ParameterValueExpression equal function return false. AggResultColumns contains: dept+1, count(wage) and dept.
            // If it is a stored procedure, there is no extra projection node. AggResultColumns: dept+1 and count(wage).
            cr = client.callProcedure("@AdHoc", "SELECT (dept+1) as tag, count(wage) from " + tb +
                    " GROUP BY dept+1 ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 3}, {3, 4} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // (2) With extra aggregation expression
            // Test complex group-by with with complex aggregation.
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept) as tag, count(wage)+1 from " + tb +
                    " GROUP BY abs(dept) ORDER BY tag DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 5}, {1, 4} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test more complex group-by with with complex aggregation.
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, count(wage)+1 from " + tb +
                    " GROUP BY abs(dept-2) ORDER BY tag;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {0, 5}, {1, 4} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // More hard general test case with multi group by columns and complex aggs
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(*)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {0,10,5,2,7}, {0,40,20,4,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3}, {1,30,15,2,4} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            if (!isHSQL()) {
                // Timestamp function for complex group by
                cr = client.callProcedure("@AdHoc", "SELECT truncate(day, tm) as tag, count(id)+1, " +
                        "sum(wage)/count(wage) from " + tb + " GROUP BY truncate(day, tm) ORDER BY tag;");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                Date time1 = dateFormat.parse("2013-06-18 00:00:00.000");
                Date time2 = dateFormat.parse("2013-07-18 00:00:00.000");
                Date time3 = dateFormat.parse("2013-08-18 00:00:00.000");
                Date time4 = dateFormat.parse("2013-09-18 00:00:00.000");
                expected = new long[][] { {time1.getTime()*1000, 2, 10}, {time2.getTime()*1000, 4, 20},
                        {time3.getTime()*1000, 2, 40},{time4.getTime()*1000, 3, 45},};
                System.out.println(vt.toString());
                compareTable(vt, expected);
            }
        }
    }

    public void testComplexGroupbyDistinctLimit() throws IOException, ProcCallException, ParseException {
        loadData();

        Client client = this.getClient();
        ClientResponse cr = null;
        VoltTable vt;
        long[][] expected;

        // id, wage, dept, rate
        String[] procs = {"R1.insert", "P1.insert"};
        for (String tb: procs) {
            cr = client.callProcedure(tb, 6,  10,  2 , "2013-07-18 02:00:00.123457");
            cr = client.callProcedure(tb, 7,  40,  2 , "2013-09-18 02:00:00.123457");
        }

        String [] tbs = {"R1","P1"};
        for (String tb: tbs) {
            // (1) Without extra aggregation expression
            // Test complex group-by (Function expression) without complex aggregation. (Depulicates: two 2s for dept)
            // Test distinct
            cr = client.callProcedure("@AdHoc", "SELECT abs(wage) as tag, count(distinct dept) from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {10, 2}, {20, 1}, {30, 1}, {40, 1}, {50, 1} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test limit
            cr = client.callProcedure("@AdHoc", "SELECT abs(wage) as tag, count(dept) from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag limit 4");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {10, 2}, {20, 1}, {30, 1}, {40, 2} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test distinct and limit
            cr = client.callProcedure("@AdHoc", "SELECT abs(wage) as tag, count(distinct dept) from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag limit 4");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {10, 2}, {20, 1}, {30, 1}, {40, 1} };
            System.out.println(vt.toString());
            compareTable(vt, expected);


            // (2) With extra aggregation expression
            // Test complex group-by with with complex aggregation. (Depulicates: two 2s for dept)
            // Test distinct
            cr = client.callProcedure("@AdHoc", "SELECT abs(wage) as tag, count(distinct dept)+1 from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {10, 3}, {20, 2}, {30, 2}, {40, 2}, {50, 2} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test limit
            cr = client.callProcedure("@AdHoc", "SELECT abs(wage) as tag, count(dept)+1 from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag limit 4");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {10, 3}, {20, 2}, {30, 2}, {40, 3}};
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test distinct and limit
            cr = client.callProcedure("@AdHoc", "SELECT abs(wage) as tag, count(distinct dept)+1 from " + tb +
                    " GROUP BY abs(wage) ORDER BY tag limit 4 ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {10, 3}, {20, 2}, {30, 2}, {40, 2} };
            System.out.println(vt.toString());
            compareTable(vt, expected);


            // (3) More hard general test case with multi group by columns and complex aggs (Depulicates: two 40s for wage)
            // Test distinct
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(distinct wage)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {0,10,5,2,7}, {0,40,20,2,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3}, {1,30,15,2,4} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test Limit
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(wage)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage LIMIT 5;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {0,10,5,2,7}, {0,40,20,4,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test distinct and limit
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(distinct wage)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage LIMIT 5;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {0,10,5,2,7}, {0,40,20,2,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3} };
            System.out.println(vt.toString());
            compareTable(vt, expected);
        }
    }


    public void testComplexGroupbyOrderbySuite() throws IOException, ProcCallException {
        loadData();

        Client client = this.getClient();
        ClientResponse cr = null;
        VoltTable vt;
        long[][] expected;

        // id, wage, dept, rate
        String[] procs = {"R1.insert", "P1.insert"};
        for (String tb: procs) {
            cr = client.callProcedure(tb, 6,  10,  2 , "2013-07-18 02:00:00.123457");
            cr = client.callProcedure(tb, 7,  40,  2 , "2013-09-18 02:00:00.123457");
        }

        String [] tbs = {"R1","P1"};
        for (String tb: tbs) {
            //(1) Test complex group-by with no extra aggregation expressions.
            // Test order by with tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept) as tag, count(wage), sum(id), avg(wage)  from " + tb +
                    " GROUP BY abs(dept) ORDER BY tag ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 3, 6, 20}, {2, 4, 22, 35} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test order by without tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept) as tag, count(wage), sum(id), avg(wage) from " + tb +
                    " GROUP BY abs(dept) ORDER BY abs(dept) ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 3, 6, 20}, {2, 4, 22, 35} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test order by without tag and not in display columns
            cr = client.callProcedure("@AdHoc", "SELECT count(wage), sum(id), avg(wage)  from " + tb +
                    " GROUP BY abs(dept) ORDER BY abs(dept) ");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {3, 6, 20}, {4, 22, 35} };
            System.out.println(vt.toString());
            compareTable(vt, expected);


            //(2) Test complex group-by with complex aggregation.
            // Test order by with tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, count(wage)+1, avg(wage)/2 from " + tb +
                    " GROUP BY abs(dept-2) ORDER BY tag;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {0, 5, 17}, {1, 4, 10} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test order by without tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, count(wage)+1, avg(wage)/2 from " + tb +
                    " GROUP BY abs(dept-2) ORDER BY abs(dept-2);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {0, 5, 17}, {1, 4, 10} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test order by without tag and not in display columns
            cr = client.callProcedure("@AdHoc", "SELECT count(wage)+1, avg(wage)/2 from " + tb +
                    " GROUP BY abs(dept-2) ORDER BY abs(dept-2);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {5, 17}, {4, 10} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            //(3) More hard general test cases with multi group by columns and complex aggs
            // Test order by with tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(*)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY tag, wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {0,10,5,2,7}, {0,40,20,4,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3}, {1,30,15,2,4} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test order by without tag
            cr = client.callProcedure("@AdHoc", "SELECT abs(dept-2) as tag, wage, wage/2, count(*)*2, " +
                    "sum(id)/count(id)+1 from " + tb + " GROUP BY abs(dept-2), wage ORDER BY abs(dept-2), wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {0,10,5,2,7}, {0,40,20,4,6}, {0,50,25,2,6}, {1,10,5,2,2}, {1,20,10,2,3}, {1,30,15,2,4} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test order by without tag and not in display columns
            cr = client.callProcedure("@AdHoc", "SELECT wage, wage/2, count(*)*2, sum(id)/count(id)+1 from " + tb +
                    " GROUP BY abs(dept-2), wage ORDER BY abs(dept-2), wage;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {10,5,2,7}, {40,20,4,6}, {50,25,2,6}, {10,5,2,2}, {20,10,2,3}, {30,15,2,4} };
            System.out.println(vt.toString());
            compareTable(vt, expected);


            //(4) Other order by expressions (id+dept), expressions on that.
            cr = client.callProcedure("@AdHoc", "SELECT id+dept, sum(wage)+1 from " + tb + " GROUP BY id+dept ORDER BY ABS(id+dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 11}, {3,21}, {4,31}, {6,41}, {7,51}, {8,11}, {9,41} };
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage) from " + tb + " GROUP BY id+dept ORDER BY ABS(id+dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 10}, {3,20}, {4,30}, {6,40}, {7,50}, {8,10}, {9,40} };
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept from " + tb + " GROUP BY id+dept ORDER BY ABS(id+dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2}, {3}, {4}, {6}, {7}, {8}, {9} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, wage from " + tb + " GROUP BY id+dept, wage ORDER BY ABS(id+dept), abs(wage)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 10}, {3,20}, {4,30}, {6,40}, {7,50}, {8,10}, {9,40} };
            System.out.println(vt.toString());
            compareTable(vt, expected);


            // Expressions on the columns from selected list
            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage) as tag from " + tb + " GROUP BY id+dept ORDER BY ABS(tag), id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 10}, {8,10}, {3,20}, {4,30}, {6,40}, {9,40}, {7,50}};
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage) as tag from " + tb + " GROUP BY id+dept ORDER BY ABS(avg(wage)), id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 10}, {8,10}, {3,20}, {4,30}, {6,40}, {9,40}, {7,50}};
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage) as tag from " + tb + " GROUP BY id+dept ORDER BY ABS(avg(wage)) + 1, id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 10}, {8,10}, {3,20}, {4,30}, {6,40}, {9,40}, {7,50}};
            compareTable(vt, expected);


            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage)+1 as tag from " + tb + " GROUP BY id+dept ORDER BY ABS(tag), id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 11}, {8,11}, {3,21}, {4,31}, {6,41}, {9,41}, {7,51}};
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage)+1 as tag from " + tb + " GROUP BY id+dept ORDER BY ABS(avg(wage)+1), id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 11}, {8,11}, {3,21}, {4,31}, {6,41}, {9,41}, {7,51}};
            compareTable(vt, expected);

            cr = client.callProcedure("@AdHoc", "SELECT id+dept, avg(wage)+1 as tag from " + tb + " GROUP BY id+dept ORDER BY ABS(avg(wage)+1) + 1, id+dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {2, 11}, {8,11}, {3,21}, {4,31}, {6,41}, {9,41}, {7,51}};
            compareTable(vt, expected);

        }
    }

    public void testSupportedCases() throws IOException, ProcCallException {
        loadData();

        Client client = this.getClient();
        ClientResponse cr = null;
        VoltTable vt;
        long[][] expected;

        Exception ex = null;
        String [] tbs = {"R1","P1"};
        for (String tb: tbs) {
            // Test order by agg without tag
            cr = client.callProcedure("@AdHoc", "SELECT dept, count(*), sum(wage) from " + tb +
                    " GROUP BY dept ORDER BY sum(wage)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 3, 60} , {2, 2, 90}};
            compareTable(vt, expected);
        }
    }

    // TODO(XIN): make the following un-taged order by cases work, ENG-4958
    // COUNT(*)
    public void testUnsupportedCases() throws IOException, ProcCallException {
        loadData();

        Client client = this.getClient();
        ClientResponse cr = null;
        VoltTable vt;
        long[][] expected;

        Exception ex = null;
        String [] tbs = {"R1","P1"};
        for (String tb: tbs) {
            // Test order by agg without tag
            // Weird, works fine if order by
            ex = null;
            try {
                cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                expected = new long[][] { {1, 3, 60} , {2, 2, 90}};
                compareTable(vt, expected);
            } catch (ProcCallException e) {
                ex = e;
            } finally {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            ex = null;
            try {
                cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*), sum(wage) from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                expected = new long[][] { {1, 3, 60} , {2, 2, 90}};
                compareTable(vt, expected);
            } catch (ProcCallException e) {
                ex = e;
            } finally {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            ex = null;
            try {
                // Test order by agg not in display columns
                cr = client.callProcedure("@AdHoc", "SELECT dept, avg(wage), sum(wage) from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                expected = new long[][] { {1, 20, 60} , {2, 45, 90}};
                compareTable(vt, expected);
            } catch (ProcCallException e) {
                ex = e;
            } finally {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            ex = null;
            try {
                // Test order by agg not in display columns
                cr = client.callProcedure("@AdHoc", "SELECT dept, count(*) from " + tb +
                        " GROUP BY dept ORDER BY sum(wage) DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                expected = new long[][] { {1, 3} , {2, 2}};
                compareTable(vt, expected);
            } catch (ProcCallException e) {
                ex = e;
            } finally {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            ex = null;
            try {
                // Test group by column not in display columns
                cr = client.callProcedure("@AdHoc", "SELECT avg(wage), sum(wage) from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                expected = new long[][] { {20, 60} , {45, 90}};
                compareTable(vt, expected);
            } catch (ProcCallException e) {
                ex = e;
            } finally {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            ex = null;
            try {
                // Test order by agg without tag
                cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) - 1 from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                expected = new long[][] { {1, 3, 59} , {2, 2, 89}};
                compareTable(vt, expected);
            } catch (ProcCallException e) {
                ex = e;
            } finally {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            ex = null;
            try {
                // Test order by agg not in display columns
                cr = client.callProcedure("@AdHoc", "SELECT dept, avg(wage), sum(wage) - 1 from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                expected = new long[][] { {1, 20, 59} , {2, 45, 89}};
                compareTable(vt, expected);
            } catch (ProcCallException e) {
                ex = e;
            } finally {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }


            ex = null;
            try {
                // Test group by column not in display columns
                cr = client.callProcedure("@AdHoc", "SELECT avg(wage), sum(wage) - 1 from " + tb +
                        " GROUP BY dept ORDER BY COUNT(*) DESC");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                expected = new long[][] { {20, 59} , {45, 89}};
                compareTable(vt, expected);
            } catch (ProcCallException e) {
                ex = e;
            } finally {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }

            ex = null;
            try {
                // Test order by without tag and not in display columns, and not equal to group by columns
                cr = client.callProcedure("@AdHoc", "SELECT count(wage), sum(id), avg(wage)  from " + tb +
                        " GROUP BY abs(dept) ORDER BY count(*) ");
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                vt = cr.getResults()[0];
                expected = new long[][] { {3, 6, 20}, {4, 22, 35} };
                compareTable(vt, expected);
            } catch (ProcCallException e) {
                ex = e;
            } finally {
                assertTrue(ex.getMessage().contains("invalid ORDER BY expression"));
            }
        }
    }

    //
    // Suite builder boilerplate
    //

    public TestPlansGroupByComplexSuite(String name) {
        super(name);
    }
    static final Class<?>[] PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.CountT1A1.class,
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.InsertF.class,
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.InsertDims.class,
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.SumGroupSingleJoin.class };

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestPlansGroupByComplexSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) );" +
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) );";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        project.addPartitionInfo("P1", "ID");
        boolean success;
        //project.addStmtProcedure("failedProcedure", "SELECT wage, SUM(wage) from R1 group by ID;");
        //project.addStmtProcedure("groupby", "SELECT (dept+1) as tag, count(wage) from R1 GROUP BY dept+1 ORDER BY tag");

        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        config = new LocalCluster("plansgroupby-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("plansgroupby-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
