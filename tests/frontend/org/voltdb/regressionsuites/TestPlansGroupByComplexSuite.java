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
                actual = (long) vt.getDouble(i);
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
            cr = client.callProcedure(tb, 1,  10,  1 , 1.2);
            cr = client.callProcedure(tb, 2,  20,  1 , 1.1);
            cr = client.callProcedure(tb, 3,  30,  1 , 1.1);
            cr = client.callProcedure(tb, 4,  40,  2 , 1.1);
            cr = client.callProcedure(tb, 5,  50,  2 , 1.2);
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
            // Test duplicates, operator expression, group by primary key
            cr = client.callProcedure("@AdHoc", "SELECT id, id, dept, dept+5 from " + tb + " GROUP BY id ORDER BY id");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{1,1,1,6}, {2,2,1,6}, {3,3,1,6}, {4,4,2,7}, {5,5,2,7} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test function expression with group by primary key
            cr = client.callProcedure("@AdHoc", "SELECT id, id + 1, sum(wage)/2, abs(dept-3) from " + tb + " GROUP BY id ORDER BY id");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{1,2,5,2}, {2,3,10,2}, {3,4,15,2}, {4,5,20,1}, {5,6,25,1} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test order by agg with tag
            cr = client.callProcedure("@AdHoc", "SELECT dept, COUNT(*) as tag, sum(wage) - 1 from " + tb + " GROUP BY dept ORDER BY tag DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 3, 59} , {2, 2, 89}};
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test order by agg expression with tag
            cr = client.callProcedure("@AdHoc", "SELECT dept, ABS(COUNT(*)/2 * -1) as tag, sum(wage) - 1 from " + tb + " GROUP BY dept ORDER BY tag, dept DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{2, 1, 89},{1, 1, 59}};
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
            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage), count(wage)+5, sum(wage)/count(wage) from " + tb + " GROUP BY dept ORDER BY dept DESC;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{2, 90, 7, 45}, {1, 60, 8, 20} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test different group by column order, non-grouped TVE, sum for column, division
            cr = client.callProcedure("@AdHoc", "SELECT sum(wage)/count(wage) + 1, dept, SUM(wage+1), SUM(wage)/2 from " + tb + " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{21 ,1, 63, 30}, {46, 2, 92, 45}};
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test Complex Agg with functions
            cr = client.callProcedure("@AdHoc", "SELECT dept, SUM(ABS(wage) - 1) as tag, (count(*)+sum(dept*2))/2 from " + tb + " GROUP BY dept ORDER BY ABS(dept)");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] { {1, 57, 4} , {2, 88, 5}};
            System.out.println(vt.toString());
            compareTable(vt, expected);

            // Test sum()/count(), Addition
            cr = client.callProcedure("@AdHoc", "SELECT dept, SUM(wage), COUNT(wage), AVG(wage), MAX(wage), MIN(wage), SUM(wage)/COUNT(wage),  " +
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
            cr = client.callProcedure(tb, 6,  10,  2 , 1.2);
            cr = client.callProcedure(tb, 7,  40,  2 , 1.1);
        }
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        String [] tbs = {"P1"};
        for (String tb: tbs) {
            // Test distinct with complex aggregations.
            cr = client.callProcedure("@AdHoc", "SELECT dept, count(wage), sum(distinct wage), sum(wage), count(distinct wage)+5, " +
                    "sum(wage)/(count(wage)+1) from " + tb + " GROUP BY dept ORDER BY dept DESC;");
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

    public void testComplexGroupby() throws IOException, ProcCallException {

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
                "RATE FLOAT, " +
                "PRIMARY KEY (ID) );" +
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "RATE FLOAT, " +
                "PRIMARY KEY (ID) );";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        project.addPartitionInfo("P1", "ID");
        boolean success;
        //project.addStmtProcedure("failedProcedure", "SELECT wage, SUM(wage) from R1 group by ID;");

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
