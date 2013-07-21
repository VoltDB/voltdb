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

    public void testComplexAggs() throws IOException, ProcCallException {
        Client client = this.getClient();
        ClientResponse cr;
        VoltTable vt;
        long[][] expected;
        // id, wage, dept, rate
        String[] procs = {"R1.insert", "P1.insert"};
        for (String tb: procs) {
            cr = client.callProcedure(tb, 1,  10,  1 , 1.2);
            cr = client.callProcedure(tb, 2,  20,  1 , 1.1);
            cr = client.callProcedure(tb, 3,  30,  1 , 1.1);
            cr = client.callProcedure(tb, 4,  40,  2 , 1.1);
            cr = client.callProcedure(tb, 5,  50,  2 , 1.2);
        }

        String [] tbs = {"R1", "P1"};
        for (String tb: tbs) {
            // Test sum()/count(), Addition
            cr = client.callProcedure("@AdHoc", "SELECT dept, SUM(wage), COUNT(wage), AVG(wage), MAX(wage), MIN(wage), SUM(wage)/COUNT(wage),  MAX(wage)+MIN(wage)+1 from " + tb + " GROUP BY dept ORDER BY dept");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{1, 60, 3, 20, 30, 10, 20, 41}, {2, 90, 2, 45, 50, 40, 45, 91}};
            compareTable(vt, expected);

            // Test Strange valid cases
            cr = client.callProcedure("@AdHoc", "SELECT id, id from " + tb + " GROUP BY id ORDER BY id");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{1,1}, {2,2}, {3,3}, {4,4}, {5,5} };
            System.out.println(vt.toString());
            compareTable(vt, expected);

            //
            cr = client.callProcedure("@AdHoc", "SELECT dept, sum(wage), count(wage)+5 from R1 GROUP BY dept;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{2, 90, 7}, {1, 60, 8} };
            System.out.println(vt.toString());
            compareTable(vt, expected);
        }

        // Test non-grouped TVE, sum for column, division
        // FIXME(XIN)
//        cr = client.callProcedure("@AdHoc", "SELECT dept, SUM(wage+1), SUM(wage)/2 from R1 GROUP BY dept ORDER BY dept");
//        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
//        vt = cr.getResults()[0];
//        expected = new long[][] {{1, 63, 30}, {2, 92, 45}};
//        System.out.println(vt.toString());
//        compareTable(vt, expected);

        // Test Order by
        // FIXME(XIN): Wrong answer


        // Test Complex Group By
        // FIXME(XIN): complex group by not supported
//        cr = client.callProcedure("@AdHoc", "SELECT ABS(dept), SUM(ABS(dept)) as tag, count(*) from R1 GROUP BY ABS(dept) ORDER BY ABS(dept)");
//        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
//        vt = cr.getResults()[0];
//        expected = new long[][] { {1, 60, 3} , {2, 90, 2}};
//        System.out.println(vt.toString());
//        compareTable(vt, expected);


    }

    public void compareTable(VoltTable vt, long[][] expected) {
        int len = expected.length;
        for (int i=0; i < len; i++) {
            compareRow(vt, expected[i]);
        }
    }

    public void compareRow(VoltTable vt, long [] expected) {
        int len = expected.length;
        assertTrue(vt.advanceRow());
        for (int i=0; i < len; i++) {
            assertEquals(expected[i], vt.getLong(i));
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

        //project.addStmtProcedure("failedProcedure", "SELECT wage, SUM(wage) from R1 group by ID;");

        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

//        config = new LocalCluster("plansgroupby-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
//        success = config.compile(project);
//        assertTrue(success);
//        builder.addServerConfig(config);
//
//        // Cluster
//        config = new LocalCluster("plansgroupby-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assertTrue(success);

        return builder;
    }
}
