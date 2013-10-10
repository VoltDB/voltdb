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

public class TestIndexReverseScanSuite extends RegressionSuite {

    private final String[] procs = {"R1.insert", "P1.insert", "P2.insert", "P3.insert"};
    private final String [] tbs = {"R1","P1","P2","P3"};

    private void loadData() throws IOException, ProcCallException {
        Client client = this.getClient();
        ClientResponse cr = null;

        // Empty data from table.
        for (String tb: tbs) {
            cr = client.callProcedure("@AdHoc", "delete from " + tb);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

        // Insert records into the table.
        // id, wage, dept, rate
        for (String tb: procs) {
            cr = client.callProcedure(tb, 1,  1, 1, 1, 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(tb, 2,  2, 2, 2, 2);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(tb, 3,  3, 3, 3, 3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(tb, 4,  4, 4, 4, 4);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure(tb, 5,  5, 5, 5, 5);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

    }

    public void testReverseScan() throws IOException, ProcCallException {
        loadData();

        Client client = this.getClient();
        ClientResponse cr;
        VoltTable vt;
        long[][] expected;

        for (String tb: tbs) {

            cr = client.callProcedure("@AdHoc", "SELECT a from " + tb + " where a < 3 order by a DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            expected = new long[][] {{2}, {1} };
            System.out.println(vt.toString());
            validateTableOfLongs(vt, expected);

        }
    }

    //
    // Suite builder boilerplate
    //

    public TestIndexReverseScanSuite(String name) {
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
                TestIndexReverseScanSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "a INTEGER, " +
                "b INTEGER, " +
                "c INTEGER, " +
                "d INTEGER, " +
                "PRIMARY KEY (ID) );" +

                "create index R1_TREE_1 on R1 (a);" +
                "create index R1_TREE_2 on R1 (b, c);" +

                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "a INTEGER, " +
                "b INTEGER, " +
                "c INTEGER, " +
                "d INTEGER, " +
                "PRIMARY KEY (ID) );" +
                "PARTITION TABLE P1 ON COLUMN ID;" +

                "create index P1_TREE_1 on P1 (a);" +
                "create index P1_TREE_2 on P1 (b, c);" +

                "CREATE TABLE P2 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "a INTEGER not null, " +
                "b INTEGER, " +
                "c INTEGER, " +
                "d INTEGER, " +
                "PRIMARY KEY (ID) );" +
                "PARTITION TABLE P2 ON COLUMN a;" +

                "create index P2_TREE_1 on P2 (a);" +
                "create index P2_TREE_2 on P2 (b, c);" +

                "CREATE TABLE P3 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "a INTEGER, " +
                "b INTEGER not null, " +
                "c INTEGER, " +
                "d INTEGER, " +
                "PRIMARY KEY (ID) );" +
                "PARTITION TABLE P3 ON COLUMN b;" +

                "create index P3_TREE_1 on P3 (a);" +
                "create index P3_TREE_2 on P3 (b, c);" +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        boolean success;

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
