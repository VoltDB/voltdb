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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;


public class TestPlansGroupByComplexMaterializedViewSuite extends RegressionSuite {

    private final String[] procs = {"R1.insert"};
    //private final String [] tbs = {"R1"};

    private void compareMVcontents(Client client, long[][] expected) {
        VoltTable vt = null;
        try {
            vt = client.callProcedure
                    ("@AdHoc", "select * from V_R1 ORDER BY V_R1_dept").getResults()[0];
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (vt.getRowCount() != 0) {
            assertEquals(expected.length, vt.getRowCount());
            compareTable(vt, expected);
        } else {
            // null result
            try {
                vt = client.callProcedure
                        ("@AdHoc", "select count(*) from V_R1").getResults()[0];
            } catch (Exception e) {
                e.printStackTrace();
            }
            assertEquals(1, vt.getRowCount());
            compareTable(vt, new long [][]{{0}});
        }
    }

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
        for (String tb: procs) {
            cr = client.callProcedure(tb, 1,  10,  1 );
            cr = client.callProcedure(tb, 2,  20,  1 );
            cr = client.callProcedure(tb, 3,  30,  1 );
            cr = client.callProcedure(tb, 4,  40,  2 );
            cr = client.callProcedure(tb, 5,  50,  2 );
        }
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    public void testMaterializedViewInsertDelete() throws IOException, ProcCallException {
        System.out.println("Test insert and delete...");

        // ID, wage, dept
        // SELECT ABS(dept), count(*), SUM(wage) FROM R1 GROUP BY ABS(dept)
        Client client = this.getClient();

        compareMVcontents(client, null);

        client.callProcedure("R1.insert", 1,  10,  -1 );
        compareMVcontents(client, new long [][]{{1, 1, 10}});


        client.callProcedure("R1.insert", 2,  20,  1 );
        compareMVcontents(client, new long [][]{{1, 2, 30}});

        client.callProcedure("R1.insert", 4,  40,  2 );
        compareMVcontents(client, new long [][]{{1, 2, 30}, {2, 1, 40}});

        client.callProcedure("R1.insert", 3,  30,  1 );
        compareMVcontents(client, new long [][]{{1, 3, 60}, {2, 1, 40}});

        client.callProcedure("R1.insert", 5,  50,  2 );
        compareMVcontents(client, new long [][]{{1, 3, 60}, {2, 2, 90}});


        client.callProcedure("R1.delete", 5);
        compareMVcontents(client, new long [][]{{1, 3, 60}, {2, 1, 40}});

        client.callProcedure("R1.delete", 3);
        compareMVcontents(client, new long [][]{{1, 2, 30}, {2, 1, 40}});

        client.callProcedure("R1.delete", 1);
        compareMVcontents(client, new long [][]{{1, 1, 20}, {2, 1, 40}});

        client.callProcedure("R1.delete", 2);
        compareMVcontents(client, new long [][]{{2, 1, 40}});

        client.callProcedure("R1.delete", 4);
        compareMVcontents(client, null);
    }

    public void testMaterializedViewUpdate() throws IOException, ProcCallException {
        System.out.println("Test update...");
        loadData();

        // ID, wage, dept
        // SELECT ABS(dept), count(*), SUM(wage) FROM R1 GROUP BY ABS(dept)
        Client client = this.getClient();

        client.callProcedure("R1.update", 2, 19, 1, 2);
        compareMVcontents(client, new long [][]{{1, 3, 59}, {2, 2, 90}});

        client.callProcedure("R1.update", 4, 41, -1, 4);
        compareMVcontents(client, new long [][]{{1, 4, 100}, {2, 1, 50}});

        client.callProcedure("R1.update", 5, 55, 1, 5);
        compareMVcontents(client, new long [][]{{1, 5, 155}});
    }


    //
    // Suite builder boilerplate
    //

    public TestPlansGroupByComplexMaterializedViewSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestPlansGroupByComplexMaterializedViewSuite.class);
        String literalSchema = null;
        boolean success = true;
        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        String captured = null;
        String[] lines = null;


        VoltProjectBuilder project0 = new VoltProjectBuilder();
        project0.setCompilerDebugPrintStream(capturing);
        literalSchema =
                "CREATE TABLE F ( " +
                "F_PKEY INTEGER NOT NULL, " +
                "F_D1   INTEGER NOT NULL, " +
                "F_D2   INTEGER NOT NULL, " +
                "F_D3   INTEGER NOT NULL, " +
                "F_VAL1 INTEGER NOT NULL, " +
                "F_VAL2 INTEGER NOT NULL, " +
                "F_VAL3 INTEGER NOT NULL, " +
                "PRIMARY KEY (F_PKEY) ); " +

                "CREATE VIEW V0 (V_D1_PKEY, V_D2_PKEY, V_D3_PKEY, V_F_PKEY, CNT, SUM_V1, SUM_V2, SUM_V3) " +
                "AS SELECT F_D1, F_D2, F_D3, F_PKEY, COUNT(*), SUM(F_VAL1)+1, SUM(F_VAL2), SUM(F_VAL3) " +
                "FROM F  GROUP BY F_D1, F_D2, F_D3, F_PKEY;"
                ;
        try {
            project0.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project0);
        assertFalse(success);
        captured = capturer.toString("UTF-8");
        lines = captured.split("\n");

        assertTrue(foundLineMatching(lines,
                ".*V0.*Expressions with aggregate functions are not currently supported in views.*"));

        VoltProjectBuilder project1 = new VoltProjectBuilder();
        project1.setCompilerDebugPrintStream(capturing);
        literalSchema =
                "CREATE TABLE F ( " +
                "F_PKEY INTEGER NOT NULL, " +
                "F_D1   INTEGER NOT NULL, " +
                "F_D2   INTEGER NOT NULL, " +
                "F_D3   INTEGER NOT NULL, " +
                "F_VAL1 INTEGER NOT NULL, " +
                "F_VAL2 INTEGER NOT NULL, " +
                "F_VAL3 INTEGER NOT NULL, " +
                "PRIMARY KEY (F_PKEY) ); " +

                "CREATE VIEW V1 (V_D1_PKEY, V_D2_PKEY, V_D3_PKEY, V_F_PKEY, CNT, SUM_V1, SUM_V2, SUM_V3) " +
                "AS SELECT F_D1, F_D2, F_D3, F_PKEY, COUNT(*) + 1, SUM(F_VAL1), SUM(F_VAL2), SUM(F_VAL3) " +
                "FROM F  GROUP BY F_D1, F_D2, F_D3, F_PKEY;"
                ;
        try {
            project1.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project1);
        assertFalse(success);
        captured = capturer.toString("UTF-8");
        lines = captured.split("\n");


        // Real config for tests
        VoltProjectBuilder project = new VoltProjectBuilder();
        literalSchema =
                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "PRIMARY KEY (ID) );" +

                "CREATE VIEW V_R1 (V_R1_dept, V_R1_CNT, V_R1_sum_wage) " +
                "AS SELECT ABS(dept), count(*), SUM(wage) FROM R1 GROUP BY ABS(dept);" +

//                "CREATE VIEW V_R1 (V_R1_dept,  V_R1_CNT, V_R1_sum_wage) " +
//                "AS SELECT dept, count(*), SUM(wage) FROM R1 GROUP BY dept;" +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
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

    static private boolean foundLineMatching(String[] lines, String pattern) {
        String contents = "";
        for (String string : lines) {
            contents += string;
        }
        if (contents.matches(pattern)) {
            return true;
        }
        return false;
    }
}
