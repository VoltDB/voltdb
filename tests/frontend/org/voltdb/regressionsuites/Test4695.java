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
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class Test4695 extends RegressionSuite {

    public void notestDMLin() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DML in keyword");
        Client client = getClient();
        VoltTable result = null;

//        result = client.callProcedure("@SystemCatalog", "PROCEDURES").getResults()[0];
//        System.out.println("******************\n" + result);

        String[] tables = {"SCORE"};
        for (String table : tables) {
            String insertProc = table +".insert";

            client.callProcedure(insertProc,  1, "b", 1, 1378827221795L, 1, 1);
            client.callProcedure(insertProc,  2, "b", 2, 1378827221795L, 2, 2);

            //                              ID, var1, RATIO
//            client.callProcedure(insertProc,  1, "a", 14.5, "2001-09-01 00:00:00.000");
//            client.callProcedure(insertProc,  2, "b", 15.5);
//            client.callProcedure(insertProc,  3, "c", 16.5);
//            client.callProcedure(insertProc,  4, "f", 17.5);
//            client.callProcedure(insertProc,  5, "g", 18.5);
//            client.callProcedure(insertProc,  6, "h", 19.5);
        }

    }

    public void testXin() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        ClientResponse cr = null;
        VoltTable vt = null;

        // Test Default
//        cr = client.callProcedure("@AdHoc", "Insert into R1 (ID) VALUES(1);");
//        cr = client.callProcedure("@AdHoc", "Insert into R1 (ID) VALUES(2);");
        cr = client.callProcedure("R1.insert", 1, null);
        cr = client.callProcedure("R1.insert", 2, null);
//        cr = client.callProcedure("R1.insert", 2, 6);
//        cr = client.callProcedure("R1.insert", 3, 10);
//        cr = client.callProcedure("R1.insert", 4, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
//        vt = client.callProcedure("@AdHoc", "SELECT ID, c1, CASE WHEN c1 < 3 THEN 5 ELSE 8 END FROM R1;").getResults()[0];

//        vt = client.callProcedure("@AdHoc", "SELECT ID, CASE C1 WHEN 2 THEN 5 ELSE 8 END FROM R1;").getResults()[0];
//        vt = client.callProcedure("@AdHoc", "SELECT ID, DECODE(c1, 2, 5, NULL) FROM R1;").getResults()[0];



//      vt = client.callProcedure("@AdHoc",
//              "SELECT ID, CASE WHEN c1 > 3 " +
//      		                  "THEN CASE WHEN c1 < 5 THEN 2 ELSE 3 END " +
//      		                  "ELSE 1 END FROM R1;").getResults()[0];



//        vt = client.callProcedure("@AdHoc", "SELECT ID, c1, CASE WHEN c1 < 3 THEN 5 END FROM R1;").getResults()[0];


//        vt = client.callProcedure("@Explain", "SELECT COUNT(ID) FROM R1 WHERE C1 > -6000000000;").getResults()[0];
//        System.out.println(vt);

//        vt = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM R1 WHERE ID = 1 AND C1 > -6000000000 ;").getResults()[0];

        vt = client.callProcedure("@AdHoc", "SELECT * FROM R1 WHERE C1 > -6000000000 ;").getResults()[0];

        validateTableOfScalarLongs(vt, new long[] {0});

        System.out.println(vt);

//        vt = client.callProcedure("@AdHoc", "SELECT NOW() FROM R_TIME;").getResults()[0];
//        System.out.println(vt);
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public Test4695(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(Test4695.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
//                "CREATE TABLE P1 ( " +
//                "ID INTEGER NOT NULL, " +
//                "var1 VARCHAR(300) Default 'NULL' , " +
//                "RATIO FLOAT Default NULL, " +
//                "tm timestamp, " +
//                "PRIMARY KEY (ID) ); " +
//                "PARTITION TABLE P1 ON COLUMN ID; " +

                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT '1' NOT NULL, " +
                "C1 INTEGER DEFAULT NULL , " +
                "PRIMARY KEY (ID) ); " +

                "create index idx_c1 on r1 (c1);" +
                "create index idx_2 on r1 (id, c1);" +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        // CONFIG #1: Local Site/Partition running on JNI backend
        config = new LocalCluster("fixedsql-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL -- disabled, the functions being tested are not HSQL compatible
//        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
//        success = config.compile(project);
//        assertTrue(success);
//        builder.addServerConfig(config);

        // no clustering tests for functions

        return builder;
    }
}
