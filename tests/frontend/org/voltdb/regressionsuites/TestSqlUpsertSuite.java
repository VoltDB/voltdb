/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

/**
 * System tests for UPSERT
 */

public class TestSqlUpsertSuite extends RegressionSuite {

    public void testUpsert() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt = null;

        String[] tables = {"P1", "R1"};
        for (String tb : tables) {
            String upsertProc = tb + ".upsert";
            String query = "select ID, wage, dept from " + tb + " order by ID";

            vt = client.callProcedure(upsertProc, 1, 1, 1).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}});

            vt = client.callProcedure(upsertProc, 2, 1, 1).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 1, 1}});


            vt = client.callProcedure(upsertProc, 2, 2, 1).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 2, 1}});


            vt = client.callProcedure(upsertProc, 1, 1, 1).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 2, 1}});

            vt = client.callProcedure(upsertProc, 1, 1, 2).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,2}, {2, 2, 1}});
        }
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlUpsertSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestSqlUpsertSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "PRIMARY KEY (ID) );" +

                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "PRIMARY KEY (ID) );" +
                "PARTITION TABLE P1 ON COLUMN ID;" +

                "CREATE TABLE P2 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL ASSUMEUNIQUE, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "PRIMARY KEY (ID, DEPT) );" +
                "PARTITION TABLE P2 ON COLUMN DEPT;" +

                "CREATE TABLE P3 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL ASSUMEUNIQUE, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "PRIMARY KEY (ID, WAGE) );" +
                "PARTITION TABLE P3 ON COLUMN WAGE;"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        config = new LocalCluster("sqlupsert-onesite.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("sqlupsert-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }

}
