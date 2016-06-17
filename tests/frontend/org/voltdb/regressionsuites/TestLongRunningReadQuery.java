/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.voltdb.BackendTarget;
import org.voltdb.LRRHelper;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.jni.ExecutionEngine;

public class TestLongRunningReadQuery extends RegressionSuite {

    private static int tableSize = 1000000;

    private void checkProcStatistics(Client client)
            throws NoConnectionsException, IOException, ProcCallException {
        VoltTable vt;

        vt = client.callProcedure("@Statistics", "PROCEDUREPROFILE", 1).getResults()[0];

        System.out.println(vt.toString());

    }

    private void fillTable(Client client) throws NoConnectionsException, IOException, ProcCallException {
        String sql;
        for (int i = 1; i <= tableSize; i++) {
            sql = "INSERT INTO R1 VALUES (" + i + ");";
            client.callProcedure("@AdHoc", sql);
            if (i % 100000 == 0)
                System.out.println("Progress: Inserted " + i + " rows.");
        }

    }


    public void testLongRunningReadQuery() throws IOException, ProcCallException {
         System.out.println("testLongRunningReadQuery...");

         Client client = getClient();

         fillTable(client);
         checkProcStatistics(client);

         subtest1Select(client);
         checkProcStatistics(client);

    }

    public void subtest1Select(Client client) throws IOException, ProcCallException {
        System.out.println("subtest1Select...");
        VoltTable files;
        VoltTable vt;
        String sql;


        sql = "SELECT * FROM R1;";
        files = client.callProcedure("@ReadOnlySlow", sql).getResults()[0];
        vt = LRRHelper.getTableFromFileTable(files);
        assertEquals(tableSize,vt.getRowCount());
    }

    //
    // Suite builder boilerplate
    //

    public TestLongRunningReadQuery(String name) {
        super(name);
    }
    static final Class<?>[] PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.CountT1A1.class,
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.SumGroupSingleJoin.class };

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestLongRunningReadQuery.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                " ID BIGINT DEFAULT 0 NOT NULL);"
                + ""
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

        return builder;
    }
}