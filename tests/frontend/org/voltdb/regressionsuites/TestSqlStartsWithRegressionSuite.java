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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.aggregates.Insert;

/**
 * System tests for basic STARTS WITH functionality
 */
public class TestSqlStartsWithRegressionSuite extends RegressionSuite {
    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    /** Row numbers in the test table */
    static final int ROWS = 7;

    /** Test patterns that are going to be inserted into 'VAL STARTS WITH xxx' */
    static final String[] testStrings = {"aaa", "abc", "AbC", "zzz", "", "a", "âxxx", "aaaaaaa",
            "abcdef", "abcdef%", "ab_d_fg", "â🀲x", "â🀲x一xxéyyԱ", "â🀲x一xéyyԱ"};

    /** Expected row numbers returned after executing 'VAL STARTS WITH xxx' */
    static final int[] testExpectedResults = {1, 3, 0, 0, 7, 4, 1, 1, 2, 0, 0, 2, 1, 0};

    /*
     * Load Data into tables method.
     */
    private void loadData() throws IOException, ProcCallException {
        Client client = this.getClient();
        ClientResponse cr;

        // Empty data from table.
        cr = client.callProcedure("@AdHoc", "delete from STRINGS");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Then insert rows into the stable
        client.callProcedure("Insert", 1, "aaaaaaa", "aaa");
        client.callProcedure("Insert", 2, "abcccc%", "abc");
        client.callProcedure("Insert", 3, "abcdefg", "abcdefg");
        client.callProcedure("Insert", 4, "abcdefg", "abc%");
        client.callProcedure("Insert", 5, "âxxxéyy", "âxxx");
        client.callProcedure("Insert", 6, "â🀲x一xxéyyԱ", "â🀲x一");
        client.callProcedure("Insert", 7, "â🀲x", "â🀲");
    }

    public void testAdHocColStartsWithString() throws IOException, ProcCallException {
        // clear tables and load data in
        loadData();

        // get current client
        Client client = getClient();

        String query;

        // test if returned row numbers is the same as preset or not
        for (int index = 0; index < testStrings.length; index++) {
            query = String.format("select * from STRINGS where val starts with '%s'", testStrings[index]);

            VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
            assertEquals(String.format("\"%s\": bad row count:", query),
                               testExpectedResults[index], result.getRowCount());
        }
    }

    public void testAdHocColNotStartsWithString() throws IOException, ProcCallException {
        // clear tables and load data in
        loadData();

        // get current client
        Client client = getClient();

        String query;

        // test if returned row numbers is the same as preset or not
        for (int index = 0; index < testStrings.length; index++) {
            query = String.format("select * from STRINGS where val not starts with '%s'", testStrings[index]);

            VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
            assertEquals(String.format("\"%s\": bad row count:", query),
                               ROWS - testExpectedResults[index], result.getRowCount());
        }
    }

    public void testStartsWithWrongType() throws IOException, ProcCallException {
        loadData();

        Client client = getClient();

        String query = "select * from STRINGS where val starts with id";
        try {
            client.callProcedure("@AdHoc", query);
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("incompatible data type in operation"));
        }
    }

    public void testAdHocColStartsWithCol() throws IOException, ProcCallException {
        loadData();

        Client client = getClient();

        String query = "select * from STRINGS where val starts with pat";
        VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
        assertEquals("STARTS WITH column test: bad row count:",
                ROWS - 1, result.getRowCount());
    }

    public void testAdHocStringStartsWithCol() throws IOException, ProcCallException {
        loadData();

        Client client = getClient();

        String query;

        int[] testResults = {1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 1, 2, 2};

        for (int index = 0; index < testStrings.length; index++) {
            query = String.format("select * from STRINGS where '%s' starts with pat", testStrings[index]);

            VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
            assertEquals(String.format("\"%s\": bad row count:", query),
                               testResults[index], result.getRowCount());
        }
    }

    public void testProcColStartsWithString() throws IOException, ProcCallException {
        loadData();

        Client client = getClient();

        // test if returned row numbers is the same as preset or not
        for (int index = 0; index < testStrings.length; index++) {
            VoltTable result = client.callProcedure("SelectStartsWith", testStrings[index]).getResults()[0];
            assertEquals(String.format("\"%s\": bad row count:", testStrings[index]),
                               testExpectedResults[index], result.getRowCount());
        }
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlStartsWithRegressionSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws IOException {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSqlStartsWithRegressionSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();

        final String schema =
                "create table STRINGS (" +
                "ID int default 0 not null, " +
                "VAL varchar(32) default null," +
                "PAT varchar(32) default null," +
                "PRIMARY KEY(ID));";

        project.addLiteralSchema(schema);

        project.addPartitionInfo("STRINGS", "ID");
        project.addStmtProcedure("Insert", "insert into strings values (?, ?, ?);");
        project.addStmtProcedure("SelectStartsWith", "select * from strings where val starts with ?;");
        project.addStmtProcedure("NotStartsWith", "select * from strings where val not starts with ?;");

        config = new LocalCluster("sqlstartswith-onesite.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqlstartswith-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqlstartswith-twosites.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }
}
