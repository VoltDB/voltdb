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

import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestMaxSuite extends RegressionSuite {

    static final Class<?>[] PROCEDURES = {};
    private static int SQL_TEXT_MAX_LENGTH = 100000;
    private static int SQL_LITERAL_MAX_ELEMENT = Short.MAX_VALUE; // literal element limit to 65535
    private static int SQL_LITERAL_MAX_LENGTH = 1024*1024; // literal length limit to 1MB
    private static String LONG_STRING_TEMPLATE = "This is a long string to test. It will make the client easier "
            + "to generate very long long string.";
    private static int APPEND_TIMES = SQL_TEXT_MAX_LENGTH / LONG_STRING_TEMPLATE.length();
    private static int PARAMETERS_MAX_JOIN = 50;
    private static int PARAMETERS_MAX_COLUMN = 1024;
    private static int PARAMETERS_MAX_IN = 6000;

    public TestMaxSuite(String name) {
        super(name);
    }

    public void testMaxSQLLength() throws Exception {
        Client client = this.getClient();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < APPEND_TIMES; i++) {
            stringBuilder.append(LONG_STRING_TEMPLATE);
        }

        client.callProcedure("max_sql_proc");

        client.callProcedure("max_parameter_proc", stringBuilder.toString());
    }

    public void testAdHocMaxSQLText() throws Exception {
        if (isValgrind()) {
            return;
        }

        Client client = this.getClient();
        StringBuilder stringBuilder = new StringBuilder(
                "select * from max_in_table where column0 in(");
        for (int i = 0; i < SQL_LITERAL_MAX_ELEMENT; i++) {
            stringBuilder.append(i);
            if (i != SQL_LITERAL_MAX_ELEMENT - 1) {
            stringBuilder.append(", ");
            }
        }
        stringBuilder.append(") order by column0;");
        assert(stringBuilder.length() > Short.MAX_VALUE);        // previous limit
        assert(stringBuilder.length() < SQL_LITERAL_MAX_LENGTH); // new limit due to ENG-10059
        try {
            VoltTable result = client.callProcedure("@AdHoc", stringBuilder.toString()).getResults()[0];
            assertEquals(0, result.getRowCount());
        } catch(Exception ex) {
            fail();
        }
    }

    public void testMaxIn() throws Exception {
        final Client client = this.getClient();

        ClientResponse resp = null;
        for (int i = 0; i < 10; i++) {
            resp = client.callProcedure("MAX_IN_TABLE.insert", i, i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        }

        StringBuilder stringBuilder = new StringBuilder(
                "select * from max_in_table where column0 in(");
        for (int i = 0; i < PARAMETERS_MAX_IN; i++) {
            stringBuilder.append(i);
            if (i != PARAMETERS_MAX_IN - 1) {
                stringBuilder.append(",");
            }
        }
        stringBuilder.append(") order by column0;");
        resp = client.callProcedure("@AdHoc", stringBuilder.toString());
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        assertEquals(1, resp.getResults().length);
        VoltTable results = resp.getResults()[0];
        int rowCount = results.getRowCount();
        assertEquals(10, rowCount);
        assertEquals(2, results.getColumnCount());
        for (int i = 0; i < rowCount; i++) {
            assertEquals(i, results.fetchRow(i).getLong(0));
        }
    }

    public void testMaxColumn() throws Exception {
        final Client client = this.getClient();

        ClientResponse resp = null;

        StringBuilder sb = new StringBuilder(
                "insert into max_column_table values(");
        for (int i = 0; i < PARAMETERS_MAX_COLUMN; i++) {
            sb.append(i);
            if (i != PARAMETERS_MAX_COLUMN - 1) {
                sb.append(",");
            }
        }
        sb.append(");");
        resp = client.callProcedure("@AdHoc", sb.toString());

        sb = new StringBuilder("select  ");
        for (int i = 0; i < PARAMETERS_MAX_COLUMN; i++) {
            sb.append("column");
            sb.append(i);
            if (i != PARAMETERS_MAX_COLUMN - 1) {
                sb.append(",");
            }
        }
        sb.append(" from max_column_table order by column0;");
        resp = client.callProcedure("@AdHoc", sb.toString());
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        assertEquals(1, resp.getResults().length);
        VoltTable results = resp.getResults()[0];

        assertEquals(1, results.getRowCount());
        assertEquals(PARAMETERS_MAX_COLUMN, results.getColumnCount());
        assertEquals(0, results.fetchRow(0).getLong(0));
    }

    private Pair<ClientResponse, String> testMaxJoin(int maxJoins) throws Exception {
        final Client client = this.getClient();

        ClientResponse resp = null;
        String errMessage = null;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < maxJoins; i++) {
            resp = client.callProcedure("MAX_JOIN_TABLE" + i + ".insert", 1, 1);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        }

        sb = new StringBuilder("select * from ");
        for (int i = 0; i < maxJoins; i++) {
            sb.append("max_join_table");
            sb.append(i);
            if (i != maxJoins - 1) {
                sb.append(",");
            }
        }

        try {
            resp = client.callProcedure("@AdHoc", sb.toString());
        } catch (Exception ex) {
            errMessage = ex.getMessage();
        }
        return Pair.of(resp, errMessage);
    }

    public void testMaxJoinSuccess() throws Exception {
        Pair<ClientResponse, String> result = testMaxJoin(PARAMETERS_MAX_JOIN);
        ClientResponse resp = result.getFirst();
        assertNotNull(resp);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        assertEquals(1, resp.getResults().length);
        VoltTable results = resp.getResults()[0];

        assertEquals(1, results.getRowCount());
        assertEquals(PARAMETERS_MAX_JOIN * 2, results.getColumnCount());
        assertEquals(1, results.fetchRow(0).getLong(0));
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestMaxSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        try {
            StringBuilder sb;

            /** for max parameter or SQL text */
            sb = new StringBuilder(
                    "CREATE TABLE max_sql_table(column0 VARCHAR(1048576) NOT NULL,"
                    + "PRIMARY KEY(column0)); ");
            sb.append("CREATE PROCEDURE max_sql_proc AS "
                    + "SELECT column0 as c1 from max_sql_table where column0 = ' ");
            for (int i = 0; i < APPEND_TIMES; i++) {
                sb.append(LONG_STRING_TEMPLATE);
            }
            sb.append("';");

            sb.append("CREATE PROCEDURE max_parameter_proc AS "
                    + "SELECT column0 as c2 from max_sql_table where column0 = ?;");
            project.addLiteralSchema(sb.toString());

            /** for max column */
            sb = new StringBuilder(
                    "CREATE TABLE max_column_table(");
            for (int i = 0; i < PARAMETERS_MAX_COLUMN; i++) {
                sb.append("column");
                sb.append(i);
                sb.append(" INTEGER NOT NULL,");
            }
            sb.append("PRIMARY KEY(column0));");
            project.addLiteralSchema(sb.toString());
            project.addPartitionInfo("max_column_table", "column0");

            /** for max join */
            sb = new StringBuilder();
            for (int i = 0; i < PARAMETERS_MAX_JOIN; i++) {
                sb.append("create table max_join_table");
                sb.append(i);
                sb.append("(column0 INTEGER NOT NULL, column1 INTEGER NOT NULL, PRIMARY KEY (column0));");
                project.addLiteralSchema(sb.toString());
                // project.addPartitionInfo("p1", "b1");
                sb.setLength(0);
            }

            /** for max in */
            project.addLiteralSchema("CREATE TABLE max_in_table(column0 INTEGER NOT NULL, column1 INTEGER NOT NULL, PRIMARY KEY (column0));");
            project.addPartitionInfo("max_in_table", "column0");

        } catch (IOException error) {
            fail(error.getMessage());
        }

        // JNI
        config = new LocalCluster("testMax-onesite.jar", 1, 1, 0,
                BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        config = new LocalCluster("testMax-hsql.jar", 1, 1, 0,
                BackendTarget.HSQLDB_BACKEND);
        boolean success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CLUSTER
        config = new LocalCluster("testMax-cluster.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);

        return builder;
    }
}
