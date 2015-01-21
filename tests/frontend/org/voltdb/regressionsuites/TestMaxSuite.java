/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.DeploymentBuilder;

public class TestMaxSuite extends RegressionSuite {
    private static int SQL_TEXT_MAX_LENGTH = 100000;
    private static String LONG_STRING_TEMPLATE = "This is a long string to test. It will make the client easier "
            + "to generate very long long string.";
    private static int APPEND_TIMES = SQL_TEXT_MAX_LENGTH / LONG_STRING_TEMPLATE.length();

    private static int PARAMETERS_MAX_JOIN = 100;
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
        Client client = this.getClient();
        StringBuilder stringBuilder = new StringBuilder(
                "select * from max_in_table where column0 in(");
        for (int i = 0; i < SQL_TEXT_MAX_LENGTH; i++) {
            stringBuilder.append(i);
            if (i != SQL_TEXT_MAX_LENGTH - 1) {
                stringBuilder.append(",");
            }
        }
        stringBuilder.append(") order by column0;");

        try {
            client.callProcedure("@AdHoc", stringBuilder.toString());
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("AdHoc SQL text exceeds the length limitation 32767"));
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

    public void testMaxJoin() throws Exception {
        final Client client = this.getClient();

        ClientResponse resp = null;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < PARAMETERS_MAX_JOIN; i++) {
            resp = client.callProcedure("MAX_JOIN_TABLE" + i + ".insert", 1, 1);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        }

        sb = new StringBuilder("select * from ");
        for (int i = 0; i < PARAMETERS_MAX_JOIN; i++) {
            sb.append("max_join_table");
            sb.append(i);
            if (i != PARAMETERS_MAX_JOIN - 1) {
                sb.append(",");
            }
        }

        resp = client.callProcedure("@AdHoc", sb.toString());
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        assertEquals(1, resp.getResults().length);
        VoltTable results = resp.getResults()[0];

        assertEquals(1, results.getRowCount());
        assertEquals(PARAMETERS_MAX_JOIN * 2, results.getColumnCount());
        assertEquals(1, results.fetchRow(0).getLong(0));
    }

    static public junit.framework.Test suite() {
        StringBuilder sb = new StringBuilder(
                /** for max in */
                "CREATE TABLE max_in_table(column0 INTEGER NOT NULL, column1 INTEGER NOT NULL, " +
                        "PRIMARY KEY (column0));\n" +
                "PARTITION TABLE max_in_table ON COLUMN column0;\n" +
                /** for max column */
                "CREATE TABLE max_column_table(\n" +
                "");
        for (int i = 0; i < PARAMETERS_MAX_COLUMN; i++) {
            sb.append("column").append(i).append(" INTEGER NOT NULL,\n");
        }
        sb.append(" PRIMARY KEY(column0));\n" +
                "PARTITION TABLE max_column_table ON COLUMN column0;\n" +
                "" +
                /** for max parameter or SQL text */
                "CREATE TABLE max_sql_table(column0 VARCHAR(1048576) NOT NULL, " +
                " PRIMARY KEY(column0));\n" +
                "CREATE PROCEDURE max_sql_proc AS " +
                " SELECT column0 as c1 from max_sql_table where column0 = ' ");
        for (int i = 0; i < APPEND_TIMES; i++) {
            sb.append(LONG_STRING_TEMPLATE);
        }
        sb.append("';\n" +
                "CREATE PROCEDURE max_parameter_proc AS " +
                " SELECT column0 as c2 from max_sql_table where column0 = ?;\n");

        /** for max join */
        for (int i = 0; i < PARAMETERS_MAX_JOIN; i++) {
            sb.append("create table max_join_table")
            .append(i)
            .append("(column0 INTEGER NOT NULL, column1 INTEGER NOT NULL, PRIMARY KEY (column0));\n");
        }

        String literalSchema = sb.toString();
        return multiClusterSuiteBuilder(TestMaxSuite.class, literalSchema,
                new DeploymentBuilder(),
                DeploymentBuilder.forHSQLBackend(),
                new DeploymentBuilder(2,3,1));
    }
}
