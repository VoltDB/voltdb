/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestMaterializedViewNonemptyTablesSuite extends RegressionSuite {
    private static final String UNSAFE_OPS_STRING
        = "because the view definition uses operations that cannot always be applied";

    public TestMaterializedViewNonemptyTablesSuite(String name) {
        super(name);
    }

    private void testOperation(Client client, String op, boolean wantSuccess) throws Exception {
        String sql = String.format("create view mv as select a, count(*), %s from safeTestFull group by a", op);
        client.callProcedure("@AdHoc", "drop view mv if exists");
        boolean success = false;
        try {
            client.callProcedure("@AdHoc", sql);
            success = true;
        } catch (Exception ex) {
            success = false;
            // If we expected failure and we actually
            // got failure, then verify that the exception
            // is one we expect.
            if ( ! wantSuccess ) {
                assertTrue(ex.getMessage().contains(UNSAFE_OPS_STRING));
            }
        }
        client.callProcedure("@AdHoc", "drop view mv if exists");
        assertEquals("Unexpected result", wantSuccess, success);
    }

    /*
     * I'm not sure how to test these.
     */
    private void testBoolOperation(Client client, String op, boolean wantSuccess) throws Exception {
    }

    private void testSafeOperation(Client client, String op) throws Exception {
        testOperation(client, op, true);
    }

    private void testUnsafeOperation(Client client, String op) throws Exception {
        testOperation(client, op, false);
    }

    private void testUnsafeBoolOperation(Client client, String op) throws Exception {
        testBoolOperation(client, op, false);
    }

    private void testSafeBoolOperation(Client client, String op) throws Exception {
        testBoolOperation(client, op, true);
    }

    private void populateSafeTables(Client client) throws Exception {
        client.callProcedure("SAFETESTFULL.insert", 300, "mumble", 301, "marble");
    }

    public void testSafeOperations() throws Exception {
        Client client = getClient();
        populateSafeTables(client);
        //
        // ExpressionType operations.
        //
        // Unsafe Operations
        testUnsafeOperation(client, "MIN(b + b)");
        testUnsafeOperation(client, "MIN(b - b)");
        testUnsafeOperation(client, "MIN(b * b)");
        testUnsafeOperation(client, "MIN(b / b)");
        testUnsafeOperation(client, "MIN(MOD(b, b))");
        testUnsafeOperation(client, "MIN(cast(b as integer))");
        testUnsafeOperation(client, "SUM(a)");

        // Unsafe Boolean Operations
        testUnsafeBoolOperation(client, "bs like 'abc'");
        testUnsafeBoolOperation(client, "bs starts with 'abc'");

        // Safe Boolean Operations
        testSafeBoolOperation(client, "b = b");
        testSafeBoolOperation(client, "b <> b");
        testSafeBoolOperation(client, "b < b");
        testSafeBoolOperation(client, "b > b");
        testSafeBoolOperation(client, "b <= b");
        testSafeBoolOperation(client, "b >= b");
        testSafeBoolOperation(client, "b not distinct b");
        testSafeBoolOperation(client, "(a = b) and (b = a)");
        testSafeBoolOperation(client, "(a = b) or (b = a)");

        // Safe Operations
        //
        testSafeOperation(client, "MIN(100)");
        testSafeOperation(client, "MAX(b)");
        testSafeOperation(client, "MIN(b)");
    }

    // Note: This is here only to test the actual behavior with
    //       empty and non-empty tables.
    public void testTablePopulationBehavior() throws Exception {
        Client client = getClient();

        // In all these tests we test the create view DML operation
        // on a set of all empty tables, then on a set of some empty
        // some populated tables, then a set of all empty tables again.
        // The tables alpha and beta are sometimes populated, and the
        // table empty is never populated.

        testCreateView(client,
                       "create view vv1 as select a, count(*), max(b + b) from alpha group by a",
                       UNSAFE_OPS_STRING);
        testCreateView(client,
                       "create view vv2 as select a, max(b + b), count(*) from alpha group by a",
                       UNSAFE_OPS_STRING);
        testCreateView(client,
                       "create view vv3 as select a, count(*), max(b + b), count(*) from alpha group by a",
                       UNSAFE_OPS_STRING);
        // This should fail if alpha and beta are both populated, as
        // they are in the second test case.
        testCreateView(client,
                       "create view vv4 as select alpha.a, count(*), max(beta.b + alpha.b) from alpha, beta group by alpha.a",
                       UNSAFE_OPS_STRING);
        testCreateView(client,
                       "create view vv4 as select alpha.a, count(*) from alpha join beta on alpha.a / alpha.a < 1 group by alpha.a;",
                       UNSAFE_OPS_STRING);
        testCreateView(client,
                       "create view vv4 as select alpha.a, count(*) from alpha, beta where alpha.a / alpha.a < 1 group by alpha.a;",
                       UNSAFE_OPS_STRING);
        testCreateView(client,
                       "create view vv4 as select alpha.a/beta.b, count(*) from alpha, beta group by alpha.a/beta.b;",
                       UNSAFE_OPS_STRING);
        testCreateView(client,
                       "create view vv4 as select alpha.a/beta.b, sum(alpha.a/beta.b), count(*) from alpha, beta group by alpha.a/beta.b;",
                       UNSAFE_OPS_STRING);
        testCreateView(client,
                       "create view vv4 as select alpha.a/beta.b, count(*), sum(alpha.a/beta.b), count(*) from alpha, beta group by alpha.a/beta.b;",
                       UNSAFE_OPS_STRING);
        // This should succeed always, since the table empty is always
        // empty.
        testCreateView(client,
                       "create view vv5 as select alpha.a, count(*), max(empty.b + empty.b) from alpha, empty group by alpha.a",
                       null);
        testCreateView(client,
                       "create view vv6 as select a, count(*), max(b) from alpha group by a",
                       null);
        testCreateView(client,
                       "create view vv7 as select a, max(b), count(*) from alpha group by a",
                       null);
        testCreateView(client,
                       "create view vv8 as select a, min(b), count(*), max(b), count(*) from alpha group by a",
                       null);
    }

    private static final String[] TABLE_NAMES = new String[] {
        "alpha",
        "beta",
        "empty"
    };

    /**
     * Test to see if we can create a view.  The view name is
     * necessary because we drop the view after creation.
     *
     * @param client The Client object.
     * @param sql The sql string.  This should start "create view name ",
     *            where "name" is the view name.
     * @param expectedDiagnostic The expected diagnostic string.  This
     *                           should be null if the creation is
     *                           expected to succeed.
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    private void testCreateView(Client client,
                                String sql,
                                String expectedDiagnostic)
            throws IOException, NoConnectionsException, ProcCallException {
        ClientResponse cr = null;
        assertTrue("SQL string should start with \"create view \"",
                   sql.startsWith("create view "));
        int pos = sql.indexOf(" ", 12);
        String viewName = sql.substring(12, pos);
        // Try to drop the view.  Even if it doesn't
        // exist, this should succeed.
        dropView(client, viewName);
        // Truncate all the tables.
        truncateTables(client, TABLE_NAMES);
        // Now try to create the view with empty tables.
        // This should always succeed.
        try {
            cr = client.callProcedure("@AdHoc", sql);
        } catch (Exception ex) {
            fail("Unexpected exception: \"" + ex.getMessage() + "\"");
        }
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Drop the view and populate the tables.
        dropView(client, viewName);
        populateTables(client);

        // Try the view creation again.  This may or may
        // not succeed.
        boolean expectedSuccess = (expectedDiagnostic == null);
        try {
            cr = client.callProcedure("@AdHoc", sql);
            if ( ! expectedSuccess) {
                fail("Unexpected SQL compilation success");
            }
        } catch (ProcCallException ex) {
            cr = ex.getClientResponse();
            if ( expectedSuccess ) {
                fail(String.format("Unexpected SQL compilation failure:\n%s",
                                   cr.getStatusString()));
            }
            assertTrue(String.format("Did not find \"%s\" in diagnostic message \"%s\"",
                                     expectedDiagnostic,
                                     cr.getStatusString()),
                       cr.getStatusString().contains(expectedDiagnostic));
        }
        // Drop the view..
        dropView(client, viewName);
        truncateTables(client, TABLE_NAMES);
        // Try creating the view again.  Sometimes after a
        // population and then truncation things go awry.
        // Again, this should always succeed.
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals("View creation on empty tables should always succeed.",
                     ClientResponse.SUCCESS, cr.getStatus());
        dropView(client, viewName);
    }

    public void testDropView() throws Exception {
        // Regression test for ENG-11497
        Client client = getClient();
        String ddl =
                "CREATE TABLE T_ENG_11497_1 (\n"
                        + "   AID integer NOT NULL,\n"
                        + "   USD float DEFAULT '0.0' NOT NULL,\n"
                        + "   PRIMARY KEY (AID)\n"
                        + ");\n"
                        + "PARTITION TABLE T_ENG_11497_1 ON COLUMN AID;\n"

                        + "CREATE TABLE T_ENG_11497_2 (\n"
                        + "   AID integer NOT NULL,\n"
                        + "   USD float DEFAULT '0.0' NOT NULL,\n"
                        + "   PRIMARY KEY (AID)\n"
                        + ");\n"
                        + "PARTITION TABLE T_ENG_11497_2 ON COLUMN AID;\n"

                        + "CREATE VIEW T_ENG_11497_1_VIEW\n"
                        + "AS\n"
                        + "   SELECT\n"
                        + "        AID,\n"
                        + "        COUNT(*) AS IGNOREME,\n"
                        + "        SUM(CAST(USD AS DECIMAL)) AS USD\n"
                        + "FROM T_ENG_11497_1\n"
                        + "GROUP BY\n"
                        + "      AID;\n"

                        + "CREATE VIEW T_ENG_11497_2_VIEW\n"
                        + "AS\n"
                        + "   SELECT\n"
                        + "        t1.AID,\n"
                        + "        COUNT(*) AS IGNOREME,\n"
                        + "        SUM(CAST(t1.USD AS DECIMAL)) AS USD\n"
                        + "FROM T_ENG_11497_1 as t1 inner join T_ENG_11497_2 as t2\n"
                        + "  ON t1.AID = t2.AID\n"
                        + "GROUP BY\n"
                        + "      t1.AID;\n"

                        // count(*) anywhere in materialized views
                        + "CREATE VIEW T_ENG_10945_1_VIEW\n"
                        + "AS\n"
                        + "   SELECT\n"
                        + "        AID,\n"
                        + "        MIN(AID),\n"
                        + "        COUNT(*) AS IGNOREME,\n"
                        + "        SUM(CAST(USD AS DECIMAL)) AS USD\n"
                        + "FROM T_ENG_11497_1\n"
                        + "GROUP BY\n"
                        + "      AID;\n"

                        // multiple count(*) anywhere in materialized views
                        + "CREATE VIEW T_ENG_10945_2_VIEW\n"
                        + "AS\n"
                        + "   SELECT\n"
                        + "        AID,\n"
                        + "        MIN(AID),\n"
                        + "        COUNT(*) AS IGNOREME,\n"
                        + "        MAX(AID),\n"
                        + "        COUNT(*) AS IGNOREME1,\n"
                        + "        SUM(CAST(USD AS DECIMAL)) AS USD\n"
                        + "FROM T_ENG_11497_1\n"
                        + "GROUP BY\n"
                        + "      AID;\n"
                        ;

        // Create some tables and some views
        client.callProcedure("@AdHoc", ddl);

        // Insert into the source tables so they're non-empty
        client.callProcedure("@AdHoc", "insert into T_ENG_11497_1 values (0, 10.0);\n");
        client.callProcedure("@AdHoc", "insert into T_ENG_11497_1 values (1, 10.0);\n");
        client.callProcedure("@AdHoc", "insert into T_ENG_11497_2 values (0, 10.0);\n");
        client.callProcedure("@AdHoc", "insert into T_ENG_11497_2 values (1, 10.0);\n");

        ClientResponse cr;

        // Make sure it's possible to drop the views
        cr = client.callProcedure("@AdHoc", "drop view T_ENG_11497_1_VIEW;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "drop view T_ENG_11497_2_VIEW;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "drop view T_ENG_10945_1_VIEW;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "drop view T_ENG_10945_2_VIEW;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "drop table T_ENG_11497_1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "drop table T_ENG_11497_2;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void populateTables(Client client) throws IOException, NoConnectionsException, ProcCallException {
        ClientResponse cr;
        cr = client.callProcedure("ALPHA.insert", 100, 101);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("BETA.insert",  200, 201);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // Leave the table EMPTY empty for now.
    }

    /**
     * Drop a view, and test that the result is success.
     * @param client The client.
     * @param viewName The name of the view.
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    private void dropView(Client client, String viewName)
            throws IOException, NoConnectionsException, ProcCallException {
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc",
                                  String.format("drop view %s if exists;",
                                                viewName));
        assertEquals("Should be able to drop a view.",
                     ClientResponse.SUCCESS,
                     cr.getStatus());
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMaterializedViewNonemptyTablesSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestMaterializedViewNonemptyTablesSuite.class.getResource("testmvnonemptytables-ddl.sql"));

        // JNI
        config = new LocalCluster("testMaterializedViewNonemptyTables-onesite.jar", 1, 1, 0,
                BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        return builder;
    }
}
