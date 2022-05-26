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
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSPBasecaseSuite extends RegressionSuite {

    static final Class<?>[] PROCEDURES = {};

    public TestSPBasecaseSuite(String name) {
        super(name);
    }

    public void testSPBasecases() throws Exception
    {
        final Client client = this.getClient();

        // insert and verify with select.
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.insert", i, Integer.toHexString(i));
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            assertTrue(resp.getResults()[0].asScalarLong() == 1);
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.select", i);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable vt = resp.getResults()[0];
            assertTrue(vt.advanceRow());
            assertTrue(vt.getLong(0) == i);
        }

        // produce constraint violations.
        for (int i=0; i < 10; i++) {
            try {
                client.callProcedure("P1.insert", i, Integer.toHexString(i));
            } catch (ProcCallException pce) {
                continue;
            }
            assertTrue("Failed to catch expected exception.", false);
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.select", i);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable vt = resp.getResults()[0];
            assertTrue(vt.advanceRow());
            assertTrue(vt.getLong(0) == i);
        }

        //  perform updates and verify with select.
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.update", i, "STR" + Integer.toHexString(i), i);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            assertTrue(resp.getResults()[0].asScalarLong() == 1);
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.select", i);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable vt = resp.getResults()[0];
            assertTrue(vt.advanceRow());
            assertTrue(vt.getString(1).equals("STR" + Integer.toHexString(i)));
        }

        // delete.
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.delete", i);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            assertTrue(resp.getResults()[0].asScalarLong() == 1);
        }
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSPBasecaseSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        // necessary because at least one procedure is required
        project.addStmtProcedure("CountP1", "select count(*) from p1;");

        try {
            // a table that should generate procedures
            // use column names such that lexical order != column order.
            project.addLiteralSchema(
                    "CREATE TABLE p1(b1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (b1));"
            );
            project.addPartitionInfo("p1", "b1");

            // a partitioned table that should not generate procedures (no pkey)
            project.addLiteralSchema(
                    "CREATE TABLE p2(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL); " +
                    "CREATE UNIQUE INDEX p2_tree_idx ON p2(a1);"
            );
            project.addPartitionInfo("p2", "a1");

            // a partitioned table that should not generate procedures (pkey not partition key)
            project.addLiteralSchema(
                    "CREATE TABLE p3(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL); " +
                    "CREATE ASSUMEUNIQUE INDEX p3_tree_idx ON p3(a1);"
            );
            project.addPartitionInfo("p3", "a2");

            // a replicated table (should not generate procedures).
            project.addLiteralSchema(
                    "CREATE TABLE r1(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (a1));"
            );

            // table with a multi-column pkey. verify that pkey column ordering is
            // in the index column order, not table order and not index column lex. order
            project.addLiteralSchema(
                    "CREATE TABLE p4(z INTEGER NOT NULL, x VARCHAR(10) NOT NULL, y INTEGER NOT NULL, PRIMARY KEY(y,x,z));"
            );
            project.addPartitionInfo("p4", "y");

        } catch (IOException error) {
            fail(error.getMessage());
        }

        // JNI
        config = new LocalCluster("sqltypes-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        // CLUSTER
        config = new LocalCluster("sqltypes-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);

        return builder;

    }

}
