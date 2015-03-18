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
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.DeploymentBuilder;

public class TestSPBasecaseSuite extends RegressionSuite {
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
        String literalSchema =
                // a table that should generate procedures
                // use column names such that lexical order != column order.
                "CREATE TABLE p1(b1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (b1));" +
                "PARTTITION TABLE p1 ON COLUMN b1;\n" +
                // a partitioned table that should not generate procedures (no pkey)
                "CREATE TABLE p2(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL); " +
                "CREATE UNIQUE INDEX p2_tree_idx ON p2(a1);" +
                "PARTTITION TABLE p2 ON COLUMN a1;\n" +
                // a partitioned table that should not generate procedures (pkey not partition key)
                "CREATE TABLE p3(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL); " +
                "CREATE ASSUMEUNIQUE INDEX p3_tree_idx ON p3(a1);" +
                "PARTTITION TABLE p3 ON COLUMN a2;\n" +
                // a replicated table.
                "CREATE TABLE r1(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (a1));" +
                // table with a multi-column pkey. verify that pkey column ordering is
                // in the index column order, not table order and not lex. order
                "CREATE TABLE p4(z INTEGER NOT NULL, x VARCHAR(10) NOT NULL, y INTEGER NOT NULL," +
                " PRIMARY KEY(y,x,z));" +
                "PARTTITION TABLE p4 ON COLUMN y;\n" +
                "";
        return multiClusterSuiteBuilder(TestSPBasecaseSuite.class, literalSchema,
                new DeploymentBuilder(),
                new DeploymentBuilder(2, 3, 1));
    }
}
