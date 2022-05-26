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

public class TestCRUDSuite extends RegressionSuite {

    static final Class<?>[] PROCEDURES = {};

    public TestCRUDSuite(String name) {
        super(name);
    }

    public void testNegativeWrongTypeParam() throws Exception {
        final Client client = this.getClient();
        ClientResponse resp = client.callProcedure("P5.insert", -1000);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        resp = client.callProcedure("@AdHoc", "select * from p5;");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(1, resp.getResults().length);
        VoltTable results = resp.getResults()[0];
        assertEquals(1, results.getRowCount());
        assertEquals(1, results.getColumnCount());
        assertEquals(-1000, results.fetchRow(0).getLong(0));
    }

    public void testPartitionedPkPartitionCol() throws Exception
    {
        final Client client = this.getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.select", i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            VoltTable vt = resp.getResults()[0];
            assertTrue(vt.advanceRow());
            assertEquals(i, vt.getLong(0));
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.update", i, "STR" + Integer.toHexString(i), i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.select", i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            VoltTable vt = resp.getResults()[0];
            assertTrue(vt.advanceRow());
            assertTrue(vt.getString(1).equals("STR" + Integer.toHexString(i)));
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.delete", i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        ClientResponse resp = client.callProcedure("CountP1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(0, resp.getResults()[0].asScalarLong());
    }

    public void testMultiColPk() throws Exception
    {
        // P4(z INTEGER, x VARCHAR(10), y INTEGER,
        // PRIMARY KEY(y, x, z));"

        // z = i
        // x = hex(i)
        // y = i *100     (partition key)

        final Client client = this.getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P4.insert", i, Integer.toHexString(i), i * 100);  // z,x,y
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P4.select", i*100, Integer.toHexString(i), i); // y,x,z
            VoltTable vt = resp.getResults()[0];
            assertTrue(vt.advanceRow());
            assertEquals(i, vt.getLong(0));
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P4.update",
                    i*10, "STR" + Integer.toHexString(i), i*100, // z,x,y (update / table order)
                    i*100, Integer.toHexString(i), i);           // y,x,z (search / index order)
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P4.select", i*100, "STR" + Integer.toHexString(i), i*10); // y,x,z
            VoltTable vt = resp.getResults()[0];
            assertTrue(vt.advanceRow());
            assertEquals(i*10, vt.getLong(0));
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P4.delete", i*100, "STR" + Integer.toHexString(i), i*10); // y,x,z
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

    }

    public void testPartitionedPkWithoutPartitionCol() throws Exception
    {
        Client client = getClient();
        try {
            client.callProcedure("P2.delete", 0, "ABC");
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("was not found"));
            return;
        }
        fail();
    }

    public void testPartitionedPkWithoutPartitionCol2() throws Exception
    {
        Client client = getClient();
        try {
            client.callProcedure("P3.delete", 0, "ABC");
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("was not found"));
            return;
        }
        fail();
    }

    public void testReplicatedTable() throws Exception
    {
        final Client client = this.getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("R1.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        try {
            client.callProcedure("R1.select", 0, "ABC");
            fail();
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("was not found"));
            return;
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("R1.update", i, "STR" + Integer.toHexString(i), i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("R1.delete", i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        ClientResponse resp = client.callProcedure("CountR1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(0, resp.getResults()[0].asScalarLong());
    }


    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCRUDSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        project.addStmtProcedure("CountP1", "select count(*) from p1;");
        project.addStmtProcedure("CountR1", "select count(*) from r1;");

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
                    "CREATE ASSUMEUNIQUE INDEX p3_tree_idx ON p3(a1); " +
                    "PARTITION TABLE P3 ON COLUMN a2;"
            );

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

            // table with a bigint pkey
            project.addLiteralSchema(
                    "CREATE TABLE p5(x BIGINT NOT NULL, PRIMARY KEY(x));"
            );
            project.addPartitionInfo("p5", "x");

        } catch (IOException error) {
            fail(error.getMessage());
        }

        // JNI
        config = new LocalCluster("crud-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        // CLUSTER
        config = new LocalCluster("crud-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);

        return builder;
    }
}
