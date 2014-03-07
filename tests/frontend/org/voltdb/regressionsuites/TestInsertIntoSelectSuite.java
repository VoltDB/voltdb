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
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.doubledown.MultiRoundInsertIntoSelect;
import org.voltdb_testprocs.regressionsuites.doubledown.MultiRoundSelectThenInsert;

public class TestInsertIntoSelectSuite extends RegressionSuite {

    private static final Class<?>[] PROCEDURES = {
        MultiRoundInsertIntoSelect.class,
        MultiRoundSelectThenInsert.class,
    };

    public TestInsertIntoSelectSuite(String name) {
        super(name);
    }

    public void testPartitionedTableSelfCopy() throws Exception
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        ClientResponse resp;
        String insertIntoSelect = "insert into P1 (b1, a2) select b1, 'z' || a2 from P1 where b1 >= 6";

        /*/ enable for debug ...
        resp = client.callProcedure("@Explain", insertIntoSelect);
        VoltTable vt = resp.getResults()[0];
        System.out.println("DEBUGGING:\n" + vt);
        // ... enable for debug */

        resp = client.callProcedure("@AdHoc", insertIntoSelect);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountP1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(14, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("@AdHoc", "select count(*) from P1 where a2 >= 'z'");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());
    }

    public void testPartitionedTable() throws Exception
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        ClientResponse resp;
        String insertIntoSelect = "insert into P2 (a1, a2) select b1, a2 from P1 where b1 >= 6";

        /*/ enable for debug ...
        resp = client.callProcedure("@Explain", insertIntoSelect);
        VoltTable vt = resp.getResults()[0];
        System.out.println("DEBUGGING:\n" + vt);
        // ... enable for debug */

        resp = client.callProcedure("@AdHoc", insertIntoSelect);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountP1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(10, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountP2");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());
    }

    public void testReplicatedTableSelfCopy() throws Exception
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("R1.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        ClientResponse resp;
        String insertIntoSelect = "insert into R1 (b1, a2) select 100+b1, a2 from R1 where b1 >= 6";

        /*/ enable for debug ...
        resp = client.callProcedure("@Explain", insertIntoSelect);
        VoltTable vt = resp.getResults()[0];
        System.out.println("DEBUGGING:\n" + vt);
        // ... enable for debug */

        resp = client.callProcedure("@AdHoc", insertIntoSelect);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountR1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(14, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("@AdHoc", "select count(*) from R1 where b1 >= 100");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());
    }

    public void testReplicatedTable() throws Exception
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("R1.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        ClientResponse resp;
        String insertIntoSelect = "insert into R2 (a1, a2) select 100+b1, a2 from R1 where b1 >= 6";

        /*/ enable for debug ...
        resp = client.callProcedure("@Explain", insertIntoSelect);
        VoltTable vt = resp.getResults()[0];
        System.out.println("DEBUGGING:\n" + vt);
        // ... enable for debug */

        resp = client.callProcedure("@AdHoc", insertIntoSelect);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountR1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(10, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountR2");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("@AdHoc", "select count(*) from R2 where a1 >= 100");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());
    }

    public void testReplicatedFilteringIntoPartitionedTable() throws Exception
    {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("R1.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        ClientResponse resp;
        String insertIntoSelect = "insert into P2 (a1, a2) select 100+b1, a2 from R1 where b1 >= 6";

        /*/ enable for debug ...
        resp = client.callProcedure("@Explain", insertIntoSelect);
        VoltTable vt = resp.getResults()[0];
        System.out.println("DEBUGGING:\n" + vt);
        // ... enable for debug */

        resp = client.callProcedure("@AdHoc", insertIntoSelect);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountR1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(10, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountP2");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("@AdHoc", "select count(*) from P2 where a1 >= 100");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());
    }

    public void testValidatePartitionedTableSmallSelfCopyStoredProc() throws Exception
    {
        partitionedTableSelfCopyStoredProc(5);
        partitionedTableSlowSelfCopyStoredProc(5);
        final Client client = getClient();
        ClientResponse resp;
        resp = client.callProcedure("@AdHoc", "select A2, B1 from P1 order by 1, 2");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable fastResult = resp.getResults()[0];
        resp = client.callProcedure("@AdHoc", "select A2, B1 from P3 order by 1, 2");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable slowResult = resp.getResults()[0];
        while (fastResult.advanceRow()) {
            assertTrue(slowResult.advanceRow());
            assertEquals(slowResult.getString(0), fastResult.getString(0));
            assertEquals(slowResult.getLong(1), fastResult.getLong(1));
        }
        assertFalse(slowResult.advanceRow());
    }

    public void testPartitionedTableSmallSelfCopyStoredProc() throws Exception
    {
        partitionedTableSelfCopyStoredProc(5);
    }

    public void testPartitionedTableSmallSlowSelfCopyStoredProc() throws Exception
    {
        partitionedTableSlowSelfCopyStoredProc(5);
    }

    public void testPartitionedTableMediumSelfCopyStoredProc() throws Exception
    {
        partitionedTableSelfCopyStoredProc(10);
    }

    public void testPartitionedTableMediumSlowSelfCopyStoredProc() throws Exception
    {
        partitionedTableSlowSelfCopyStoredProc(10);
    }

    public void testPartitionedTableLargeSelfCopyStoredProc() throws Exception
    {
        partitionedTableSelfCopyStoredProc(15);
    }

    public void UNtestPartitionedTableLargeSlowSelfCopyStoredProc() throws Exception
    {
        partitionedTableSlowSelfCopyStoredProc(15);
    }

    // Too slow to run normally
    // public void UNtestPartitionedTableHugeSelfCopyStoredProc() throws Exception
    //{
    //    partitionedTableSelfCopyStoredProc(20);
    //}
    //
    //public void UNtestPartitionedTableHugeSlowSelfCopyStoredProc() throws Exception
    //{
    //    partitionedTableSlowSelfCopyStoredProc(20);
    //}

    public void partitionedTableSelfCopyStoredProc(long iterations) throws Exception
    {
        ClientResponse resp;
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            resp = client.callProcedure("P3.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        resp = client.callProcedure(MultiRoundInsertIntoSelect.class.getSimpleName(), iterations);
    }

    public void partitionedTableSlowSelfCopyStoredProc(long iterations) throws Exception
    {
        ClientResponse resp;
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            resp = client.callProcedure("P1.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        resp = client.callProcedure(MultiRoundSelectThenInsert.class.getSimpleName(), iterations);
    }

    public void FIXMEtestFailingProjection() throws Exception {
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("R1.insert", i, Integer.toHexString(i));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }

        ClientResponse resp;
        String insertIntoSelect = "insert into R2 (a1) select 100+b1 from R1 where b1 >= 6";

        //* enable for debug ...
        resp = client.callProcedure("@Explain", insertIntoSelect);
        VoltTable vt = resp.getResults()[0];
        System.out.println("DEBUGGING:\n" + vt);
        // ... enable for debug */

        insertIntoSelect = "insert into R2 (a1) values (9)";
        //*/ enable for debug ...
        resp = client.callProcedure("@Explain", insertIntoSelect);
        vt = resp.getResults()[0];
        System.out.println("DEBUGGING:\n" + vt);
        // ... enable for debug */

        insertIntoSelect = "insert into R2 (a2, a1) values ('x', 9)";

        //*/ enable for debug ...
        resp = client.callProcedure("@Explain", insertIntoSelect);
        vt = resp.getResults()[0];
        System.out.println("DEBUGGING:\n" + vt);
        // ... enable for debug */

        insertIntoSelect = "insert into R2 (a2, a1) select a2||'.', 100+b1 from R1 where b1 >= 6";

        resp = client.callProcedure("@AdHoc", insertIntoSelect);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountR1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(10, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("CountR2");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        resp = client.callProcedure("@AdHoc", "select count(*) from R2 where a1 >= 100");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestInsertIntoSelectSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        try {
            // a table that should generate procedures
            // use column names such that lexical order != column order.
            project.addLiteralSchema(
                    "CREATE TABLE p1(b1 BIGINT NOT NULL, a2 VARCHAR(100) NOT NULL);" +
                    "PARTITION TABLE p1 ON COLUMN b1;" +

                    "CREATE TABLE p2(a1 BIGINT NOT NULL, a2 VARCHAR(100) NOT NULL); " +
                    "PARTITION TABLE p2 ON COLUMN a1;" +
                    "CREATE UNIQUE INDEX p2_tree_idx ON p2(a1);" +

                    "CREATE TABLE p3(b1 BIGINT NOT NULL, a2 VARCHAR(100) NOT NULL);" +
                    "PARTITION TABLE p3 ON COLUMN b1;" +

                    "CREATE TABLE r1(b1 BIGINT NOT NULL, a2 VARCHAR(100) NOT NULL, PRIMARY KEY (b1));" +

                    "CREATE TABLE r2(a1 BIGINT NOT NULL, a2 VARCHAR(100) DEFAULT 'xyz' NOT NULL, PRIMARY KEY (a1));" +

                    "CREATE PROCEDURE CountP1 AS select count(*) from p1;" +
                    "CREATE PROCEDURE CountP2 AS select count(*) from p2;" +
                    "CREATE PROCEDURE CountR1 AS select count(*) from r1;" +
                    "CREATE PROCEDURE CountR2 AS select count(*) from r2;" +
                    "");
            project.addProcedures(PROCEDURES);
        } catch (IOException error) {
            fail(error.getMessage());
        }

        // JNI
        config = new LocalCluster("iisf-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        //*/ CLUSTER (disable to opt for speed over coverage...
        config = new LocalCluster("iisf-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);
        // ... disable for speed) */
        return builder;
    }
}
