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

public class TestInsertIntoSelectSuite extends RegressionSuite {

    public TestInsertIntoSelectSuite(String name) {
        super(name);
    }

    static final String vcDefault = "dachshund";
    static final long intDefault = 121;

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestInsertIntoSelectSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        try {
                project.addLiteralSchema(
                                "CREATE TABLE target_p (bi bigint not null," +
                                                           "vc varchar(100) default '" + vcDefault +"'," +
                                                           "ii integer default " + intDefault + "," +
                                                           "ti tinyint default " + intDefault + ");" +
                            "partition table target_p on column bi;" +

                                "CREATE TABLE source_p1 (bi bigint not null," +
                                           "vc varchar(100)," +
                                           "ii integer," +
                                           "ti tinyint);" +
                                           "partition table source_p1 on column bi;" +

                                "CREATE TABLE source_p2 (bi bigint not null," +
                                           "vc varchar(100)," +
                                           "ii integer," +
                                           "ti tinyint);" +
                                           "partition table source_p2 on column bi;" +

                            "create procedure insert_p_source_p as insert into target_p (bi, vc, ii, ti) select * from source_p1 where bi = ?;" +
                            "partition procedure insert_p_source_p on table target_p column bi;" +

                            "create procedure insert_p_use_defaults as insert into target_p (bi, ti) select bi, ti from source_p1 where bi = ?;" +
                            "partition procedure insert_p_use_defaults on table target_p column bi;" +

                            "create procedure insert_p_use_defaults_reorder as insert into target_p (ti, bi) select ti, bi from source_p1 where bi = ?;" +
                            "partition procedure insert_p_use_defaults_reorder on table target_p column bi;" +

                            "create procedure CountTargetP as select count(*) from target_p;" +

                                        "create procedure InsertIntoSelectWithJoin as " +
                                                "insert into target_p " +
                                                        "select sp1.bi, sp1.vc, sp2.ii, sp2.ti " +
                                                        "from source_p1 as sp1 inner join source_p2 as sp2 on sp1.bi = sp2.bi and sp1.ii = sp2.ii " +
                                                        "where sp1.bi = ?;" +
                                        "partition procedure InsertIntoSelectWithJoin on table target_p column bi;" +
                                "");
        } catch (IOException error) {
            fail(error.getMessage());
        }

        // JNI
        config = new LocalCluster("iisf-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        // CLUSTER (disable to opt for speed over coverage...
        config = new LocalCluster("iisf-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);
        // ... disable for speed) */
        return builder;
    }

    private static void initializeTables(Client client) throws Exception {

        ClientResponse resp = client.callProcedure("@AdHoc", "delete from source_p1");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        resp = client.callProcedure("@AdHoc", "delete from source_p2");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        resp = client.callProcedure("@AdHoc", "delete from target_p");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());


        for (int i=0; i < 10; i++) {

            resp = client.callProcedure("SOURCE_P1.insert", i, Integer.toHexString(i), i, i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P1.insert", i, Integer.toHexString(-i), -i, -i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P1.insert", i, Integer.toHexString(i * 11), i * 11, i * 11);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P1.insert", i, Integer.toHexString(i * -11), i * -11, i * -11);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            int j = i + 5;

            resp = client.callProcedure("SOURCE_P2.insert", j, Integer.toHexString(j), j, j);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P2.insert", j, Integer.toHexString(-j), -j, -j);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P2.insert", j, Integer.toHexString(j * 11), j * 11, (j * 11) % 128);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = client.callProcedure("SOURCE_P2.insert", j, Integer.toHexString(j * -11), j * -11, -((j * 11) % 128));
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        }
    }

    public void testPartitionedTableSimple() throws Exception
    {
        final Client client = getClient();

        initializeTables(client);

        ClientResponse resp;

        resp = client.callProcedure("insert_p_source_p", 5);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        long numRows = resp.getResults()[0].asScalarLong();
        assertEquals(4, numRows);

        resp = client.callProcedure("CountTargetP");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(numRows, resp.getResults()[0].asScalarLong());

        // verify that the corresponding rows in both tables are the same
        String selectAllSource = "select * from source_p1 where bi = 5 order by bi, ii";
        String selectAllTarget = "select * from target_p order by bi, ii";

        resp = client.callProcedure("@AdHoc", selectAllSource);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable sourceRows = resp.getResults()[0];

        resp = client.callProcedure("@AdHoc", selectAllTarget);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable targetRows = resp.getResults()[0];

        while(sourceRows.advanceRow()) {
                assertEquals(true, targetRows.advanceRow());
                assertEquals(sourceRows.getLong(0), targetRows.getLong(0));
                assertEquals(sourceRows.getString(1), targetRows.getString(1));
                assertEquals(sourceRows.getLong(2), targetRows.getLong(2));
                assertEquals(sourceRows.getLong(3), targetRows.getLong(3));
        }
    }

    public void testPartitionedTableWithSelectJoin() throws Exception
    {
        final Client client = getClient();
        initializeTables(client);

        // source_p1 contains 0..9
        // source_p2 contains 5..14

        final long partitioningValue = 7;
        ClientResponse resp = client.callProcedure("InsertIntoSelectWithJoin", partitioningValue);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(4, resp.getResults()[0].asScalarLong());

        String selectSp1 = "select * from source_p1 where bi = " + partitioningValue;
        String selectSp2 = "select * from source_p2 where bi = " + partitioningValue;
        String selectTarget = "select * from target_p";

        resp = client.callProcedure("@AdHoc", selectTarget);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable targetRows = resp.getResults()[0];

        resp = client.callProcedure("@AdHoc", selectSp1);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable sp1Rows = resp.getResults()[0];

        resp = client.callProcedure("@AdHoc", selectSp2);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable sp2Rows = resp.getResults()[0];

        while(targetRows.advanceRow()) {
                assertTrue(sp1Rows.advanceRow());
                assertTrue(sp2Rows.advanceRow());

                assertEquals(sp1Rows.getLong(0), targetRows.getLong(0));
                assertEquals(sp1Rows.getString(1), targetRows.getString(1));
                assertEquals(sp2Rows.getLong(2), targetRows.getLong(2));
                assertEquals(sp2Rows.getLong(3), targetRows.getLong(3));
        }
    }

    public void testInsertIntoSelectWithDefaults() throws Exception {
        final Client client = getClient();

        ClientResponse resp;
        long partitioningValue = 8;

        String[] procs = new String[] {"insert_p_use_defaults", "insert_p_use_defaults_reorder"};

        for (String proc : procs) {
            initializeTables(client);

            resp = client.callProcedure(proc, partitioningValue);
            validateTableOfScalarLongs(resp.getResults()[0], new long[] {4});

            resp = client.callProcedure("@AdHoc", "select * from target_p");

            String selectSp1 = "select * from source_p1 where bi = " + partitioningValue;
            String selectTarget = "select * from target_p";

            resp = client.callProcedure("@AdHoc", selectTarget);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            VoltTable targetRows = resp.getResults()[0];

            resp = client.callProcedure("@AdHoc", selectSp1);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            VoltTable sp1Rows = resp.getResults()[0];

            while (targetRows.advanceRow()) {
                assertTrue(sp1Rows.advanceRow());

                assertEquals(sp1Rows.getLong(0), targetRows.getLong(0));
                assertEquals(vcDefault, targetRows.getString(1));
                assertEquals(intDefault, targetRows.getLong(2));
                assertEquals(sp1Rows.getLong(3), targetRows.getLong(3));
            }
        }
    }

    public void testInsertIntoSelectAdHocFails() throws IOException {
        // for now only SP/SP is supported for insert-into-select
        verifyStmtFails(getClient(), "insert into target_p select * from source_p1",
                "only supported for single-partition stored procedures");
    }
}
