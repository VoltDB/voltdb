/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestLimitOffsetSuite extends RegressionSuite {
    public TestLimitOffsetSuite(String name) {
        super(name);
    }

    private void load(Client client)
    throws NoConnectionsException, IOException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            SyncCallback cb = new SyncCallback();
            client.callProcedure(cb, "InsertA", i, i);
            cb.waitForResponse();
            assertEquals(1, cb.getResponse().getResults()[0].asScalarLong());
        }

        for (int i = 0; i < 10; i++) {
            SyncCallback cb = new SyncCallback();
            client.callProcedure(cb, "InsertB", i, i);
            cb.waitForResponse();
            assertEquals(1, cb.getResponse().getResults()[0].asScalarLong());
        }
    }

    private void doLimitOffsetAndCheck(String proc)
    throws IOException, InterruptedException, ProcCallException {
        Client client = this.getClient();
        load(client);

        ClientResponse resp = client.callProcedure(proc, 4, 0);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable[] results = resp.getResults();
        assertEquals(1, results.length);
        VoltTable vt = results[0];
        int i = 0;
        while (vt.advanceRow()) {
            assertEquals(i++, vt.getLong(1));
        }
        assertEquals(4, i);

        resp = client.callProcedure(proc, 3, 1);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        results = resp.getResults();
        assertEquals(1, results.length);
        vt = results[0];
        i = 1;
        while (vt.advanceRow()) {
            assertEquals(i++, vt.getLong(1));
        }
        assertEquals(4, i);
    }

    public void testMultiPartInlineLimit() throws IOException, InterruptedException, ProcCallException {
        doLimitOffsetAndCheck("LimitAPKEY");
    }

    public void testMultiPartLimit() throws IOException, InterruptedException, ProcCallException {
        doLimitOffsetAndCheck("LimitAI");
    }

    public void testReplicatedInlineLimit() throws IOException, InterruptedException, ProcCallException {
        doLimitOffsetAndCheck("LimitBPKEY");
    }

    public void testReplicatedLimit() throws IOException, InterruptedException, ProcCallException {
        doLimitOffsetAndCheck("LimitBI");
    }

    public void testDistinctLimitOffset() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 1);
        client.callProcedure("InsertA", 1, 1);
        client.callProcedure("InsertA", 2, 2);
        VoltTable result = null;

        result = client.callProcedure("@AdHoc", "SELECT DISTINCT I FROM A LIMIT 1 OFFSET 1;").getResults()[0];
        assertEquals(1, result.getRowCount());

        result = client.callProcedure("@AdHoc", "SELECT DISTINCT I FROM A LIMIT 0 OFFSET 1;").getResults()[0];
        assertEquals(0, result.getRowCount());
}

    public void testJoinAndLimitOffset() throws IOException, ProcCallException, InterruptedException {
        Client client = this.getClient();
        load(client);
        client.callProcedure("InsertA", -1, 0);
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM A, B WHERE A.PKEY < B.PKEY LIMIT 1 OFFSET 1;")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT A.PKEY FROM A, B WHERE A.PKEY = B.PKEY LIMIT 1;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
        result.advanceRow();
        assertEquals(0, result.getLong(0));
        result = client.callProcedure("@AdHoc", "SELECT A.PKEY FROM A, B WHERE A.PKEY = B.PKEY LIMIT 2 OFFSET 2;")
                .getResults()[0];
        assertEquals(2, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT A.PKEY FROM A LEFT JOIN B ON A.I = B.I LIMIT 10 OFFSET 5;")
                .getResults()[0];
        assertEquals(6, result.getRowCount());
    }

    public void testENG3487() throws IOException, ProcCallException {
        Client client = this.getClient();

        client.callProcedure("A.insert", 1, 1);
        client.callProcedure("A.insert", 2, 1);
        client.callProcedure("A.insert", 3, 1);
        client.callProcedure("A.insert", 4, 4);
        client.callProcedure("A.insert", 5, 4);
        client.callProcedure("A.insert", 6, 9);


        VoltTable result = client.callProcedure("@AdHoc", "select I, count(*) as tag from A group by I order by tag, I limit 1")
                .getResults()[0];

        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        //System.err.println("Result:\n" + result);
        assertEquals(9, result.getLong(0));
        assertEquals(1, result.getLong(1));

    }

    public void testENG1808() throws IOException, ProcCallException {
        Client client = this.getClient();

        client.callProcedure("A.insert", 1, 1);

        VoltTable result = client.callProcedure("@AdHoc", "select I from A limit 0").getResults()[0];

        assertEquals(0, result.getRowCount());
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestLimitOffsetSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestLimitOffsetSuite.class.getResource("testlimitoffset-ddl.sql"));
        project.addPartitionInfo("A", "PKEY");

        project.addStmtProcedure("InsertA", "INSERT INTO A VALUES(?, ?);");
        project.addStmtProcedure("InsertB", "INSERT INTO B VALUES(?, ?);");
        project.addStmtProcedure("LimitAPKEY", "SELECT * FROM A ORDER BY PKEY LIMIT ? OFFSET ?;");
        project.addStmtProcedure("LimitBPKEY", "SELECT * FROM B ORDER BY PKEY LIMIT ? OFFSET ?;");
        project.addStmtProcedure("LimitAI", "SELECT * FROM A ORDER BY I LIMIT ? OFFSET ?;");
        project.addStmtProcedure("LimitBI", "SELECT * FROM B ORDER BY I LIMIT ? OFFSET ?;");

        // local
        config = new LocalCluster("testlimitoffset-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testlimitoffset-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // HSQL for baseline
        config = new LocalCluster("testlimitoffset-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);
        return builder;
    }
}
