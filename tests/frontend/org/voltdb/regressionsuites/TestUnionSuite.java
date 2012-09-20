/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

public class TestUnionSuite extends RegressionSuite {
    public TestUnionSuite(String name) {
        super(name);
    }

    public void testUnion() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 1);
        client.callProcedure("InsertB", 1, 1);
        client.callProcedure("InsertB", 2, 1);
        client.callProcedure("InsertC", 1, 2);
        client.callProcedure("InsertC", 2, 3);
        VoltTable result = client.callProcedure("@AdHoc", "SELECT PKEY FROM A UNION SELECT I FROM B UNION SELECT I FROM C;")
                                 .getResults()[0];
        assertEquals(4, result.getRowCount());
    }

    public void testUnionAll() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 1);
        client.callProcedure("InsertB", 1, 1);
        client.callProcedure("InsertB", 2, 1);
        client.callProcedure("InsertC", 1, 2);
        client.callProcedure("InsertC", 2, 3);
        VoltTable result = client.callProcedure("@AdHoc", "SELECT PKEY FROM A UNION ALL SELECT I FROM B UNION ALL SELECT I FROM C;")
                                 .getResults()[0];
        int c = result.getRowCount();
        System.out.println("testUnionAll " + c);
        assertEquals(5, result.getRowCount());
    }

    private void load(Client client)
    throws NoConnectionsException, IOException, InterruptedException {
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestUnionSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestUnionSuite.class.getResource("testunion-ddl.sql"));
        project.addPartitionInfo("A", "PKEY");
        project.addStmtProcedure("InsertA", "INSERT INTO A VALUES(?, ?);");
        project.addStmtProcedure("InsertB", "INSERT INTO B VALUES(?, ?);");
        project.addStmtProcedure("InsertC", "INSERT INTO C VALUES(?, ?);");
        project.addStmtProcedure("UnionABC", "SELECT PKEY FROM A UNION SELECT I FROM B UNION SELECT I FROM C;");

        // local
        config = new LocalCluster("testunion-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testunion-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }

}
