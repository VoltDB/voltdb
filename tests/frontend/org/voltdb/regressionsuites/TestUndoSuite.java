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
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.basecase.LoadP1;
import org.voltdb_testprocs.regressionsuites.basecase.LoadP1_MP;
import org.voltdb_testprocs.regressionsuites.basecase.LoadP1_SP;
import org.voltdb_testprocs.regressionsuites.basecase.LoadR1;

public class TestUndoSuite extends RegressionSuite {

    public TestUndoSuite(String name) {
        super(name);
    }

    private void doSpLoad(Client client, int count) throws NoConnectionsException, IOException, ProcCallException
    {
        for (int i = 0; i < count; i++) {
            client.callProcedure("LoadP1_SP", i);
        }
    }

    private void doMpLoad(Client client, int count) throws NoConnectionsException, IOException, ProcCallException
    {
        for (int i = 0; i < count; i++) {
            client.callProcedure("LoadP1_MP", i);
        }
    }

    private void doSpRollback(Client client, int key) throws NoConnectionsException, IOException, ProcCallException
    {
        boolean threw = false;
        try {
            client.callProcedure("LoadP1_SP", key);
        }
        catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue(threw);
    }

    private void doMpRollback(Client client, int key) throws NoConnectionsException, IOException
    {
        boolean threw = false;
        try {
            client.callProcedure("LoadP1_MP", key);
        }
        catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue(threw);
    }

    public void testMultibatchMPGoodThenMPFail() throws IOException, ProcCallException {
        final Client client = this.getClient();
        doMpLoad(client, 100);
        doMpRollback(client, 0);
        doMpRollback(client, 1);
        doMpRollback(client, 2);
        VoltTable[] results = client.callProcedure("CountP1").getResults();
        assertEquals(100, results[0].asScalarLong());
    }

    public void testMultibatchMPGoodThenSPFail() throws IOException, ProcCallException {
        final Client client = this.getClient();
        doMpLoad(client, 100);
        doSpRollback(client, 0);
        doSpRollback(client, 1);
        doSpRollback(client, 2);
        VoltTable[] results = client.callProcedure("CountP1").getResults();
        assertEquals(100, results[0].asScalarLong());
    }

    public void testMultibatchSPGoodThenMPFail() throws IOException, ProcCallException {
        final Client client = this.getClient();
        doSpLoad(client, 100);
        doMpRollback(client, 0);
        doMpRollback(client, 1);
        doMpRollback(client, 2);
        VoltTable[] results = client.callProcedure("CountP1").getResults();
        assertEquals(100, results[0].asScalarLong());
    }

    public void testMultibatchSPGoodThenSPFail() throws IOException, ProcCallException {
        final Client client = this.getClient();
        doSpLoad(client, 100);
        doSpRollback(client, 0);
        doSpRollback(client, 1);
        doSpRollback(client, 2);
        VoltTable[] results = client.callProcedure("CountP1").getResults();
        assertEquals(100, results[0].asScalarLong());
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestUndoSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        project.addStmtProcedure("CountP1", "select count(*) from p1;");
        try {
            project.addLiteralSchema(
                    "CREATE TABLE p1(key INTEGER NOT NULL, b1 INTEGER NOT NULL ASSUMEUNIQUE, " +
                    "b2 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (b1, key)); " +
                    "PARTITION TABLE P1 ON COLUMN key;"
            );

            // a replicated table (should not generate procedures).
            project.addLiteralSchema(
                    "CREATE TABLE r1(key INTEGER NOT NULL, b1 INTEGER NOT NULL, " +
                    "b2 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (b1));"
            );

            project.addProcedure(LoadP1.class);
            project.addProcedure(LoadP1_MP.class);
            project.addProcedure(LoadP1_SP.class, "p1.key: 0");
            project.addProcedure(LoadR1.class);
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
