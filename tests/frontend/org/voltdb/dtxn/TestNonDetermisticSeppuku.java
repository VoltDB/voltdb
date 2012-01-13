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

package org.voltdb.dtxn;

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

public class TestNonDetermisticSeppuku extends TestCase {

    static final String SCHEMA =
            "create table kv (" +
            "key bigint default 0 not null, " +
            "value bigint default 0 not null, " +
            "PRIMARY KEY(key));";

    LocalCluster cluster;
    Client client;


    @Override
    public void setUp()
    {
        try {
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(SCHEMA);
            builder.addPartitionInfo("kv", "key");
            builder.addStmtProcedure("Select", "select * from kv;", null);
            builder.addProcedures(NonDeterministicSPProc.class);

            cluster = new LocalCluster("det1.jar", 1, 2, 1, BackendTarget.NATIVE_EE_JNI);
            cluster.compile(builder);

            cluster.setHasLocalServer(false);

            client = ClientFactory.createClient();

            cluster.startUp();

            for (String s : cluster.getListenerAddresses()) {
                client.createConnection(s);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Override
    public void tearDown() throws Exception {
        client.close();
        cluster.shutDown();
        assertTrue(cluster.areAllNonLocalProcessesDead());
    }

    /**
     * Call a single partition proc that returns a table with
     * one row, but with different values at different replicas.
     */
    public void testMismatchValueDeath() throws Exception {
        try {
            client.callProcedure(
                    "NonDeterministicSPProc",
                    0,
                    NonDeterministicSPProc.MISMATCH_VALUES);
            fail();
        }
        catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database"));
            // success!
        }
    }

    /**
     * Call a single-partition proc that returns a different number
     * of identical rows from two different replicas.
     */
    public void testDifferentResultLengthDeath() throws Exception {
        try {
            client.callProcedure(
                    "NonDeterministicSPProc",
                    0,
                    NonDeterministicSPProc.MISMATCH_LENGTH);
            fail();
        }
        catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database"));
            // success!
        }
    }

    /**
     * Do a non-deterministic insertion. Then do a multi-partition select.
     * This exercises the work-unit's test that all dependencies are the same.
     */
    public void testDifferentIntermediateResultDeath() throws Exception {
        client.callProcedure(
                "NonDeterministicSPProc",
                0,
                NonDeterministicSPProc.MISMATCH_INSERTION);
        try {
            client.callProcedure("Select");
            fail();
        }
        catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database"));
            // success!!
        }
    }

}
