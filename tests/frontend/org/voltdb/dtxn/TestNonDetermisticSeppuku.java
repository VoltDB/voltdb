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

package org.voltdb.dtxn;

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestNonDetermisticSeppuku extends TestCase {

    static final String SCHEMA =
            "create table kv (" +
            "key bigint not null, " +
            "nondetval bigint unique not null, " +  // non-deterministic value (host ID)
            "PRIMARY KEY(key));";

    LocalCluster cluster;
    Client client;


    @Override
    public void setUp() {
        if (!MiscUtils.isPro()) { return; } // feature disabled in community

        try {
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(SCHEMA);
            builder.addPartitionInfo("kv", "key");
            builder.addProcedures(NonDeterministicSPProc.class,
                                  NonDeterministic_RO_MP.class,
                                  NonDeterministic_RO_SP.class,
                                  Deterministic_RO_SP.class);

            cluster = new LocalCluster("det1.jar", 1, 2, 1, BackendTarget.NATIVE_EE_JNI);
            cluster.overrideAnyRequestForValgrind();
            assertTrue("Catalog compilation failed", cluster.compile(builder));

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
        if (!MiscUtils.isPro()) { return; } // feature disabled in community

        client.close();
        cluster.shutDown();
        assertTrue(cluster.areAllNonLocalProcessesDead());
    }

    /**
     * Do a non-deterministic insertion
     */
    public void testNonDeterministicInsert() throws Exception {
        if (!MiscUtils.isPro()) { return; } // feature disabled in community

        try {
            client.callProcedure(
                    "NonDeterministicSPProc",
                    0,
                    0,
                    NonDeterministicSPProc.MISMATCH_INSERTION);
            fail("Mismatch insertion failed");
        }
        catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database") ||
                    e.getMessage().contains("Transaction dropped"));
            // success
        }
    }

    /**
     * Do a non-deterministic insertion followed by a single partition read-only operation.
     * ENG-3288 - Expect non-deterministic read-only queries to succeed.
     */
    public void testNonDeterministic_RO_SP() throws Exception {
        if (!MiscUtils.isPro()) { return; } // feature disabled in community

        client.callProcedure(
                "NonDeterministicSPProc",
                0,
                0,
                NonDeterministicSPProc.NO_PROBLEM);
        try {
            client.callProcedure("NonDeterministic_RO_SP", 0);
            // success!!
        }
        catch (ProcCallException e) {
            fail("R/O SP mismatch failed?! " + e.toString());
        }
    }

    /**
     * Negative test that expects a deterministic proc to fail due to mismatched results.
     */
    public void testDeterministicProc() throws Exception {
        if (!MiscUtils.isPro()) { return; } // feature disabled in community

        client.callProcedure(
                "NonDeterministicSPProc",
                0,
                0,
                NonDeterministicSPProc.NO_PROBLEM);
        try {
            client.callProcedure("Deterministic_RO_MP", 0);
            fail("Deterministic procedure succeeded for non-deterministic results?");
        }
        catch (ProcCallException e) {
            // success!!
        }
    }

    /**
     * Test that different whitespace fails the determinism CRC check on SQL
     */
    public void testWhitespaceChanges() throws Exception {
        if (!MiscUtils.isPro()) { return; } // feature disabled in community

        try {
            client.callProcedure(
                "NonDeterministicSPProc",
                0,
                0,
                NonDeterministicSPProc.MISMATCH_WHITESPACE_IN_SQL);
            fail("Whitespace changes not picked up by determinism CRC");
        }
        catch (ProcCallException e) {
            // success!!
        }
    }
}
