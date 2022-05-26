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
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.basecase.LoadP1;
import org.voltdb_testprocs.regressionsuites.basecase.LoadR1;
import org.voltdb_testprocs.regressionsuites.basecase.MultiRoundMixReadsAndWrites;
import org.voltdb_testprocs.regressionsuites.basecase.MultiRoundMixReplicatedReadsAndWrites;
import org.voltdb_testprocs.regressionsuites.basecase.MultiRoundMixedReads;
import org.voltdb_testprocs.regressionsuites.basecase.MultiRoundP1Count;
import org.voltdb_testprocs.regressionsuites.basecase.MultiRoundR1Count;

public class TestMPMultiRoundTripSuite extends RegressionSuite {

    public TestMPMultiRoundTripSuite(String name) {
        super(name);
    }

    void loadData(Client client) throws IOException, ProcCallException
    {
        loadData("P1.insert", client);
    }

    void loadData(String procname, Client client) throws IOException, ProcCallException
    {
        // inserts
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure(procname, i, i, i, Integer.toHexString(i));
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            assertTrue(resp.getResults()[0].asScalarLong() == 1);
        }
    }

    public void testMultiRoundWrite() throws Exception
    {
        final Client client = this.getClient();
        ClientResponse resp = client.callProcedure("LoadP1", 10);
        assertTrue("Successful multi-roundtrip write.", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Expect touched=10", 10L, resp.getResults()[0].asScalarLong());
    }

    public void testMultiRoundReplicatedWrite() throws Exception
    {
        final Client client = this.getClient();
        ClientResponse resp = client.callProcedure("LoadR1", 10);
        assertTrue("Successful multi-roundtrip replicated write.", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Expect touched=10", 10L, resp.getResults()[0].asScalarLong());
    }

    public void testMultiRoundRead() throws Exception
    {
        final Client client = this.getClient();
        loadData(client);

        ClientResponse resp = client.callProcedure("MultiRoundP1Count", 10);
        assertTrue("Successful multi-roundtrip read.", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Expect count=100", 100L, resp.getResults()[0].asScalarLong());
    }

    public void testMultiRoundReplicatedRead() throws Exception
    {
        final Client client = this.getClient();
        client.callProcedure("LoadR1", 10);

        ClientResponse resp = client.callProcedure("MultiRoundR1Count", 10);
        assertTrue("Successful multi-roundtrip read.", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Expect count=100", 100L, resp.getResults()[0].asScalarLong());
    }

    public void testMultiRoundTripMixReadTypes() throws Exception
    {
        final Client client = this.getClient();
        client.callProcedure("LoadP1", 11);
        client.callProcedure("LoadR1", 13);

        ClientResponse resp = client.callProcedure("MultiRoundMixedReads", 10);
        assertTrue("Successful multi-roundtrip read.", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Expect count=1430(11*13*10)", 1430L, resp.getResults()[0].asScalarLong());
    }

    public void testMultiRoundtripMixReadsAndWrites() throws Exception
    {
        final Client client = this.getClient();
        // Make the die setting > than the # of batches to avoid death
        ClientResponse resp = client.callProcedure("MultiRoundMixReadsAndWrites", 10, 20);
        assertTrue("Successful multi-roundtrip read/write.", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Expect count=55", 55L, resp.getResults()[0].asScalarLong());
    }

    public void testMultiRoundtripMixReplicatedReadsAndWrites() throws Exception
    {
        final Client client = this.getClient();
        ClientResponse resp = client.callProcedure("MultiRoundMixReplicatedReadsAndWrites", 10, 20);
        assertTrue("Successful multi-roundtrip read/write.", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Expect count=55", 55L, resp.getResults()[0].asScalarLong());
    }

    public void testMultiRoundTripMixReadsWritesConstraintViolation() throws Exception
    {
        final Client client = this.getClient();
        boolean caught = false;
        try {
            client.callProcedure("MultiRoundMixReadsAndWrites", 10, 6);
            assertFalse("Failed to produce constraint violation", true);
        }
        catch (ProcCallException e) {
            assertEquals("Client response is rollback.",
                    ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            caught = true;
        }
        assertTrue("Expected exception.", caught);

        // Entire proc should have been rolled back
        ClientResponse resp = client.callProcedure("CountP1");
        assertEquals("Expected count=0", 0L, resp.getResults()[0].asScalarLong());
    }

    public void testMultiRoundTripMixReplicatedReadsWritesConstraintViolation() throws Exception
    {
        final Client client = this.getClient();
        boolean caught = false;
        try {
            client.callProcedure("MultiRoundMixReplicatedReadsAndWrites", 10, 6);
            assertFalse("Failed to produce constraint violation", true);
        }
        catch (ProcCallException e) {
            assertEquals("Client response is rollback.",
                    ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            caught = true;
        }
        assertTrue("Expected exception.", caught);

        // Entire proc should have been rolled back
        ClientResponse resp = client.callProcedure("CountP1");
        assertEquals("Expected count=0", 0L, resp.getResults()[0].asScalarLong());
    }

    public void testSimultaneousMultiAndSinglePartTxns() throws Exception
    {
        int test_size = 100;
        final Client client = this.getClient();
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException("Failed with response: " + clientResponse.getStatusString());
                }
            }
        };
        client.callProcedure(callback, "MultiRoundMixReadsAndWrites", test_size, test_size * 2);
        // stall a little to try to avoid doing one of the SPs before the MP
        // takes control of the whole cluster
        Thread.sleep(1000);
        for (int i = 0; i < test_size; i++) {
            client.callProcedure(callback, "UpdateP1SP", i);
        }
        client.drain();
        ClientResponse resp2 = client.callProcedure("GetP1");
        ClientResponse resp = client.callProcedure("SumP1");
        assertEquals(resp2.getResults()[0].toString(), test_size * 2, resp.getResults()[0].asScalarLong());
    }

    public void testSimultaneousMultiAndSinglePartTxnsWithRollback() throws Exception
    {
        int test_size = 100;
        final Client client = this.getClient();
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException("Failed with response: " + clientResponse.getStatusString());
                }
            }
        };
        client.callProcedure(callback, "MultiRoundMixReadsAndWrites", test_size, test_size / 2);
        for (int i = 0; i < test_size; i++) {
            client.callProcedure(callback, "P1.insert", i, i, 2, Integer.toHexString(i));
        }
        client.drain();
        ClientResponse resp2 = client.callProcedure("GetP1");
        ClientResponse resp = client.callProcedure("SumP1");
        assertEquals(resp2.getResults()[0].toString(), test_size * 2, resp.getResults()[0].asScalarLong());
    }


    /** Procedures used by this suite */
    static final Class<?>[] MP_PROCEDURES = { LoadP1.class, LoadR1.class, MultiRoundP1Count.class,
                                           MultiRoundR1Count.class,
                                           MultiRoundMixedReads.class,
                                           MultiRoundMixReadsAndWrites.class,
                                           MultiRoundMixReplicatedReadsAndWrites.class };

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMPMultiRoundTripSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addStmtProcedure("CountP1", "select count(*) from p1;");
        project.addStmtProcedure("GetP1", "select * from p1;");

        // update non-unique, non-partitioning attribute
        project.addStmtProcedure("UpdateP1", "update p1 set b2 = 2");
        project.addStmtProcedure("SumP1", "select sum(b2) from p1;");

        project.addStmtProcedure("UpdateR1", "update r1 set b2 = 2");
        project.addStmtProcedure("SumR1", "select sum(b2) from r1;");

        // update all pkeys to the same value.
        project.addStmtProcedure("ConstraintViolationUpdate", "update p1 set b1 = 1");
        project.addStmtProcedure("SumB1", "select sum(b1) from p1;");

        project.addStmtProcedure("ConstraintViolationUpdate_R", "update r1 set b1 = 1");
        project.addStmtProcedure("SumB1_R", "select sum(b1) from r1;");

        // update all partitioning keys to the same value.
        project.addStmtProcedure("PartitionViolationUpdate", "update p1 set key = 1");
        project.addStmtProcedure("SumKey", "select sum(key) from p1;");

        // Single-part update
        project.addStmtProcedure("UpdateP1SP", "update p1 set b2 = 2 where key = ?", "p1.key:0");

        project.addMultiPartitionProcedures(MP_PROCEDURES);

        try {
            project.addLiteralSchema(
                    "CREATE TABLE p1(key INTEGER NOT NULL, b1 INTEGER NOT NULL ASSUMEUNIQUE, " +
                    "b2 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (b1,key)); " +
                    "PARTITION TABLE P1 ON COLUMN key;"
            );

            // a replicated table (should not generate procedures).
            project.addLiteralSchema(
                    "CREATE TABLE r1(key INTEGER NOT NULL, b1 INTEGER NOT NULL, " +
                    "b2 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (b1));"
            );

        } catch (IOException error) {
            fail(error.getMessage());
        }

        // JNI
        config = new LocalCluster("sqltypes-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        config = new LocalCluster("sqltypes-onesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean t3 = config.compile(project);
        assertTrue(t3);
        builder.addServerConfig(config);

        // CLUSTER
        config = new LocalCluster("sqltypes-cluster.jar", 2, 2, 0, BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);

        return builder;
    }
}
