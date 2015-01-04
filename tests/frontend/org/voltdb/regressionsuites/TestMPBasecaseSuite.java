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

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.basecase.eng7181;

public class TestMPBasecaseSuite extends RegressionSuite {

    static final Class<?>[] PROCEDURES = {eng7181.class};

    public TestMPBasecaseSuite(String name) {
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

    public void testOneShotRead() throws Exception
    {
        final Client client = this.getClient();
        loadData(client);

        // testcase: single-stmt read.
        ClientResponse resp = client.callProcedure("CountP1");
        assertTrue("Successful oneshot read.", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Expect count=10", 10L, resp.getResults()[0].asScalarLong());
    }

    public void testOneShotWrite() throws Exception
    {
        final Client client = this.getClient();
        loadData(client);

        ClientResponse resp = client.callProcedure("UpdateP1");
        assertTrue("Successful oneshot write.", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Touched 10 rows", 10L, resp.getResults()[0].asScalarLong());

        // verify the update results.
        resp = client.callProcedure("SumP1");
        assertTrue("Verified updates", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Updated sum=20", 20L, resp.getResults()[0].asScalarLong());
    }

    public void testOneShotConstraintViolationAllSites() throws Exception
    {
        final Client client = this.getClient();
        loadData(client);
        boolean caught = false;
        try {
            client.callProcedure("ConstraintViolationUpdate");
            assertFalse("Failed to produce constraint violation", true);
        }
        catch (ProcCallException e) {
            assertEquals("Client response is rollback.",
                    ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            caught = true;
        }
        assertTrue("Expected exception.", caught);

        // verify initial result is unchanged (transactions!)
        ClientResponse resp = client.callProcedure("SumB1");
        assertTrue("Verified updates", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Updated sum=45", 45L, resp.getResults()[0].asScalarLong());
    }

    public void testOneshotPartitionViolationAllSites() throws Exception
    {
        // Restrict to clustered tests (configured with > 1 partition)
        LocalCluster config = (LocalCluster)this.getServerConfig();
        int sites = config.m_siteCount;
        int nodes = config.m_hostCount;
        int k = config.m_kfactor;
        int parts = (nodes * sites) / (k + 1);
        if (parts == 1) {
            return;
        }

        final Client client = this.getClient();
        loadData(client);
        try {
            client.callProcedure("PartitionViolationUpdate");
            assertFalse("Failed to produce violation", true);
        }
        catch (ProcCallException e) {
            assertEquals("Client response is not graceful failure.",
                    ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
        }
        // verify initial result is unchanged (transactions!)
        ClientResponse resp = client.callProcedure("SumKey");
        assertTrue("Verified updates", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Updated sum=45", 45L, resp.getResults()[0].asScalarLong());
        // see ENG-2941
        assertTrue(resp.getStatusString() == null);
    }

    public void testOneshotReplicatedWriteAndRead() throws Exception
    {
        final Client client = this.getClient();
        loadData("R1.insert", client);

        ClientResponse resp =  client.callProcedure("SumR1");
        assertTrue("OK response", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Expected sum=45", 45L, resp.getResults()[0].asScalarLong());
    }

    public void testOneshotReplicatedConstraintViolation() throws Exception
    {
        final Client client = this.getClient();
        boolean caught = false;
        loadData("R1.insert", client);
        try {
            client.callProcedure("ConstraintViolationUpdate_R");
            assertTrue("Failed to produce constraint violation", false);
        }
        catch (ProcCallException e) {
            assertEquals("Client response is rollback.",
                    ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            caught = true;
        }
        assertTrue("Caught expected", caught);

        // verify initial result is unchanged (transactions!)
        ClientResponse resp = client.callProcedure("SumB1_R");
        assertTrue("Verified updates", resp.getStatus() == ClientResponse.SUCCESS);
        assertEquals("Updated sum=45", 45L, resp.getResults()[0].asScalarLong());
    }

    // Validate that if the first thing done by a transaction is a read from a
    // replicated table, that we don't mistakenly fail to create and use undo
    // tokens on rollback.
    public void testEng7181() throws Exception
    {
        System.out.println("-------------\n\n testEng7181 \n\n-------------");
        final Client client = this.getClient();
        ClientResponse resp = client.callProcedure("CountR1");
        assertEquals(0, resp.getResults()[0].getRowCount());
        // Add a row
        resp = client.callProcedure("eng7181", 42, 0);
        resp = client.callProcedure("CountR1");
        assertEquals(1, resp.getResults()[0].getRowCount());
        try {
            // do borrow work, then insert, then rollback
            resp = client.callProcedure("eng7181", 43, 1);
        }
        catch (ProcCallException pce) {
        }
        // Verify rollback happened
        resp = client.callProcedure("CountR1");
        assertEquals(1, resp.getResults()[0].getRowCount());
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMPBasecaseSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addStmtProcedure("CountP1", "select count(*) from p1;");
        project.addStmtProcedure("CountR1", "select * from r1;");

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

        project.addProcedures(PROCEDURES);

        try {
            project.addLiteralSchema(
                    "CREATE TABLE p1(key INTEGER NOT NULL, b1 INTEGER NOT NULL assumeunique, " +
                    "b2 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (b1, key)); " +
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
        config = new LocalCluster("sqltypes-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);

        return builder;
    }
}
