package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.*;
import org.voltdb.client.*;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestCRUDSuite extends RegressionSuite {

    static final Class<?>[] PROCEDURES = {};

    public TestCRUDSuite(String name) {
        super(name);
    }

    public void testPartitionedPkPartitionCol() throws Exception
    {
        final Client client = this.getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.insert", i, Integer.toHexString(i));
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            assertTrue(resp.getResults()[0].asScalarLong() == 1);
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.select", i);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable vt = resp.getResults()[0];
            assertTrue(vt.advanceRow());
            assertTrue(vt.getLong(0) == i);
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.update", i, "STR" + Integer.toHexString(i), i);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            assertTrue(resp.getResults()[0].asScalarLong() == 1);
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.select", i);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable vt = resp.getResults()[0];
            assertTrue(vt.advanceRow());
            assertTrue(vt.getString(1).equals("STR" + Integer.toHexString(i)));
        }

        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("P1.delete", i);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            assertTrue(resp.getResults()[0].asScalarLong() == 1);
        }

        ClientResponse resp = client.callProcedure("CountP1");
        assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
        assertTrue(resp.getResults()[0].asScalarLong() == 0);
    }

    public void testPartitionedPkWithoutPartitionCol() throws Exception
    {
        Client client = getClient();
        try {
            client.callProcedure("P2.insert", 0, "ABC");
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
            client.callProcedure("P3.insert", 0, "ABC");
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("was not found"));
            return;
        }
        fail();
    }

    public void testReplicatedTable() throws Exception
    {
        Client client = getClient();
        try {
            client.callProcedure("R1.insert", 0, "ABC");
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("was not found"));
            return;
        }
        fail();
    }


    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCRUDSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        // necessary because at least one procedure is required
        project.addStmtProcedure("CountP1", "select count(*) from p1;");

        try {
            // a table that should generate procedures
            project.addLiteralSchema(
                    "CREATE TABLE p1(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (a1));"
            );
            project.addPartitionInfo("p1", "a1");

            // a partitioned table that should not generate procedures (no pkey)
            project.addLiteralSchema(
                    "CREATE TABLE p2(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL); " +
                    "CREATE UNIQUE INDEX p2_tree_idx ON p2(a1);"
            );
            project.addPartitionInfo("p2", "a1");

            // a partitioned table that should not generate procedures (pkey not partition key)
            project.addLiteralSchema(
                    "CREATE TABLE p3(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL); " +
                    "CREATE UNIQUE INDEX p3_tree_idx ON p3(a1);"
            );
            project.addPartitionInfo("p3", "a2");

            // a replicated table (should not generate procedures).
            project.addLiteralSchema(
                    "CREATE TABLE r1(a1 INTEGER NOT NULL, a2 VARCHAR(10) NOT NULL, PRIMARY KEY (a1));"
            );
        } catch (IOException error) {
            fail(error.getMessage());
        }

        // JNI
        config = new LocalSingleProcessServer("sqltypes-onesite.jar", 1, BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        // CLUSTER
        config = new LocalCluster("sqltypes-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);

        return builder;

    }

}
