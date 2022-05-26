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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

public class TestRestoreSysprocSuite extends SaveRestoreBase{

    public TestRestoreSysprocSuite(String name) {
        super(name);
    }

    public void testSaveAndPartialRestoreExcludetables() throws Exception {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSaveAndPartialRestoreExcludetables");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;
        VoltTable repl_table = TestSaveRestoreSysprocSuite.createReplicatedTable(num_replicated_items, 0, null);
        VoltTable partition_table = TestSaveRestoreSysprocSuite.createPartitionedTable(num_partitioned_items, 0);

        Client client = getClient();
        TestSaveRestoreSysprocSuite.loadTable(client, "REPLICATED_TESTER", true, repl_table);
        TestSaveRestoreSysprocSuite.loadTable(client, "PARTITION_TESTER", false, partition_table);
        TestSaveRestoreSysprocSuite.saveTablesWithDefaultOptions(client, TESTNONCE);
        TestSaveRestoreSysprocSuite.validateSnapshot(true, TESTNONCE);
        m_config.shutDown();
        m_config.startUp();

        client = getClient();
        JSONObject jsObj = new JSONObject();
        try {
            jsObj.put(SnapshotUtil.JSON_PATH, TMPDIR);
            jsObj.put(SnapshotUtil.JSON_NONCE, TESTNONCE);
            jsObj.put(SnapshotUtil.JSON_SKIPTABLES, new String[] {"REPLICATED_TESTER"});
        } catch (JSONException e) {
            fail("JSON exception" + e.getMessage());
        }
        VoltTable[] results;
        results = client.callProcedure("@SnapshotRestore", jsObj.toString()).getResults();

        while(results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                fail(results[0].getString("ERR_MSG"));
            }
            assert(!results[0].getString("TABLE").equals("REPLICATED_TESTER"));
        }

        ClientResponse cr = client.callProcedure("@AdHoc", "select count(*) from PARTITION_TESTER;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        Long rowCount = cr.getResults()[0].asScalarLong();
        assert(rowCount == 126);
    }

    public void testSaveAndPartialRestoreIncludeTables() throws Exception {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSaveAndPartialRestoreIncludeTables");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;
        VoltTable repl_table = TestSaveRestoreSysprocSuite.createReplicatedTable(num_replicated_items, 0, null);
        VoltTable partition_table = TestSaveRestoreSysprocSuite.createPartitionedTable(num_partitioned_items, 0);

        Client client = getClient();
        TestSaveRestoreSysprocSuite.loadTable(client, "REPLICATED_TESTER", true, repl_table);
        TestSaveRestoreSysprocSuite.loadTable(client, "PARTITION_TESTER", false, partition_table);
        TestSaveRestoreSysprocSuite.saveTablesWithDefaultOptions(client, TESTNONCE);
        TestSaveRestoreSysprocSuite.validateSnapshot(true, TESTNONCE);
        m_config.shutDown();
        m_config.startUp();

        client = getClient();
        JSONObject jsObj = new JSONObject();
        try {
            jsObj.put(SnapshotUtil.JSON_PATH, TMPDIR);
            jsObj.put(SnapshotUtil.JSON_NONCE, TESTNONCE);
            jsObj.put(SnapshotUtil.JSON_TABLES, new String[] {"REPLICATED_TESTER"});
        } catch (JSONException e) {
            fail("JSON exception" + e.getMessage());
        }

        VoltTable[] results;
        results = client.callProcedure("@SnapshotRestore", jsObj.toString()).getResults();

        while(results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                fail(results[0].getString("ERR_MSG"));
            }
            assert(results[0].getString("TABLE").equals("REPLICATED_TESTER"));
        }

        ClientResponse cr = client.callProcedure("@AdHoc", "select count(*) from REPLICATED_TESTER;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        Long rowCount = cr.getResults()[0].asScalarLong();
        assert(rowCount == 1000);
    }

    public void testSaveAndPartialRestoreWithNonExistingIncludeTables() throws Exception {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSaveAndPartialRestoreWithNonExistingIncludeTables");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;
        VoltTable repl_table = TestSaveRestoreSysprocSuite.createReplicatedTable(num_replicated_items, 0, null);
        VoltTable partition_table = TestSaveRestoreSysprocSuite.createPartitionedTable(num_partitioned_items, 0);

        Client client = getClient();
        TestSaveRestoreSysprocSuite.loadTable(client, "REPLICATED_TESTER", true, repl_table);
        TestSaveRestoreSysprocSuite.loadTable(client, "PARTITION_TESTER", false, partition_table);
        TestSaveRestoreSysprocSuite.saveTablesWithDefaultOptions(client, TESTNONCE);
        TestSaveRestoreSysprocSuite.validateSnapshot(true, TESTNONCE);
        m_config.shutDown();
        m_config.startUp();

        client = getClient();
        JSONObject jsObj = new JSONObject();
        try {
            jsObj.put(SnapshotUtil.JSON_PATH, TMPDIR);
            jsObj.put(SnapshotUtil.JSON_NONCE, TESTNONCE);
            jsObj.put(SnapshotUtil.JSON_TABLES, new String[] {"REPLICATED_TESTER", "DUMMYTABLE"});
        } catch (JSONException e) {
            fail("JSON exception" + e.getMessage());
        }
        VoltTable[] results = client.callProcedure("@SnapshotRestore", jsObj.toString()).getResults();
        while(results[0].advanceRow()) {
            assert(results[0].getString("RESULT").equals("FAILURE"));
            assert(results[0].getString("ERR_MSG").equals("org.voltdb.VoltProcedure$VoltAbortException: Table DUMMYTABLE is not in the savefile data."));
        }
    }

    public void testSaveAndPartialRestoreWithNonExistingExcludeTables() throws Exception {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSaveAndPartialRestoreWithNonExistingExcludeTables");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;
        VoltTable repl_table = TestSaveRestoreSysprocSuite.createReplicatedTable(num_replicated_items, 0, null);
        VoltTable partition_table = TestSaveRestoreSysprocSuite.createPartitionedTable(num_partitioned_items, 0);

        Client client = getClient();
        TestSaveRestoreSysprocSuite.loadTable(client, "REPLICATED_TESTER", true, repl_table);
        TestSaveRestoreSysprocSuite.loadTable(client, "PARTITION_TESTER", false, partition_table);
        TestSaveRestoreSysprocSuite.saveTablesWithDefaultOptions(client, TESTNONCE);
        TestSaveRestoreSysprocSuite.validateSnapshot(true, TESTNONCE);
        m_config.shutDown();
        m_config.startUp();

        client = getClient();
        JSONObject jsObj = new JSONObject();
        try {
            jsObj.put(SnapshotUtil.JSON_PATH, TMPDIR);
            jsObj.put(SnapshotUtil.JSON_NONCE, TESTNONCE);
            jsObj.put(SnapshotUtil.JSON_SKIPTABLES, new String[] {"REPLICATED_TESTER", "DUMMYTABLE"});
        } catch (JSONException e) {
            fail("JSON exception" + e.getMessage());
        }

        VoltTable[] results;
        results = client.callProcedure("@SnapshotRestore", jsObj.toString()).getResults();

        while(results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                fail(results[0].getString("ERR_MSG"));
            }
            assert(!results[0].getString("TABLE").equals("REPLICATED_TESTER") && !results[0].getString("TABLE").equals("DUMMYTABLE"));
        }

        ClientResponse cr = client.callProcedure("@AdHoc", "select count(*) from PARTITION_TESTER;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        Long rowCount = cr.getResults()[0].asScalarLong();
        assert(rowCount == 126);
    }

    @Test
    public void testSnapshotRestoreWithAutoGeneratedStreams() throws Exception {
        System.out.println("Running testSnapshotRestoreWithAutoGeneratedStreams.");
        LocalCluster cluster1 = null;
        LocalCluster cluster2 = null;
        Client cluster1Client = null;
        Client cluster2Client = null;

        try {
            int cluster1ReplicationPort = 11000;
            int cluster2ReplicationPort = 11100;
            String cluster1Root = "/tmp/" + System.getProperty("user.name") + "-cluster1";
            // Start cluster1 with consumer connection disabled.
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.setDrConsumerConnectionDisabled();
            cluster1 = LocalCluster.createLocalCluster("", 1, 1, 0, 1,
                    cluster1ReplicationPort, cluster2ReplicationPort, cluster1Root,
                    "restore-with-xdcr-stream-cluster1.jar", DrRoleType.XDCR, false, builder);

            System.out.println("Getting client connected to cluster1.");
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setProcedureCallTimeout(10 * 60 * 1000); // 10 min
            cluster1Client = ClientFactory.createClient(clientConfig);
            cluster1Client.createConnection(cluster1.getListenerAddress(0));
            String simpleSchema =
                    "CREATE TABLE helloworld(" +
                        "ID INTEGER NOT NULL," +
                        "N1 INTEGER NOT NULL," +
                        "N2 INTEGER NOT NULL," +
                                          ");" +
                       "CREATE TABLE object1(" +
                        "ID INTEGER NOT NULL," +
                        "V1 INTEGER NOT NULL," +
                        "V2 INTEGER NOT NULL," +
                                          ");" +
                       "CREATE TABLE object2(" +
                        "ID INTEGER NOT NULL," +
                        "C1 INTEGER NOT NULL," +
                        "C2 INTEGER NOT NULL," +
                                           ");";

            ClientResponse cr = cluster1Client.callProcedure("@AdHoc", simpleSchema);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = cluster1Client.callProcedure("@AdHoc", "insert into helloworld values (1132,3,5);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = cluster1Client.callProcedure("@AdHoc", "insert into object1 values (1133,4,5);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = cluster1Client.callProcedure("@AdHoc", "insert into object2 values (1143,6,8);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            TestSaveRestoreSysprocSuite.saveTablesWithDefaultOptions(cluster1Client, TESTNONCE);
            TestSaveRestoreSysprocSuite.validateSnapshot(true, TESTNONCE);
            m_config.shutDown();
            m_config.startUp();

            cr = cluster1Client.callProcedure("@AdHoc", "drop table helloworld;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = cluster1Client.callProcedure("@AdHoc", "drop table object1;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = cluster1Client.callProcedure("@AdHoc", "drop table object2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            JSONObject jsObj = new JSONObject();
            try {
                jsObj.put(SnapshotUtil.JSON_PATH, TMPDIR);
                jsObj.put(SnapshotUtil.JSON_NONCE, TESTNONCE);
            } catch (JSONException e) {
                fail("JSON exception" + e.getMessage());
            }
            VoltTable[] results;
            results = cluster1Client.callProcedure("@SnapshotRestore", jsObj.toString()).getResults();
            while(results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }

            ClientResponse resp = cluster1Client.callProcedure("@AdHoc", "select count(*) from helloworld;");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            Long rowCount = resp.getResults()[0].asScalarLong();
            assert(rowCount == 1);
            resp = cluster1Client.callProcedure("@AdHoc", "select count(*) from object1;");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            rowCount = resp.getResults()[0].asScalarLong();
            assert(rowCount == 1);
            resp = cluster1Client.callProcedure("@AdHoc", "select count(*) from object2;");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            rowCount = resp.getResults()[0].asScalarLong();
            assert(rowCount == 1);

        } finally {
            System.out.println("Shutting everything down.");
            if (cluster1Client != null) {
                try { cluster1Client.close(); } catch (Exception e) {}
            }
            if (cluster2Client != null) {
                try { cluster2Client.close(); } catch (Exception e) {}
            }
            if (cluster1 != null) {
                try { cluster1.shutDown(); } catch (Exception e) {}
            }
            if (cluster2 != null) {
                try { cluster2.shutDown(); } catch (Exception e) {}
            }
        }
    }

    /**
     * Build a list of the tests to be run. Use the regression suite
     * helpers to allow multiple back ends.
     * JUnit magic that uses the regression suite helper classes.
     */
    static public junit.framework.Test suite() {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestRestoreSysprocSuite.class);

        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();

        config =
            new CatalogChangeSingleProcessServer(JAR_NAME, 3,
                                                 BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);

        return builder;
    }
}
