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

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

public class TestViewsInPartialSnapshotRestore extends SaveRestoreBase{

    public TestViewsInPartialSnapshotRestore(String name) {
        super(name);
    }

    public void testPartialRestoreWithViewsIncludetables() throws Exception {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testPartialRestoreWithViewsIncludetables");
        LocalCluster cluster = null;
        Client client = null;

        try {
            int clusterReplicationPort = 11000;
            String clusterRoot = TMPDIR;
            // Start cluster1 with consumer connection disabled.
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.setDrConsumerConnectionDisabled();
            cluster = LocalCluster.createLocalCluster("", 2, 2, 0, 1,
                    clusterReplicationPort, clusterReplicationPort + 100, clusterRoot,
                    "restore-with-views1.jar", DrRoleType.MASTER, false, builder);

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setProcedureCallTimeout(10 * 60 * 1000); // 10 min
            client = ClientFactory.createClient(clientConfig);
            client.createConnection(cluster.getListenerAddress(0));

            //Create a schema with table inventory and a view on table inventory.
            //Insert data into inventory and verify table and view entries.
            String createTable = "CREATE TABLE inventory (" +
                                 "productID INTEGER NOT NULL," +
                                 "warehouse INTEGER NOT NULL" +
                                 ");";
            String insertData = "INSERT INTO inventory VALUES (61, 1);" +
                                "INSERT INTO inventory VALUES (67, 1);" +
                                "INSERT INTO inventory VALUES (273, 3);" +
                                "INSERT INTO inventory VALUES (399, 2);" +
                                "INSERT INTO inventory VALUES (873, 2);";
            String createView = "CREATE VIEW inventory_count_by_warehouse (" +
                                "warehouse," +
                                "inventory_count" +
                                ") AS SELECT " +
                                "warehouse," +
                                "COUNT(*) " +
                                "FROM inventory GROUP BY warehouse;";

            ClientResponse cr = client.callProcedure("@AdHoc", createTable);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc", insertData);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc", createView);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            TestSaveRestoreSysprocSuite.saveTablesWithDefaultOptions(client, TESTNONCE);
            TestSaveRestoreSysprocSuite.validateSnapshot(true, TESTNONCE);
            m_config.shutDown();
            m_config.startUp();

            cr = client.callProcedure("@AdHoc", "drop view inventory_count_by_warehouse if exists");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc", "drop table inventory;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            //In a fresh db, do a partial restore where inventory is included in the list of tables to be restored.
            //After the data is restored, make sure that view data is also correct.
            JSONObject restore = new JSONObject();
            try {
                restore.put(SnapshotUtil.JSON_PATH, TMPDIR);
                restore.put(SnapshotUtil.JSON_NONCE, TESTNONCE);
                restore.put(SnapshotUtil.JSON_TABLES, new String[] {"inventory"});
            } catch (JSONException e) {
                fail("JSON exception" + e.getMessage());
            }
            VoltTable[] results;
            results = client.callProcedure("@SnapshotRestore", restore.toString()).getResults();

            while(results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }

            cr = client.callProcedure("@AdHoc", "select COUNT(*) from inventory;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            Long rowCount = cr.getResults()[0].asScalarLong();
            assert(rowCount == 5);
            cr = client.callProcedure("@AdHoc", "select COUNT(*) from inventory_count_by_warehouse;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            rowCount = cr.getResults()[0].asScalarLong();
            assert(rowCount == 3);

        } finally {
            System.out.println("Shutting everything down.");
            if (client != null) {
                try { client.close(); } catch (Exception e) {}
            }
            if (cluster != null) {
                try { cluster.shutDown(); } catch (Exception e) {}
            }
        }
    }

    public void testPartialRestoreWithViewsIncludeViews() throws Exception {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testPartialRestoreWithViewsIncludeViews");
        LocalCluster cluster = null;
        Client client = null;

        try {
            int clusterReplicationPort = 11000;
            String clusterRoot = TMPDIR;
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.setDrConsumerConnectionDisabled();
            cluster = LocalCluster.createLocalCluster("", 2, 2, 0, 1,
                    clusterReplicationPort, clusterReplicationPort + 100, clusterRoot,
                    "restore-with-views2.jar", DrRoleType.MASTER, false, builder);

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setProcedureCallTimeout(10 * 60 * 1000); // 10 min
            client = ClientFactory.createClient(clientConfig);
            client.createConnection(cluster.getListenerAddress(0));

            //Create a schema with table inventory and a view on table inventory.
            //Insert data into inventory and verify table and view entries.
            String createTable = "CREATE TABLE inventory (" +
                                 "productID INTEGER NOT NULL," +
                                 "warehouse INTEGER NOT NULL" +
                                 ");";
            String insertData = "INSERT INTO inventory VALUES (61, 1);" +
                                "INSERT INTO inventory VALUES (67, 1);" +
                                "INSERT INTO inventory VALUES (273, 3);" +
                                "INSERT INTO inventory VALUES (399, 2);" +
                                "INSERT INTO inventory VALUES (873, 2);";
            String createView = "CREATE VIEW inventory_count_by_warehouse (" +
                                "warehouse," +
                                "inventory_count" +
                                ") AS SELECT " +
                                "warehouse," +
                                "COUNT(*) " +
                                "FROM inventory GROUP BY warehouse;";
            ClientResponse cr = client.callProcedure("@AdHoc", createTable);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc", insertData);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc", createView);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            List<Long> prevCnt = new ArrayList<>();
            String cnt1 = "SELECT inventory_count FROM inventory_count_by_warehouse WHERE warehouse=1";
            String cnt2 = "SELECT inventory_count FROM inventory_count_by_warehouse WHERE warehouse=2";
            String cnt3 = "SELECT inventory_count FROM inventory_count_by_warehouse WHERE warehouse=3";

            cr = client.callProcedure("@AdHoc", cnt1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            prevCnt.add(cr.getResults()[0].asScalarLong());

            cr = client.callProcedure("@AdHoc", cnt2);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            prevCnt.add(cr.getResults()[0].asScalarLong());

            cr = client.callProcedure("@AdHoc", cnt3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            prevCnt.add(cr.getResults()[0].asScalarLong());

            TestSaveRestoreSysprocSuite.saveTablesWithDefaultOptions(client, TESTNONCE);
            TestSaveRestoreSysprocSuite.validateSnapshot(true, TESTNONCE);
            m_config.shutDown();
            m_config.startUp();

            cr = client.callProcedure("@AdHoc", "drop view inventory_count_by_warehouse if exists");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc", "drop table inventory;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            //In a fresh db, do a partial restore where the view inventory_count_by_warehouse is included in the list of tables to be restored.
            //After the data is restored, make sure that the table inventory is empty, the view is correct
            JSONObject restore1 = new JSONObject();
            try {
                restore1.put(SnapshotUtil.JSON_PATH, TMPDIR);
                restore1.put(SnapshotUtil.JSON_NONCE, TESTNONCE);
                restore1.put(SnapshotUtil.JSON_TABLES, new String[] {"inventory_count_by_warehouse"});
            } catch (JSONException e) {
                fail("JSON exception" + e.getMessage());
            }
            VoltTable[] results;
            results = client.callProcedure("@SnapshotRestore", restore1.toString()).getResults();

            while(results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }

            cr = client.callProcedure("@AdHoc", "select COUNT(*) from inventory;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            Long rowCount = cr.getResults()[0].asScalarLong();
            assert(rowCount == 0);
            cr = client.callProcedure("@AdHoc", "select COUNT(*) from inventory_count_by_warehouse;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            rowCount = cr.getResults()[0].asScalarLong();
            assert(rowCount == 3);

            //Do another partial restore where inventory is included.
            //After the data is restored, make sure that the table data is correct,
            //and the data in the view doubled in value.
            JSONObject restore2 = new JSONObject();
            try {
                restore2.put(SnapshotUtil.JSON_PATH, TMPDIR);
                restore2.put(SnapshotUtil.JSON_NONCE, TESTNONCE);
                restore2.put(SnapshotUtil.JSON_TABLES, new String[] {"inventory"});
            } catch (JSONException e) {
                fail("JSON exception" + e.getMessage());
            }
            results = client.callProcedure("@SnapshotRestore", restore2.toString()).getResults();

            while(results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }

            cr = client.callProcedure("@AdHoc", "select COUNT(*) from inventory;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            rowCount = cr.getResults()[0].asScalarLong();
            assert(rowCount == 5);
            cr = client.callProcedure("@AdHoc", "select COUNT(*) from inventory_count_by_warehouse;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            rowCount = cr.getResults()[0].asScalarLong();
            assert(rowCount == 3);

            List<Long> afterCnt = new ArrayList<>();

            cr = client.callProcedure("@AdHoc", cnt1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            afterCnt.add(cr.getResults()[0].asScalarLong());

            cr = client.callProcedure("@AdHoc", cnt2);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            afterCnt.add(cr.getResults()[0].asScalarLong());

            cr = client.callProcedure("@AdHoc", cnt3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            afterCnt.add(cr.getResults()[0].asScalarLong());

            for(int i = 0 ; i < 3 ; i++) {
                assert(prevCnt.get(i) * 2 == afterCnt.get(i));
            }

        } finally {
            System.out.println("Shutting everything down.");
            if (client != null) {
                try { client.close(); } catch (Exception e) {}
            }
            if (cluster != null) {
                try { cluster.shutDown(); } catch (Exception e) {}
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
            new MultiConfigSuiteBuilder(TestViewsInPartialSnapshotRestore.class);

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
