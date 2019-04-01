/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.export;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientUtils;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.TestExportBaseSocketExport.ServerListener;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

public class TestMigrateExport extends ExportLocalClusterBase {

    private LocalCluster m_cluster = null;

    private static final String SCHEMA= "CREATE table NIBBLE_EXPORT (" +
            "PKEY          INTEGER          NOT NULL," +
            " A_TIMESTAMP   TIMESTAMP        NOT NULL," +
            " A_INLINE_S1   VARCHAR(4)       NOT NULL," +
            " A_INLINE_S2   VARCHAR(63)      NOT NULL, PRIMARY KEY(PKEY)," +
            ") using TTL 10 seconds on column A_TIMESTAMP BATCH_SIZE 400 MIGRATE to TARGET NIBBLE_EXPORT;" +
            " \nPARTITION table NIBBLE_EXPORT on column PKEY;" +
            " \nCREATE INDEX MINDEX1 ON NIBBLE_EXPORT(A_TIMESTAMP) WHERE NOT MIGRATING;";

    private static final String SCHEMA1= "CREATE table NIBBLE_EXPORT_DELETE (" +
            "PKEY          INTEGER          NOT NULL," +
            " A_TIMESTAMP   TIMESTAMP        NOT NULL," +
            " A_INLINE_S1   VARCHAR(4)       NOT NULL," +
            " A_INLINE_S2   VARCHAR(63)      NOT NULL, PRIMARY KEY(PKEY)," +
            ") using TTL 10 seconds on column A_TIMESTAMP BATCH_SIZE 400 MIGRATE to TARGET NIBBLE_EXPORT1;" +
            " \nPARTITION table NIBBLE_EXPORT_DELETE on column PKEY;" +
            " \nCREATE INDEX MINDEX2 ON NIBBLE_EXPORT_DELETE(A_TIMESTAMP) WHERE NOT MIGRATING;";

    private static final String SCHEMA2= "CREATE table NIBBLE_EXPORT_UPDATE (" +
            "PKEY          INTEGER          NOT NULL," +
            " A_TIMESTAMP   TIMESTAMP        NOT NULL," +
            " A_INLINE_S1   VARCHAR(4)       NOT NULL," +
            " A_INLINE_S2   VARCHAR(63)      NOT NULL, PRIMARY KEY(PKEY)," +
            ") using TTL 10 seconds on column A_TIMESTAMP BATCH_SIZE 400 MIGRATE to TARGET NIBBLE_EXPORT2;" +
            " \nPARTITION table NIBBLE_EXPORT_UPDATE on column PKEY;" +
            " \nCREATE INDEX MINDEX3 ON NIBBLE_EXPORT_UPDATE(A_TIMESTAMP) WHERE NOT MIGRATING;";

    private static final String COUNT1 = "select count(*) from %s;";
    private static final String NOT_MIGRATING_QUERY = "select count(*) from %s WHERE NOT MIGRATING AND A_TIMESTAMP < CURRENT_TIMESTAMP();";
    private static final String INSERT = "INSERT INTO %s VALUES(%d, CURRENT_TIMESTAMP(), 'xx', 'yy');";

    public static class UpdateConstraintProc extends VoltProcedure {
        public final SQLStmt stmt = new SQLStmt("UPDATE NIBBLE_EXPORT_UPDATE SET A_INLINE_S2='MYTEST' WHERE PKEY < 100;");
        public final SQLStmt stmt2 = new SQLStmt("INSERT INTO NIBBLE_EXPORT_UPDATE VALUES(200, CURRENT_TIMESTAMP(), 'xx', 'yy');");
        public VoltTable[] run(int partitionKey) {
            voltQueueSQL(stmt);
            voltExecuteSQL(false);
            voltQueueSQL(stmt2);
            return voltExecuteSQL(true);
        }
    }

    public static class DeleteConstraintProc extends VoltProcedure {
        public final SQLStmt stmt = new SQLStmt("DELETE FROM NIBBLE_EXPORT_DELETE WHERE PKEY < 100;");
        public final SQLStmt stmt2 = new SQLStmt("INSERT INTO NIBBLE_EXPORT_DELETE VALUES(200, CURRENT_TIMESTAMP(), 'xx', 'yy');");
        public VoltTable[] run(int partitionKey) {
            voltQueueSQL(stmt);
            voltExecuteSQL(false);
            voltQueueSQL(stmt2);
            return voltExecuteSQL(true);
        }
    }

    private static  final String PROCS =
            "CREATE PROCEDURE FROM CLASS org.voltdb.export.TestMigrateExport$UpdateConstraintProc;" +
                    "PARTITION PROCEDURE TestMigrateExport$UpdateConstraintProc ON TABLE NIBBLE_EXPORT COLUMN PKEY;" +
                    "CREATE PROCEDURE FROM CLASS org.voltdb.export.TestMigrateExport$DeleteConstraintProc;" +
                    "PARTITION PROCEDURE TestMigrateExport$DeleteConstraintProc ON TABLE NIBBLE_EXPORT COLUMN PKEY;";

    private static final int k_factor = 1;

    @Before
    public void setUp() throws Exception
    {
        resetDir();
        VoltFile.resetSubrootForThisProcess();
        Map<String, String> additionalEnv = new HashMap<String, String>();
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(SCHEMA);
        project.addLiteralSchema(SCHEMA1);
        project.addLiteralSchema(SCHEMA2);
        project.addLiteralSchema(PROCS);
        project.addExport(true, "custom", createSocketExportProperties("NIBBLE_EXPORT", false ), "NIBBLE_EXPORT");
        project.addExport(true, "custom", createSocketExportProperties("NIBBLE_EXPORT1", false ), "NIBBLE_EXPORT1");
        project.addExport(true, "custom", createSocketExportProperties("NIBBLE_EXPORT2", false ), "NIBBLE_EXPORT2");
        startListener();

        m_cluster = new LocalCluster("test_migrate_export_enabled.jar", 4, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        m_cluster.setHasLocalServer(false);
        m_cluster.setMaxHeap(1024);
        boolean compile = m_cluster.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("test_migrate_export_enabled.xml"));
        m_cluster.startUp(true);

        project = new VoltProjectBuilder();
        project.addLiteralSchema(SCHEMA);
        project.addLiteralSchema(SCHEMA1);
        project.addLiteralSchema(SCHEMA2);
        project.addLiteralSchema(PROCS);
        project.addExport(false, "custom", createSocketExportProperties("NIBBLE_EXPORT", false ), "NIBBLE_EXPORT");
        project.addExport(false, "custom", createSocketExportProperties("NIBBLE_EXPORT1", false ), "NIBBLE_EXPORT1");
        project.addExport(false, "custom", createSocketExportProperties("NIBBLE_EXPORT2", false ), "NIBBLE_EXPORT2");

        LocalCluster config = new LocalCluster("test_migrate_export_disabled.jar", 4, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("test_migrate_export_disabled.xml"));

        m_verifier = new ExportTestExpectedData(m_serverSockets, false, true, k_factor + 1);
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("Shutting down client and server");
        for (Entry<String, ServerListener> entry : m_serverSockets.entrySet()) {
            ServerListener serverSocket = entry.getValue();
            if (serverSocket != null) {
                serverSocket.closeClient();
                serverSocket.close();
            }
        }
        m_cluster.shutDown();
    }

    @Test
    public void testMigrateExport() throws Exception
    {
        System.out.println("testMigrateExport");
        final Client client = getClient(m_cluster);

        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }
        for (int i=0; i < 1000; i++) {
            client.callProcedure("@AdHoc", String.format(INSERT, "NIBBLE_EXPORT", i));
        }
        verifyExport(client, 1000, false, "NIBBLE_EXPORT");
        verifyCount(client, String.format(COUNT1,"NIBBLE_EXPORT"),  0);
    }

    @Test
    public void testMigrateExportUpdate() throws Exception {
        System.out.println("testMigrateExportUpdate");
        final Client client = getClient(m_cluster);
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        // disable export
        String deploymentURL = Configuration.getPathToCatalogForTest("test_migrate_export_disabled.xml");
        String depBytes = new String(ClientUtils.fileToBytes(new File(deploymentURL)), Constants.UTF8ENCODING);
        ClientResponse deploymentUpdateResp = client.callProcedure("@UpdateApplicationCatalog", null, depBytes);
        assertTrue(deploymentUpdateResp.getStatus() == ClientResponse.SUCCESS);

        // insert data while export is disabled
        System.out.println("Stream disabled and insert 1000 rows");
        for (int i=0; i < 1000; i++) {
            client.callProcedure("@AdHoc", String.format(INSERT,"NIBBLE_EXPORT_UPDATE", i));
        }

        // verify all tuples are migrated
        verifyCount(client, String.format(NOT_MIGRATING_QUERY,"NIBBLE_EXPORT_UPDATE") ,0);
        System.out.println("All 1000 rows were migrated. Verifing export stats");
        verifyExport(client, 1000, false, "NIBBLE_EXPORT_UPDATE");

        // Update 100 tuples
        System.out.println("Update migrated rows.");
        client.callProcedure("@AdHoc", "UPDATE NIBBLE_EXPORT_UPDATE SET A_INLINE_S2='MYTEST' WHERE PKEY < 100;");
        VoltTable vt = client.callProcedure("@AdHoc", String.format(NOT_MIGRATING_QUERY,"NIBBLE_EXPORT_UPDATE")).getResults()[0];

        // There must be not_migrating rows
        assert(0 < vt.asScalarLong());

        // The 100 updated rows will migrated again. Now the total migrated rows are 1100.
        verifyExport(client, 1100, false, "NIBBLE_EXPORT_UPDATE");

        // Test update failure, constraint violation
        // Test update failure, constraint violation and undo
        ProcedureCallback callback = cr -> {
            assert(cr.getStatusString().indexOf("VOLTDB ERROR: CONSTRAINT VIOLATION") > -1);
        };
        client.callProcedure(callback,"TestMigrateExport$UpdateConstraintProc", 1);

        // Still 1100?
        Thread.sleep(20000);
        verifyExport(client, 1100, false, "NIBBLE_EXPORT_UPDATE");

        // Enable the connector
        deploymentURL = Configuration.getPathToCatalogForTest("test_migrate_export_enabled.xml");
        depBytes = new String(ClientUtils.fileToBytes(new File(deploymentURL)), Constants.UTF8ENCODING);
        ClientResponse resp = client.callProcedure("@UpdateApplicationCatalog", null, depBytes);
        assertTrue(resp.getStatus() == ClientResponse.SUCCESS);

        // All rows in persistent table are deleted
        verifyCount(client, String.format(COUNT1, "NIBBLE_EXPORT_UPDATE"), 0);
    }

    @Test
    public void testMigrateExportDelete() throws Exception {
        System.out.println("testMigrateExportDelete");
        final Client client = getClient(m_cluster);
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        // Disable export
        String deploymentURL = Configuration.getPathToCatalogForTest("test_migrate_export_disabled.xml");
        String depBytes = new String(ClientUtils.fileToBytes(new File(deploymentURL)), Constants.UTF8ENCODING);
        ClientResponse deploymentUpdateResp = client.callProcedure("@UpdateApplicationCatalog", null, depBytes);
        assertTrue(deploymentUpdateResp.getStatus() == ClientResponse.SUCCESS);

        // Insert data while export is disabled
        System.out.println("Stream disabled and insert 1000 rows");
        for (int i=0; i < 1000; i++) {
            client.callProcedure("@AdHoc", String.format(INSERT, "NIBBLE_EXPORT_DELETE", i));
        }

        // Verify all tuples are migrated
        verifyCount(client, String.format(NOT_MIGRATING_QUERY,"NIBBLE_EXPORT_DELETE"), 0);
        System.out.println("All 1000 rows were migrated. Verifing export stats");

        verifyExport(client, 1000, false, "NIBBLE_EXPORT_DELETE");

        // Delete 100 tuples
        client.callProcedure("@AdHoc", "DELETE FROM NIBBLE_EXPORT_DELETE WHERE PKEY < 100;");
        verifyCount(client, String.format(COUNT1,"NIBBLE_EXPORT_DELETE"), 900);

        // Test update failure, constraint violation and undo
        ProcedureCallback callback = cr -> {
            assert(cr.getStatusString().indexOf("VOLTDB ERROR: CONSTRAINT VIOLATION") > -1);
        };
        client.callProcedure(callback,"TestMigrateExport$DeleteConstraintProc", 1);

        // Still 900
        verifyCount(client, String.format(COUNT1,"NIBBLE_EXPORT_DELETE"), 900);

        System.out.println("Enable connector");
        deploymentURL = Configuration.getPathToCatalogForTest("test_migrate_export_enabled.xml");
        depBytes = new String(ClientUtils.fileToBytes(new File(deploymentURL)), Constants.UTF8ENCODING);
        ClientResponse resp = client.callProcedure("@UpdateApplicationCatalog", null, depBytes);
        assertTrue(resp.getStatus() == ClientResponse.SUCCESS);

        System.out.println("Connector enabled.");

        verifyExport(client, 0, true, "NIBBLE_EXPORT_DELETE");

        // All rows in persistent table are deleted
        verifyCount(client,  String.format(COUNT1,"NIBBLE_EXPORT_DELETE"), 0);
    }

    private void verifyCount(Client client, String countQuery, int expectedCount) throws Exception {
        long count = Long.MAX_VALUE;
        int numberOfAttempts = 10;
        while (count != expectedCount && numberOfAttempts > 0) {
            Thread.sleep(5000);
            VoltTable vt = client.callProcedure("@AdHoc", countQuery).getResults()[0];
            count = vt.asScalarLong();
            numberOfAttempts--;
        }
        assert(count == expectedCount);
    }

    private static void verifyExport(Client client, long expectedCount, boolean checkPending, String tableName){

        //allow time to get the stats
        final long maxSleep = TimeUnit.MINUTES.toMillis(2);
        boolean success = false;
        long start = System.currentTimeMillis();
        while (!success) {
            try {
                VoltTable vt = client.callProcedure("@Statistics", "EXPORT").getResults()[0];
                long count = 0;
                while (vt.advanceRow()) {
                    if (tableName.equalsIgnoreCase(vt.getString("SOURCE")) && "TRUE".equalsIgnoreCase(vt.getString("ACTIVE"))) {
                        if (checkPending) {
                            count += vt.getLong("TUPLE_PENDING");
                        } else {
                            count +=vt.getLong("TUPLE_COUNT");
                        }
                    }
                }
                if (count == expectedCount) {
                    success = true;
                } else {
                    if (maxSleep < (System.currentTimeMillis() - start)) {
                        break;

                    }
                    try { Thread.sleep(5000); } catch (Exception ignored) { }
                }
            } catch (Exception e) {
            }
        }
        assert(success);
    }
}
