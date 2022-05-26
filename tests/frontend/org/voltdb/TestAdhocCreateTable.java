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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.TestExportBaseSocketExport.ServerListener;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestAdhocCreateTable extends AdhocDDLTestBase {

    // Add a test for partitioning a table
    @Test
    public void testBasicCreateTable() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            assertFalse(findTableInSystemCatalogResults("FOO"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create table FOO (ID int default 0, VAL varchar(64 bytes));");
            }
            catch (ProcCallException pce) {
                fail("create table should have succeeded");
            }
            assertTrue(findTableInSystemCatalogResults("FOO"));
            // make sure we can't create the same table twice
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "create table FOO (ID int default 0, VAL varchar(64 bytes));");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't have been able to create table FOO twice.", threw);

            // make sure cannot create table with default value equals to minimum value
            String[] testType = {"tinyint", "smallint", "integer", "bigint"};
            String[] minVal = {"-128", "-32768", "-2147483648", "-9223372036854775808"};

            for (int i = 0; i < testType.length; i++) {
                threw = false;
                try {
                    m_client.callProcedure("@AdHoc",
                            "create table t (c " + testType[i] + " default " + minVal[i] + ");");
                }
                catch (ProcCallException pce) {
                    assertTrue(pce.getMessage().contains("data exception: numeric value out of range"));
                    threw = true;
                }
                assertTrue("Creating numeric table column using the reserved minimum value as "
                        + "default should throw an exception.", threw);
            }
        }
        finally {
            teardownSystem();
        }
    }

    // Test creating a table when we feed a statement containing newlines.
    // I honestly didn't expect this to work yet --izzy
    @Test
    public void testMultiLineCreateTable() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            // Check basic drop of partitioned table that should work.
            ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertFalse(findTableInSystemCatalogResults("FOO"));
            System.out.println(resp.getResults()[0]);
            try {
                m_client.callProcedure("@AdHoc",
                        "create table FOO (\n" +
                        "ID int default 0 not null,\n" +
                        "VAL varchar(32 bytes)\n" +
                        ");");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("create table should have succeeded");
            }
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("FOO"));
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testCreatePartitionedTable() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            // Check basic create of partitioned table that should work.
            ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertFalse(findTableInSystemCatalogResults("FOO"));
            System.out.println(resp.getResults()[0]);
            try {
                m_client.callProcedure("@AdHoc",
                        "create table FOO (\n" +
                        "ID int default 0 not null,\n" +
                        "VAL varchar(32 bytes),\n" +
                        "VAL2 bigint not null assumeunique\n" +
                        ");\n" +
                        "partition table FOO on column ID;\n"
                        );
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("create table should have succeeded");
            }
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("FOO"));
            assertTrue(isColumnPartitionColumn("FOO", "ID"));

            // This, however, not being batched, won't work until the empty table
            // check goes in.
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertFalse(findTableInSystemCatalogResults("BAR"));
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "create table BAR (\n" +
                        "ID int default 0 not null,\n" +
                        "VAL varchar(32 bytes),\n" +
                        // check that we can create a table with assumeunique
                        // (starts replicated) and partition it in a separate @AdHoc call
                        "VAL2 bigint not null assumeunique,\n" +
                        "constraint blerg assumeunique(VAL)\n" +
                        ");\n");
                m_client.callProcedure("@AdHoc",
                        "partition table BAR on column ID;\n"
                        );
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertFalse("Failed to partition an already created table.", threw);
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertTrue(findTableInSystemCatalogResults("BAR"));
            assertTrue(isColumnPartitionColumn("BAR", "ID"));
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testCreateDRTable() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            // Check basic create of partitioned table that should work.
            ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertFalse(findTableInSystemCatalogResults("FOO"));
            System.out.println(resp.getResults()[0]);
            try {
                m_client.callProcedure("@AdHoc",
                        "create table FOO (\n" +
                        "ID int default 0 not null,\n" +
                        "VAL varchar(32 bytes),\n" +
                        "VAL2 bigint not null assumeunique\n" +
                        ");\n" +
                        "DR table FOO;\n"
                        );
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("create table should have succeeded");
            }
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("FOO"));
            assertTrue(isDRedTable("FOO"));
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testCreateZeroColumnTable() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);
            try {
                // this ddl try to create a zero length table, which is not allowed.
                m_client.callProcedure("@AdHoc",
                        "create table T();");
            } catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("zero-column table is not allowed"));
                assertFalse(findTableInSystemCatalogResults("T"));
                return;
            }
            fail("create zero length table should fail.");
        } finally {
            teardownSystem();
        }
    }

    @Test
    public void testCreateMigrateTable() throws Exception {
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();

        Properties props = new Properties();
        props.put("replicated", String.valueOf(false));
        props.put("skipinternals", "false");
        props.put("socket.dest", "localhost:5001");
        props.put("timezone", "GMT");

        builder.addExport(true, ServerExportEnum.CUSTOM, props, "MigrateTableTarget");
        ServerListener serverSocket = new ServerListener(5001);
        serverSocket.start();
        builder.setUseDDLSchema(true);
        LocalCluster m_cluster = new LocalCluster("test_migrate_export_enabled.jar", 2, 1, 0,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        m_cluster.setHasLocalServer(false);
        m_cluster.setMaxHeap(1024);
        boolean success = m_cluster.compile(builder);

        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);
            // test create table is working
            m_client.callProcedure("@AdHoc", "create table T migrate to target MigrateTableTarget (id int not null);");
            m_client.callProcedure("@AdHoc", "create index migratingIndex on t(id) where not migrating;");
            assertTrue(findTableInSystemCatalogResults("T"));
            m_client.callProcedure("@AdHoc", "insert into t values(1);");
            m_client.callProcedure("@AdHoc", "insert into t values(2);");
            m_client.callProcedure("@AdHoc", "insert into t values(3);");
            // test migrating index is working
            VoltTable tb = m_client.callProcedure("@AdHoc", "select * from t where not migrating;").getResults()[0];
            // if no ttl column is set then it will behave like a regular table
            assertEquals(tb.getRowCount(), 3);

            m_client.callProcedure("@AdHoc",
                    "create table T2 migrate to target MigrateTableTarget (ts1 timestamp not null, ts2 timestamp not null);");
            assertTrue(findTableInSystemCatalogResults("T2"));
            // test ALTER table ADD / DROP / ALTER TTL column
            boolean threw;
            try {
                threw = false;
                m_client.callProcedure("@AdHoc",
                        "alter table T2 add using ttl 10 on column ts1;");
            } catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("May not add TTL column"));
                threw = true;
            }
            assertTrue("Shouldn't have been able to alter add ttl column in migrate table.", threw);
            try {
                threw = false;
                m_client.callProcedure("@AdHoc",
                        "alter table T2 drop ttl;");
            } catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("May not drop TTL column"));
                threw = true;
            }
            assertTrue("Shouldn't have been able to alter drop ttl column in migrate table.", threw);
        } finally {
            teardownSystem();
        }
    }
}
