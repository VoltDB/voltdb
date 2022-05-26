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

import java.io.File;

import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

public class TestLiveDDLSchemaSwitch extends AdhocDDLTestBase {
    // Test cases:
    // 1) Configure for master and UAC
    //    - Verify UAC works
    //    - Verify deployment-only UAC works
    //    - Verify Adhoc DDL fails
    //    - Verify adhoc query succeeds
    // 2) Configure for master and Adhoc DDL
    //    - Verify UAC fails
    //    - Verify deployment-only UAC works
    //    - Verify Adhoc DDL succeeds
    //    - Verify adhoc query succeeds
    // 3) Configure for replica and UAC
    //    - Verify failures (dunno how to fabricate "replicated" txns
    //    - Verify adhoc queries work
    //    - Promote cluster
    //    - Re-verify (1)
    // 4) Configure for replica and adhoc DDL
    //    - Verify failures
    //    - Verify adhoc queries
    //    - Promote
    //    - Re-verify (2)

    String m_pathToCatalog;
    String m_pathToDeployment;
    String m_pathToReplicaDeployment;
    String m_pathToOtherCatalog;
    String m_pathToOtherDeployment;
    String m_pathToOtherReplicaDeployment;

    void generateCatalogsAndDeployments(boolean useLiveDDL) throws Exception
    {
        m_pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        m_pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");
        m_pathToReplicaDeployment = Configuration.getPathToCatalogForTest("replicaadhocddl.xml");
        m_pathToOtherCatalog = Configuration.getPathToCatalogForTest("newadhocddl.jar");
        m_pathToOtherDeployment = Configuration.getPathToCatalogForTest("newadhocddl.xml");
        m_pathToOtherReplicaDeployment = Configuration.getPathToCatalogForTest("newreplicaadhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint PK_TREE primary key (ID)" +
                ");\n" +
                "create table FOO_R (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint PK_TREE_R primary key (ID)" +
                ");\n"
                );
        builder.addPartitionInfo("FOO", "ID");
        builder.setUseDDLSchema(useLiveDDL);
        builder.setDRMasterHost("localhost"); // fake DR connection so that replica can start
        boolean success = builder.compile(m_pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), m_pathToDeployment);

        builder.setDrReplica();
        success = builder.compile(m_pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), m_pathToReplicaDeployment);

        // get an alternate deployment file
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table BAZ (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint PK_TREE primary key (ID)" +
                ");\n" +
                "create table FOO_R (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint PK_TREE_R primary key (ID)" +
                ");\n"
                );
        builder.addPartitionInfo("BAZ", "ID");
        builder.setUseDDLSchema(useLiveDDL);
        builder.setDRMasterHost("localhost");
        builder.setDeadHostTimeout(6);
        success = builder.compile(m_pathToOtherCatalog, 2, 1, 0);
        assertTrue("2nd schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), m_pathToOtherDeployment);

        builder.setDrReplica();
        success = builder.compile(m_pathToOtherCatalog, 2, 1, 0);
        assertTrue("2nd schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), m_pathToOtherReplicaDeployment);
    }

    int getHeartbeatTimeout() throws Exception
    {
        boolean found = false;
        int timeout = -1;
        VoltTable result = m_client.callProcedure("@SystemInformation", "DEPLOYMENT").getResults()[0];
        while (result.advanceRow()) {
            if (result.getString("PROPERTY").equalsIgnoreCase("heartbeattimeout")) {
                found = true;
                timeout = Integer.valueOf(result.getString("VALUE"));
            }
        }
        assertTrue(found);
        return timeout;
    }

    void verifyDeploymentOnlyUAC() throws Exception
    {
        int timeout = getHeartbeatTimeout();
        assertEquals(org.voltcore.common.Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS, timeout);
        ClientResponse results =
            UpdateApplicationCatalog.update(m_client, null, new File(m_pathToOtherDeployment));
        assertEquals(ClientResponse.SUCCESS, results.getStatus());
        timeout = getHeartbeatTimeout();
        assertEquals(6, timeout);
    }

    void verifyAdhocQuery() throws Exception
    {
        ClientResponse result = m_client.callProcedure("@AdHoc", "select * from foo;");
        assertEquals(ClientResponse.SUCCESS, result.getStatus());
    }

    // GOing to want to retest this after we promote a replica, so bust it out
    void verifyMasterWithUAC() throws Exception
    {
        // UAC should work.
        ClientResponse results = UpdateApplicationCatalog.update(m_client, new File(m_pathToCatalog), null);
        assertEquals(ClientResponse.SUCCESS, results.getStatus());
        assertTrue(findTableInSystemCatalogResults("FOO"));

        // Adhoc DDL should be rejected
        assertFalse(findTableInSystemCatalogResults("BAR"));
        boolean threw = false;
        try {
            results = m_client.callProcedure("@AdHoc",
                    "create table BAR (ID integer, VAL varchar(50));");
        }
        catch (ProcCallException pce) {
            threw = true;
            assertTrue(pce.getMessage().contains("AdHoc DDL is forbidden"));
        }
        assertTrue("Adhoc DDL should have failed", threw);
        assertFalse(findTableInSystemCatalogResults("BAR"));

        // @UpdateClasses should be rejected
        assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));
        threw = false;
        try {
            InMemoryJarfile jarfile = new InMemoryJarfile();
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, org.voltdb_testprocs.fullddlfeatures.testImportProc.class);
            m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
        }
        catch (ProcCallException pce) {
            threw = true;
            assertTrue(pce.getMessage().contains("@UpdateClasses is forbidden"));
        }
        assertTrue("@UpdateClasses should have failed", threw);
        assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));

        verifyAdhocQuery();
    }

    @Test
    public void testMasterWithUAC() throws Exception
    {
        generateCatalogsAndDeployments(false);

        // Fire up a cluster with no catalog
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToDeployment = m_pathToDeployment;

        try {
            startSystem(config);
            verifyDeploymentOnlyUAC();
            verifyMasterWithUAC();
        }
        finally {
            teardownSystem();
        }
    }

    void verifyMasterWithAdhocDDL() throws Exception
    {
        // UAC with schema should fail
        assertFalse(findTableInSystemCatalogResults("FOO"));
        boolean threw = false;
        try {
            UpdateApplicationCatalog.update(m_client, new File(m_pathToCatalog), null);
        }
        catch (ProcCallException pce) {
            threw = true;
            assertTrue(pce.getMessage().contains("Use of @UpdateApplicationCatalog is forbidden"));
        }
        assertTrue("@UAC should have failed", threw);
        assertFalse(findTableInSystemCatalogResults("FOO"));

        // But, we can create that table with Adhoc DDL
        try {
            m_client.callProcedure("@AdHoc",
                    "create table FOO (ID integer, VAL varchar(50));");
        }
        catch (ProcCallException pce) {
            fail("Should be able to use Adhoc DDL to create a table.");
        }
        assertTrue(findTableInSystemCatalogResults("FOO"));

        // And so should adhoc queries
        verifyAdhocQuery();

        // If the procedure doesn't already exist, add it using @UpdateClasses
        if (!findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc")) {
            // Also, @UpdateClasses should only work with adhoc DDL
            InMemoryJarfile jarfile = new InMemoryJarfile();
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, org.voltdb_testprocs.fullddlfeatures.testImportProc.class);
            try {
                m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            } catch (ProcCallException pce) {
                fail("Should be able to call @UpdateClasses when adhoc DDL enabled.");
            }
            assertTrue(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));
        }
    }

    @Test
    public void testMasterWithAdhocDDL() throws Exception
    {
        generateCatalogsAndDeployments(true);

        // Fire up a cluster with no catalog
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToDeployment = m_pathToDeployment;

        try {
            startSystem(config);
            // Deployment-only UAC should work, though
            verifyDeploymentOnlyUAC();
            verifyMasterWithAdhocDDL();
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testReplicaWithUAC() throws Exception
    {
        generateCatalogsAndDeployments(false);

        // Fire up a cluster with no catalog
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = m_pathToOtherCatalog;
        config.m_pathToDeployment = m_pathToReplicaDeployment;

        try {
            startSystem(config);
            // UAC with schema should succeed
            assertFalse(findTableInSystemCatalogResults("FOO"));
            boolean threw = false;
            try {
                UpdateApplicationCatalog.update(m_client, new File(m_pathToCatalog), null);
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertFalse("@UAC add table should be accepted on the consumer cluster", threw);
            assertTrue(findTableInSystemCatalogResults("FOO"));

            // deployment-only UAC should succeed
            threw = false;
            try {
                UpdateApplicationCatalog.update(m_client, null, new File(m_pathToOtherReplicaDeployment));
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertFalse("@UAC to new catalog on consumer cluster should have succeed", threw);
            assertEquals(getHeartbeatTimeout(), 6);
            // Adhoc DDL should be rejected
            assertFalse(findTableInSystemCatalogResults("BAR"));
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "create table BAR (ID integer, VAL varchar(50));");
            }
            catch (ProcCallException pce) {
                threw = true;
                System.out.println(pce.getMessage());
                assertTrue(pce.getMessage().contains("AdHoc DDL is forbidden"));
            }
            assertTrue("Adhoc DDL should have failed", threw);
            assertFalse(findTableInSystemCatalogResults("BAR"));

            // @UpdateClasses (which is an AdHoc capability) should be rejected
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));
            threw = false;
            try {
                InMemoryJarfile jarfile = new InMemoryJarfile();
                VoltCompiler comp = new VoltCompiler(false);
                comp.addClassToJar(jarfile, org.voltdb_testprocs.fullddlfeatures.testImportProc.class);
                m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            }
            catch (ProcCallException pce) {
                threw = true;
                assertTrue(pce.getMessage().contains("@UpdateClasses is forbidden"));
            }
            assertTrue("@UpdateClasses should have failed", threw);
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));

            // Promote, should behave like the original master test
            m_client.callProcedure("@Promote");
            verifyMasterWithUAC();
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testReplicaWithAdhocDDL() throws Exception
    {
        generateCatalogsAndDeployments(true);

        // Fire up a cluster with no catalog
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = m_pathToOtherCatalog;
        config.m_pathToDeployment = m_pathToReplicaDeployment;

        try {
            startSystem(config);
            // UAC with schema should fail
            assertFalse(findTableInSystemCatalogResults("FOO"));
            boolean threw = false;
            try {
                UpdateApplicationCatalog.update(m_client, new File(m_pathToCatalog), null);
            }
            catch (ProcCallException pce) {
                threw = true;
                assertTrue(pce.getMessage().contains("Cluster is configured to use AdHoc DDL"));
            }
            assertTrue("@UAC should have failed", threw);
            assertFalse(findTableInSystemCatalogResults("FOO"));

            // deployment-only UAC should fail
            threw = false;
            try {
                UpdateApplicationCatalog.update(m_client, null, new File(m_pathToOtherReplicaDeployment));
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertFalse("@UAC should should succeed with just a deployment file", threw);
            assertEquals(getHeartbeatTimeout(), 6);

            // Adhoc DDL should be rejected
            assertFalse(findTableInSystemCatalogResults("BAR"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create table BAR (ID integer, VAL varchar(50));");
            }
            catch (ProcCallException pce) {
                fail("@AdHoc should succeed on replica cluster");
            }
            assertTrue(findTableInSystemCatalogResults("BAR"));

            // Adhoc DML updates should be rejected in the replica
            threw = false;
            try {
                m_client.callProcedure("@AdHoc", "insert into BAR values (100, 'ABC');");
            }
            catch (ProcCallException pce) {
                threw = true;
                System.out.println(pce.getMessage());
                assertTrue(pce.getMessage().contains("Write procedure @AdHoc_RW_MP is not allowed in replica cluster"));
            }
            assertTrue("Adhoc DDL should have failed", threw);

            // @UpdateClasses should be rejected
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));
            threw = false;
            try {
                InMemoryJarfile jarfile = new InMemoryJarfile();
                VoltCompiler comp = new VoltCompiler(false);
                comp.addClassToJar(jarfile, org.voltdb_testprocs.fullddlfeatures.testImportProc.class);
                m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            }
            catch (ProcCallException pce) {
                threw = true;
                assertTrue(pce.getMessage().contains("Write procedure @UpdateClasses is not allowed"));
            }
            assertFalse("@UpdateClasses should have worked", threw);
            assertTrue(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));

            // adhoc queries still work
            ClientResponse result = m_client.callProcedure("@AdHoc", "select * from baz;");
            assertEquals(ClientResponse.SUCCESS, result.getStatus());

            // Promote, should behave like the original master test
            m_client.callProcedure("@Promote");
            verifyMasterWithAdhocDDL();
        }
        finally {
            teardownSystem();
        }
    }
}
