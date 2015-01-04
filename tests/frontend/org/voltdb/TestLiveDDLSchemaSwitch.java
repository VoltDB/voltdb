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

package org.voltdb;

import java.io.File;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientUtils;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
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
    String m_pathToOtherCatalog;
    String m_pathToOtherDeployment;

    void generateCatalogsAndDeployments(boolean useLiveDDL) throws Exception
    {
        m_pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        m_pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");
        m_pathToOtherCatalog = Configuration.getPathToCatalogForTest("newadhocddl.jar");
        m_pathToOtherDeployment = Configuration.getPathToCatalogForTest("newadhocddl.xml");

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
        boolean success = builder.compile(m_pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), m_pathToDeployment);

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
        builder.setDeadHostTimeout(6);
        success = builder.compile(m_pathToOtherCatalog, 2, 1, 0);
        assertTrue("2nd schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), m_pathToOtherDeployment);
    }

    void verifyDeploymentOnlyUAC() throws Exception
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
        assertEquals(org.voltcore.common.Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS, timeout);
        ClientResponse results =
            m_client.updateApplicationCatalog(null, new File(m_pathToOtherDeployment));
        assertEquals(ClientResponse.SUCCESS, results.getStatus());
        found = false;
        timeout = -1;
        result = m_client.callProcedure("@SystemInformation", "DEPLOYMENT").getResults()[0];
        while (result.advanceRow()) {
            if (result.getString("PROPERTY").equalsIgnoreCase("heartbeattimeout")) {
                found = true;
                timeout = Integer.valueOf(result.getString("VALUE"));
            }
        }
        assertTrue(found);
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
        assertFalse(findTableInSystemCatalogResults("FOO"));
        // UAC should work.
        ClientResponse results = m_client.updateApplicationCatalog(new File(m_pathToCatalog), null);
        assertEquals(ClientResponse.SUCCESS, results.getStatus());
        assertTrue(findTableInSystemCatalogResults("FOO"));
        verifyDeploymentOnlyUAC();

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
            VoltCompiler comp = new VoltCompiler();
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

    public void testMasterWithUAC() throws Exception
    {
        generateCatalogsAndDeployments(false);

        // Fire up a cluster with no catalog
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToDeployment = m_pathToDeployment;

        try {
            startSystem(config);
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
            m_client.updateApplicationCatalog(new File(m_pathToCatalog), null);
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

        // Deployment-only UAC should work, though
        verifyDeploymentOnlyUAC();
        // And so should adhoc queries
        verifyAdhocQuery();

        // Also, @UpdateClasses should only work with adhoc DDL
        assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));
        InMemoryJarfile jarfile = new InMemoryJarfile();
        VoltCompiler comp = new VoltCompiler();
        comp.addClassToJar(jarfile, org.voltdb_testprocs.fullddlfeatures.testImportProc.class);
        try {
            m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
        }
        catch (ProcCallException pce) {
            fail("Should be able to call @UpdateClasses when adhoc DDL enabled.");
        }
        assertTrue(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));
    }

    public void testMasterWithAdhocDDL() throws Exception
    {
        generateCatalogsAndDeployments(true);

        // Fire up a cluster with no catalog
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToDeployment = m_pathToDeployment;

        try {
            startSystem(config);
            verifyMasterWithAdhocDDL();
        }
        finally {
            teardownSystem();
        }
    }

    void verifyUACfromMasterToReplica() throws Exception
    {
        // uac from master?
        TxnEgo txnid = TxnEgo.makeZero(MpInitiator.MP_INIT_PID);
        Object[] params = new Object[2];
        params[0] = ClientUtils.fileToBytes(new File(m_pathToCatalog));
        params[1] = null;
        txnid = txnid.makeNext();
        // We're going to get odd responses for the sentinels, so catch and ignore the exceptions
        try {
            ((ClientImpl)m_client).callProcedure(txnid.getTxnId(), 0L, "@SendSentinel", 0);
        } catch (ProcCallException pce) {}
        try {
            ((ClientImpl)m_client).callProcedure(txnid.getTxnId(), 0L, "@SendSentinel", 1);
        } catch (ProcCallException pce) {}
        ClientResponse r = ((ClientImpl)m_client).callProcedure(txnid.getTxnId(), 0L,
                "@UpdateApplicationCatalog", params);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());

        // adhoc queries still work
        verifyAdhocQuery();
        // undo our previous catalog update through the remote side so the promote test works
        params = new Object[2];
        params[0] = ClientUtils.fileToBytes(new File(m_pathToOtherCatalog));
        params[1] = null;
        txnid = txnid.makeNext();
        // We're going to get odd responses for the sentinels, so catch and ignore the exceptions
        try {
            ((ClientImpl)m_client).callProcedure(txnid.getTxnId(), 0L, "@SendSentinel", 0);
        } catch (ProcCallException pce) {}
        try {
            ((ClientImpl)m_client).callProcedure(txnid.getTxnId(), 0L, "@SendSentinel", 1);
        } catch (ProcCallException pce) {}
        r = ((ClientImpl)m_client).callProcedure(txnid.getTxnId(), 0L,
                "@UpdateApplicationCatalog", params);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testReplicaWithUAC() throws Exception
    {
        generateCatalogsAndDeployments(false);

        // Fire up a cluster with no catalog
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = m_pathToOtherCatalog;
        config.m_pathToDeployment = m_pathToDeployment;
        config.m_replicationRole = ReplicationRole.REPLICA;

        try {
            startSystem(config);
            // UAC with schema should fail
            assertFalse(findTableInSystemCatalogResults("FOO"));
            boolean threw = false;
            try {
                m_client.updateApplicationCatalog(new File(m_pathToCatalog), null);
            }
            catch (ProcCallException pce) {
                threw = true;
                assertTrue(pce.getMessage().contains("Write procedure @UpdateApplicationCatalog is not allowed"));
            }
            assertTrue("@UAC should have failed", threw);
            assertFalse(findTableInSystemCatalogResults("FOO"));

            // deployment-only UAC should fail
            threw = false;
            try {
                m_client.updateApplicationCatalog(null, new File(m_pathToOtherDeployment));
            }
            catch (ProcCallException pce) {
                threw = true;
                assertTrue(pce.getMessage().contains("Write procedure @UpdateApplicationCatalog is not allowed"));
            }
            assertTrue("@UAC should have failed", threw);

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
                assertTrue(pce.getMessage().contains("Write procedure @AdHoc is not allowed"));
            }
            assertTrue("Adhoc DDL should have failed", threw);
            assertFalse(findTableInSystemCatalogResults("BAR"));

            // @UpdateClasses should be rejected
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));
            threw = false;
            try {
                InMemoryJarfile jarfile = new InMemoryJarfile();
                VoltCompiler comp = new VoltCompiler();
                comp.addClassToJar(jarfile, org.voltdb_testprocs.fullddlfeatures.testImportProc.class);
                m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            }
            catch (ProcCallException pce) {
                threw = true;
                assertTrue(pce.getMessage().contains("Write procedure @UpdateClasses is not allowed"));
            }
            assertTrue("@UpdateClasses should have failed", threw);
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));

            verifyUACfromMasterToReplica();

            // Promote, should behave like the original master test
            m_client.callProcedure("@Promote");
            verifyMasterWithUAC();
        }
        finally {
            teardownSystem();
        }
    }

    public void testReplicaWithAdhocDDL() throws Exception
    {
        generateCatalogsAndDeployments(true);

        // Fire up a cluster with no catalog
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = m_pathToOtherCatalog;
        config.m_pathToDeployment = m_pathToDeployment;
        config.m_replicationRole = ReplicationRole.REPLICA;

        try {
            startSystem(config);
            // UAC with schema should fail
            assertFalse(findTableInSystemCatalogResults("FOO"));
            boolean threw = false;
            try {
                m_client.updateApplicationCatalog(new File(m_pathToCatalog), null);
            }
            catch (ProcCallException pce) {
                threw = true;
                assertTrue(pce.getMessage().contains("Write procedure @UpdateApplicationCatalog is not allowed"));
            }
            assertTrue("@UAC should have failed", threw);
            assertFalse(findTableInSystemCatalogResults("FOO"));

            // deployment-only UAC should fail
            threw = false;
            try {
                m_client.updateApplicationCatalog(null, new File(m_pathToOtherDeployment));
            }
            catch (ProcCallException pce) {
                threw = true;
                assertTrue(pce.getMessage().contains("Write procedure @UpdateApplicationCatalog is not allowed"));
            }
            assertTrue("@UAC should have failed", threw);

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
                assertTrue(pce.getMessage().contains("Write procedure @AdHoc is not allowed"));
            }
            assertTrue("Adhoc DDL should have failed", threw);
            assertFalse(findTableInSystemCatalogResults("BAR"));

            // @UpdateClasses should be rejected
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));
            threw = false;
            try {
                InMemoryJarfile jarfile = new InMemoryJarfile();
                VoltCompiler comp = new VoltCompiler();
                comp.addClassToJar(jarfile, org.voltdb_testprocs.fullddlfeatures.testImportProc.class);
                m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            }
            catch (ProcCallException pce) {
                threw = true;
                assertTrue(pce.getMessage().contains("Write procedure @UpdateClasses is not allowed"));
            }
            assertTrue("@UpdateClasses should have failed", threw);
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.fullddlfeatures.testImportProc"));

            verifyUACfromMasterToReplica();

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
