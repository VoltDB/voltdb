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

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.File;

import org.junit.After;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.SyncCallback;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

/*
 * A test suite for testing concurrent catalog update. When multiple catalog
 * update takes place, only one of them should succeed.
 *
 * The tested system calls include @AdHoc, @UpdateApplicationCatalog, @UpdateClasses
 */
public class TestConcurrentUpdateCatalog {
    private static final int SITES_PER_HOST = 2;
    private static final int HOSTS = 2;
    private static final int K = MiscUtils.isPro() ? 1 : 0;

    private static LocalCluster cluster = null;
    private static VoltProjectBuilder builder = null;
    private final boolean m_usingCalcite = Boolean.parseBoolean(System.getProperty("plan_with_calcite", "false"));

    @Test
    public void testConcurrentUAC() throws Exception {
        init(false, false);
        Client client = getClient();

        LocalCluster config = new LocalCluster("concurrentCatalogUpdate-cluster-addtable.jar",
                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("CREATE TABLE NEWTABLE (A1 INTEGER, PRIMARY KEY (A1));");
        project.setDeadHostTimeout(6);
        assertTrue(config.compile(project));
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-addtable.xml"));

        config = new LocalCluster("concurrentCatalogUpdate-cluster-addmoretable.jar",
                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project.addLiteralSchema("CREATE TABLE NEWTABLE (A1 INTEGER, PRIMARY KEY (A1));");
        // Add many tables to make sure this catalog is slow to write
        for (int i = 0; i < 100; i++) {
            project.addLiteralSchema("CREATE TABLE NEWTABLE" + Integer.toString(i) +
                                     " (A1 INTEGER, PRIMARY KEY (A1));");
        }
        project.setDeadHostTimeout(6);
        assertTrue(config.compile(project));
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-addmoretable.xml"));

        String deploymentURL = Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-base.xml");
        String newCatalogURL1 = Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-addtable.jar");
        String newCatalogURL2 = Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-addmoretable.jar");

        SyncCallback cb1 = new SyncCallback();
        SyncCallback cb2 = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb1, new File(newCatalogURL1), new File(deploymentURL));
        UpdateApplicationCatalog.update(client, cb2, new File(newCatalogURL2), new File(deploymentURL));

        checkResults(cb1, cb2);
    }

    @Test
    public void testConcurrentUpdateClasses() throws Exception {
        init(true, false);
        Client client = getClient();

        InMemoryJarfile jar = new InMemoryJarfile();
        VoltCompiler comp = new VoltCompiler(false);
        // Add some dummy classes
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.InnerClassesTestProc.class);
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.testImportProc.class);
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.TestProcWithSQLStmt.class);
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.testCreateProcFromClassProc.class);
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.NoMeaningClass.class);

        SyncCallback cb1 = new SyncCallback();
        SyncCallback cb2 = new SyncCallback();

        client.callProcedure(cb1, "@UpdateClasses", jar.getFullJarBytes(), null);
        client.callProcedure(cb2, "@UpdateClasses", jar.getFullJarBytes(), null);

        checkResults(cb1, cb2);
    }

    @Test
    public void testConcurrentAdHoc() throws Exception {
        init(true, false);

        Client client  = getClient();
        SyncCallback cb1 = new SyncCallback();
        SyncCallback cb2 = new SyncCallback();
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb1.append("CREATE TABLE T").append(i).append(" (ID INT PRIMARY KEY, A INT UNIQUE);");
            sb2.append("CREATE TABLE K").append(i).append(" (ID INT, A INT UNIQUE, B VARCHAR(30));");
        }
        client.callProcedure(cb1, "@AdHoc", sb1.toString());
        client.callProcedure(cb2, "@AdHoc", sb2.toString());

        checkResults(cb1, cb2);
    }

    @Test
    public void testConcurrentPromote() throws Exception {
        // This is a fake dr cluster
        init(true, true);

        Client client = getClient();

        SyncCallback cb1 = new SyncCallback();
        SyncCallback cb2 = new SyncCallback();
        client.callProcedure(cb1, "@Promote");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append("CREATE TABLE K").append(i).append(" (ID INT, A INT UNIQUE, B VARCHAR(30));");
        }
        client.callProcedure(cb2, "@AdHoc", sb.toString());

        cb1.waitForResponse();
        cb2.waitForResponse();

        // ENG-16619 The check is voided because of calling SqlParserFactory.parse(sql) in SqlTaskImpl.java
        if (! m_usingCalcite) {
            checkResults(cb1, cb2);
        }
    }

    @Test
    public void testConcurrentMixedUpdate() throws Exception {
        init(true, false);
        Client client = getClient();

        SyncCallback cb1 = new SyncCallback();
        SyncCallback cb2 = new SyncCallback();
        StringBuilder sb = new StringBuilder();

        InMemoryJarfile jar = new InMemoryJarfile();
        VoltCompiler comp = new VoltCompiler(false);
        // Add some dummy classes
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.InnerClassesTestProc.class);
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.testImportProc.class);
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.TestProcWithSQLStmt.class);
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.testCreateProcFromClassProc.class);
        comp.addClassToJar(jar, org.voltdb_testprocs.updateclasses.NoMeaningClass.class);

        for (int i = 0; i < 200; i++) {
            sb.append("CREATE TABLE K").append(i).append(" (ID INT, A INT UNIQUE, B VARCHAR(30));");
        }

        client.callProcedure(cb1, "@AdHoc", sb.toString());
        client.callProcedure(cb2, "@UpdateClasses", jar.getFullJarBytes(), null);

        // ENG-16619 The check is voided because of calling SqlParserFactory.parse(sql) in SqlTaskImpl.java
        if (! m_usingCalcite) {
            checkResults(cb1, cb2);
        }
    }

    /*
     * Initialization
     */
    private void init(boolean useAdHocDDL, boolean drMaster) throws Exception {
        builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(useAdHocDDL);
        if (drMaster) {
            builder.setDRMasterHost("localhost");
            builder.setDrReplica();
        }

        cluster = new LocalCluster("concurrentCatalogUpdate-cluster-base.jar",
                                                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        cluster.setHasLocalServer(false);
        assertTrue(cluster.compile(builder));

        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-base.xml"));

        cluster.startUp();
    }

    @After
    public void shutdown() throws Exception {
        if (cluster != null)
            cluster.shutDown();
        cluster = null;
    }

    private static Client getClient() throws Exception {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProcedureCallTimeout(2 * 60 * 1000); // 2 min
        final Client client = ClientFactory.createClient(clientConfig);
        client.createConnection(cluster.getAdminAddress(1));
        return client;
    }

    /*
     * Helper function to check the results of 2 concurrent calls, assuming
     * only one of them should become the winner
     */
    private void checkResults(SyncCallback cb1, SyncCallback cb2) throws Exception {
        cb1.waitForResponse();
        cb2.waitForResponse();

        // Only one of them should succeed

        // as the NT-Procedure conversion, it's still like single-thread execution
        // however, they may race with each other as UAC NT thread is using a different zk lock
        // with @UpdateCore transactional path
        // At least one should fail, it could lead to both failures
        final boolean succ1 = ClientResponse.SUCCESS == cb1.getResponse().getStatus(),
                succ2 = ClientResponse.SUCCESS == cb2.getResponse().getStatus();
        assertFalse("At most one call could succeed", succ1 && succ2);
    }
}
