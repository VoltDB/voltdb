/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
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

    @Test
    public void testConcurrentUAC() throws Exception {
        // A new catalog with new table added
        LocalCluster config = new LocalCluster("concurrentCatalogUpdate-cluster-addtable.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        VoltProjectBuilder project = new VoltProjectBuilder();
        project = new TPCCProjectBuilder();
        project.addLiteralSchema("CREATE TABLE NEWTABLE (A1 INTEGER PRIMARY KEY);");
        project.setDeadHostTimeout(6);
        assertTrue(config.compile(project));
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-addtable.xml"));

        // A new catalog with many new tables added
        config = new LocalCluster("concurrentCatalogUpdate-cluster-addmoretable.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project = new TPCCProjectBuilder();
        project.addLiteralSchema("CREATE TABLE NEWTABLE (A1 INTEGER PRIMARY KEY);");
        for (int i = 0; i < 100; i++) {
            String s = "CREATE TABLE NEWTABLE" + Integer.toString(i) + " (A1 INTEGER PRIMARY KEY);";
            project.addLiteralSchema(s);
        }
        project.setDeadHostTimeout(6);
        assertTrue(config.compile(project));
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-addmoretable.xml"));

        ClientImpl client = getClient();

        String newCatalogURL1 = Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-addtable.jar");
        String newCatalogURL2 = Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-addmoretable.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-base.xml");

        SyncCallback cb1 = new SyncCallback();
        client.updateApplicationCatalog(cb1,
                new File(newCatalogURL1), new File(deploymentURL));

        SyncCallback cb2 = new SyncCallback();
        client.updateApplicationCatalog(cb2,
                new File(newCatalogURL2), new File(deploymentURL));

        cb1.waitForResponse();
        cb2.waitForResponse();

        // Only one of them should succeed
        assertTrue(ClientResponse.USER_ABORT == cb2.getResponse().getStatus()
                || ClientResponse.USER_ABORT == cb1.getResponse().getStatus());
        assertTrue(ClientResponse.SUCCESS == cb2.getResponse().getStatus()
                || ClientResponse.SUCCESS == cb1.getResponse().getStatus());
    }

    @Test
    public void testConcurrentUpdateClasses() {

    }

    @Test
    public void testConcurrentAdHoc() {

    }

    @Test
    public void testConcurrentMixedUpdate() {

    }

    /*
     * Initialization
     */
    @Before
    public void init() throws Exception {
        builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(false);

        cluster = new LocalCluster("concurrentCatalogUpdate-cluster-base.jar",
                                                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        cluster.setHasLocalServer(false);
        assertTrue(cluster.compile(builder));

        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("concurrentCatalogUpdate-cluster-base.xml"));

        cluster.startUp();
    }

    @After
    public void shutdown() throws Exception {
        cluster.shutDown();
    }

    private static ClientImpl getClient() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProcedureCallTimeout(2 * 60 * 1000); // 2 min
        Client client = ClientFactory.createClient(clientConfig);
        client.createConnection(cluster.getAdminAddress(1));

        return (ClientImpl) client;
    }
}
