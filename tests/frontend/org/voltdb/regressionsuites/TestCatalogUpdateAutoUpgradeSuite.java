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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.compiler.CatalogUpgradeTools;
import org.voltdb.utils.MiscUtils;

import junit.framework.Test;

/**
 * Tests catalog update with auto-upgrade.
 */
public class TestCatalogUpdateAutoUpgradeSuite extends RegressionSuite {

    static final int SITES_PER_HOST = 2;
    static final int HOSTS = 2;
    static final int K = 0;

    private static String upgradeCatalogBasePath;
    private static String upgradeCatalogXMLPath;
    private static String upgradeCatalogJarPath;

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestCatalogUpdateAutoUpgradeSuite(String name) {
        super(name);
    }

    AtomicInteger m_outstandingCalls = new AtomicInteger(0);

    boolean callbackSuccess;

    class CatTestCallback implements ProcedureCallback {

        final byte m_expectedStatus;

        CatTestCallback(byte expectedStatus) {
            m_expectedStatus = expectedStatus;
            m_outstandingCalls.incrementAndGet();
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_outstandingCalls.decrementAndGet();
            if (m_expectedStatus != clientResponse.getStatus()) {
                if (clientResponse.getStatusString() != null)
                    System.err.println(clientResponse.getStatusString());
                callbackSuccess = false;
            }
        }
    }

    private void loadSomeData(Client client, int start, int count) throws IOException, ProcCallException {
        for (int i = start; i < (start + count); i++) {
            CatTestCallback callback = new CatTestCallback(ClientResponse.SUCCESS);
            client.callProcedure(callback, InsertNewOrder.class.getSimpleName(), i, i, (short)i);
        }
    }

    public void testCatalogUpgrade() throws IOException, ProcCallException, InterruptedException {
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);
        String tweakedJarPath = upgradeCatalogBasePath + "-tweaked.jar";

        OutputWatcher watcher = new OutputWatcher("catalog was automatically upgraded", 20, TimeUnit.MILLISECONDS);
        ((LocalCluster)m_config).setOutputWatcher(watcher);

        CatalogUpgradeTools.dorkJar(upgradeCatalogJarPath, tweakedJarPath, null);

        File tweakedJarFile = new File(tweakedJarPath);
        try {
            try {
                VoltTable[] results = UpdateApplicationCatalog.update(client,
                        tweakedJarFile, new File(upgradeCatalogXMLPath)).getResults();
                assertTrue(results.length == 1);
                boolean found = watcher.waitForString();
                assertTrue(found);
            }
            catch (ProcCallException e) {
                fail(String.format("@UpdateApplicationCatalog: ProcCallException: %s", e.getLocalizedMessage()));
            }
        }
        finally {
            tweakedJarFile.delete();
        }
    }

    public void testCatalogUpgradeWithBadDDL() throws IOException, ProcCallException, InterruptedException {
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);
        String tweakedJarPath = upgradeCatalogBasePath + "-tweaked.jar";

        OutputWatcher watcher = new OutputWatcher("Failed to generate upgraded catalog", 20, TimeUnit.MILLISECONDS);
        ((LocalCluster)m_config).setOutputWatcher(watcher);

        // Add a bad statement and tweak the version.
        CatalogUpgradeTools.dorkJar(upgradeCatalogJarPath, tweakedJarPath, "CREATE SQUIZZLE");

        File tweakedJarFile = new File(tweakedJarPath);
        try {
            try {
                UpdateApplicationCatalog.update(client,
                        tweakedJarFile, new File(upgradeCatalogXMLPath)).getResults();
                fail("Expect ProcCallException");
            }
            catch (ProcCallException e) {
                assertTrue(e.getLocalizedMessage().contains("Catalog upgrade failed"));
                boolean found = watcher.waitForString();
                assertTrue(found);
            }
        }
        finally {
            tweakedJarFile.delete();
        }
    }

    public void testCatalogUpgradeWithGoodProcedure() throws IOException, ProcCallException, InterruptedException {
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);
        String tweakedJarPath = upgradeCatalogBasePath + "-tweaked.jar";

        OutputWatcher watcher = new OutputWatcher("Failed to generate upgraded catalog", 20, TimeUnit.MILLISECONDS);
        ((LocalCluster)m_config).setOutputWatcher(watcher);

        // Add a procedure to the catalog that should never fail
        CatalogUpgradeTools.dorkJar(upgradeCatalogJarPath, tweakedJarPath,
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedEmptyProcedure");

        File tweakedJarFile = new File(tweakedJarPath);
        try {
            try {
                VoltTable[] results = UpdateApplicationCatalog.update(client,
                        tweakedJarFile, new File(upgradeCatalogXMLPath)).getResults();
                assertTrue(results.length == 1);
                boolean found = watcher.waitForString();
                assertTrue(found);
            }
            catch (ProcCallException e) {
                fail(String.format("@UpdateApplicationCatalog: ProcCallException: %s", e.getLocalizedMessage()));
            }
        }
        finally {
            tweakedJarFile.delete();
        }
    }

    public void testCatalogUpgradeWithBadStaticInitProcedure() throws IOException, ProcCallException, InterruptedException {
        // Connect the client to HostId 0 to ensure that the new procedure's static initializer will fault
        // during the catalog compilation.
        Client client = getClientToHostId(0);
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);
        String tweakedJarPath = upgradeCatalogBasePath + "-tweaked.jar";

        OutputWatcher watcher = new OutputWatcher("Failed to generate upgraded catalog", 20, TimeUnit.MILLISECONDS);
        ((LocalCluster)m_config).setOutputWatcher(watcher);

        // Add a procedure that will fault in the static initializer if when the hostId is 0
        CatalogUpgradeTools.dorkJar(upgradeCatalogJarPath, tweakedJarPath,
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedEmptyStaticInitializerProcedure");

        File tweakedJarFile = new File(tweakedJarPath);
        try {
            try {
                UpdateApplicationCatalog.update(client,
                        tweakedJarFile, new File(upgradeCatalogXMLPath)).getResults();
                fail("Expect ProcCallException");
            }
            catch (ProcCallException e) {
                assertTrue(e.getLocalizedMessage().contains("Catalog upgrade failed"));
                boolean found = watcher.waitForString();
                assertTrue(found);
            }
        }
        finally {
            tweakedJarFile.delete();
        }
    }

    static public Test suite() throws Exception {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCatalogUpdateAutoUpgradeSuite.class);

        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();

        project.addProcedure(org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                new ProcedurePartitionData("NEW_ORDER", "NO_W_ID", "2"));
        project.addProcedure(org.voltdb.benchmark.tpcc.procedures.SelectAll.class);
        project.addProcedure(org.voltdb.benchmark.tpcc.procedures.delivery.class,
                new ProcedurePartitionData("WAREHOUSE", "W_ID"));

        upgradeCatalogBasePath = Configuration.getPathToCatalogForTest("catalogupdate-for-upgrade");
        upgradeCatalogXMLPath = upgradeCatalogBasePath + ".xml";
        upgradeCatalogJarPath = upgradeCatalogBasePath + ".jar";

        HashMap<String, String> env = new HashMap<String, String>();
        // If we are doing something special with a stored procedure it will be on HostId 0
        env.put("__VOLTDB_TARGET_CLUSTER_HOSTID__", "0");
        LocalCluster config = new LocalCluster("catalogupdate-for-upgrade.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI, env);
        boolean compile = config.compile(project);
        assertTrue(compile);
        config.setHasLocalServer(false);
        builder.addServerConfig(config);

        MiscUtils.copyFile(project.getPathToDeployment(), upgradeCatalogXMLPath);

        return builder;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        callbackSuccess = true;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        assertTrue(callbackSuccess);
    }
}
