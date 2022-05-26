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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.BackendTarget;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientUtils;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.SyncCallback;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.utils.MiscUtils;

import junit.framework.Test;


/**
 * Tests a mix of multi-partition and single partition procedures on a
 * mix of replicated and partititioned tables on a mix of single-site and
 * multi-site VoltDB instances.
 *
 */
public class TestUpdateDeployment extends RegressionSuite {

    static final int SITES_PER_HOST = 2;
    static final int HOSTS = 2;
    static final int K = MiscUtils.isPro() ? 1 : 0;

    // users used by these test
    static final RoleInfo GROUPS[] = new RoleInfo[] {
        new RoleInfo("export", false, false, false, false, false, false),
        new RoleInfo("proc", true, false, true, true, false, false),
        new RoleInfo("admin", true, false, true, true, false, false)
    };

    static final UserInfo[] USERS = new UserInfo[] {
        new UserInfo("fancy pants", "export", new String[]{"export"}),
        new UserInfo("default", "password", new String[]{"proc"}),
        new UserInfo("admin", "admin", new String[]{"proc", "admin"})
    };

    static final UserInfo[] USERS_BAD_PASSWORD = new UserInfo[] {
            new UserInfo("user1", "E7FA8F38396EF1332A60B629BA69257C462CBF3B95C81F3C556DDB79BD2226BEBCF2086983707FF5CFA72BE03B8B763199BBFFD3", new String[]{"admin"}, false),
            new UserInfo("user2", "password", new String[]{"admin", "proc"}, false)
    };

    static final int DEAD_HOST_TIMEOUT = 6;
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestUpdateDeployment(String name) {
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
                if (clientResponse.getStatusString() != null) {
                    System.err.println(clientResponse.getStatusString());
                }
                callbackSuccess = false;
            }
        }
    }

    /**
     * Start with snapshots disabled. Enable them to one directory, check that the snapshot files are created
     * with the correct prefix. Update the catalog to do the snapshots in a different directory with a
     * different prefix and check to make sure they start going to the right place. Update the catalog
     * to disable them and then make sure no snapshots appear.
     * @throws Exception
     */
    public void testEnableModifyDisableSnapshot() throws Exception {
        m_config.deleteDirectory(new File("/tmp/snapshotdir1"));
        m_config.deleteDirectory(new File("/tmp/snapshotdir2"));
        try {
            m_config.createDirectory(new File("/tmp/snapshotdir1"));
            m_config.createDirectory(new File("/tmp/snapshotdir2"));
            Client client = getClient();

            //
            // Test that we can enable snapshots
            //
            String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-enable_snapshot.xml");
            // Mix in various ways to specify no catalog.  Use java null here.
            String depBytes = new String(ClientUtils.fileToBytes(new File(deploymentURL)),
                    Constants.UTF8ENCODING);
            VoltTable[] results = client.callProcedure("@UpdateApplicationCatalog", null, depBytes).getResults();
            //client.updateApplicationCatalog(null, new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);
            Thread.sleep(5000);

            //
            // Make sure snapshot files are generated
            //
            for (File f : m_config.listFiles(new File("/tmp/snapshotdir1"))) {
                assertTrue(f.getName().startsWith("foo1"));
            }

            //
            // Test that we can change settings like the path
            //
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot.xml");
            // Mix in various ways to specify no catalog.  Use empty string here.
            depBytes = new String(ClientUtils.fileToBytes(new File(deploymentURL)),
                    Constants.UTF8ENCODING);
            results = client.callProcedure("@UpdateApplicationCatalog", "", depBytes).getResults();
            assertTrue(results.length == 1);
            Thread.sleep(5000);

            //
            // Check that files are made in the new path
            //
            for (File f : m_config.listFiles(new File("/tmp/snapshotdir2"))) {
                assertTrue(f.getName().startsWith("foo2"));
            }

            //
            // Change the snapshot path to something that doesn't exist, no crashes
            //
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot_dir_not_exist.xml");
            // Mix in various ways to specify no catalog.  Use empty array here.
            depBytes = new String(ClientUtils.fileToBytes(new File(deploymentURL)),
                    Constants.UTF8ENCODING);
            results = client.callProcedure("@UpdateApplicationCatalog", new byte[] {}, depBytes).getResults();
            assertTrue(results.length == 1);

            System.out.println("Waiting for failed snapshots");
            Thread.sleep(5000);

            //
            // Change it back
            //
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
            // Mix in various ways to specify no catalog.  Make sure the client convenience method
            // works with a null file.
            results = UpdateApplicationCatalog.update(client, null, new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);
            Thread.sleep(5000);

            //
            // Make sure snapshots resume
            //
            for (File f : m_config.listFiles(new File("/tmp/snapshotdir2"))) {
                assertTrue(f.getName().startsWith("foo2"));
            }

            //
            // Make sure you can disable snapshots
            //
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
            results = UpdateApplicationCatalog.update(client, null, new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);
            for (File f : m_config.listFiles(new File("/tmp/snapshotdir2"))) {
                f.delete();
            }

            Thread.sleep(5000);

            //
            // Make sure you can reenable snapshot files
            //
            assertEquals( 0, m_config.listFiles(new File("/tmp/snapshotdir2")).size());

            //
            // Test that we can enable snapshots
            //
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-enable_snapshot.xml");
            results = UpdateApplicationCatalog.update(client, null, new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);
            Thread.sleep(5000);

            //
            // Make sure snapshot files are generated
            //
            for (File f : m_config.listFiles(new File("/tmp/snapshotdir1"))) {
                assertTrue(f.getName().startsWith("foo1"));
            }

            //
            // Turn snapshots off so that we can clean up
            //
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
            results = UpdateApplicationCatalog.update(client, null, new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);
            Thread.sleep(1000);

            m_config.deleteDirectory(new File("/tmp/snapshotdir1"));
            m_config.deleteDirectory(new File("/tmp/snapshotdir2"));
            m_config.createDirectory(new File("/tmp/snapshotdir1"));
            m_config.createDirectory(new File("/tmp/snapshotdir2"));
            Thread.sleep(5000);
            assertTrue(m_config.listFiles(new File("/tmp/snapshotdir1")).isEmpty());
            assertTrue(m_config.listFiles(new File("/tmp/snapshotdir2")).isEmpty());
        } finally {
            deleteDirectory(new File("/tmp/snapshotdir1"));
            deleteDirectory(new File("/tmp/snapshotdir2"));
        }
    }

    private void loadSomeData(Client client, int start, int count) throws Exception
    {
        for (int i = start; i < (start + count); i++) {
            CatTestCallback callback = new CatTestCallback(ClientResponse.SUCCESS);
            client.callProcedure(callback, InsertNewOrder.class.getSimpleName(), i, i, (short)i);
        }
    }

    public void testConsecutiveCatalogDeploymentRace() throws Exception
    {
        System.out.println("\n\n-----\n testConsecutiveCatalogDeploymentRace \n-----\n\n");
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtable.jar");
        // 6 seconds heart beat timeout deployment change
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtable.xml");
        // Asynchronously attempt consecutive catalog update and deployment update
        SyncCallback cb1 = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb1,
                new File(newCatalogURL), null);
        // Then, update the users in the deployment
        SyncCallback cb2 = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb2, null, new File(deploymentURL));
        cb1.waitForResponse();
        cb2.waitForResponse();

         System.out.println("cb1: " + Byte.toString(cb1.getResponse().getStatus()) + " " + cb1.getResponse().getStatusString());
         System.out.println("cb2: " + Byte.toString(cb2.getResponse().getStatus()) + " " + cb2.getResponse().getStatusString());

        // At least one should fail, it could lead to both failures
        assertTrue(ClientResponse.SUCCESS != cb2.getResponse().getStatus()
                || ClientResponse.SUCCESS != cb1.getResponse().getStatus());

        // Verify the heartbeat timeout change didn't take
        Client client3 = getClient();
        boolean found = false;
        int timeout = -1;
        VoltTable result = client3.callProcedure("@SystemInformation", "DEPLOYMENT").getResults()[0];
        while (result.advanceRow()) {
            if (result.getString("PROPERTY").equalsIgnoreCase("heartbeattimeout")) {
                found = true;
                timeout = Integer.valueOf(result.getString("VALUE"));
            }
        }
        assertTrue(found);

        if (cb1.getResponse().getStatus() == ClientResponse.SUCCESS) {
            assertEquals(org.voltcore.common.Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS, timeout);

            // Verify that table A exists
            ClientResponse response = client3.callProcedure("@AdHoc", "insert into NEWTABLE values (100);");
            assertEquals(ClientResponse.SUCCESS, response.getStatus());
        } else if (cb2.getResponse().getStatus() == ClientResponse.SUCCESS) {
            // cb2 success, heart beat time out set to 6 seconds
            assertEquals(DEAD_HOST_TIMEOUT, timeout);
        } else {
            // both failed
            assertTrue(cb1.getResponse().getStatusString().contains("catalog update is active") ||
                    cb2.getResponse().getStatusString().contains("catalog update is active"));
        }
    }

    public void testUpdateSchemaModificationIsBlacklisted() throws Exception
    {
        System.out.println("\n\n-----\n testUpdateSchemaModificationIsBlacklisted \n-----\n\n");
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_schema_update.xml");
        // Try to change the schem setting
        SyncCallback cb = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb, null, new File(deploymentURL));
        cb.waitForResponse();
        assertEquals(ClientResponse.UNSUPPORTED_DYNAMIC_CHANGE, cb.getResponse().getStatus());
        System.out.println(cb.getResponse().getStatusString());
        assertTrue(cb.getResponse().getStatusString().contains("May not dynamically modify"));
    }

    public void testUpdateSecurityNoUsers() throws Exception
    {
        System.out.println("\n\n-----\n testUpdateSecurityNoUsers \n-----\n\n");
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-security-no-users.xml");
        // Try to change the schem setting
        SyncCallback cb = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb, null, new File(deploymentURL));
        cb.waitForResponse();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, cb.getResponse().getStatus());
        System.out.println(cb.getResponse().getStatusString());
        assertTrue(cb.getResponse().getStatusString().contains("Unable to update"));
    }

    public void testUpdateBadExport() throws Exception
    {
        System.out.println("\n\n-----\n testUpdateBadExport \n-----\n\n");
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");
        Map<String, String> additionalEnv = new HashMap<>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");
        LocalCluster config = new LocalCluster("catalogupdate-bad-export.jar", SITES_PER_HOST, HOSTS, K,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        Properties props = buildProperties(
                "type", "csv",
                "batched", "false",
                "with-schema", "true",
                "complain", "true",
                "outdir", "/tmp/" + System.getProperty("user.name"));
        project.addExport(true, ServerExportEnum.CUSTOM, props);
        // build the jarfile
        boolean compile = config.compile(project);
        assertTrue(compile);

        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // Try to change the schem setting
        SyncCallback cb = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb, null, new File(project.getPathToDeployment()));
        cb.waitForResponse();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, cb.getResponse().getStatus());
        System.out.println(cb.getResponse().getStatusString());
        assertTrue(cb.getResponse().getStatusString().contains("Unable to update"));
    }

    public void testUpdateSecurityBadUsername() throws Exception
    {
        System.out.println("\n\n-----\n testUpdateSecurityBadUsername \n-----\n\n");
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-bad-username.xml");
        // Try to change the schem setting
        SyncCallback cb = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb, null, new File(deploymentURL));
        cb.waitForResponse();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, cb.getResponse().getStatus());
        System.out.println(cb.getResponse().getStatusString());
        assertTrue(cb.getResponse().getStatusString().contains("Unable to update"));
    }

    public void testBadMaskPassword() throws Exception {
        System.out.println("\n\n-----\n testBadMaskPassword \n-----\n\n");
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-bad-masked-password.xml");
        // Try to change schema setting
        SyncCallback cb = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb, null, new File(deploymentURL));
        cb.waitForResponse();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, cb.getResponse().getStatus());
        assertTrue(cb.getResponse().getStatusString().contains("Unable to update deployment configuration"));
    }

    public void testUpdateSitesPerHost() throws IOException, InterruptedException {
        System.out.println("\n\n-----\n testUpdateSitesPerHost \n-----\n\n");
        Client client = getClient();
        assertTrue(callbackSuccess);

        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-change-sitesperhost.xml");
        // Try to change schema setting
        SyncCallback cb = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb, null, new File(deploymentURL));
        cb.waitForResponse();
        assertEquals(ClientResponse.UNSUPPORTED_DYNAMIC_CHANGE, cb.getResponse().getStatus());
        assertTrue(cb.getResponse().getStatusString().contains("Unable to update deployment configuration"));
    }

    private void deleteDirectory(File dir) {
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        for (File f : dir.listFiles()) {
            assertTrue(f.delete());
        }
        assertTrue(dir.delete());
    }

    static private void addBaseProcedures(VoltProjectBuilder project) {
        project.addProcedure(org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                "NEW_ORDER.NO_W_ID: 2");
        project.addProcedure(org.voltdb.benchmark.tpcc.procedures.SelectAll.class);
        project.addProcedure(org.voltdb.benchmark.tpcc.procedures.delivery.class,
                "WAREHOUSE.W_ID: 0");
    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     * @throws Exception
     */
    static public Test suite() throws Exception {
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(2));

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestUpdateDeployment.class);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        VoltServerConfig config = new LocalCluster("catalogupdate-cluster-base.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);

        // Catalog upgrade test(s) sporadically fail if there's a local server because
        // a file pipe isn't available for grepping local server output.
        ((LocalCluster) config).setHasLocalServer(true);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        // build the jarfile
        boolean basecompile = config.compile(project);
        assertTrue(basecompile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml"));

        // add this config to the set of tests to run
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);

        /////////////////////////////////////////////////////////////
        // DELTA CATALOGS FOR TESTING
        /////////////////////////////////////////////////////////////

        // Generate a catalog that adds a table and a deployment file that changes the dead host timeout.
        config = new LocalCluster("catalogupdate-cluster-addtable.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addLiteralSchema("CREATE TABLE NEWTABLE (A1 INTEGER, PRIMARY KEY (A1));");
        project.setDeadHostTimeout(DEAD_HOST_TIMEOUT);
        boolean compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtable.xml"));

        // A catalog change that enables snapshots
        config = new LocalCluster("catalogupdate-cluster-enable_snapshot.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        project.setSnapshotSettings( "1s", 3, "/tmp/snapshotdir1", "foo1");
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-enable_snapshot.xml"));

        //Another catalog change to modify the schedule
        config = new LocalCluster("catalogupdate-cluster-change_snapshot.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        project.setSnapshotSettings( "1s", 3, "/tmp/snapshotdir2", "foo2");
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot.xml"));

        //Another catalog change to modify the schedule
        config = new LocalCluster("catalogupdate-cluster-change_snapshot_dir_not_exist.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        project.setSnapshotSettings( "1s", 3, "/tmp/snapshotdirasda2", "foo2");
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot_dir_not_exist.xml"));

        // A deployment change that changes the schema change mechanism
        config = new LocalCluster("catalogupdate-cluster-change_schema_update.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        project.setUseDDLSchema(true);
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_schema_update.xml"));

        // A deployment change that changes the schema change mechanism
        config = new LocalCluster("catalogupdate-security-no-users.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        project.setSecurityEnabled(true, false);
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-security-no-users.xml"));

        // A deployment change that changes the schema change mechanism
        config = new LocalCluster("catalogupdate-bad-username.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        project.setSecurityEnabled(true,true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-bad-username.xml"));

        // A deployment change that has bad masked password
        config = new LocalCluster("catalogupdate-bad-masked-password.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        project.setSecurityEnabled(true,true);
        project.addRoles(GROUPS);
        project.addUsers(USERS_BAD_PASSWORD);
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-bad-masked-password.xml"));

        // A deployment change that alter the sites per host setting (it's disallowed!)
        config = new LocalCluster("catalogupdate-change-sitesperhost.jar", SITES_PER_HOST + 2, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        addBaseProcedures(project);
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-change-sitesperhost.xml"));

        return builder;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        assertTrue(callbackSuccess);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        callbackSuccess = true;
    }
}
