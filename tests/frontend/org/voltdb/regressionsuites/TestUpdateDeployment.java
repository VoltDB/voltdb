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

package org.voltdb.regressionsuites;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;
import junit.framework.TestCase;

import org.voltdb.TheHashinator;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.benchmark.tpcc.procedures.SelectAll;
import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientUtils;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.SyncCallback;
import org.voltdb.common.Constants;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb.utils.MiscUtils;

/**
 * Tests a mix of multi-partition and single partition procedures on a
 * mix of replicated and partititioned tables on a mix of single-site and
 * multi-site VoltDB instances.
 *
 */
public class TestUpdateDeployment extends RegressionSuite {
    private static final Class<? extends TestCase> TESTCASECLASS = TestUpdateDeployment.class;

    private static final int SITES_PER_HOST = 2;
    private static final int HOSTS = 2;
    private static final int K = MiscUtils.isPro() ? 1 : 0;

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestUpdateDeployment(String name) {
        super(name);
    }

    private AtomicInteger m_outstandingCalls = new AtomicInteger(0);

    private boolean callbackSuccess;

    class CatTestCallback implements ProcedureCallback {

        private final byte m_expectedStatus;

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
            results = client.updateApplicationCatalog(null, new File(deploymentURL)).getResults();
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
            results = client.updateApplicationCatalog(null, new File(deploymentURL)).getResults();
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
            results = client.updateApplicationCatalog(null, new File(deploymentURL)).getResults();
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
            results = client.updateApplicationCatalog(null, new File(deploymentURL)).getResults();
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
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtable.xml");
        // Asynchronously attempt consecutive catalog update and deployment update
        client.updateApplicationCatalog(new CatTestCallback(ClientResponse.SUCCESS),
                new File(newCatalogURL), null);
        // Then, update the users in the deployment
        SyncCallback cb2 = new SyncCallback();
        client.updateApplicationCatalog(cb2, null, new File(deploymentURL));
        cb2.waitForResponse();
        assertEquals(ClientResponse.USER_ABORT, cb2.getResponse().getStatus());
        assertTrue(cb2.getResponse().getStatusString().contains("Invalid catalog update"));

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
        assertEquals(org.voltcore.common.Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS, timeout);

        // Verify that table A exists
        ClientResponse response = client3.callProcedure("@AdHoc", "insert into NEWTABLE values (100);");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
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
        client.updateApplicationCatalog(cb, null, new File(deploymentURL));
        cb.waitForResponse();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, cb.getResponse().getStatus());
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
        client.updateApplicationCatalog(cb, null, new File(deploymentURL));
        cb.waitForResponse();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, cb.getResponse().getStatus());
        System.out.println(cb.getResponse().getStatusString());
        assertTrue(cb.getResponse().getStatusString().contains("Unable to update"));
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

    private static void writeConfigFiles(String string, CatalogBuilder altCb, DeploymentBuilder altDb) {
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
        TheHashinator.initializeAsConfiguredForPartitions(2);

        final Class<?>[] BASEPROCS = { InsertNewOrder.class, SelectAll.class, delivery.class };

        CatalogBuilder cb = TPCCProjectBuilder.catalogBuilderNoProcs()
        .addProcedures(BASEPROCS)
        ;
        DeploymentBuilder db = new DeploymentBuilder(SITES_PER_HOST, HOSTS, K);
        // build the jarfile
        LocalCluster cluster = LocalCluster.configure(TESTCASECLASS.getSimpleName(), cb, db);
        assertNotNull("LocalCluster failed to compile", cluster);

        CatalogBuilder altCb;
        DeploymentBuilder altDb;
        /////////////////////////////////////////////////////////////
        // DELTA CATALOGS FOR TESTING
        /////////////////////////////////////////////////////////////

        // Generate a catalog that adds a table and a deployment file that changes the dead host timeout.
        altCb = TPCCProjectBuilder.catalogBuilderNoProcs()
        .addLiteralSchema("CREATE TABLE NEWTABLE (A1 INTEGER, PRIMARY KEY (A1));");
        altDb = new DeploymentBuilder(SITES_PER_HOST, HOSTS, K)
        .setDeadHostTimeout(6);
        writeConfigFiles("-timeout", altCb, altDb);

        // A catalog change that enables snapshots
        altCb = TPCCProjectBuilder.catalogBuilderNoProcs()
        .addProcedures(BASEPROCS);
        altDb = new DeploymentBuilder(SITES_PER_HOST, HOSTS, K)
        .setSnapshotSettings("1s", 3, "/tmp/snapshotdir1", "foo1");
        // build the jarfile
        writeConfigFiles("-enable-snapshot", altCb, altDb);

        //Another catalog change to modify the schedule
        altDb = new DeploymentBuilder(SITES_PER_HOST, HOSTS, K)
        .setSnapshotSettings("1s", 3, "/tmp/snapshotdir2", "foo2");
        writeConfigFiles("-change-snapshot", altCb, altDb);

        //Another catalog change to modify the schedule
        altDb = new DeploymentBuilder(SITES_PER_HOST, HOSTS, K)
        .setSnapshotSettings( "1s", 3, "/tmp/snapshotdirasda2", "foo2");
        writeConfigFiles("-change_snapshot-dir-not-exist", altCb, altDb);

        // A deployment change that changes the schema change mechanism
        altDb = new DeploymentBuilder(SITES_PER_HOST, HOSTS, K)
        .setUseAdHocDDL(true);
        writeConfigFiles("-change-schema-update", altCb, altDb);

        // A deployment change that changes the schema change mechanism
        altDb = new DeploymentBuilder(SITES_PER_HOST, HOSTS, K)
        .setSecurityEnabled(true, false);
        writeConfigFiles("-secure-no-users", altCb, altDb);

        return new MultiConfigSuiteBuilder(TESTCASECLASS, cluster);
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
