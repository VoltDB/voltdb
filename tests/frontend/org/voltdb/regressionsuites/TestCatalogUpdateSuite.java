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
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.zk.ZKUtil;
import org.voltdb.BackendTarget;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.MiscUtils;

/**
 * Tests a mix of multi-partition and single partition procedures on a
 * mix of replicated and partititioned tables on a mix of single-site and
 * multi-site VoltDB instances.
 *
 */
public class TestCatalogUpdateSuite extends RegressionSuite {

    static final int SITES_PER_HOST = 2;
    static final int HOSTS = 2;
    static final int K = MiscUtils.isPro() ? 1 : 0;

    // procedures used by these tests
    static Class<?>[] BASEPROCS = { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                    org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                    org.voltdb.benchmark.tpcc.procedures.delivery.class };

    static Class<?>[] BASEPROCS_OPROCS =  { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                            org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                            org.voltdb.benchmark.tpcc.procedures.delivery.class,
                                            org.voltdb_testprocs.regressionsuites.orderbyprocs.InsertO1.class};


    static Class<?>[] EXPANDEDPROCS = { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class,
                                        org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class };

    static Class<?>[] CONFLICTPROCS = { org.voltdb.catalog.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class };

    static Class<?>[] SOMANYPROCS =   { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.neworder.class,
                                        org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId.class,
                                        org.voltdb.benchmark.tpcc.procedures.ostatByCustomerName.class,
                                        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId.class,
                                        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerName.class,
                                        org.voltdb.benchmark.tpcc.procedures.slev.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class };

    // testUpdateHonkingBigCatalog constants and statistics. 100/100/40 makes a ~2MB jar.
    private static final int HUGE_TABLES = 100;
    private static final int HUGE_COLUMNS = 100;
    private static final int HUGE_NAME_SIZE = 40;
    private static double hugeCompileElapsed = 0.0;
    private static double hugeTestElapsed = 0.0;
    private static String hugeCatalogXMLPath;
    private static String hugeCatalogJarPath;

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestCatalogUpdateSuite(String name) {
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

    public void testUpdateWithNoDeploymentFile() throws Exception {
        System.out.println("\n\n-----\n testUpdateWithNoDeploymentFile \n-----\n\n");
        Client client = getClient();
        String newCatalogURL;
        CatTestCallback callback;

        loadSomeData(client, 0, 25);
        client.drain();
        assertTrue(callbackSuccess);

        negativeTests(client);
        assertTrue(callbackSuccess);

        // asynchronously call some random inserts
        loadSomeData(client, 25, 25);
        assertTrue(callbackSuccess);

        // add a procedure "InsertOrderLineBatched"
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar");
        callback = new CatTestCallback(ClientResponse.SUCCESS);
        client.updateApplicationCatalog(callback, new File(newCatalogURL), null);

        // don't care if this succeeds or fails.
        // calling the new proc before the cat change returns is not guaranteed to work
        // we just hope it doesn't crash anything
        int x = 3;
        SyncCallback cb = new SyncCallback();
        client.callProcedure(cb,
                org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});
        cb.waitForResponse();

        // make sure the previous catalog change has completed
        client.drain();
        assertTrue(callbackSuccess);

        // now calling the new proc better work
        x = 2;
        client.callProcedure(org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, (short)x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});

        loadSomeData(client, 50, 5);
        assertTrue(callbackSuccess);
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
            String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-enable_snapshot.jar");
            String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-enable_snapshot.xml");
            VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
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
            newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot.jar");
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot.xml");
            results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
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
            newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot_dir_not_exist.jar");
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot_dir_not_exist.xml");
            results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);

            System.out.println("Waiting for failed snapshots");
            Thread.sleep(5000);

            //
            // Change it back
            //
            newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
            results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
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
            newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
            results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
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
            newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-enable_snapshot.jar");
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-enable_snapshot.xml");
            results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
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
            newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
            results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
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

    public void testUpdate() throws Exception {
        Client client = getClient();
        String newCatalogURL;
        String deploymentURL;
        VoltTable[] results;
        CatTestCallback callback;

        loadSomeData(client, 0, 25);
        client.drain();
        assertTrue(callbackSuccess);

        negativeTests(client);
        assertTrue(callbackSuccess);

        // asynchronously call some random inserts
        loadSomeData(client, 25, 25);
        assertTrue(callbackSuccess);

        // add a procedure "InsertOrderLineBatched"
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.xml");
        callback = new CatTestCallback(ClientResponse.SUCCESS);
        client.updateApplicationCatalog(callback, new File(newCatalogURL), new File(deploymentURL));

        // don't care if this succeeds or fails.
        // calling the new proc before the cat change returns is not guaranteed to work
        // we just hope it doesn't crash anything
        int x = 3;
        SyncCallback cb = new SyncCallback();
        client.callProcedure(cb,
                org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});
        cb.waitForResponse();

        // make sure the previous catalog change has completed
        client.drain();
        assertTrue(callbackSuccess);

        // now calling the new proc better work
        x = 2;
        client.callProcedure(org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, (short)x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});

        loadSomeData(client, 50, 5);
        assertTrue(callbackSuccess);

        // this is a do nothing change... shouldn't affect anything
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);
        client.drain();
        assertTrue(callbackSuccess);

        // now calling the new proc better work
        x = 4;
        client.callProcedure(org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, (short)x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});

        loadSomeData(client, 55, 5);

        // remove the procedure we just added async
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        callback = new CatTestCallback(ClientResponse.SUCCESS);
        client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL));

        // don't care if this works now
        x = 4;
        cb = new SyncCallback();
        client.callProcedure(cb,
                org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, (short)x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});
        cb.waitForResponse();

        // make sure the previous catalog change has completed
        client.drain();
        assertTrue(callbackSuccess);

        // now calling the new proc better fail
        x = 5;
        cb = new SyncCallback();
        client.callProcedure(cb,
                org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, (short)x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});
        cb.waitForResponse();
        assertNotSame(cb.getResponse().getStatus(), ClientResponse.SUCCESS);

        loadSomeData(client, 60, 5);

        // change the insert new order procedure
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-conflict.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-conflict.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // call the new proc and make sure the one we want gets run
        results = client.callProcedure(InsertNewOrder.class.getSimpleName(), 100, 100, 100, 100, (short)100, 100, 1.0, "a").getResults();
        assertEquals(1, results.length);
        assertEquals(1776, results[0].asScalarLong());

        // load a big catalog change just to make sure nothing fails horribly
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-many.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-many.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        loadSomeData(client, 65, 5);

        //Check that if a catalog update blocker exists the catalog update fails
        ZooKeeper zk = ZKUtil.getClient(((LocalCluster) m_config).zkinterface(0), 10000, new HashSet<Long>());
        final String catalogUpdateBlockerPath = zk.create(
                VoltZK.elasticJoinActiveBlocker,
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL );
        try {
            /*
             * Update the catalog and expect failure
             */
            newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
            boolean threw = false;
            try {
                client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL));
            } catch (ProcCallException e) {
                e.printStackTrace();
                threw = true;
            }
            assertTrue(threw);
        }
        finally {
            zk.delete(catalogUpdateBlockerPath, -1);
        }

        //Expect success
        client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL));

        client.drain();
        assertTrue(callbackSuccess);
        assertTrue(true);
    }

    public void testEnableSecurityAndHeartbeatTimeoutChange()
    throws IOException, ProcCallException, InterruptedException
    {
        System.out.println("\n\n-----\n testEnabledSecurity \n-----\n\n");
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base-secure.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base-secure.xml");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // a new client should need a username/password other than the regression suite default.
        boolean caught = false;
        try {
            getClient();
        } catch (IOException e) {
            caught = true;
        }
        assertTrue(caught);

        // create a valid client and call some procedures
        this.m_username = "user1";
        this.m_password = "userpass1";
        Client client3 = getClient();
        loadSomeData(client3, 50, 10);
        client3.drain();
        assertTrue(callbackSuccess);

        // the old client should not work because the user has been removed.
        loadSomeData(client, 100, 10);
        client.drain();
        assertFalse(callbackSuccess);
        callbackSuccess = true;

        checkDeploymentPropertyValue(client3, "heartbeattimeout", "6000");
    }

    private void loadSomeData(Client client, int start, int count) throws IOException, ProcCallException {
        for (int i = start; i < (start + count); i++) {
            CatTestCallback callback = new CatTestCallback(ClientResponse.SUCCESS);
            client.callProcedure(callback, InsertNewOrder.class.getSimpleName(), i, i, (short)i);
        }
    }

    public void negativeTests(Client client) throws UnsupportedEncodingException {
        // this fails because the catalog URL isn't a real thing but needs to point at
        // a file that actually exists.  Point to the compiled java class for this suite
        URL url = TestCatalogUpdateSuite.class.getResource("TestCatalogUpdateSuite.class");
        String newCatalogURL = URLDecoder.decode(url.getPath(), "UTF-8");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.xml");
        try {
            client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL));
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Database catalog not found"));
        }
    }

    public static long indexEntryCountFromStats(Client client, String tableName, String indexName) throws Exception {
        ClientResponse callProcedure = client.callProcedure("@Statistics", "INDEX", 0);
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = callProcedure.getResults()[0];
        long tupleCount = 0;
        while (result.advanceRow()) {
            if (result.getString("TABLE_NAME").equals(tableName) && result.getString("INDEX_NAME").equals(indexName)) {
                tupleCount += result.getLong("ENTRY_COUNT");
            }
        }
        return tupleCount;
    }

    public void testAddDropIndex() throws Exception
    {
        ClientResponse callProcedure;
        String explanation;
        VoltTable result;

        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // check that no index was used by checking the plan itself
        callProcedure = client.callProcedure("@Explain", "select * from NEW_ORDER where NO_O_ID = 5;");
        explanation = callProcedure.getResults()[0].fetchRow(0).getString(0);
        assertFalse(explanation.contains("INDEX SCAN"));

        // add index to NEW_ORDER
        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addindex.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addindex.xml");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // check the index for non-zero size

        long tupleCount = -1;
        while (tupleCount <= 0) {
            tupleCount = indexEntryCountFromStats(client, "NEW_ORDER", "NEWINDEX");
        }
        assertTrue(tupleCount > 0);

        // verify that the new table(s) support an insert
        callProcedure = client.callProcedure("@AdHoc", "insert into NEW_ORDER values (-1, -1, -1);");
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        // do a call that uses the index
        callProcedure = client.callProcedure("@AdHoc", "select * from NEW_ORDER where NO_O_ID = 5;");
        result = callProcedure.getResults()[0];
        result.advanceRow();
        assertEquals(5, result.getLong("NO_O_ID"));

        // check that an index was used by checking the plan itself
        callProcedure = client.callProcedure("@Explain", "select * from NEW_ORDER where NO_O_ID = 5;");
        explanation = callProcedure.getResults()[0].fetchRow(0).getString(0);
        assertTrue(explanation.contains("INDEX SCAN"));

        // tables can still be accessed
        loadSomeData(client, 20, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // check table for the right number of tuples
        callProcedure = client.callProcedure("@AdHoc", "select count(*) from NEW_ORDER;");
        long rowCount = callProcedure.getResults()[0].asScalarLong();
        // check the index for even biggerer size from stats
        long newTupleCount = indexEntryCountFromStats(client, "NEW_ORDER", "NEWINDEX");
        while (newTupleCount != (rowCount * (K + 1))) {
            newTupleCount = indexEntryCountFromStats(client, "NEW_ORDER", "NEWINDEX");
        }
        assertTrue(newTupleCount > tupleCount);
        assertEquals(newTupleCount, rowCount * (K + 1)); // index count is double for k=1

        // revert to the original schema
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // do a call that uses the index
        callProcedure = client.callProcedure("@AdHoc", "select * from NEW_ORDER where NO_O_ID = 5;");
        result = callProcedure.getResults()[0];
        result.advanceRow();
        assertEquals(5, result.getLong("NO_O_ID"));

        // check that no index was used by checking the plan itself
        callProcedure = client.callProcedure("@Explain", "select * from NEW_ORDER where NO_O_ID = 5;");
        explanation = callProcedure.getResults()[0].fetchRow(0).getString(0);
        assertFalse(explanation.contains("INDEX SCAN"));

        // and loading still succeeds
        loadSomeData(client, 30, 10);
        client.drain();
        assertTrue(callbackSuccess);
    }

    public void testAddDropExpressionIndex() throws Exception
    {
        ClientResponse callProcedure;
        String explanation;
        VoltTable result;

        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // check that no index was used by checking the plan itself
        callProcedure = client.callProcedure("@Explain", "select * from NEW_ORDER where (NO_O_ID+NO_O_ID)-NO_O_ID = 5;");
        explanation = callProcedure.getResults()[0].fetchRow(0).getString(0);
        assertFalse(explanation.contains("INDEX SCAN"));

        // add index to NEW_ORDER
        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addexpressindex.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addexpressindex.xml");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // check the index for non-zero size

        long tupleCount = -1;
        while (tupleCount <= 0) {
            tupleCount = indexEntryCountFromStats(client, "NEW_ORDER", "NEWEXPRESSINDEX");
        }
        assertTrue(tupleCount > 0);

        // verify that the new table(s) support an insert
        callProcedure = client.callProcedure("@AdHoc", "insert into NEW_ORDER values (-1, -1, -1);");
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        // do a call that uses the index
        callProcedure = client.callProcedure("@AdHoc", "select * from NEW_ORDER where (NO_O_ID+NO_O_ID)-NO_O_ID = 5;");
        result = callProcedure.getResults()[0];
        result.advanceRow();
        assertEquals(5, result.getLong("NO_O_ID"));

        // check that an index was used by checking the plan itself
        callProcedure = client.callProcedure("@Explain", "select * from NEW_ORDER where (NO_O_ID+NO_O_ID)-NO_O_ID = 5;");
        explanation = callProcedure.getResults()[0].fetchRow(0).getString(0);
        assertTrue(explanation.contains("INDEX SCAN"));

        // tables can still be accessed
        loadSomeData(client, 20, 10);
        client.drain();
        assertTrue(callbackSuccess);


        // check table for the right number of tuples
        callProcedure = client.callProcedure("@AdHoc", "select count(*) from NEW_ORDER;");
        long rowCount = callProcedure.getResults()[0].asScalarLong();
        // check the index for even biggerer size from stats
        long newTupleCount = -1;
        while (newTupleCount != (rowCount * (K + 1))) {
            newTupleCount = indexEntryCountFromStats(client, "NEW_ORDER", "NEWEXPRESSINDEX");
        }
        assertTrue(newTupleCount > tupleCount);
        assertEquals(newTupleCount, rowCount * (K + 1)); // index count is double for k=1

        // revert to the original schema
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // do a call that uses the index
        callProcedure = client.callProcedure("@AdHoc", "select * from NEW_ORDER where (NO_O_ID+NO_O_ID)-NO_O_ID = 5;");
        result = callProcedure.getResults()[0];
        result.advanceRow();
        assertEquals(5, result.getLong("NO_O_ID"));

        // check that no index was used by checking the plan itself
        callProcedure = client.callProcedure("@Explain", "select * from NEW_ORDER where (NO_O_ID+NO_O_ID)-NO_O_ID = 5;");
        explanation = callProcedure.getResults()[0].fetchRow(0).getString(0);
        assertFalse(explanation.contains("INDEX SCAN"));

        // and loading still succeeds
        loadSomeData(client, 30, 10);
        client.drain();
        assertTrue(callbackSuccess);
    }

    public void testAddDropTable() throws IOException, ProcCallException, InterruptedException
    {
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // verify that an insert w/o a table fails.
        try {
            client.callProcedure("@AdHoc", "insert into O1 values (1, 1, 'foo', 'foobar');");
            fail();
        }
        catch (ProcCallException e) {
        }

        // Also can't call this not-yet-existing stored procedure
        try {
            client.callProcedure("InsertO1", new Integer(100), new Integer(200), "foo", "bar");
            fail();
        }
        catch (ProcCallException e) {
        }

        // add tables O1, O2, O3
        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.xml");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // verify that the new table(s) support an insert
        ClientResponse callProcedure = client.callProcedure("@AdHoc", "insert into O1 values (1, 1, 'foo', 'foobar');");
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        callProcedure = client.callProcedure("@AdHoc", "insert into O2 values (1, 1, 'foo', 'foobar');");
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        callProcedure = client.callProcedure("@AdHoc", "select * from O1");
        VoltTable result = callProcedure.getResults()[0];
        result.advanceRow();
        assertTrue(result.get(2, VoltType.STRING).equals("foo"));

        // old tables can still be accessed
        loadSomeData(client, 20, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // and this new procedure is happy like clams
        callProcedure = client.callProcedure("InsertO1", new Integer(100), new Integer(200), "foo", "bar");
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        // revert to the original schema
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // requests to the dropped table should fail
        try {
            client.callProcedure("@AdHoc", "insert into O1 values (1, 1, 'foo', 'foobar');");
            fail();
        }
        catch (ProcCallException e) {
        }

        try {
            client.callProcedure("InsertO1", new Integer(100), new Integer(200), "foo", "bar");
            fail();
        }
        catch (ProcCallException e) {
        }

        // and other requests still succeed
        loadSomeData(client, 30, 10);
        client.drain();
        assertTrue(callbackSuccess);
    }

    public void testAddTableWithMatView() throws IOException, ProcCallException, InterruptedException {
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // add new tables and materialized view
        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtableswithmatview.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtableswithmatview.xml");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // verify that the new table(s) support an insert
        for (int i=0; i < 10; ++i) {
            ClientResponse callProcedure = client.callProcedure("@AdHoc", "insert into O1 values (" + i + ", " + i % 2 + ", 'foo', 'foobar');");
            assertTrue(callProcedure.getResults().length == 1);
            assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        }

        // read it - expect 10 rows
        ClientResponse callProcedure = client.callProcedure("@AdHoc", "select * from O1");
        VoltTable result = callProcedure.getResults()[0];
        assertTrue(result.getRowCount() == 10);

        // read the mat view. expect two rows (grouped on x % 2)
        callProcedure = client.callProcedure("@AdHoc", "select C1,NUM from MATVIEW_O1 order by C1");
        result = callProcedure.getResults()[0];

        System.out.println("MATVIEW:"); System.out.println(result);
        assertEquals(10, result.getRowCount());
    }

    private void loadSomeDataForNewTable(Client client, int start, int count) throws IOException, ProcCallException {
        for (int i = start; i < (start + count); i++) {
            CatTestCallback callback = new CatTestCallback(ClientResponse.SUCCESS);
            client.callProcedure(callback, "O1.insert", i, i, "abcdefg", "voltdb is a great startup with potential success in future");
        }
    }

    public void testAddDropTableRepeat() throws Exception {
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        /*
         * Reduced from 100 to 30 so that it doesn't take quite as long
         * We run tests often enough that this will get plenty of fuzzing.
         */
        for (int i=0; i < 30; i++)
        {
            // add tables O1, O2, O3
            String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.jar");
            String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.xml");
            VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);

            // Thread.sleep(2000);
            // Load table into the new added tables
            loadSomeDataForNewTable(client, 0, 1000 *10);
            client.drain();

            // revert to the original schema
            newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
            results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);
        }
    }

    public void testUpdateHonkingBigCatalog() throws IOException, ProcCallException, InterruptedException {
        System.out.println("\n\n-----\n testUpdateHonkingBigCatalog\n");
        System.out.printf("jar: %s (%.2f MB)\n", hugeCatalogJarPath, new File(hugeCatalogJarPath).length() / 1048576.0);
        System.out.printf("compile: %.2f seconds (%.2f/second)\n", hugeCompileElapsed, HUGE_TABLES / hugeCompileElapsed);
        long t = System.currentTimeMillis();
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        try {
            VoltTable[] results = client.updateApplicationCatalog(new File(hugeCatalogJarPath), new File(hugeCatalogXMLPath)).getResults();
            assertTrue(results.length == 1);
        }
        catch (ProcCallException e) {
            fail(String.format("@UpdateApplicationCatalog: ProcCallException: %s", e.getLocalizedMessage()));
        }
        hugeTestElapsed = (System.currentTimeMillis() - t) / 1000.0;
        System.out.printf("test: %.2f seconds (%.2f/second)\n", hugeTestElapsed, HUGE_TABLES / hugeTestElapsed);
        System.out.println("-----\n\n");
    }

    public void testSystemSettingsUpdateTimeout() throws IOException, ProcCallException, InterruptedException  {
        Client client = getClient();
        String newCatalogURL, deploymentURL;
        VoltTable[] results;

        // check the catalog update with query timeout
        String key = "querytimeout";
        checkDeploymentPropertyValue(client, key, "0"); // check default value

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-timeout-1000.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-timeout-1000.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);
        checkDeploymentPropertyValue(client, key, "1000");

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-timeout-5000.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-timeout-5000.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);
        checkDeploymentPropertyValue(client, key, "5000");

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-timeout-600.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-timeout-600.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);
        checkDeploymentPropertyValue(client, key, "600");

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);
        checkDeploymentPropertyValue(client, key, "0"); // check default value

        // check the catalog update with elastic duration and throughput
        String duration = "elasticduration", throughput = "elasticthroughput";
        checkDeploymentPropertyValue(client, duration, "50"); // check default value
        checkDeploymentPropertyValue(client, throughput, "2"); // check default value

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-elastic-100-5.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-elastic-100-5.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);
        checkDeploymentPropertyValue(client, duration, "100");
        checkDeploymentPropertyValue(client, throughput, "5");
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

    private static String generateRandomDDL(String name, int ntables, int ncols, int width)
            throws IOException {
        // Generate huge DDL file. Make it relatively uncompressible with randomness.
        File temp = File.createTempFile(name, ".sql");
        temp.deleteOnExit();
        FileWriter out = new FileWriter(temp);
        char[] charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_".toCharArray();
        Random random = new Random(99);
        for (int itable = 0; itable < ntables; itable++) {
            out.write(String.format("\nCREATE TABLE HUGE_TABLE_%d (\n", itable));
            out.write("C_FIRST INTEGER,\n");
            for (int icolumn = 0; icolumn < ncols; icolumn++) {
                String columnID = RandomStringUtils.random(width,
                                                           0,
                                                           charset.length,
                                                           false,
                                                           false,
                                                           charset,
                                                           random);
                out.write(String.format("C_%s INTEGER,\n", columnID));
            }
            out.write("PRIMARY KEY (C_FIRST));\n");
        }
        out.close();
        return URLEncoder.encode(temp.getAbsolutePath(), "UTF-8");
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
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCatalogUpdateSuite.class);

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
        project.addProcedures(BASEPROCS);
        // build the jarfile
        boolean basecompile = config.compile(project);
        assertTrue(basecompile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml"));

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // DELTA CATALOGS FOR TESTING
        /////////////////////////////////////////////////////////////

        // As catalogupdate-cluster-base but with security enabled. This requires users and groups..
        // We piggy-back the heartbeat change here.
        RoleInfo groups[] = new RoleInfo[] {new RoleInfo("group1", false, false, true, false, false, false)};
        UserInfo users[] = new UserInfo[] {new UserInfo("user1", "userpass1", new String[] {"group1"})};
        ProcedureInfo procInfo = new ProcedureInfo(new String[] {"group1"}, InsertNewOrder.class);

        config = new LocalCluster("catalogupdate-cluster-base-secure.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addUsers(users);
        project.addRoles(groups);
        project.addProcedures(procInfo);
        project.setSecurityEnabled(true, true);
        project.setDeadHostTimeout(6000);
        boolean compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-base-secure.xml"));

        //config = new LocalSingleProcessServer("catalogupdate-local-addtables.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-addtables.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addSchema(TestCatalogUpdateSuite.class.getResource("testorderby-ddl.sql").getPath());
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS_OPROCS);
        project.setElasticDuration(100);
        project.setElasticThroughput(50);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.xml"));

        // as above but also with a materialized view added to O1
        try {
            config = new LocalCluster("catalogupdate-cluster-addtableswithmatview.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
            project = new TPCCProjectBuilder();
            project.addDefaultSchema();
            project.addSchema(TestCatalogUpdateSuite.class.getResource("testorderby-ddl.sql").getPath());
            project.addLiteralSchema("CREATE VIEW MATVIEW_O1(C1, C2, NUM) AS SELECT A_INT, PKEY, COUNT(*) FROM O1 GROUP BY A_INT, PKEY;");
            project.addDefaultPartitioning();
            project.addProcedures(BASEPROCS_OPROCS);
            compile = config.compile(project);
            assertTrue(compile);
            MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtableswithmatview.xml"));
        } catch (IOException e) {
            fail();
        }

        config = new LocalCluster("catalogupdate-cluster-addindex.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addLiteralSchema("CREATE INDEX NEWINDEX ON NEW_ORDER (NO_O_ID);");
        // history is good because this new index is the only one (no pkey)
        project.addLiteralSchema("CREATE INDEX NEWINDEX2 ON HISTORY (H_C_ID);");
        // unique index
        project.addLiteralSchema("CREATE UNIQUE INDEX NEWINDEX3 ON STOCK (S_I_ID, S_W_ID, S_QUANTITY);");

        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-addindex.xml"));

        config = new LocalCluster("catalogupdate-cluster-addexpressindex.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addLiteralSchema("CREATE INDEX NEWEXPRESSINDEX ON NEW_ORDER ((NO_O_ID+NO_O_ID)-NO_O_ID);");
        // history is good because this new index is the only one (no pkey)
        project.addLiteralSchema("CREATE INDEX NEWEXPRESSINDEX2 ON HISTORY ((H_C_ID+H_C_ID)-H_C_ID);");
        // unique index
        // This needs to wait until the test for unique index coverage for indexed expressions can parse out any simple column expressions
        // and discover a unique index on some subset.
        //TODO: project.addLiteralSchema("CREATE UNIQUE INDEX NEWEXPRESSINDEX3 ON STOCK (S_I_ID, S_W_ID, S_QUANTITY+S_QUANTITY-S_QUANTITY);");

        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-addexpressindex.xml"));

        //config = new LocalSingleProcessServer("catalogupdate-local-expanded.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-expanded.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(EXPANDEDPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.xml"));

        //config = new LocalSingleProcessServer("catalogupdate-local-conflict.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-conflict.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(CONFLICTPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-conflict.xml"));

        //config = new LocalSingleProcessServer("catalogupdate-local-many.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-many.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(SOMANYPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-many.xml"));


        // A catalog change that enables snapshots
        config = new LocalCluster("catalogupdate-cluster-enable_snapshot.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
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
        project.addProcedures(BASEPROCS);
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
        project.addProcedures(BASEPROCS);
        project.setSnapshotSettings( "1s", 3, "/tmp/snapshotdirasda2", "foo2");
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot_dir_not_exist.xml"));

        //A huge catalog update to test size limits
        config = new LocalCluster("catalogupdate-cluster-huge.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        long t = System.currentTimeMillis();
        String hugeSchemaURL = generateRandomDDL("catalogupdate-cluster-huge",
                                                  HUGE_TABLES, HUGE_COLUMNS, HUGE_NAME_SIZE);
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addSchema(hugeSchemaURL);
        project.addProcedures(BASEPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        hugeCompileElapsed = (System.currentTimeMillis() - t) / 1000.0;
        hugeCatalogXMLPath = Configuration.getPathToCatalogForTest("catalogupdate-cluster-huge.xml");
        hugeCatalogJarPath = Configuration.getPathToCatalogForTest("catalogupdate-cluster-huge.jar");
        MiscUtils.copyFile(project.getPathToDeployment(), hugeCatalogXMLPath);

        config = new LocalCluster("catalogupdate-cluster-change_snapshot_dir_not_exist.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        project.setSnapshotSettings( "1s", 3, "/tmp/snapshotdirasda2", "foo2");
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot_dir_not_exist.xml"));

        // Catalogs with different system settings on query time out
        config = new LocalCluster("catalogupdate-cluster-timeout-1000.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.setQueryTimeout(1000);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-timeout-1000.xml"));

        config = new LocalCluster("catalogupdate-cluster-timeout-5000.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.setQueryTimeout(5000);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-timeout-5000.xml"));

        config = new LocalCluster("catalogupdate-cluster-timeout-600.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.setQueryTimeout(600);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-timeout-600.xml"));

        // elastic duration and throughput catalog update tests
        config = new LocalCluster("catalogupdate-cluster-elastic-100-5.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        // build the jarfile
        project.setElasticDuration(100);
        project.setElasticThroughput(5);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-elastic-100-5.xml"));


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
