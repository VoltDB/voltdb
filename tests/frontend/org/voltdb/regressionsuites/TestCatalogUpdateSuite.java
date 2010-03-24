/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.catalog.LoadCatalogToString;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.SyncCallback;
import org.voltdb.types.TimestampType;

/**
 * Tests a mix of multi-partition and single partition procedures on a
 * mix of replicated and partititioned tables on a mix of single-site and
 * multi-site VoltDB instances.
 *
 */
public class TestCatalogUpdateSuite extends RegressionSuite {

    // procedures used by these tests
    static Class<?>[] BASEPROCS =     { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class };
    static Class<?>[] EXPANDEDPROCS = { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class,
                                        org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class };
    static Class<?>[] CONFLICTPROCS = { org.voltdb.catalog.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class };
    static Class<?>[] SOMANYPROCS =   { org.voltdb.catalog.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.neworder.class,
                                        org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId.class,
                                        org.voltdb.benchmark.tpcc.procedures.ostatByCustomerName.class,
                                        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId.class,
                                        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerName.class,
                                        org.voltdb.benchmark.tpcc.procedures.slev.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestCatalogUpdateSuite(String name) {
        super(name);
    }

    AtomicInteger m_outstandingCalls = new AtomicInteger(0);

    class CatTestCallback extends ProcedureCallback {

        final byte m_expectedStatus;

        CatTestCallback(byte expectedStatus) {
            m_expectedStatus = expectedStatus;
            m_outstandingCalls.incrementAndGet();
        }

        @Override
        protected void clientCallback(ClientResponse clientResponse) {
            m_outstandingCalls.decrementAndGet();
            if (m_expectedStatus != clientResponse.getStatus()) {
                if (clientResponse.getExtra() != null)
                    System.err.println(clientResponse.getExtra());
                if (clientResponse.getException() != null)
                    clientResponse.getException().printStackTrace();
                assertTrue(false);
            }
        }
    }

    public void blockUntilNoOutstandingTransactions() {

    }

    public void testUpdate() throws Exception {
        Client client = getClient();

        String newCatalogURL;
        VoltTable[] results;
        CatTestCallback callback;

        loadSomeData(client, 0, 25);
        client.drain();

        testStuffThatShouldObviouslyFail(client);

        // asynchronously call some random inserts
        loadSomeData(client, 25, 25);

        // add a procedure "InsertOrderLineBatched"
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar");
        callback = new CatTestCallback(ClientResponse.SUCCESS);
        client.callProcedure(callback, "@UpdateApplicationCatalog", newCatalogURL);

        client.drain();

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

        // now calling the new proc better work
        x = 2;
        client.callProcedure(org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});

        loadSomeData(client, 50, 5);

        // this is a do nothing change... shouldn't affect anything
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar");
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
        assertTrue(results.length == 0);

        client.drain();

        // now calling the new proc better work
        x = 4;
        client.callProcedure(org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});

        // remove the procedure we just added async
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        callback = new CatTestCallback(ClientResponse.SUCCESS);
        client.callProcedure(callback, "@UpdateApplicationCatalog", newCatalogURL);

        // don't care if this works now
        x = 4;
        cb = new SyncCallback();
        client.callProcedure(cb,
                org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});
        cb.waitForResponse();

        // make sure the previous catalog change has completed
        client.drain();

        // now calling the new proc better fail
        x = 5;
        cb = new SyncCallback();
        client.callProcedure(cb,
                org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class.getSimpleName(),
                new long[] {x}, new long[] {x}, x, new long[] {x},
                new long[] {x}, new long[] {x}, new TimestampType[] { new TimestampType() }, new long[] {x},
                new double[] {x}, new String[] {"a"});
        cb.waitForResponse();
        assertNotSame(cb.getResponse().getStatus(), ClientResponse.SUCCESS);

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-conflict.jar");
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
        assertTrue(results.length == 0);

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-many.jar");
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
        assertTrue(results.length == 0);

        assertTrue(true);
    }

    public void loadSomeData(Client client, int start, int count) throws NoConnectionsException, ProcCallException {
        for (int i = start; i < (start + count); i++) {
            CatTestCallback callback = new CatTestCallback(ClientResponse.SUCCESS);
            client.callProcedure(callback, InsertNewOrder.class.getSimpleName(), i, i, i);
        }
    }



    public void queryAndVerifySomeData() {

    }

    public void testStuffThatShouldObviouslyFail(Client client) throws UnsupportedEncodingException {
        // this fails because it tries to change schema
        String newCatalogURL;
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.jar");
        try {
            client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("The requested catalog change is not"));
        }

        // this fails because the catalog URL isn't a real thing
        URL url = LoadCatalogToString.class.getResource("catalog.txt");
        newCatalogURL = URLDecoder.decode(url.getPath(), "UTF-8");
        try {
            client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Unable to read from catalog"));
        }
    }


    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCatalogUpdateSuite.class);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        //VoltServerConfig config = new LocalSingleProcessServer("catalogupdate-local-base.jar", 2, BackendTarget.NATIVE_EE_JNI);
        VoltServerConfig config = new LocalCluster("catalogupdate-cluster-base.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        // build the jarfile
        config.compile(project);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // DELTA CATALOGS FOR TESTING
        /////////////////////////////////////////////////////////////

        // Build a new catalog
        //config = new LocalSingleProcessServer("catalogupdate-local-addtables.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-addtables.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addSchema(TestCatalogUpdateSuite.class.getResource("testorderby-ddl.sql").getPath());
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        config.compile(project);

        // Build a new catalog
        //config = new LocalSingleProcessServer("catalogupdate-local-expanded.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-expanded.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(EXPANDEDPROCS);
        config.compile(project);

        // Build a new catalog
        //config = new LocalSingleProcessServer("catalogupdate-local-conflict.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-conflict.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(CONFLICTPROCS);
        config.compile(project);

        // Build a new catalog
        //config = new LocalSingleProcessServer("catalogupdate-local-many.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-many.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(SOMANYPROCS);
        config.compile(project);

        return builder;
    }
}
