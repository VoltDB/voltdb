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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.voltdb.*;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.catalog.LoadCatalogToString;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
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
    static Class<?>[] BASEPROCS = { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                    org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                    org.voltdb.benchmark.tpcc.procedures.delivery.class };

    static Class<?>[] BASEPROCS_OPROCS =  { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                            org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                            org.voltdb.benchmark.tpcc.procedures.delivery.class,
                                            org.voltdb.regressionsuites.orderbyprocs.InsertO1.class};


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
                if (clientResponse.getException() != null)
                    clientResponse.getException().printStackTrace();
                callbackSuccess = false;
            }
        }
    }

    public void testUpdate() throws Exception {
        Client client = getClient();
        String newCatalogURL;
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
        callback = new CatTestCallback(ClientResponse.SUCCESS);
        client.callProcedure(callback, "@UpdateApplicationCatalog", newCatalogURL);

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
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL).getResults();
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
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-basewithdeployment.jar");
        callback = new CatTestCallback(ClientResponse.SUCCESS);
        client.callProcedure(callback, "@UpdateApplicationCatalog", newCatalogURL);

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
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL).getResults();
        assertTrue(results.length == 1);

        // call the new proc and make sure the one we want gets run
        results = client.callProcedure(InsertNewOrder.class.getSimpleName(), 100, 100, 100, 100, (short)100, 100, 1.0, "a").getResults();
        assertEquals(1, results.length);
        assertEquals(1776, results[0].asScalarLong());

        // load a big catalog change just to make sure nothing fails horribly
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-many.jar");
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL).getResults();
        assertTrue(results.length == 1);

        loadSomeData(client, 65, 5);

        client.drain();
        assertTrue(callbackSuccess);
        assertTrue(true);
    }

    public void loadSomeData(Client client, int start, int count) throws IOException, ProcCallException {
        for (int i = start; i < (start + count); i++) {
            CatTestCallback callback = new CatTestCallback(ClientResponse.SUCCESS);
            client.callProcedure(callback, InsertNewOrder.class.getSimpleName(), i, i, (short)i);
        }
    }

    public void negativeTests(Client client) throws UnsupportedEncodingException {
        // this fails because the catalog URL isn't a real thing
        URL url = LoadCatalogToString.class.getResource("catalog.txt");
        String newCatalogURL = URLDecoder.decode(url.getPath(), "UTF-8");
        try {
            client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Unable to read from catalog"));
        }
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
        VoltTable[] results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL).getResults();
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
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-basewithdeployment.jar");
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL).getResults();
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
        VoltTable[] results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL).getResults();
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
        assertTrue(result.getRowCount() == 2);
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
        boolean basecompile = config.compile(project);
        assertTrue(basecompile);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // DELTA CATALOGS FOR TESTING
        /////////////////////////////////////////////////////////////

        // Build a copy of the starting catalog and compile the deployment into it immediately
        config = new LocalCluster("catalogupdate-cluster-basewithdeployment.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        boolean compile = config.compile(project, true);
        assertTrue(compile);

        // Build a new catalog and compile the deployment into it immediately
        //config = new LocalSingleProcessServer("catalogupdate-local-addtables.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-addtables.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addSchema(TestCatalogUpdateSuite.class.getResource("testorderby-ddl.sql").getPath());
        project.addDefaultPartitioning();
        project.addPartitionInfo("O1", "PKEY");
        project.addProcedures(BASEPROCS_OPROCS);
        compile = config.compile(project, true);
        assertTrue(compile);

        // as above but also with a materialized view added to O1
        try {
            config = new LocalCluster("catalogupdate-cluster-addtableswithmatview.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
            project = new TPCCProjectBuilder();
            project.addDefaultSchema();
            project.addSchema(TestCatalogUpdateSuite.class.getResource("testorderby-ddl.sql").getPath());
            project.addLiteralSchema("CREATE VIEW MATVIEW_O1(C1,NUM) AS SELECT A_INT, COUNT(*) FROM O1 GROUP BY A_INT;");
            project.addDefaultPartitioning();
            project.addPartitionInfo("O1", "PKEY");
            project.addProcedures(BASEPROCS_OPROCS);
            compile = config.compile(project, true);
            assertTrue(compile);
        } catch (IOException e) {
            fail();
        }

        // Build a new catalog and compile the deployment into it immediately
        //config = new LocalSingleProcessServer("catalogupdate-local-expanded.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-expanded.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(EXPANDEDPROCS);
        compile = config.compile(project, true);
        assertTrue(compile);

        // Build a new catalog and compile the deployment into it immediately
        //config = new LocalSingleProcessServer("catalogupdate-local-conflict.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-conflict.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(CONFLICTPROCS);
        compile = config.compile(project, true);
        assertTrue(compile);

        // Build a new catalog and compile the deployment into it immediately
        //config = new LocalSingleProcessServer("catalogupdate-local-many.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-many.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(SOMANYPROCS);
        compile = config.compile(project, true);
        assertTrue(compile);

        return builder;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        assertTrue(callbackSuccess);
    }

    @Override
    public void setUp() {
        super.setUp();
        callbackSuccess = true;
    }
}
