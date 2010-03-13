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

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.LoadCatalogToString;
import org.voltdb.client.Client;

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
                                        org.voltdb.benchmark.tpcc.procedures.slev.class };
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

    public void testUpdate() throws Exception {
        Client client = getClient();

        String newCatalogURL;
        VoltTable[] results;

        testStuffThatShouldObviouslyFail(client);

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-onesite-expanded.jar");
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
        assertTrue(results.length == 0);

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-onesite-expanded.jar");
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
        assertTrue(results.length == 0);

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-onesite-base.jar");
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
        assertTrue(results.length == 0);

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-onesite-conflict.jar");
        results = client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
        assertTrue(results.length == 0);

        assertTrue(true);
    }

    public void loadSomeData() {

    }

    public void queryAndVerifySomeData() {

    }

    public void testStuffThatShouldObviouslyFail(Client client) throws UnsupportedEncodingException {
        String newCatalogURL;

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-onesite-addtables.jar");
        try {
            client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("The requested catalog change is not"));
        }

        URL url = LoadCatalogToString.class.getResource("catalog.txt");
        newCatalogURL = URLDecoder.decode(url.getPath(), "UTF-8");
        try {
            client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Unable to read from catalog"));
        }

        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-onesite-many.jar");
        try {
            client.callProcedure("@UpdateApplicationCatalog", newCatalogURL);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("The requested catalog change is too large"));
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
        VoltServerConfig config = new LocalSingleProcessServer("catalogupdate-onesite-base.jar", 1, BackendTarget.NATIVE_EE_JNI);

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
        config = new LocalSingleProcessServer("catalogupdate-onesite-addtables.jar", 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addSchema(TestCatalogUpdateSuite.class.getResource("testorderby-ddl.sql").getPath());
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        config.compile(project);

        // Build a new catalog
        config = new LocalSingleProcessServer("catalogupdate-onesite-expanded.jar", 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(EXPANDEDPROCS);
        config.compile(project);

        // Build a new catalog
        config = new LocalSingleProcessServer("catalogupdate-onesite-conflict.jar", 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(CONFLICTPROCS);
        config.compile(project);

        // Build a new catalog
        config = new LocalSingleProcessServer("catalogupdate-onesite-many.jar", 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(SOMANYPROCS);
        config.compile(project);

        // Build a new catalog
        /*config = new LocalSingleProcessServer("catalogupdate-onesite-expanded.jar", 1, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        UserInfo[] users;
        project.addUsers(users)
        config.compile(project);*/

        return builder;
    }
}
