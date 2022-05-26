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

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.UpdateNewOrder;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MispartitionedInsert;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MispartitionedUpdate;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MultiSiteDelete;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MultiSiteIndexSelect;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MultiSiteSelect;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MultiSiteSelectDuped;

import junit.framework.Test;

/**
 * Tests a mix of multi-partition and single partition procedures on a
 * mix of replicated and partititioned tables on a mix of single-site and
 * multi-site VoltDB instances.
 *
 */
public class TestMultiPartitionSuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] MP_PROCEDURES = {
        MultiSiteSelect.class,
        MultiSiteSelectDuped.class,
        MultiSiteIndexSelect.class,
        MultiSiteDelete.class,
        UpdateNewOrder.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestMultiPartitionSuite(String name) {
        super(name);
    }

    public void testSimpleScan() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("InsertNewOrder", 1L, 1L, 1L);
            client.callProcedure("InsertNewOrder", 2L, 2L, 2L);
            client.callProcedure("InsertNewOrder", 3L, 3L, 3L);
            client.callProcedure("InsertNewOrder", 4L, 4L, 5L);

            System.out.println("\nBEGIN TEST\n==================\n");

            VoltTable[] results = client.callProcedure("MultiSiteSelect").getResults();

            assertTrue(results.length == 1);
            VoltTable resultAll = results[0];

            System.out.println("All Got " + String.valueOf(resultAll.getRowCount()) + " rows.");
            assertTrue(resultAll.getRowCount() == 4);

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        assertTrue(true);
    }

    public void testIndexScan() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("InsertNewOrder", 1L, 1L, 1L);
            client.callProcedure("InsertNewOrder", 2L, 2L, 2L);
            client.callProcedure("InsertNewOrder", 3L, 3L, 3L);
            client.callProcedure("InsertNewOrder", 4L, 4L, 4L);

            System.out.println("\nBEGIN TEST\n==================\n");

            VoltTable[] results = client.callProcedure("MultiSiteIndexSelect").getResults();

            assertTrue(results.length == 2);

            System.out.println("All Got " + String.valueOf(results[0].getRowCount()) + " rows.");
            System.out.println("Index: " + results[0].toString());
            assertTrue(results[0].getRowCount() == 4);

            System.out.println("Index2 Got " + String.valueOf(results[1].getRowCount()) + " rows.");
            System.out.println("Index2: " + results[1].toString());
            assertTrue(results[1].getRowCount() == 1);

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        assertTrue(true);
    }

    public void testDelete() throws IOException {
        Client client = getClient();

        try {
            client.callProcedure("InsertNewOrder", 1L, 1L, 1L);
            client.callProcedure("InsertNewOrder", 2L, 2L, 2L);
            client.callProcedure("InsertNewOrder", 3L, 3L, 3L);
            client.callProcedure("InsertNewOrder", 4L, 4L, 4L);

            System.out.println("\nBEGIN TEST\n==================\n");

            // delete a tuple
            VoltTable[] results = client.callProcedure("MultiSiteDelete").getResults();
            assertTrue(results.length == 1);
            VoltTable resultModCount = results[0];
            long modCount = resultModCount.asScalarLong();
            assertTrue(modCount == 1);

            // check for three remaining tuples
            results = client.callProcedure("MultiSiteSelect").getResults();
            assertTrue(results.length == 1);
            VoltTable allData = results[0];
            System.out.println("Leftover: " + allData.toString());
            assertTrue(allData.getRowCount() == 3);

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        assertTrue(true);
    }


    public void testUpdate() throws IOException {
        Client client = getClient();

        try {
            // parameters to InsertNewOrder are order, district, warehouse
            client.callProcedure("InsertNewOrder", 1L, 1L, 1L);

            // parameters to UpdateNewOrder are no_o_id, alwaysFail
            VoltTable[] results = client.callProcedure("UpdateNewOrder", 1L, 1L).getResults();
            assertTrue(results.length == 1);
            assertTrue(results[0].asScalarLong() == 1);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }

        try {
            // will always fail.
            client.callProcedure("UpdateNewOrder", 1L, 0L);
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    public void testDuplicateSQL() throws IOException {
        Client client = getClient();

        try {
            int dupes = 5;
            client.callProcedure("InsertNewOrder", 1L, 1L, 1L);
            client.callProcedure("InsertNewOrder", 2L, 2L, 2L);
            client.callProcedure("InsertNewOrder", 3L, 3L, 3L);
            client.callProcedure("InsertNewOrder", 4L, 4L, 5L);

            System.out.println("\nBEGIN TEST\n==================\n");

            VoltTable[] results = client.callProcedure("MultiSiteSelectDuped", dupes).getResults();

            assertEquals(dupes, results.length);
            VoltTable resultAll = results[0];

            System.out.println("All Got " + String.valueOf(resultAll.getRowCount()) + " rows.");
            assertTrue(resultAll.getRowCount() == 4);

            for (int i = 1; i < dupes; i++)
            {
                assertTrue(results[0].hasSameContents(results[i]));
            }

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
        assertTrue(true);
    }

    public void testWrongPartitioning() throws IOException, ProcCallException {
        // Restrict to clustered tests (configured with > 1 partition)
        LocalCluster config = (LocalCluster)this.getServerConfig();
        int sites = config.m_siteCount;
        int nodes = config.m_hostCount;
        int k = config.m_kfactor;
        int parts = (nodes * sites) / (k + 1);
        if (parts == 1) {
            return;
        }

        Client client = getClient();

        // test mispartitioned insert
        try {
            client.callProcedure(MispartitionedInsert.class.getSimpleName(), 0);
            fail();
        }
        catch (Exception e) {
            System.err.println("==========");
            System.err.println("Following stacktrace is expected:");
            e.printStackTrace();
            System.err.println("==========");

            // check for the expected error
            assertTrue(e.getMessage().contains("Mispartitioned tuple"));
        }

        // test mispartitioned update
        client.callProcedure("NEW_ORDER.insert", 0, 0, 0);
        try {
            client.callProcedure(MispartitionedUpdate.class.getSimpleName(), 0);
            fail();
        }
        catch (Exception e) {
            System.err.println("==========");
            System.err.println("Following stacktrace is expected:");
            e.printStackTrace();
            System.err.println("==========");

            // check for the expected error
            assertTrue(e.getMessage().contains("An update to a partitioning column"));
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
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMultiPartitionSuite.class);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        VoltServerConfig config = new LocalCluster("distregression-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addMultiPartitionProcedures(MP_PROCEDURES);
        project.addProcedure(MispartitionedInsert.class, "NEW_ORDER.NO_W_ID: 0");
        project.addProcedure(MispartitionedUpdate.class, "NEW_ORDER.NO_W_ID: 0");

        project.addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");
        // build the jarfile
        boolean success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        // get a server config that similar, but doesn't use the same backend
        config = new LocalCluster("distregression-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);

        // build the jarfile (note the reuse of the TPCC project)
        success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: N=3 K=1 Cluster
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("distregression-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);

        builder.addServerConfig(config);

        return builder;
    }
}
