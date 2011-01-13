/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.net.URL;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.matviewprocs.AddPerson;
import org.voltdb_testprocs.regressionsuites.matviewprocs.AddThing;
import org.voltdb_testprocs.regressionsuites.matviewprocs.AggAges;
import org.voltdb_testprocs.regressionsuites.matviewprocs.AggThings;
import org.voltdb_testprocs.regressionsuites.matviewprocs.DeletePerson;
import org.voltdb_testprocs.regressionsuites.matviewprocs.Eng798Insert;
import org.voltdb_testprocs.regressionsuites.matviewprocs.OverflowTest;
import org.voltdb_testprocs.regressionsuites.matviewprocs.SelectAllPeople;
import org.voltdb_testprocs.regressionsuites.matviewprocs.UpdatePerson;


public class TestMaterializedViewSuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        AddPerson.class, DeletePerson.class, UpdatePerson.class, AggAges.class,
        SelectAllPeople.class, AggThings.class, AddThing.class, OverflowTest.class,
        Eng798Insert.class
    };

    public TestMaterializedViewSuite(String name) {
        super(name);
    }

    public void testInsertSinglePartition() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 0);
        assert(results != null);

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 27500.20, 7).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 28920.99, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 1, 3L, 32L, 63250.01, -1).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 2);
        assert(results != null);
    }

    public void testDeleteSinglePartition() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 0);
        assert(results != null);

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 27500.20, 7).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 28920.99, 3).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("DeletePerson", 1, 1L).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 1);
        while (results[0].advanceRow()) {
            assertEquals(31L, results[0].getLong(0));
            assertEquals(1L, results[0].getLong(1));
            assertTrue(Math.abs(results[0].getDouble(2) - 28920.99) < .01);
            assertEquals(3L, results[0].getLong(3));
        }
        assert(results != null);

        results = client.callProcedure("DeletePerson", 1, 2L).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 0);
        assert(results != null);
    }

    public void testUpdateSinglePartition() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 0);
        assert(results != null);

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 27500.20, 7).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 28920.99, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 1, 3L, 33L, 28920.99, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("UpdatePerson", 1, 2L, 31L, 15000.00, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("UpdatePerson", 1, 1L, 31L, 15000.00, 5).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggAges", 1).getResults();
        assert(results != null);
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 2);
        System.out.println(results[0].toString());
        VoltTableRow r1 = results[0].fetchRow(0);
        VoltTableRow r2 = results[0].fetchRow(1);
        assertEquals(r1.getLong(0), 31L);
        assertEquals(r1.getLong(1), 2L);
        assertTrue(Math.abs(r1.getDouble(2) - 30000.0) < .01);
        assertEquals(r1.getLong(3), 8L);

        assertEquals(r2.getLong(0), 33L);
        assertEquals(r2.getLong(1), 1L);
        assertTrue(Math.abs(r2.getDouble(2) - 28920.99) < .01);
        assertEquals(r2.getLong(3), 3L);
    }

    public void testSinglePartitionWithPredicates() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 0);
        assert(results != null);

        // expecting the 2yr old won't make it
        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 7).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 1, 2L, 2L, 2000.0, 3).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 1);
        assert(results != null);

        results = client.callProcedure("UpdatePerson", 1, 1L, 3L, 1000.0, 6).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 0);
        assert(results != null);

        results = client.callProcedure("UpdatePerson", 1, 2L, 50L, 4000.0, 4).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 1);
        assert(results != null);

        results = client.callProcedure("DeletePerson", 1, 1L).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 1);
        assert(results != null);
    }

    public void testMultiPartitionSimple() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 0);
        assert(results != null);

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 1, 2L, 2L, 1000.0, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 2, 3L, 23L, 1000.0, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 2, 4L, 23L, 1000.0, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 2, 5L, 35L, 1000.0, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddPerson", 2, 6L, 35L, 1000.0, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("UpdatePerson", 1, 2L, 32L, 1000.0, 3).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("DeletePerson", 2, 6L).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggAges", 1).getResults();
        assert(results != null);
        assertEquals(results.length, 1);

        VoltTable results2[] = client.callProcedure("AggAges", 2).getResults();
        assert(results != null);
        assertEquals(results2.length, 1);

        int totalRows = results[0].getRowCount() + results2[0].getRowCount();
        // unfortunately they're both 4 in the hsql case, the fact that partitioning
        // can change behavior between backends if not used smartly should be corrected
        assertTrue((4 == totalRows) ||
                   (results[0].getRowCount() == 4) || (results2[0].getRowCount() == 4));
    }

    public void testInsertReplicated() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable[] results = null;

        results = client.callProcedure("AggThings").getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 0);
        assert(results != null);

        results = client.callProcedure("AddThing", 1L, 10L).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddThing", 2L, 12L).getResults();
        assertEquals(results.length, 1);
        results = client.callProcedure("AddThing", 3L, 10L).getResults();
        assertEquals(results.length, 1);

        results = client.callProcedure("AggThings").getResults();
        assertEquals(results.length, 1);
        assertEquals(results[0].getRowCount(), 2);
        assert(results != null);
    }

    public void testInsertAndOverflowSum() throws IOException, ProcCallException {
        if (isHSQL()) {
            return;
        }
        Client client = getClient();
        int invocationIndex = 0;
        VoltTable[] results = client.callProcedure("OverflowTest", 0, 0, invocationIndex++).getResults();
        results = client.callProcedure("OverflowTest", 2, 0, invocationIndex++).getResults();
        results = client.callProcedure("OverflowTest", 1, 0, 0).getResults();
        results[0].advanceRow();
        long preRollbackValue = results[0].getLong(3);
        boolean threwException = false;
        try {
            results = client.callProcedure("OverflowTest", 0, 0, invocationIndex++).getResults();
        } catch (Exception e) {
           threwException = true;
        }
        assertTrue(threwException);
        results = client.callProcedure("OverflowTest", 1, 0, 0).getResults();
        results[0].advanceRow();
        assertEquals(preRollbackValue, results[0].getLong(3));
        preRollbackValue = 0;
        threwException = false;
        while (!threwException) {
            try {
                results = client.callProcedure("OverflowTest", 2, 0, invocationIndex++).getResults();
                results = client.callProcedure("OverflowTest", 1, 0, 0).getResults();
                results[0].advanceRow();
                preRollbackValue = results[0].getLong(2);
            } catch (Exception e) {
                threwException = true;
                break;
            }
        }
        results = client.callProcedure("OverflowTest", 1, 0, 0).getResults();
        results[0].advanceRow();
        assertEquals(preRollbackValue, results[0].getLong(2));
    }

    /** Test a view that re-orders the source table's columns */
    public void testENG798() throws Exception {
        if (isHSQL()) {
            return;
        }

        // this would throw on a bad cast in the broken case.
        Client client = getClient();
        ClientResponse callProcedure = client.callProcedure("Eng798Insert", "clientname");
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getResults()[0].asScalarLong() == 1);
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
        URL url = AddPerson.class.getResource("matviewsuite-ddl.sql");
        url.getPath();

        String schemaPath = url.getPath();

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMaterializedViewSuite.class);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        //VoltServerConfig config = new LocalSingleProcessServer("matview-onesite.jar", 1, BackendTarget.NATIVE_EE_IPC);
        VoltServerConfig config = new LocalSingleProcessServer("matview-onesite.jar", 1, BackendTarget.NATIVE_EE_JNI);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        //project.setBackendTarget(BackendTarget.NATIVE_EE_IPC);
        project.addSchema(schemaPath);
        project.addPartitionInfo("PEOPLE", "PARTITION");
        project.addPartitionInfo("OVERFLOWTEST", "COL_1");
        project.addPartitionInfo("ENG798", "C1");
        project.addProcedures(PROCEDURES);
        // build the jarfile
        //config.compile(project);

        // add this config to the set of tests to run
        //builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with two sites/partitions
        config = new LocalSingleProcessServer("matview-twosites.jar", 2, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile (note the reuse of the TPCC project)
        boolean compile = config.compile(project);
        assertTrue(compile);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalSingleProcessServer("matview-hsql.jar", 1, BackendTarget.HSQLDB_BACKEND);
        boolean compile2 = config.compile(project);
        assertTrue(compile2);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("matview-cluster.jar", 2, 2,
                                  1, BackendTarget.NATIVE_EE_JNI);
        boolean compile3 = config.compile(project);
        assertTrue(compile3);
        builder.addServerConfig(config);

        return builder;
    }
}
