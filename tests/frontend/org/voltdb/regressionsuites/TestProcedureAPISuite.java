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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.BufferArrayProc;
import org.voltdb_testprocs.regressionsuites.CurrentTimestampProcedure;
import org.voltdb_testprocs.regressionsuites.LastBatchLie;
import org.voltdb_testprocs.regressionsuites.VariableBatchSizeMP;
import org.voltdb_testprocs.regressionsuites.VariableBatchSizeSP;

import junit.framework.Test;

public class TestProcedureAPISuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        VariableBatchSizeMP.class, LastBatchLie.class, BufferArrayProc.class,
        CurrentTimestampProcedure.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestProcedureAPISuite(String name) {
        super(name);
    }

    /**
     * Given a size, build a batch that contains a list of ops
     * drawn from some subset of the complete list of possible opts
     */
    int[] buildABatch(int size) {
        int[] batch = new int[size];

        Random r = new Random(0);
        for (int i = 0; i < size; i++) {
            List<Integer> ops = getOpsMixFromAnyInt(size);
            batch[i] = ops.get(r.nextInt(ops.size()));
        }

        return batch;
    }

    /**
     * Convert an integer into a set of possible operations.
     * No real method here, just trying to get a mix
     */
    List<Integer> getOpsMixFromAnyInt(int value) {
        List<Integer> ops = new ArrayList<Integer>();
        if ((value & VariableBatchSizeMP.P_READ) != 0) ops.add(VariableBatchSizeMP.P_READ);
        if ((value & VariableBatchSizeMP.R_READ) != 0) ops.add(VariableBatchSizeMP.R_READ);
        if ((value & VariableBatchSizeMP.P_WRITE) != 0) ops.add(VariableBatchSizeMP.P_WRITE);
        if ((value & VariableBatchSizeMP.R_WRITE) != 0) ops.add(VariableBatchSizeMP.R_WRITE);
        // have at least one op
        if (ops.size() == 0) ops.add(VariableBatchSizeMP.P_READ);
        return ops;
    }

    public void testBatching() throws NoConnectionsException, IOException, ProcCallException {
        Client client = getClient();

        // the test procs assume one row is present in each table with value one
        client.callProcedure("P1.insert", 1);
        client.callProcedure("R1.insert", 1);

        // test a bunch of different batch sizes
        ArrayList<Integer> batchSizes = new ArrayList<Integer>();
        for (int j = 1; j <= 32; j++) batchSizes.add(j);
        for (int j = 190; j <= 220; j++) batchSizes.add(j);
        for (int j = 999; j <= 1001; j++) batchSizes.add(j);
        for (int j = 1200; j <= 1200; j++) batchSizes.add(j);

        for (int batchSize : batchSizes) {
            int[] batch = buildABatch(batchSize);
            client.callProcedure(VariableBatchSizeMP.class.getSimpleName(), 1, batch, batch);
        }

        for (int batchSize : batchSizes) {
            int[] batch = buildABatch(batchSize);
            client.callProcedure(VariableBatchSizeSP.class.getSimpleName(), 1, batch, batch);
        }
    }

    // This was the root cause of the defect reported in ENG-2875.
    // Rather than deadlock, we'll abort the procedure if we see
    // two voltExecuteSQL() calls with final batch as true.
    public void testLastBatchLie() throws IOException
    {
        System.out.println("STARTING testLastBatchLie");
        Client client = getClient();

        boolean threw = false;
        try
        {
            client.callProcedure("LastBatchLie").getResults();
            fail();
        }
        catch (ProcCallException e)
        {
            if (e.getMessage().contains("claiming a previous batch was final")) {
                threw = true;
            }
            else {
                e.printStackTrace();
            }
        }
        assertTrue(threw);
    }

    public void testCallArrayOfArrays() throws IOException, ProcCallException {
        Client client = getClient();
        byte[][] data = new byte[10][];
        for (int i = 0; i < data.length; i++) {
            data[i] = "Hello".getBytes(Constants.UTF8ENCODING);
        }
        String[] data3 = new String[3];
        data3[0] = "AAbbff00";
        data3[1] = "AAbbff0011";
        data3[2] = "1234567890abcdef";

        client.callProcedure(BufferArrayProc.class.getSimpleName(), data, data, data3);
    }

    public void testMultiPartitionCURRENT_TIMESTAMP() throws IOException, ProcCallException {
        if (isHSQL()) {
            return;
        }
        Client client = getClient();
        client.callProcedure(CurrentTimestampProcedure.class.getSimpleName());
    }

    /**
     * Build a list of the tests that will be run when TestProcedureAPISuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestProcedureAPISuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestProcedureAPISuite.class.getResource("procedureapisuite-ddl.sql"));
        project.addPartitionInfo("P1", "ID");
        project.addMultiPartitionProcedures(PROCEDURES);
        project.addProcedure(VariableBatchSizeSP.class, "P1.ID:0");


        /////////////////////////////////////////////////////////////
        // CONFIG #1: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with two sites/partitions
        VoltServerConfig config = new LocalCluster("failures-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile (note the reuse of the TPCC project)
        if (!config.compile(project)) fail();

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        // get a server config that similar, but doesn't use the same backend
        config = new LocalCluster("failures-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);

        // build the jarfile (note the reuse of the TPCC project)
        if (!config.compile(project)) fail();

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: N=2 K=1 Cluster
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("failures-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }
}
