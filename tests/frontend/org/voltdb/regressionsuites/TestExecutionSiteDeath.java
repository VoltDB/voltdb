/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb_testprocs.regressionsuites.executionsitekillers.SinglePartitionKiller;
import org.voltdb_testprocs.regressionsuites.rollbackprocs.SinglePartitionJavaError;

public class TestExecutionSiteDeath extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        SinglePartitionKiller.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestExecutionSiteDeath(String name) {
        super(name);
    }

    public void testSinglePartitionKiller() throws Exception {
        Client client = getClient();
        LocalCluster lc_config = (LocalCluster)m_config;

        // We start with 2 local cluster nodes
        assertEquals(2, lc_config.getLiveNodeCount());
        // This should kill one copy of a replica successfully.
        try
        {
            client.callProcedure("SinglePartitionKiller", (byte)2);
        }
        catch (ProcCallException e)
        {
            // Expected result if we cap the server out from under the client.
            // keep on keeping on
        }
        //In Jenkins you will occasionally see lc_config.getLiveNodeCount() return 2
        //I am assuming this is because the process has closed sockets but not propagated a return
        //value yet. Spin a bit to see if giving it some time makes it better.
        for (int ii = 0; ii < 5000; ii++) {
            if (lc_config.getLiveNodeCount() == 1) return;
            Thread.sleep(1);
        }
        // And now we should only have 1 local cluster node.
        assertEquals(1, lc_config.getLiveNodeCount());
    }

    static public Test suite() {
        VoltServerConfig config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestExecutionSiteDeath.class);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addSchema(SinglePartitionJavaError.class.getResource("tpcc-extraview-ddl.sql"));
        project.addDefaultPartitioning();
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");

        boolean success;
        config = new LocalCluster("rollback-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        LocalCluster lc_config = (LocalCluster)config;
        lc_config.setHasLocalServer(false);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestExecutionSiteDeath.class);
    }
}
