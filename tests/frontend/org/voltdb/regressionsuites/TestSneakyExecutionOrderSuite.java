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

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.sneakyprocs.MultiPartition;
import org.voltdb_testprocs.regressionsuites.sneakyprocs.SinglePartition;

public class TestSneakyExecutionOrderSuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        MultiPartition.class, SinglePartition.class
    };

    AtomicInteger answersReceived = new AtomicInteger(0);

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestSneakyExecutionOrderSuite(String name) {
        super(name);
    }

    class MPCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == ClientResponse.CONNECTION_LOST){
                return;
            }
            if (clientResponse.getStatus() != ClientResponse.SUCCESS){
                System.out.println(clientResponse.getStatusString());
            }
            assertTrue(clientResponse.getStatus() == ClientResponse.SUCCESS);
            answersReceived.decrementAndGet();
        }
    }
    class SPCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == ClientResponse.CONNECTION_LOST){
                return;
            }
            if (clientResponse.getStatus() != ClientResponse.SUCCESS){
                System.out.println(clientResponse.getStatusString());
            }
            assertTrue(clientResponse.getStatus() == ClientResponse.SUCCESS);
            answersReceived.decrementAndGet();
        }
    }

    public void testSneakingInAProc() throws Exception {
        System.out.println("STARTING testSneakingInAProc");
        Client client = getClient();

        int ctr = 0;
        for (int i = 0; i < 10; i++) {
            client.callProcedure(new MPCallback(), "MultiPartition");
            ctr++;
            client.callProcedure(new SPCallback(), "SinglePartition", ctr, ctr);
            ctr++;
            client.callProcedure(new SPCallback(), "SinglePartition", ctr, ctr);
            ctr++;
            client.callProcedure(new SPCallback(), "SinglePartition", ctr, ctr);
            ctr++;
            client.callProcedure(new SPCallback(), "SinglePartition", ctr, ctr);
            ctr++;
        }

        answersReceived.addAndGet(ctr);

        client.drain();

        while (answersReceived.get() > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSneakyExecutionOrderSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(MultiPartition.class.getResource("sneaky-ddl.sql"));
        project.addPartitionInfo("P1", "P");
        project.addProcedures(PROCEDURES);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        //VoltServerConfig config = new LocalCluster("sneaky.jar", 2, 2, BackendTarget.NATIVE_EE_JNI);
        VoltServerConfig config = new LocalSingleProcessServer("sneaky-twosites.jar", 2, BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("sneaky-cluster.jar", 2, 2,
                                  1, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        return builder;
    }
}
