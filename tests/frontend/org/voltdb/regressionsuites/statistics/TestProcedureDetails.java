/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.regressionsuites.statistics;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb_testprocs.regressionsuites.proceduredetail.ProcedureDetailTestMP;
import org.voltdb_testprocs.regressionsuites.proceduredetail.ProcedureDetailTestSP;

import junit.framework.Test;

public class TestProcedureDetails extends RegressionSuite {

    public TestProcedureDetails(String name) {
        super(name);
    }

    private final class ProcedureDetailTestConfig {
        /* Meaning of the bits in the configuration byte:
         * x, x, x, x, s, b, r, e
         * x: not used;
         * s: single partition?
         * b: queue two batches?
         * r: read + write?
         * e: generate an error?
         */
        public ProcedureDetailTestConfig(int configValue) {
            boolean twoBatch = (configValue & m_option2BATCHMask) > 0;
            boolean rw = (configValue & m_optionRWMask) > 0;
            boolean singlePartition = (configValue & m_singlePartitionMask) > 0;
            boolean err = (configValue & m_optionERRMask) > 0;
            if (singlePartition) {
                // Run the single partition procedure.
                m_nameOfProcedureToCall = "ProcedureDetailTestSP";
            }
            else {
                // Run the multi-partition procedure.
                m_nameOfProcedureToCall = "ProcedureDetailTestMP";
            }
            StringBuilder argBuilder = new StringBuilder();
            if (twoBatch) {
                argBuilder.append("2batch ");
            }
            if (rw) {
                argBuilder.append("rw ");
            }
            if (err) {
                argBuilder.append("err");
            }
            m_argString = argBuilder.toString();
            m_expectsException = err && ! (singlePartition && twoBatch);
        }

        public String getNameOfProcedureToCall() {
            return m_nameOfProcedureToCall;
        }

        public String getArgumentString() {
            return m_argString;
        }

        public boolean expectsException() {
            return m_expectsException;
        }

        private String m_nameOfProcedureToCall;
        private String m_argString;
        private boolean m_expectsException;
        static final int m_singlePartitionMask = 1 << 3;
        static final int m_option2BATCHMask = 1 << 2;
        static final int m_optionRWMask = 1 << 1;
        static final int m_optionERRMask = 1 << 0;
        static final int m_optionCount = 4;
    }

    private void validateProcedureDetail(ProcedureDetailTestConfig testConfig, VoltTable procedureDetail) {

    }

    public void testProcedureDetail() throws Exception {
        Client client = getClient();
        // Exhaust all the combinatorial possibilities of the m_optionCount options.
        int maxConfigValue = 1 << ProcedureDetailTestConfig.m_optionCount;
        for (int configValue = 0; configValue < maxConfigValue; configValue++) {
            ProcedureDetailTestConfig testConfig = new ProcedureDetailTestConfig(configValue);
            System.out.println("========================================================================================");
            System.out.println(String.format("exec %s %d '%s'",
                testConfig.getNameOfProcedureToCall(),
                configValue, testConfig.getArgumentString()));
            try {
                client.callProcedure(testConfig.getNameOfProcedureToCall(),
                        configValue, testConfig.getArgumentString());
            }
            catch (ProcCallException pce) {
                if (! testConfig.expectsException()) {
                    throw pce;
                }
                continue;
            }
            finally {
                VoltTable procedureDetail = client.callProcedure("@Statistics", "PROCEDUREDETAIL", 1).getResults()[0];
                System.out.println(procedureDetail.toFormattedString());
                validateProcedureDetail(testConfig, procedureDetail);
            }
            if (testConfig.expectsException()) {
                throw new Exception(String.format("Expects an exception from exec %s %d '%s', but did not get it.",
                                                    testConfig.getNameOfProcedureToCall(),
                                                    configValue, testConfig.getArgumentString()));
            }
        }
        VoltTable procedureDetail = client.callProcedure("@Statistics", "PROCEDUREDETAIL", 0).getResults()[0];
        System.out.println(procedureDetail.toFormattedString());
    }

    /**
     * Build a list of the tests that will be run when TestProcedureDetails gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestProcedureDetails.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addLiteralSchema("CREATE TABLE ENG11890 (a INTEGER NOT NULL, b VARCHAR(10));");
        project.addPartitionInfo("ENG11890", "a");
        project.addProcedures(ProcedureDetailTestSP.class, ProcedureDetailTestMP.class);

        // 2-node cluster, 2 sites per host, k = 0 running on the JNI backend
        LocalCluster config = new LocalCluster("proceduredetail-jni.jar", 2, 2, 0, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        assertTrue(config.compile(project));
        // add this config to the set of tests to run
        builder.addServerConfig(config);

        // 2-node cluster, 2 sites per host, k = 0 running on the IPC backend
//        config = new LocalCluster("proceduredetail-ipc.jar", 2, 2, 0, BackendTarget.NATIVE_EE_IPC);
//        // build the jarfile
//        assertTrue(config.compile(project));
//        // add this config to the set of tests to run
//        builder.addServerConfig(config);

        return builder;
    }
}
