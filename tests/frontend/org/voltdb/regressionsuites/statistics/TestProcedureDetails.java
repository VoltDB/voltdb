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
            boolean readwrite = (configValue & m_optionRWMask) > 0;
            boolean singlePartition = (configValue & m_singlePartitionMask) > 0;
            boolean failure = (configValue & m_optionFAILMask) > 0;
            boolean abort = (configValue & m_optionABORTMask) > 0;
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
                argBuilder.append("twobatch ");
            }
            if (readwrite) {
                argBuilder.append("readwrite ");
            }
            if (failure) {
                argBuilder.append("failure ");
            }
            if (abort) {
                argBuilder.append("abort ");
            }
            m_argString = argBuilder.toString();
            // If both "abort" and "twobatch" are present, the abort exception in the first
            // batch will be handled so that the second batch can still run.
            // But if the first batch in a multi-partition procedure failed, the procedure will abort
            // anyway even if the exception is being handled:
            // Multi-partition procedure xxx attempted to execute new batch after hitting EE exception in a previous batch
            m_expectsException = failure || (abort && ! (singlePartition && twoBatch));
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

        static final int m_singlePartitionMask = 1 << 4;
        static final int m_option2BATCHMask = 1 << 3;
        static final int m_optionRWMask = 1 << 2;
        static final int m_optionFAILMask = 1 << 1;
        static final int m_optionABORTMask = 1 << 0;
        static final int m_optionCount = 5;
    }

    private void validateProcedureDetail(ProcedureDetailTestConfig testConfig, VoltTable procedureDetail) {

    }

    public void testProcedureDetail() throws Exception {
        Client client = getClient();
        // Exhaust all the combinatorial possibilities of the *m_optionCount* options.
        // In total, 32 (2^5) different scenarios are being tested here.
        int maxConfigValue = 1 << ProcedureDetailTestConfig.m_optionCount;
        for (int configValue = 0; configValue < maxConfigValue; configValue++) {
            ProcedureDetailTestConfig testConfig = new ProcedureDetailTestConfig(configValue);
            System.out.println("\n========================================================================================");
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
                System.out.println("\nCaught exception as expected:\n" + pce.getMessage());
                continue;
            }
            finally {
                // Note that pass 1 as the second parameter to get incremental statistics.
                VoltTable procedureDetail = client.callProcedure("@Statistics", "PROCEDUREDETAIL", 1).getResults()[0];
                System.out.println(procedureDetail.toFormattedString());
                validateProcedureDetail(testConfig, procedureDetail);
            }
            // The test configuration says an exception is expected, but we did not get it.
            if (testConfig.expectsException()) {
                throw new Exception(String.format("Expects an exception from exec %s %d '%s', but did not get it.",
                                                  testConfig.getNameOfProcedureToCall(),
                                                  configValue, testConfig.getArgumentString()));
            }
        }
        VoltTable procedureDetail = client.callProcedure("@Statistics", "PROCEDUREDETAIL", 0).getResults()[0];
//        System.out.println(procedureDetail.toFormattedString());
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
        // Note that those two stored procedures have @ProcStatsOption annotations,
        // every invocation of them will be sampled in the procedure detail table.
        project.addProcedures(ProcedureDetailTestSP.class, ProcedureDetailTestMP.class);

        // 2-node cluster, 2 sites per host, k = 0 running on the JNI backend
        LocalCluster config = new LocalCluster("proceduredetail-jni.jar", 2, 2, 0, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        assertTrue(config.compile(project));
        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
