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

package org.voltdb.regressionsuites.statistics;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.iv2.MpInitiator;
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

        private String m_nameOfProcedureToCall;
        private String m_argString;
        private boolean m_expectsException;
        private boolean m_singlePartition;
        private boolean m_abort;
        private boolean m_failure;
        private long m_workingPartition = 0;
        private int m_insertCount = 1;
        private int m_updateCount = 1;
        private int m_deleteCount = 1;
        private int m_selectCount = 0;
        private boolean m_twoBatch;

        static final int m_singlePartitionMask = 1 << 4;
        static final int m_option2BATCHMask = 1 << 3;
        static final int m_optionRWMask = 1 << 2;
        static final int m_optionFAILMask = 1 << 1;
        static final int m_optionABORTMask = 1 << 0;
        static final int m_optionCount = 5;

        public ProcedureDetailTestConfig(final int configValue) {
            m_twoBatch = (configValue & m_option2BATCHMask) > 0;
            final boolean readWrite = (configValue & m_optionRWMask) > 0;
            m_singlePartition = (configValue & m_singlePartitionMask) > 0;
            m_failure = (configValue & m_optionFAILMask) > 0;
            m_abort = (configValue & m_optionABORTMask) > 0;
            if (m_singlePartition) {
                // Run the single partition procedure.
                m_nameOfProcedureToCall = "ProcedureDetailTestSP";
            }
            else {
                // Run the multi-partition procedure.
                m_nameOfProcedureToCall = "ProcedureDetailTestMP";
            }
            // If both "abort" and "twobatch" are present, the abort exception in the first
            // batch will be handled so that the second batch can still run.
            // But if the first batch in a multi-partition procedure failed, the procedure will abort
            // anyway even if the exception is being handled:
            // Multi-partition procedure xxx attempted to execute new batch after hitting EE exception in a previous batch
            m_expectsException = m_failure || (m_abort && ! (m_singlePartition && m_twoBatch));
            StringBuilder argBuilder = new StringBuilder();
            if (m_twoBatch) {
                argBuilder.append("twobatch ");
                if (! m_expectsException) {
                    m_insertCount++;
                    m_deleteCount++;
                    m_updateCount++;
                    if (readWrite) {
                        m_selectCount++;
                    }
                }
            }
            if (readWrite) {
                argBuilder.append("readwrite ");
                m_selectCount++;
            }
            if (m_failure) {
                argBuilder.append("failure ");
            }
            if (m_abort) {
                argBuilder.append("abort ");
                m_insertCount++;
                m_deleteCount--;
            }
            m_argString = argBuilder.toString();
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

        public boolean hasStatementFailure() {
            return m_abort;
        }

        public boolean hasProcedureFailure() {
            // this logic is terrible, but matches what the proc does
            // the SP proc swallows the sql exception if batched
            if (m_singlePartition) {
                return m_failure || (m_abort && !m_twoBatch);
            }
            // the MP proc tries to swallow if batched, but fails
            return m_failure || m_abort;
        }

        public boolean isSinglePartition() {
            return m_singlePartition;
        }

        public long getWorkingPartition() {
            return m_workingPartition;
        }

        public void setWorkingPartition(long value) {
            m_workingPartition = value;
        }

        public int getInsertCount() {
            return m_insertCount;
        }

        public int getUpdateCount() {
            return m_updateCount;
        }

        public int getDeleteCount() {
            return m_deleteCount;
        }

        public int getSelectCount() {
            return m_selectCount;
        }
    }

    private void trivialVerification(VoltTable procedureDetail) {
        assertTrue(procedureDetail.getLong("TIMESTAMP") > 0);
        assertTrue(procedureDetail.getLong("MIN_EXECUTION_TIME") > 0);
        assertTrue(procedureDetail.getLong("MAX_EXECUTION_TIME") > 0);
        assertTrue(procedureDetail.getLong("AVG_EXECUTION_TIME") > 0);
        assertTrue(procedureDetail.getLong("MIN_RESULT_SIZE") >= 0);
        assertTrue(procedureDetail.getLong("MAX_RESULT_SIZE") >= 0);
        assertTrue(procedureDetail.getLong("AVG_RESULT_SIZE") >= 0);
        assertTrue(procedureDetail.getLong("MIN_PARAMETER_SET_SIZE") >= 0);
        assertTrue(procedureDetail.getLong("MAX_PARAMETER_SET_SIZE") >= 0);
        assertTrue(procedureDetail.getLong("AVG_PARAMETER_SET_SIZE") >= 0);
    }

    private void verifyRowsForStatement(String stmtName, long expectedInvocationCount,
                        ProcedureDetailTestConfig testConfig, VoltTable procedureDetail) {
        for (long i = 0; i < 4; i++) {
            assertTrue(procedureDetail.advanceRow());
            trivialVerification(procedureDetail);
            assertEquals(expectedInvocationCount, procedureDetail.getLong("INVOCATIONS"));
            assertEquals(expectedInvocationCount, procedureDetail.getLong("TIMED_INVOCATIONS"));
            assertEquals(0, procedureDetail.getLong("ABORTS"));
            if (stmtName.equals("anInsert") && testConfig.hasStatementFailure()) {
                assertEquals(1, procedureDetail.getLong("FAILURES"));
            }
            else {
                assertEquals(0, procedureDetail.getLong("FAILURES"));
            }

            if (testConfig.isSinglePartition()) {
                long partitionId = procedureDetail.getLong("PARTITION_ID");
                assertEquals(partitionId, testConfig.getWorkingPartition());
                assertEquals("org.voltdb_testprocs.regressionsuites.proceduredetail.ProcedureDetailTestSP",
                        procedureDetail.getString("PROCEDURE"));
                break;
            }
            assertEquals("org.voltdb_testprocs.regressionsuites.proceduredetail.ProcedureDetailTestMP",
                    procedureDetail.getString("PROCEDURE"));
        }
    }

    private void verifyProcedureDetailResult(Client client, ProcedureDetailTestConfig testConfig,
            VoltTable procedureDetail) throws NoConnectionsException, IOException, ProcCallException {
        assertNotNull(procedureDetail);
        procedureDetail.resetRowPosition();

        assertTrue(procedureDetail.advanceRow());
        // The first row is the <ALL> row.
        trivialVerification(procedureDetail);
        long hostId = procedureDetail.getLong("HOST_ID");
        long siteId = procedureDetail.getLong("SITE_ID");
        long partitionId = procedureDetail.getLong("PARTITION_ID");
        if (testConfig.isSinglePartition()) {
            assertEquals("org.voltdb_testprocs.regressionsuites.proceduredetail.ProcedureDetailTestSP",
                    procedureDetail.getString("PROCEDURE"));
            // See which partition this query went to.
            testConfig.setWorkingPartition(partitionId);
        }
        else {
            assertEquals("org.voltdb_testprocs.regressionsuites.proceduredetail.ProcedureDetailTestMP",
                    procedureDetail.getString("PROCEDURE"));

            assertEquals(getHostIdForMpI(client), hostId);
            assertEquals(2, siteId);
            assertEquals(16383, partitionId);
        }
        assertEquals("<ALL>", procedureDetail.getString("STATEMENT"));
        assertEquals(1, procedureDetail.getLong("INVOCATIONS"));
        assertEquals(1, procedureDetail.getLong("TIMED_INVOCATIONS"));
        assertEquals(testConfig.hasProcedureFailure() ? 1 : 0, procedureDetail.getLong("FAILURES"));
        if (testConfig.expectsException() && ! testConfig.hasProcedureFailure()) {
            assertEquals(1, procedureDetail.getLong("ABORTS"));
        }
        else {
            assertEquals(0, procedureDetail.getLong("ABORTS"));
        }
        if (testConfig.getDeleteCount() > 0) {
            verifyRowsForStatement("aDelete", testConfig.getDeleteCount(), testConfig, procedureDetail);
        }
        if (testConfig.getSelectCount() > 0) {
            verifyRowsForStatement("aSelect", testConfig.getSelectCount(), testConfig, procedureDetail);
        }
        verifyRowsForStatement("anInsert", testConfig.getInsertCount(), testConfig, procedureDetail);
        verifyRowsForStatement("anUpdate", testConfig.getUpdateCount(), testConfig, procedureDetail);
    }

    public void testProcedureDetail() throws Exception {
        Client client = getClient();
        // Exhaust all the combinatorial possibilities of the *m_optionCount* options.
        // In total, 32 (2^5) different scenarios are being tested here.
        int maxConfigValue = 1 << ProcedureDetailTestConfig.m_optionCount;
        for (int configValue = 0; configValue < maxConfigValue; configValue++) {
            ProcedureDetailTestConfig testConfig = new ProcedureDetailTestConfig(configValue);
            System.out.println("\n========================================================================================");
            System.out.println(String.format("exec %s %d '%s'", testConfig.getNameOfProcedureToCall(),
                    configValue, testConfig.getArgumentString()));
            boolean caughtException = false;
            try {
                client.callProcedure(testConfig.getNameOfProcedureToCall(),
                                     configValue, testConfig.getArgumentString());
            }
            catch (ProcCallException pce) {
                if (! testConfig.expectsException()) {
                    throw pce;
                }
                System.out.println("\nCaught exception as expected:\n" + pce.getMessage());
                caughtException = true;
            }
            finally {
                // Wait for a little while so that the statistics will be updated correctly.
                Thread.sleep(100);
                // Note that pass 1 as the second parameter to get incremental statistics.
                VoltTable procedureDetail = client.callProcedure("@Statistics", "PROCEDUREDETAIL", 1).getResults()[0];
                System.out.println(procedureDetail.toFormattedString());
                verifyProcedureDetailResult(client, testConfig, procedureDetail);
            }
            // The test configuration says an exception is expected, but we did not get it.
            if (testConfig.expectsException() && ! caughtException) {
                fail(String.format("Expects an exception from exec %s %d '%s', but did not get it.",
                        testConfig.getNameOfProcedureToCall(),
                        configValue, testConfig.getArgumentString()));
            }
        }
    }

    private int getHostIdForMpI(Client client) throws NoConnectionsException, IOException, ProcCallException {
        VoltTable topo = client.callProcedure("@Statistics", "TOPO", 0).getResults()[0];
        while (topo.advanceRow()) {
            if (topo.getLong("Partition") == MpInitiator.MP_INIT_PID) {
                String leader = topo.getString("Leader");
                return Integer.parseInt(leader.substring(0, leader.indexOf(':')));
            }
        }
        fail("No leader for MP");
        return -1;
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
        project.addProcedure(ProcedureDetailTestSP.class, "ENG11890.a: 0");
        project.addProcedure(ProcedureDetailTestMP.class);

        // 2-node cluster, 2 sites per host, k = 0 running on the JNI backend
        LocalCluster config = new LocalCluster("proceduredetail-jni.jar", 2, 2, 0, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        assertTrue(config.compile(project));
        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
