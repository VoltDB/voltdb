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
import org.voltdb.ProcedurePartitionData;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;

public class TestQueryTimeout extends RegressionSuite {

    // These are dimensionless multipliers.
    private static final double TIMEOUT_INCREASE = 50.0;
    private static final double TIMEOUT_DECREASE = 1/500.0;

    // These are all in milliseconds.
    private static final int TIMEOUT_NORMAL = 1000;
    private static final int TIMEOUT_LONG   = (int)(1000 * TIMEOUT_INCREASE);
    private static final int TIMEOUT_SHORT  = (int)(1000 * TIMEOUT_DECREASE);

    // DEBUG build of EE runs much slower, so the timing part is not deterministic.
    private static String ERRORMSG = "A SQL query was terminated after";

    private static final String INITIAL_STATUS = "";
    private String m_errorStatusString;
    ProcedureCallback m_callback = new ProcedureCallback() {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            m_errorStatusString = INITIAL_STATUS;
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                m_errorStatusString = clientResponse.getStatusString();
            }
        }
    };

    private void checkCallbackTimeoutError(String errorMsg) {
        assertTrue(m_errorStatusString + " did not contain " + ERRORMSG,
                   m_errorStatusString.contains(errorMsg));
    }

    private void checkCallbackSuccess() {
        assertEquals(INITIAL_STATUS, m_errorStatusString);
    }

    private void loadData(Client client, String tb, int scale)
            throws NoConnectionsException, IOException, ProcCallException {
        for (int i = 0; i < scale; i++) {
            client.callProcedure(m_callback, tb + ".insert", i, "MA", i % 6);
        }
        System.out.println("Finish loading " + scale + " rows for table " + tb);
    }

    private void truncateData(Client client, String tb)
            throws NoConnectionsException, IOException, ProcCallException {
        client.callProcedure("@AdHoc", "Truncate table " + tb);
        validateTableOfScalarLongs(client, "Select count(*) from " + tb, new long[]{0});
    }

    private void loadTables(Client client, int scaleP, int scaleR)
            throws IOException, ProcCallException, InterruptedException {
        loadData(client, "P1", scaleP);
        loadData(client, "R1", scaleR);
        client.drain();
    }

    private void truncateTables(Client client)
            throws IOException, ProcCallException, InterruptedException {
        truncateData(client, "P1");
        truncateData(client, "R1");
    }

    public void testReplicatedProcTimeout() throws IOException, ProcCallException, InterruptedException {
        // If the authentication information is
        // incorrect, shutdown will not succeed.
        // So, we need to set this in any case.
        // Feel free to reset it later on.
        if (isValgrind() || isDebug()) {
            // Disable the memcheck/debug for this test, it takes too long
            return;
        }
        System.out.println("test replicated table procedures timeout...");

        m_username = "adminUser";
        m_password = "password";
        Client client = this.getClient();
        loadTables(client, 0, 5000);

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT_NORMAL));

        //
        // Replicated table procedure tests
        //
        try {
            client.callProcedure("ReplicatedReadOnlyProc");
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                       ex.getMessage().contains(ERRORMSG));
        }

        // It's a write procedure and it's timed out safely because the MPI has not issue
        // any write query before it's timed out
        try {
            client.callProcedure("ReplicatedReadWriteProc");
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                       ex.getMessage().contains(ERRORMSG));
        }

        // It's a write procedure and should not be timed out.
        try {
            client.callProcedure("ReplicatedWriteReadProc");
        }
        catch (Exception ex) {
            fail("Write procedure should not be timed out");
        }
    }

    public void testPartitionedProcTimeout() throws IOException, ProcCallException, InterruptedException {
        // If the authentication information is
        // incorrect, shutdown will not succeed.
        // So, we need to set this in any case.
        // Feel free to reset it later on.
        if (isValgrind() || isDebug()) {
            // Disable the memcheck/debug for this test, it takes too long
            return;
        }
        System.out.println("test partitioned table procedures timeout...");

        m_username = "adminUser";
        m_password = "password";
        Client client = this.getClient();
        loadTables(client, 10000, 3000);

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT_NORMAL));

        //
        // Partition table procedure tests
        //
        try {
            client.callProcedure("PartitionReadOnlyProc");
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                       ex.getMessage().contains(ERRORMSG));
        }

        // Read on partition table should not have MPI optimizations
        // so the MPI should mark it write and not time out the procedure
        try {
            client.callProcedure("PartitionReadWriteProc");
        }
        catch (Exception ex) {
            fail("Write procedure should not be timed out");
        }

        // It's a write procedure and should not be timed out.
        try {
            client.callProcedure("PartitionWriteReadProc");
        }
        catch (Exception ex) {
            fail("Write procedure should not be timed out");
        }
    }

    final String PROMPTMSG = " is supposed to time out, but succeeded eventually!";

    private void checkTimeoutIncreasedProcFailed(boolean sync, Client client, String procName, Object...params)
            throws IOException, ProcCallException, InterruptedException {
        try {
            client.callProcedure(procName, params);
            fail(procName + PROMPTMSG);
        }
        catch (ProcCallException ex) {
            assertTrue("Unexpected exception raised in procedure \""
                       + procName + "\": "
                       + ex.getMessage(),
                       ex.getMessage().contains(ERRORMSG));
        }

        // increase the individual timeout value in order to succeed running this long procedure
        // However, for non-admin user, timeout value can not override system timeout value.
        if (sync) {
            try {
                client.callProcedureWithTimeout(TIMEOUT_LONG, procName, params);
                fail(procName + PROMPTMSG);
            }
            catch (ProcCallException ex) {
                assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                           ex.getMessage().contains(ERRORMSG));
            }
        }
        else {
            client.callProcedureWithTimeout(m_callback, TIMEOUT_LONG, procName, params);
            client.drain();
            checkCallbackTimeoutError(ERRORMSG);
        }

         // run the same procedure again to verify the global timeout value still applies
        try {
            client.callProcedure(procName, params);
            fail(procName + PROMPTMSG);
        }
        catch (ProcCallException ex) {
            assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                       ex.getMessage().contains(ERRORMSG));
        }

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT_NORMAL));
    }

    private void checkIndividualProcTimeout(Client client)
            throws IOException, ProcCallException, InterruptedException {
        // negative tests on the timeout value
        subtestNegativeIndividualProcTimeout(client);

        final String longRunningCrossJoinAggReplicated =
                "SELECT t1.contestant_number, t2.state, COUNT(*) "
                        + "FROM R1 t1, R1 t2 "
                        + "GROUP BY t1.contestant_number, t2.state;";

        final String longRunningCrossJoinAggPartitioned =
                "SELECT t1.contestant_number, t2.state, COUNT(*) "
                        + "FROM P1 t1, R1 t2 "
                        + "GROUP BY t1.contestant_number, t2.state;";


        boolean syncs[] = {true, false};
        truncateTables(client);
        // load more data
        loadTables(client, 10000, 3000);

        for (boolean sync : syncs) {
            System.out.println("Testing " + (sync ? "synchronously": "asynchronously") + "  call");
            // truncate the data
            checkTimeoutIncreasedProcFailed(sync, client, "SPPartitionReadOnlyProc", 1);
            checkTimeoutIncreasedProcFailed(sync, client, "PartitionReadOnlyProc");
            checkTimeoutIncreasedProcFailed(sync, client, "ReplicatedReadOnlyProc");
            checkTimeoutIncreasedProcFailed(sync, client, "@AdHoc", longRunningCrossJoinAggReplicated);
            checkTimeoutIncreasedProcFailed(sync, client, "@AdHoc", longRunningCrossJoinAggPartitioned);
            checkTimeoutIncreasedProcFailed(sync, client, "AdHocPartitionReadOnlyProc");
            // first replicated read will be treated as READ ONLY
            checkTimeoutIncreasedProcFailed(sync, client, "ReplicatedReadWriteProc");

            // write procedure no timing out
            checkNoTimingOutWriteProcedure(sync, client, "ReplicatedWriteReadProc");
            checkNoTimingOutWriteProcedure(sync, client, "PartitionReadWriteProc");
            checkNoTimingOutWriteProcedure(sync, client, "PartitionWriteReadProc");
        }
    }

    public void testIndividualProcTimeout() throws IOException, ProcCallException, InterruptedException {
        // If the authentication information is
        // incorrect, shutdown will not succeed.
        // So, we need to set this in any case.
        // Feel free to reset it later on.
        if (isValgrind() || isDebug()) {
            // Disable the memcheck/debug for this test, it takes too long
            return;
        }
        m_username = "userWithAllProc";
        m_password = "password";
        Client client = getClient();
        checkIndividualProcTimeout(client);
    }

    private void subtestNegativeIndividualProcTimeout(Client client) throws IOException, ProcCallException, InterruptedException {
        // negative timeout value
        String errorMsg = "Timeout value can't be negative";
        try {
            client.callProcedureWithTimeout(TIMEOUT_NORMAL * -1, "PartitionReadOnlyProc");
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + errorMsg,
                       ex.getMessage().contains(errorMsg));
        }

        try {
            client.callProcedureWithTimeout(m_callback, TIMEOUT_NORMAL * -1, "PartitionReadOnlyProc");
            client.drain();
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + errorMsg,
                       ex.getMessage().contains(errorMsg));
        }

        // Integer overflow
        try {
            client.callProcedureWithTimeout(TIMEOUT_NORMAL * Integer.MAX_VALUE, "PartitionReadOnlyProc");
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + errorMsg,
                       ex.getMessage().contains(errorMsg));
        }

        // underflow, asynchronously should be on the same path...
    }

    private void checkNoTimingOutWriteProcedure(boolean sync, Client client, String procName, Object...params)
            throws IOException, ProcCallException, InterruptedException {

        if (sync) {
            try {
                client.callProcedureWithTimeout(TIMEOUT_SHORT, procName, params);
            }
            catch (Exception ex) {
                System.err.println(ex.getMessage());
                fail(procName + " is supposed to succeed!");
            }
        }
        else {
            client.callProcedureWithTimeout(m_callback, TIMEOUT_SHORT, procName, params);
            client.drain();
            checkCallbackSuccess();
        }
    }

    //
    // Suite builder boilerplate
    //

    public TestQueryTimeout(String name) {
        super(name);
    }
    static final Class<?>[] MP_PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.querytimeout.ReplicatedReadOnlyProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.ReplicatedReadWriteProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.ReplicatedWriteReadProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.PartitionReadOnlyProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.PartitionReadWriteProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.PartitionWriteReadProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.AdHocPartitionReadOnlyProc.class
    };

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestQueryTimeout.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                "phone_number INTEGER NOT NULL, " +
                "state VARCHAR(2) NOT NULL, " +
                "contestant_number INTEGER NOT NULL);" +

                "CREATE TABLE P1 ( " +
                "phone_number INTEGER NOT NULL, " +
                "state VARCHAR(2) NOT NULL, " +
                "contestant_number INTEGER NOT NULL);" +

                "PARTITION TABLE P1 ON COLUMN phone_number;" +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        }
        catch (IOException e) {
            fail();
        }
        project.addMultiPartitionProcedures(MP_PROCEDURES);
        project.addProcedure(org.voltdb_testprocs.regressionsuites.querytimeout.SPPartitionReadOnlyProc.class,
                new ProcedurePartitionData("P1", "PHONE_NUMBER", "0"));

        project.setQueryTimeout(TIMEOUT_NORMAL);

        UserInfo users[] = new UserInfo[] {
                new UserInfo("adminUser", "password", new String[] {"AdMINISTRATOR"}),
                new UserInfo("userWithAllProc", "password", new String[] {"GroupWithAllProcPerm"})
        };
        project.addUsers(users);

        RoleInfo groups[] = new RoleInfo[] {
                new RoleInfo("GroupWithAllProcPerm", true, true, false, true, true, true)
        };
        project.addRoles(groups);
        // suite defines its own ADMINISTRATOR user
        project.setSecurityEnabled(true, false);
        config = new LocalCluster("querytimeout-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
    public void tearDown() throws Exception {
        if (m_fatalFailure) {
            System.exit(0);
        } else if (m_completeShutdown) {
            m_config.shutDown();
        }
        for (final Client c : m_clients) {
            c.close();
        }
        m_clients.clear();
    }

}
