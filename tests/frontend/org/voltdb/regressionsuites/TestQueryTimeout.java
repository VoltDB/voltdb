/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;

public class TestQueryTimeout extends RegressionSuite {

    private static final int TIMEOUT = 1000;

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
        if (isValgrind() || isDebug()) {
            // Disable the memcheck/debug for this test, it takes too long
            return;
        }
        System.out.println("test replicated table procedures timeout...");

        m_username = "userWithAllProc";
        m_password = "password";
        Client client = this.getClient();
        loadTables(client, 0, 5000);

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

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
        if (isValgrind() || isDebug()) {
            // Disable the memcheck/debug for this test, it takes too long
            return;
        }
        System.out.println("test partitioned table procedures timeout...");

        m_username = "userWithAllProc";
        m_password = "password";
        Client client = this.getClient();
        loadTables(client, 10000, 3000);

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

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

    final String PROMPTMSG = " is supposed to timed out, but succeed eventually!";

    private void checkTimeoutIncreasedProcSucceed(boolean sync, Client client, String procName, Object...params)
            throws IOException, ProcCallException, InterruptedException {
        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

        try {
            client.callProcedure(procName, params);
            fail(procName + PROMPTMSG);
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                       ex.getMessage().contains(ERRORMSG));
        }

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

        // increase the individual timeout value in order to succeed running this long procedure
        if (sync) {
            try {
                client.callProcedureWithTimeout(TIMEOUT*50, procName, params);
            }
            catch (Exception ex) {
                System.err.println(ex.getMessage());
                fail(procName + " is supposed to succeed!");
            }
        }
        else {
            client.callProcedureWithTimeout(m_callback, TIMEOUT*50, procName, params);
            client.drain();
            checkCallbackSuccess();
        }

        // check the global timeout value again
        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

        // run the same procedure again to verify the global timeout value still applies
        try {
            client.callProcedure(procName, params);
            fail(procName + PROMPTMSG);
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                       ex.getMessage().contains(ERRORMSG));
        }

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));
    }

    private void checkTimeoutIncreasedProcFailed(boolean sync, Client client, String procName, Object...params)
            throws IOException, ProcCallException, InterruptedException {
        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

        try {
            client.callProcedure(procName, params);
            fail(procName + PROMPTMSG);
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                       ex.getMessage().contains(ERRORMSG));
        }

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

        // increase the individual timeout value in order to succeed running this long procedure
        // However, for non-admin user, timeout value can not override system timeout value.
        if (sync) {
            try {
                client.callProcedureWithTimeout(TIMEOUT*50, procName, params);
                fail(procName + PROMPTMSG);
            }
            catch (Exception ex) {
                assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                           ex.getMessage().contains(ERRORMSG));
            }
        }
        else {
            client.callProcedureWithTimeout(m_callback, TIMEOUT*50, procName, params);
            client.drain();
            checkCallbackTimeoutError(ERRORMSG);
        }

        // check the global timeout value again
        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

        // run the same procedure again to verify the global timeout value still applies
        try {
            client.callProcedure(procName, params);
            fail(procName + PROMPTMSG);
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                       ex.getMessage().contains(ERRORMSG));
        }

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));
    }

    private void checkTimeoutDecreaseProcFailed(boolean sync, Client client, String procName, Object...params)
            throws IOException, ProcCallException, InterruptedException {
        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

        try {
            client.callProcedure(procName, params);
        }
        catch (Exception ex) {
            fail(procName + " is supposed to succeed, but failed with message +\n" + ex.getMessage());
        }

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

        // decrease the individual timeout value in order to timeout the running this long procedure
        if (sync) {
            try {
                client.callProcedureWithTimeout(TIMEOUT / 500, procName, params);
                fail(procName + PROMPTMSG);
            }
            catch (Exception ex) {
                assertTrue(ex.toString() + " did not contain " + ERRORMSG,
                           ex.getMessage().contains(ERRORMSG));
            }
        }
        else {
            client.callProcedureWithTimeout(m_callback, TIMEOUT / 500, procName, params);
            client.drain();
            checkCallbackTimeoutError(ERRORMSG);;
        }

        // check the global timeout value again
        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));

        // run the same procedure again to verify the global timeout value still applies
        try {
            client.callProcedure(procName, params);
        }
        catch (Exception ex) {
            fail(procName + " is supposed to succeed!");
        }

        checkDeploymentPropertyValue(client, "querytimeout", Integer.toString(TIMEOUT));
    }

    private void checkIndividualProcTimeout(Client client, boolean isAdmin)
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
        for (boolean sync : syncs) {
            System.out.println("Testing " + (sync ? "synchronously": "asynchronously") + "  call");
            // truncate the data
            truncateTables(client);
            // load more data
            loadTables(client, 10000, 3000);

            if (isAdmin) {
                checkTimeoutIncreasedProcSucceed(sync, client, "SPPartitionReadOnlyProc", 1);
                checkTimeoutIncreasedProcSucceed(sync, client, "PartitionReadOnlyProc");
                checkTimeoutIncreasedProcSucceed(sync, client, "ReplicatedReadOnlyProc");
                checkTimeoutIncreasedProcSucceed(sync, client, "@AdHoc", longRunningCrossJoinAggReplicated);
                checkTimeoutIncreasedProcSucceed(sync, client, "@AdHoc", longRunningCrossJoinAggPartitioned);
                checkTimeoutIncreasedProcSucceed(sync, client, "AdHocPartitionReadOnlyProc");
                // first replicated read will be treated as READ ONLY
                checkTimeoutIncreasedProcSucceed(sync, client, "ReplicatedReadWriteProc");
            }
            else {
                checkTimeoutIncreasedProcFailed(sync, client, "SPPartitionReadOnlyProc", 1);
                checkTimeoutIncreasedProcFailed(sync, client, "PartitionReadOnlyProc");
                checkTimeoutIncreasedProcFailed(sync, client, "ReplicatedReadOnlyProc");
                checkTimeoutIncreasedProcFailed(sync, client, "@AdHoc", longRunningCrossJoinAggReplicated);
                checkTimeoutIncreasedProcFailed(sync, client, "@AdHoc", longRunningCrossJoinAggPartitioned);
                checkTimeoutIncreasedProcFailed(sync, client, "AdHocPartitionReadOnlyProc");
                // first replicated read will be treated as READ ONLY
                checkTimeoutIncreasedProcFailed(sync, client, "ReplicatedReadWriteProc");
            }

            // write procedure no timing out
            checkNoTimingOutWriteProcedure(sync, client, "ReplicatedWriteReadProc");
            checkNoTimingOutWriteProcedure(sync, client, "PartitionReadWriteProc");
            checkNoTimingOutWriteProcedure(sync, client, "PartitionWriteReadProc");


            // truncate the data
            truncateTables(client);
            loadTables(client, 1000, 300);

            checkTimeoutDecreaseProcFailed(sync, client, "SPPartitionReadOnlyProc", 1);
            checkTimeoutDecreaseProcFailed(sync, client, "PartitionReadOnlyProc");
            checkTimeoutDecreaseProcFailed(sync, client, "ReplicatedReadOnlyProc");
            checkTimeoutDecreaseProcFailed(sync, client, "@AdHoc", longRunningCrossJoinAggReplicated);
            checkTimeoutDecreaseProcFailed(sync, client, "@AdHoc", longRunningCrossJoinAggPartitioned);
            checkTimeoutDecreaseProcFailed(sync, client, "AdHocPartitionReadOnlyProc");
            // first replicated read will be treated as READ ONLY
            checkTimeoutDecreaseProcFailed(sync, client, "ReplicatedReadWriteProc");
            // write procedure no timing out
            checkNoTimingOutWriteProcedure(sync, client, "ReplicatedWriteReadProc");
            checkNoTimingOutWriteProcedure(sync, client, "PartitionReadWriteProc");
            checkNoTimingOutWriteProcedure(sync, client, "PartitionWriteReadProc");

            // truncate the data
            truncateTables(client);
        }

    }

    public void testIndividualProcTimeout() throws IOException, ProcCallException, InterruptedException {
        if (isValgrind() || isDebug()) {
            // Disable the memcheck/debug for this test, it takes too long
            return;
        }
        Client client;

        m_username = "adminUser";
        m_password = "password";
        client = getClient();
        checkIndividualProcTimeout(client, true);

        m_username = "userWithAllProc";
        m_password = "password";
        client = getClient();
        checkIndividualProcTimeout(client, false);
    }

    private void subtestNegativeIndividualProcTimeout(Client client) throws IOException, ProcCallException, InterruptedException {
        // negative timeout value
        String errorMsg = "Timeout value can't be negative";
        try {
            client.callProcedureWithTimeout(TIMEOUT * -1, "PartitionReadOnlyProc");
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + errorMsg,
                       ex.getMessage().contains(errorMsg));
        }

        try {
            client.callProcedureWithTimeout(m_callback, TIMEOUT * -1, "PartitionReadOnlyProc");
            client.drain();
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.toString() + " did not contain " + errorMsg,
                       ex.getMessage().contains(errorMsg));
        }

        // Integer overflow
        try {
            client.callProcedureWithTimeout(TIMEOUT * Integer.MAX_VALUE, "PartitionReadOnlyProc");
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
                client.callProcedureWithTimeout(TIMEOUT / 500, procName, params);
            }
            catch (Exception ex) {
                System.err.println(ex.getMessage());
                fail(procName + " is supposed to succeed!");
            }
        }
        else {
            client.callProcedureWithTimeout(m_callback, TIMEOUT / 500, procName, params);
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
    static final Class<?>[] PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.querytimeout.ReplicatedReadOnlyProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.ReplicatedReadWriteProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.ReplicatedWriteReadProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.PartitionReadOnlyProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.PartitionReadWriteProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.PartitionWriteReadProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.SPPartitionReadOnlyProc.class,
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
        project.addProcedures(PROCEDURES);
        project.setQueryTimeout(TIMEOUT);

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

        boolean success;

        config = new LocalCluster("querytimeout-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
/* disabled until we work the kinks out of ipc support for fragment progress updates
        config = new LocalCluster("querytimeout-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
*/
        // Cluster
        config = new LocalCluster("querytimeout-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
