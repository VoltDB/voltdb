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
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.Test;

public class TestAdminModeFromCommandLine extends RegressionSuite {

    public TestAdminModeFromCommandLine(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE T(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));");
        builder.addPartitionInfo("T", "A1");
        builder.addStmtProcedure("InsertA", "INSERT INTO T VALUES(?,?);",
                new ProcedurePartitionData("T", "A1"));
        builder.addStmtProcedure("CountA", "SELECT COUNT(*) FROM T");
        builder.addStmtProcedure("SelectA", "SELECT * FROM T");
        return builder;
    }

    void checkSystemInformationClusterState(VoltTable sysinfo, String state) {
        for (int i = 0; i < sysinfo.getRowCount(); i++) {
            sysinfo.advanceRow();
            if (sysinfo.get("KEY", VoltType.STRING).equals("CLUSTERSTATE")) {
                assertTrue(state.equalsIgnoreCase((String) sysinfo.get("VALUE",
                        VoltType.STRING)));
            }
        }
    }

    public void testPausedModeStartup() throws Exception {
        final Client client = getClient();
        final Client adminclient = getAdminClient();

        try {
            // Try to use the normal port and verify that the server reports
            // that it is unavailable (and that nothing happened via the admin port)
            boolean admin_start = false;
            try {
                client.callProcedure("InsertA", 0, 1000);
            } catch (ProcCallException e) {
                assertEquals("Server did not report itself as unavailable on production port",
                        ClientResponse.SERVER_UNAVAILABLE, e.getClientResponse().getStatus());
                admin_start = true;
            }
            assertTrue("Server did not report itself as unavailable on production port",
                    admin_start);
            VoltTable[] results = adminclient.callProcedure("CountA").getResults();
            assertEquals(0, results[0].asScalarLong());

            // verify that the admin port works for general use in admin mode
            // add several tuples
            for (int i = 0; i < 100; i++) {
                adminclient.callProcedure("InsertA", i, 1000 + i);
            }
            adminclient.drain();
            results = adminclient.callProcedure("CountA").getResults();
            assertEquals(100, results[0].asScalarLong());

            // Verify that @SystemInformation tells us the right thing
            results = adminclient.callProcedure("@SystemInformation").getResults();
            checkSystemInformationClusterState(results[0], "Paused");

            // exit admin mode and get busy from both ports
            adminclient.callProcedure("@Resume");
            results = client.callProcedure("CountA").getResults();
            assertEquals(100, results[0].asScalarLong());
            results = adminclient.callProcedure("CountA").getResults();
            assertEquals(100, results[0].asScalarLong());
            // Verify that @SystemInformation tells us the right thing
            results = adminclient.callProcedure("@SystemInformation").getResults();
            checkSystemInformationClusterState(results[0], "Running");

            // verify admin mode sysprocs not available on production port
            boolean admin_failed = false;
            try {
                client.callProcedure("@Pause");
            } catch (ProcCallException e) {
                admin_failed = true;
                assertTrue("Server returned an unexpected error",
                        e.getClientResponse().getStatusString().
                        contains("is not available to this client"));
            }
            assertTrue("Server allowed admin mode sysproc on production port",
                    admin_failed);

            admin_failed = false;
            try {
                client.callProcedure("@Resume");
            } catch (ProcCallException e) {
                admin_failed = true;
                assertTrue("Server returned an unexpected error",
                        e.getClientResponse().getStatusString().
                        contains("is not available to this client"));
            }
            assertTrue("Server allowed admin mode sysproc on production port",
                    admin_failed);

            // turn admin mode back on.
            adminclient.callProcedure("@Pause");

            // XXX-ADMIN add polling here although it shouldn't matter for
            // this synchronous, slow access.  We'll add another test for
            // clearing the backlog.
            // Try to use the normal port and verify that the server reports
            // that it is unavailable (and that nothing happened via the admin port)
            boolean admin_reentered = false;
            try {
                client.callProcedure("InsertA", 0, 1000);
            } catch (ProcCallException e) {
                assertEquals("Server did not report itself as unavailable on production port",
                        ClientResponse.SERVER_UNAVAILABLE, e.getClientResponse().getStatus());
                admin_reentered = true;
            }
            assertTrue("Server did not report itself as unavailable on production port",
                    admin_reentered);
            results = adminclient.callProcedure("CountA").getResults();
            assertEquals(100, results[0].asScalarLong());
            // Verify that @SystemInformation tells us the right thing
            results = adminclient.callProcedure("@SystemInformation").getResults();
            checkSystemInformationClusterState(results[0], "Paused");
        } finally {
            adminclient.close();
            client.close();
        }
    }

    @SuppressWarnings("deprecation")
    static public Test suite() throws IOException {
        // Set system property for 4sec CLIENT_HANGUP_TIMEOUT
        System.setProperty("CLIENT_HANGUP_TIMEOUT", "4000");

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAdminModeFromCommandLine.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        project.setUseDDLSchema(true);

        boolean success;
        LocalCluster config = new LocalCluster("admin-mode1.jar", 6, 2, 0, BackendTarget.NATIVE_EE_JNI);
        config.setToStartPaused();

        // Start in admin mode
        success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
