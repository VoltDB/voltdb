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

package org.voltdb.regressionsuites;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrashJVM;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrashVoltDBProc;

public class TestLocalClusterLogSearchAPI extends JUnit4LocalClusterTest {
    static final int SITES_PER_HOST = 2;
    static final int HOSTS = 3;
    static final int K = 1;
    VoltProjectBuilder builder;
    LocalCluster cluster;
    String listener;
    Client client;
    String voltDbRootPath;
    String voltDBRootParentPath;

    @Before
    public void setUp() throws Exception {
        String simpleSchema =
                "create table blah (" +
                "ival bigint default 0 not null, " +
                "PRIMARY KEY(ival));";

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addProcedures(CrashJVM.class);
        builder.addProcedures(CrashVoltDBProc.class);
        builder.setUseDDLSchema(true);

        cluster = new LocalCluster(true, "collect.jar",
                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        boolean success = cluster.compile(builder);
        assert (success);
        File voltDbRoot;
        cluster.startUp(true);
        //Get server specific root after startup.
        if (cluster.isNewCli()) {
            voltDbRoot = new File(cluster.getServerSpecificRoot("1"));
        } else {
            String voltDbFilePrefix = cluster.getSubRoots().get(0).getPath();
            voltDbRoot = new File(voltDbFilePrefix, builder.getPathToVoltRoot().getPath());
        }
        voltDbRootPath = voltDbRoot.getCanonicalPath();
        voltDBRootParentPath = voltDbRoot.getParentFile().getCanonicalPath();
        listener = cluster.getListenerAddresses().get(0);
        client = ClientFactory.createClient();
        client.createConnection(listener);
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        cluster.shutDown();
    }

    /*
     * Conventional test to check the logs when the cluster is correctly initialized and running
     */
    @Test
    public void testLogSearch() throws Exception {
        // Test the log search utility
        assertEquals(true, localClusterAllInitLogsContain("Initialized VoltDB root directory"));
        assertEquals(true, localClusterAllHostLogsContain(".*VoltDB [a-zA-Z]* Edition.*"));
        for (int i : getLocalClusterHostIds()) {
            assertEquals(true, localClusterInitlogContains(i, "Initialized VoltDB .* directory"));
            assertEquals(true, localClusterHostLogContains(i, "VoltDB [a-zA-Z]* Edition"));
        }
    }

    /*
     * Test the log search utility when a single host is shutdown
     */
    public void testHostShutDownLogSearch() throws Exception {
        // Shutdown a single host and restart
        cluster.killSingleHost(1);

        cluster.allHostLogsContain("Host 1 failed");

        cluster.setNewCli(false);  // This is needed to perform rejoin
        cluster.recoverOne(1, 1, "");

        // In community edition this should fail ? Since rejoin is only supported in enterprise
        // edition
        boolean rejoinMsg = localClusterHostLogContains(1, "VoltDB Community Edition only supports .*") // failure
                            || localClusterHostLogContains(1, "Initializing VoltDB .*");    // Success
        assertEquals(true, rejoinMsg);
    }

    /*
     * Test the log search utility when the whole LocalCluster is shutdown and restarted
     */
    public void testClustShutdownLogSearch() throws Exception {
        // Shutdown and startup the whole cluster
        cluster.shutDown();    // After shutdown the in-memory logs are cleared

        // Check the on-disk files instead
        // Note that host 1 is not successfully joined, therefore its log cannot be trusted
        for (int i : getLocalClusterHostIds()) {
            assertTrue(localClusterHostLogContains(i, "VoltDB has encountered an unrecoverable error and is exiting."));
        }
        cluster.startUp();

        // Test the log search utility
        assertEquals(true, localClusterAllInitLogsContain("Initialized VoltDB root directory"));
        assertEquals(true, localClusterAllHostLogsContain(".*VoltDB [a-zA-Z]* Edition.*"));
        for (int i : getLocalClusterHostIds()) {
            // No init log after restart (newCli == false)
            assertEquals(true, localClusterHostLogContains(i, "VoltDB [a-zA-Z]* Edition"));
        }
    }

    /*
     * Check for the existence of a regex expression in all the LocalCluster's
     * init logs.
     */
    private boolean localClusterAllInitLogsContain(String regex) {
        return cluster.allInitLogsContain(regex);
    }

    /*
     * Check for the existence of a regex expression in all the LocalCluster's
     * host logs.
     */
    private boolean localClusterAllHostLogsContain(String regex) {
        return cluster.allHostLogsContain(regex);
    }

    /*
     * Check the existence of a regex expression in a particular host's log
     */
    private boolean localClusterInitlogContains(int hostId, String regex) {
        return cluster.initLogContains(hostId, regex);
    }

    private boolean localClusterHostLogContains(int hostId, String regex) {
        return cluster.hostLogContains(hostId, regex);
    }

    /*
     * Get HostIds in the LocalCluster, make sure the server configuration
     * is an instance of LocalCluster
     */
    private Set<Integer> getLocalClusterHostIds() {
        return cluster.getHostIds();
    }

}
