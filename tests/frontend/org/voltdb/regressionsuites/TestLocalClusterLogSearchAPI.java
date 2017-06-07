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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    static final int HOSTS = 2;
    static final int K = 0;
    VoltProjectBuilder builder;
    LocalCluster cluster;
    String listener;
    Client client;

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
        setupLogSearch();
        // Test the log search utility
        assertEquals(true, cluster.allInitLogsContain("Initialized VoltDB root directory"));
        assertEquals(true, cluster.allHostLogsContain(".*VoltDB [a-zA-Z]* Edition.*"));
        for (int i : cluster.getHostIds()) {
            assertEquals(true, cluster.initLogContains(i, "Initialized VoltDB .* directory"));
            assertEquals(true, cluster.hostLogContains(i, "VoltDB [a-zA-Z]* Edition"));
        }
    }

    /*
     * Test the log search utility when a single host is shutdown
     */
    @Test
    public void testHostShutDownLogSearch() throws Exception {
        setupLogSearch();
        // Shutdown a single host and restart
        cluster.killSingleHost(1);

        cluster.allHostLogsContain("Host 1 failed");

        cluster.setNewCli(false);  // This is needed to perform rejoin
        cluster.recoverOne(1, 1, "");

        // In community edition this should fail ? Since rejoin is only supported in enterprise
        // edition
        boolean rejoinMsg = cluster.hostLogContains(1, "VoltDB Community Edition only supports .*") // failure
                            || cluster.hostLogContains(1, "Initializing VoltDB .*");    // Success
        assertEquals(true, rejoinMsg);
    }

    /*
     * Test the log search utility when the whole LocalCluster is shutdown and restarted
     */
    @Test
    public void testClusterShutdownLogSearch() throws Exception {
        setupLogSearch();
        // Shutdown and startup the whole cluster
        cluster.shutDown();    // After shutdown the in-memory logs are cleared
        cluster.startUp();

        // Test the log search utility
        assertEquals(true, cluster.allInitLogsContain("Initialized VoltDB root directory"));
        assertEquals(true, cluster.allHostLogsContain(".*VoltDB [a-zA-Z]* Edition.*"));
        for (int i : cluster.getHostIds()) {
            // No init log after restart (newCli == false)
            assertEquals(true, cluster.hostLogContains(i, "VoltDB [a-zA-Z]* Edition"));
        }
    }

    /*
     * Conventional test to check the logs when the cluster is correctly initialized and running
     */
    @Test
    public void testPreCompiledLogSearch() throws Exception {
        setupPreCompiledLogSearch();
        cluster.verifyRegexesExistInAllHosts(Arrays.asList(new String[] {"Initialized VoltDB root directory"}));
        cluster.verifyRegexesExistInAllHosts(Arrays.asList(new String[] {".*VoltDB [a-zA-Z]* Edition.*"}));

        for (int i = 0; i < HOSTS; i++) {
            cluster.verifyRegexesExist(Arrays.asList(new Integer[] {i}),
                    Arrays.asList(new String[] {"Initialized VoltDB root directory"}));
            cluster.verifyRegexesExist(Arrays.asList(new Integer[] {i}),
                    Arrays.asList(new String[] {".*VoltDB [a-zA-Z]* Edition.*"}));
        }
        cluster.verifyRegexesNotExistInAnyHosts(Arrays.asList(new String[] {".*FATAL.*"}));
    }

    /*
     * Test the log search utility when a single host is shutdown
     */
    @Test
    public void testPreCompiledHostShutDownLogSearch() throws Exception {
        setupPreCompiledLogSearch();
        // Shutdown a single host and restart
        cluster.killSingleHost(1);

        cluster.verifyRegexesExistInAllHosts(Arrays.asList(new String[] {"Host 1 failed"}));

        cluster.resetAllPreCompRegexResults();

        cluster.setNewCli(false);  // This is needed to perform rejoin
        cluster.recoverOne(1, 1, "");

        // In community edition this should fail ? Since rejoin is only supported in enterprise
        // edition
        cluster.verifyRegexesExistInAllHosts(Arrays
                .asList(new String[] {"VoltDB Community Edition only supports .*"}));
    }

    /*
     * Test the log search utility when the whole LocalCluster is shutdown and restarted
     */
    @Test
    public void testPreCompiledClusterShutdownLogSearch() throws Exception {
        setupPreCompiledLogSearch();
        // Shutdown and startup the whole cluster
        cluster.shutDown();
        cluster.startUp();

        cluster.verifyRegexesExistInAllHosts(Arrays.asList(new String[] {".*VoltDB [a-zA-Z]* Edition.*"}));
        for (int i = 0; i < HOSTS; i++) {
            cluster.verifyRegexesExist(Arrays.asList(new Integer[] {i}),
                    Arrays.asList(new String[] {".*VoltDB [a-zA-Z]* Edition.*"}));
        }
    }

    private void setupLogSearch() throws Exception {
        cluster = new LocalCluster(false, true, "collect.jar",
                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        boolean success = cluster.compile(builder);
        assert (success);
        cluster.startUp(true);
        listener = cluster.getListenerAddresses().get(0);
        client = ClientFactory.createClient();
        client.createConnection(listener);
    }

    private void setupPreCompiledLogSearch() throws Exception {
        List<String> patterns = new ArrayList<>();
        patterns.add("Initialized VoltDB root directory");  // pattern #0
        patterns.add(".*VoltDB [a-zA-Z]* Edition.*");   // pattern #1
        patterns.add("Host 1 failed");  // pattern #2
        patterns.add("VoltDB Community Edition only supports .*");  // pattern #3

        cluster = new LocalCluster("collect.jar", false, patterns,
                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        boolean success = cluster.compile(builder);
        assert (success);
        cluster.startUp(true);
        listener = cluster.getListenerAddresses().get(0);
        client = ClientFactory.createClient();
        client.createConnection(listener);
    }
}
