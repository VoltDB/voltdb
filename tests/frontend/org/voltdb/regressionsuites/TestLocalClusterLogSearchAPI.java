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

import static org.junit.Assert.assertTrue;

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
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrashJVM;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrashVoltDBProc;

public class TestLocalClusterLogSearchAPI extends JUnit4LocalClusterTest {
    static final int SITES_PER_HOST = 2;
    static final int HOSTS = 4;
    static final int K = 1;
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

        // Add the patterns to be searched for in advance
        List<String> patterns = new ArrayList<>();
        patterns.add("Initialized VoltDB root directory");
        patterns.add(".*VoltDB [a-zA-Z]* Edition.*");
        patterns.add("Cluster has become unviable");
        patterns.add("VoltDB Community Edition only supports .*");
        patterns.add("Host 1 failed");
        patterns.add(".*FATAL.*");
        patterns.add("Cluster has become unviable");
        patterns.add("Some partitions have no replicas");

        cluster = new LocalCluster("collect.jar", patterns,
                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        boolean success = cluster.compile(builder);
        assert (success);
        cluster.startUp(true);
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
    public void testPreCompiledLogSearch() throws Exception {
        assertTrue(cluster.regexInAllHosts("Initialized VoltDB root directory"));
        assertTrue(cluster.regexInAllHosts(".*VoltDB [a-zA-Z]* Edition.*"));
        assertTrue(cluster.verifyRegexesExistInAllHosts(Arrays.asList(new String[] {"Initialized VoltDB root directory",
                                                                                    ".*VoltDB [a-zA-Z]* Edition.*"})));

        for (int i = 0; i < HOSTS; i++) {
            assertTrue(cluster.regexInHost(i, "Initialized VoltDB root directory"));
            assertTrue(cluster.regexInHost(i, ".*VoltDB [a-zA-Z]* Edition.*"));
        }
        assertTrue(cluster.regexNotInAnyHosts(".*FATAL.*"));
    }

    /*
     * Test the log search utility when a single host is shutdown
     */
    @Test
    public void testPreCompiledHostShutDownLogSearch() throws Exception {
        // Shutdown a single host and restart
        cluster.killSingleHost(1);

        // For community edition: since K-safety is violated, the cluster
        // should shutdown by itself
        // For pro edition: the host fail message should show on other hosts
        for (int i = 0; i < HOSTS; i++) {
            if (i != 1) {
                // In community edition the feature is not enabled, in pro version
                // the cluster should still be running
                boolean r = (cluster.regexInHost(i, "Cluster has become unviable") &&
                             cluster.regexInHost(i, "Some partitions have no replicas")) ||
                             cluster.regexInHost(i, "Host 1 failed");
                assertTrue(r);
            }
        }

        // For pro edition, try rejoin
        if (MiscUtils.isPro()) {
            cluster.setNewCli(false);  // This is needed to perform rejoin
            cluster.recoverOne(1, 1, "");
        }
    }

    /*
     * Test the log search utility when the whole LocalCluster is shutdown and restarted
     */
    @Test
    public void testPreCompiledClusterShutdownLogSearch() throws Exception {
        // Shutdown and startup the whole cluster
        cluster.shutDown();
        cluster.startUp();

        assertTrue(cluster.regexInAllHosts(".*VoltDB [a-zA-Z]* Edition.*"));
        for (int i = 0; i < HOSTS; i++) {
            assertTrue(cluster.regexInHost(i, ".*VoltDB [a-zA-Z]* Edition.*"));
        }
    }
}
