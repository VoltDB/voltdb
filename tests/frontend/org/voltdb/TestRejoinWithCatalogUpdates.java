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

package org.voltdb;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestRejoinWithCatalogUpdates extends RejoinTestBase {

    private static final String TESTNONCE = "testnonce";

    private boolean didRestore() throws Exception {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        return zk.exists(VoltZK.perPartitionTxnIds, false) != null;
    }

    @Test
    public void testRestoreThenRejoinPropagatesRestore() throws Exception {
        System.out.println("=-=-=-= testRestoreThenRejoinThenRestore =-=-=-=");
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2/*sites per host*/, 2/*host count*/, 1/*k factor*/, BackendTarget.NATIVE_EE_JNI);
        //TODO: Do this in new cli when snapshot is updated.
        cluster.setMaxHeap(256);
        cluster.overrideAnyRequestForValgrind();
        ServerThread localServer = null;
        try {
            boolean success = cluster.compileWithAdminMode(builder, -1, false); // note, this admin port is ignored
            assertTrue(success);
            MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
            cluster.setHasLocalServer(false);
            cluster.setEnableVoltSnapshotPrefix(true);

            cluster.startUp();

            Client client;

            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost", cluster.port(0));

            String tmpDir = cluster.getServerSpecificScratchDir("0");
            client.callProcedure("@SnapshotSave", tmpDir,
                    TESTNONCE, (byte)1).getResults();

            client.callProcedure("@SnapshotRestore", tmpDir, TESTNONCE);

            cluster.killSingleHost(0);
            Thread.sleep(1000);

            VoltDB.Configuration config = new VoltDB.Configuration(cluster.portGenerator);
            config.m_startAction = StartAction.PROBE;
            config.m_hostCount = 2;
            // We point to root of killed server to start in process.
            config.m_voltdbRoot = new File(cluster.getServerSpecificRoot("0"));
            config.m_leader = ":" + cluster.internalPort(1);
            config.m_coordinators = cluster.coordinators(1);

            config.m_isRejoinTest = true;
            cluster.setPortsFromConfig(0, config);
            localServer = new ServerThread(config);

            localServer.start();
            localServer.waitForInitialization();

            Thread.sleep(2000);

            client.close();

            assertTrue(didRestore());

            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost", cluster.port(0));

            // Also make sure a catalog update doesn't reset m_haveDoneRestore
            File newCatalog = new File(Configuration.getPathToCatalogForTest("rejoin.jar"));
            File deployment = new File(Configuration.getPathToCatalogForTest("rejoin.xml"));

            VoltTable[] results =
                UpdateApplicationCatalog.update(client, newCatalog, deployment).getResults();
            assertTrue(results.length == 1);

            client.close();

            assertTrue(didRestore());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cluster.shutDown();
            if (localServer != null) {
                localServer.shutdown();
            }
        }
    }

    @Test
    public void testCatalogUpdateAfterRejoin() throws Exception {
        System.out.println("=-=-=-= testCatalogUpdateAfterRejoin =-=-=-=");
        VoltProjectBuilder builder = getBuilderForTest();

        LocalCluster cluster = new LocalCluster("rejoin.jar", 2, 2, 1,
                BackendTarget.NATIVE_EE_JNI);
        cluster.setMaxHeap(256);
        cluster.overrideAnyRequestForValgrind();
        cluster.setHasLocalServer(false);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

        try {
            cluster.startUp();

            for (int ii = 0; ii < 3; ii++) {
                cluster.killSingleHost(1);
                Thread.sleep(1000);
                cluster.recoverOne( 1, 0);

                File newCatalog = new File(Configuration.getPathToCatalogForTest("rejoin.jar"));
                File deployment = new File(Configuration.getPathToCatalogForTest("rejoin.xml"));

                Client client = ClientFactory.createClient();
                client.createConnection("localhost", cluster.port(0));

                VoltTable[] results =
                    UpdateApplicationCatalog.update(client, newCatalog, deployment).getResults();
                assertTrue(results.length == 1);
                client.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cluster.shutDown();
        }
    }
}
