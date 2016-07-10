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
package org.voltdb;

import java.io.IOException;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;
import static org.junit.Assert.assertTrue;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public class TestMixedPauseModeCluster {

    static final int K = MiscUtils.isPro() ? 1 : 0;

    static final String JAR_NAME = "mixed.jar";
    static final VoltProjectBuilder m_builder = new VoltProjectBuilder();

    private class MixedPauseCluster {

        LocalCluster m_cluster = null;

        MixedPauseCluster(String[] modes) {
            assert (modes != null);

            m_cluster = new LocalCluster(
                    JAR_NAME,
                    2,
                    modes.length,
                    K,
                    BackendTarget.NATIVE_EE_JNI);
            m_cluster.setOverridesForModes(modes);
            m_cluster.setHasLocalServer(false);
            m_cluster.setDeploymentAndVoltDBRoot(
                    m_builder.getPathToDeployment(),
                    m_builder.getPathToVoltRoot().getAbsolutePath());
        }

        boolean start() {
            m_cluster.startUp();

            return true;
        }

        boolean killAndRejoin(String mode) {
            try {
                m_cluster.killSingleHost(2);
                // just set the override for the last host
                m_cluster.setOverridesForModes(new String[]{"", "", mode});
                return m_cluster.recoverOne(2, 0, "");
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        boolean killAndRejoin() {
            try {
                m_cluster.killSingleHost(2);
                return m_cluster.recoverOne(2, 0, "");
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        void shutdown() throws InterruptedException {
            if (m_cluster != null) {
                m_cluster.shutDown();
            }
        }
    }

    @BeforeClass
    public static void compileCatalog() throws IOException {
        m_builder.addLiteralSchema("CREATE TABLE V0 (id BIGINT);");
        m_builder.configureLogging(null, null, false, false, 200, Integer.MAX_VALUE, null);
        assertTrue(m_builder.compile(Configuration.getPathToCatalogForTest(JAR_NAME), 2, 3, K));
    }

    void checkSystemInformationClusterState(final Client client) throws IOException, NoConnectionsException, ProcCallException {
        VoltTable sysinfo = client.callProcedure("@SystemInformation").getResults()[0];
        for (int i = 0; i < sysinfo.getRowCount(); i++) {
            sysinfo.advanceRow();
            if (sysinfo.get("KEY", VoltType.STRING).equals("CLUSTERSTATE")) {
                assertTrue("Paused".equalsIgnoreCase((String) sysinfo.get("VALUE",
                        VoltType.STRING)));
            }
        }
    }

    void checkClusterDoesNotAllowWrite(Client client) throws IOException, NoConnectionsException, ProcCallException {
        boolean admin_start = false;
        try {
            client.callProcedure("@AdHoc", "insert into V0 values(0);");
        } catch (ProcCallException e) {
            assertEquals("Server did not report itself as unavailable.", ClientResponse.SERVER_UNAVAILABLE, e.getClientResponse().getStatus());
            admin_start = true;
        }
        assertTrue("Server did not report itself as unavailable.", admin_start);
    }

    @Test
    public void testStartupConfigurations() throws InterruptedException {
        try {
            MixedPauseCluster cluster = null;

            // should work
            cluster = new MixedPauseCluster(new String[]{"", "paused", ""});

            assertTrue(cluster.start());
            Client client = ClientFactory.createClient();
            client.createConnection(cluster.m_cluster.getListenerAddress(0));
            checkSystemInformationClusterState(client);
            checkClusterDoesNotAllowWrite(client);
            client.close();
            cluster.shutdown();

            // should work
            cluster = new MixedPauseCluster(
                    new String[]{"paused", "", ""});

            assertTrue(cluster.start());
            client = ClientFactory.createClient();
            client.createConnection(cluster.m_cluster.getListenerAddress(0));
            checkSystemInformationClusterState(client);
            checkClusterDoesNotAllowWrite(client);
            client.close();
            cluster.shutdown();

            // should work
            cluster = new MixedPauseCluster(
                    new String[]{"", "", "paused"});

            assertTrue(cluster.start());
            client = ClientFactory.createClient();
            client.createConnection(cluster.m_cluster.getListenerAddress(0));
            checkSystemInformationClusterState(client);
            checkClusterDoesNotAllowWrite(client);
            client.close();
            cluster.shutdown();

            // should work
            cluster = new MixedPauseCluster(
                    new String[]{"paused", "paused", "paused"});

            assertTrue(cluster.start());
            client = ClientFactory.createClient();
            client.createConnection(cluster.m_cluster.getListenerAddress(0));
            checkSystemInformationClusterState(client);
            checkClusterDoesNotAllowWrite(client);
            client.close();
            cluster.shutdown();
        } catch (Exception ex) {
            fail("Failed with: " + ex);
        }

    }

    @Test
    public void testJoins() throws InterruptedException {
        if (!MiscUtils.isPro()) {
            return;
        } // join tests are pro only
        try {
            MixedPauseCluster cluster = null;

            // test some rejoins
            cluster = new MixedPauseCluster(new String[]{"paused", "", ""});

            assertTrue(cluster.start());
            final Client client = ClientFactory.createClient();
            client.createConnection(cluster.m_cluster.getListenerAddress(0));
            checkSystemInformationClusterState(client);

            assertTrue(cluster.killAndRejoin());
            checkSystemInformationClusterState(client);
            checkClusterDoesNotAllowWrite(client);
            assertTrue(cluster.killAndRejoin());
            checkSystemInformationClusterState(client);
            checkClusterDoesNotAllowWrite(client);

            cluster.shutdown();
        } catch (Exception ex) {
            fail("Failed with: " + ex);
        }
    }
}
