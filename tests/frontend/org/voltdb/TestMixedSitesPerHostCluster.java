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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.Maps;

public class TestMixedSitesPerHostCluster extends JUnit4LocalClusterTest {
    static final int K = MiscUtils.isPro() ? 1 : 0;

    static final String JAR_NAME = "mixed.jar";
    static final VoltProjectBuilder m_builder = new VoltProjectBuilder();

    private class MixedSitesPerHostCluster {
        LocalCluster m_cluster = null;

        MixedSitesPerHostCluster(Map<Integer, Integer> sphMap) {
            assert(sphMap != null);
            assertFalse(sphMap.isEmpty());

            m_cluster = new LocalCluster(
                    JAR_NAME,
                    2,
                    sphMap.size(),
                    K,
                    BackendTarget.NATIVE_EE_JNI);
            m_cluster.setOverridesForSitesperhost(sphMap);
            m_cluster.setHasLocalServer(false);
            m_cluster.setDeploymentAndVoltDBRoot(
                    m_builder.getPathToDeployment(),
                    m_builder.getPathToVoltRoot().getAbsolutePath());
        }

        boolean start() {
            m_cluster.startUp();

            return true;
        }

        boolean killAndRejoin(int hostId, Map<Integer, Integer> sphMap) {
            try {
                m_cluster.killSingleHost(hostId);
                // just set the override for the last host
                m_cluster.setOverridesForSitesperhost(sphMap);
                return m_cluster.recoverOne(hostId, 0, "");
            }
            catch (Exception e) {
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
        // just use it to fool VoltDB compiler, use overrides CLI option to provide actual sitesperhost
        final int fakeSph = 2;
        final int hostCount = 3;
        m_builder.addLiteralSchema("CREATE TABLE V0 (id BIGINT);");
        m_builder.configureLogging(null, null, false, false, 200, Integer.MAX_VALUE, null);
        assertTrue(m_builder.compile(Configuration.getPathToCatalogForTest(JAR_NAME), fakeSph, hostCount, K));
    }

    @Test
    public void testSameSitesPerHost() throws InterruptedException {
        MixedSitesPerHostCluster cluster = null;

        // Same sitesperhost across cluster, should work
        Map<Integer, Integer> sameSphMap = Maps.newHashMap();
        sameSphMap.put(0, 2);
        sameSphMap.put(1, 2);
        sameSphMap.put(2, 2);
        cluster = new MixedSitesPerHostCluster(sameSphMap);

        assertTrue(cluster.start());
        cluster.shutdown();
    }

    @Test
    public void testMixedSitesPerHostOptimal() throws InterruptedException {
        MixedSitesPerHostCluster cluster = null;

        // Mixed sitesperhost, optimal, should work
        Map<Integer, Integer> mixedSphMap = Maps.newHashMap();
        mixedSphMap.put(0, 2);
        mixedSphMap.put(1, 6);
        mixedSphMap.put(2, 4);
        cluster = new MixedSitesPerHostCluster(mixedSphMap);
        assertTrue(cluster.start());
        cluster.shutdown();
    }

    @Test
    public void testMixedSitesPerHostSubOptimal() throws InterruptedException {
        if (!MiscUtils.isPro()) { return; } // join tests are pro only

        MixedSitesPerHostCluster cluster = null;

        // Mixed sitesperhost, suboptimal, shouldn't allow to start
        Map<Integer, Integer> subOptimalSphMap = Maps.newHashMap();
        subOptimalSphMap.put(0, 2);
        subOptimalSphMap.put(1, 6);
        subOptimalSphMap.put(2, 3);
        cluster = new MixedSitesPerHostCluster(subOptimalSphMap);
        try {
            cluster.start();
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage().endsWith("external processes failed to start"));
        }
        finally {
            cluster.shutdown();
        }
    }

    @Test
    public void testSameSitesPerHostJoins() throws InterruptedException {
        if (!MiscUtils.isPro()) { return; } // join tests are pro only

        MixedSitesPerHostCluster cluster = null;

        // test same sitesperhost rejoins
        Map<Integer, Integer> sameSphMap = Maps.newHashMap();
        sameSphMap.put(0, 2);
        sameSphMap.put(1, 2);
        sameSphMap.put(2, 2);
        cluster = new MixedSitesPerHostCluster(sameSphMap);
        assertTrue(cluster.start());

        assertTrue(cluster.killAndRejoin(0, sameSphMap));
        assertTrue(cluster.killAndRejoin(1, sameSphMap));
        assertTrue(cluster.killAndRejoin(2, sameSphMap));
        cluster.shutdown();
    }

    @Test
    public void testMixedSitesPerHostJoins() throws InterruptedException {
        if (!MiscUtils.isPro()) { return; } // join tests are pro only

        MixedSitesPerHostCluster cluster = null;
        // test mixed sitesperhost rejoins
        Map<Integer, Integer> mixedSphMap = Maps.newHashMap();
        mixedSphMap.put(0, 6);
        mixedSphMap.put(1, 1);
        mixedSphMap.put(2, 5);
        cluster = new MixedSitesPerHostCluster(mixedSphMap);

        assertTrue(cluster.start());

        assertTrue(cluster.killAndRejoin(0, mixedSphMap));
        assertTrue(cluster.killAndRejoin(1, mixedSphMap));
        assertTrue(cluster.killAndRejoin(2, mixedSphMap));
        cluster.shutdown();
    }
}
