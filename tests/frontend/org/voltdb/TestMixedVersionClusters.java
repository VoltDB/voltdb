/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestMixedVersionClusters {

    static final int K = MiscUtils.isPro() ? 1 : 0;

    static final String JAR_NAME = "mixed.jar";
    static final VoltProjectBuilder m_builder = new VoltProjectBuilder();

    private class MixedVersionCluster {
        LocalCluster m_cluster = null;

        MixedVersionCluster(String[] versions, String[] regexOverrides) {
            assert(versions != null);
            assert(regexOverrides != null);
            assert(versions.length == regexOverrides.length);

            m_cluster = new LocalCluster(
                    JAR_NAME,
                    2,
                    versions.length,
                    K,
                    BackendTarget.NATIVE_EE_JNI);
            m_cluster.setOverridesForHotfix(versions, regexOverrides);
            m_cluster.setHasLocalServer(false);
            m_cluster.setDeploymentAndVoltDBRoot(
                    m_builder.getPathToDeployment(),
                    m_builder.getPathToVoltRoot().getAbsolutePath());
        }

        boolean start() {
            m_cluster.startUp();

            return true;
        }

        boolean killAndRejoin(String version, String regexMatcher) {
            try {
                m_cluster.killSingleHost(2);
                // just set the override for the last host
                m_cluster.setOverridesForHotfix(new String[] {"", "", version},
                                                new String[] {"", "", regexMatcher});
                return m_cluster.recoverOne(2, 0, "");
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
        m_builder.addLiteralSchema("CREATE TABLE V0 (id BIGINT);");
        m_builder.configureLogging(null, null, false, false, 200, Integer.MAX_VALUE, null);
        assertTrue(m_builder.compile(Configuration.getPathToCatalogForTest(JAR_NAME), 2, 3, K));
    }

    //
    // This test will bomb out if a version isn't compatible with itself. We shouldn't ship that.
    //
    @Test
    public void testCurrentVersionIsSelfCompatible() {
        assertTrue(RealVoltDB.staticIsCompatibleVersionString(RealVoltDB.m_defaultVersionString));
    }

    @Test
    public void testStartupConfigurations() throws InterruptedException {
        MixedVersionCluster cluster = null;

        // should work
        cluster = new MixedVersionCluster(
                new String[] {"4.1.1", "4.1.2", "4.1.1"},
                new String[] {"^4\\.1(\\.\\d+)*\\z", "^4\\.1(\\.\\d+)*\\z", "^4\\.1(\\.\\d+)*\\z"});

        assertTrue(cluster.start());
        cluster.shutdown();

        // Three different versions are not allowed simultaniously
        cluster = new MixedVersionCluster(
                new String[] {"4.1", "5.6", "4.1"},
                new String[] {"^4\\.1(\\.\\d+)*\\z", "^5\\.6(\\.\\d+)*\\z", "^4\\.1(\\.\\d+)*\\z"});

        try {
            cluster.start();
            cluster.shutdown();
            fail();
        }
        catch (RuntimeException e) {}

        // 4.1 doesn't know about 4.1hp, but 4.1hp knows about 4.1
        cluster = new MixedVersionCluster(
                new String[] {"4.1", "4.1hp", "4.1hp"},
                new String[] {"^4\\.1(\\.\\d+)*\\z", "^4\\.1(\\w+)*\\z", "^4\\.1(\\w+)*\\z"});

        assertTrue(cluster.start());
        cluster.shutdown();

        // These versions are not allowed by the regex
        cluster = new MixedVersionCluster(
                new String[] {"4.1", "5.6", "4.1"},
                new String[] {"^4\\.1(\\.\\d+)*\\z", "^5\\.6(\\.\\d+)*\\z", "^4\\.1(\\.\\d+)*\\z"});

        try {
            cluster.start();
            cluster.shutdown();
            fail();
        }
        catch (RuntimeException e) {}
    }

    @Test
    public void testJoins() throws InterruptedException {
        if (!MiscUtils.isPro()) { return; } // join tests are pro only

        MixedVersionCluster cluster = null;

        // test some rejoins
        cluster = new MixedVersionCluster(
                new String[] {"4.1.1", "4.1.1", "4.1.1"},
                new String[] {"^4\\.1(\\.\\d+)*\\z", "^4\\.1(\\.\\d+)*\\z", "^4\\.1(\\.\\d+)*\\z"});

        assertTrue(cluster.start());

        assertTrue(cluster.killAndRejoin("4.1.2", "^4\\.1(\\.\\d+)*\\z"));
        assertTrue(cluster.killAndRejoin("4.1hp", "^4\\.1(\\.\\d+)*(\\w+)*\\z"));

        cluster.shutdown();

        // test that three versions fail
        cluster = new MixedVersionCluster(
                new String[] {"4.1.1", "4.1.2", "4.1.1"},
                new String[] {"^4\\.1(\\.\\d+)*\\z", "^4\\.1(\\.\\d+)*\\z", "^4\\.1(\\.\\d+)*\\z"});

        assertTrue(cluster.start());

        assertFalse(cluster.killAndRejoin("4.1.3", "^4\\.1(\\.\\d+)*\\z"));

        cluster.shutdown();
    }
}
