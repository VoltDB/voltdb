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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.InvocationDispatcher;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.util.*;

/**
 * Created by bshaw on 7/7/17.
 */
public class TestImporterWithMultipleNodeKill {

    private LocalCluster m_cluster = null;

    @Before
    public void setUp() throws Exception {
        final int HOST_COUNT = 5;

        String bundleLocation = System.getProperty("user.dir").split("obj", 2)[0] + "/bundles";
        System.out.println(bundleLocation);
        System.setProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME, bundleLocation);
        // Lightweight 5 node, K4 cluster to allow 2 nodes to be killed and rejoined
        m_cluster = new LocalCluster(
                "CREATE TABLE test ( val BIGINT NOT NULL );",
                null,
                1, HOST_COUNT, HOST_COUNT - 1, 0,
                BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING,
                false, false, null);
        m_cluster.setHasLocalServer(false);
        m_cluster.overrideAnyRequestForValgrind();
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        // configure socket importer 1
        for (int i = 0; i < 224; i++) {
            String portStartString = Integer.toString(7001 + i * HOST_COUNT);
            Properties props = RegressionSuite.buildProperties(
                    "port", portStartString,
                    "decode", "true",
                    "procedure", "test.insert");
            project.addImport(true, "custom", "csv", "socketstream.jar", props);
        }

        m_cluster.compileDeploymentOnly(project);
        new File(project.getPathToDeployment()).deleteOnExit();
        m_cluster.setExpectedToCrash(false);

        m_cluster.startUp();
    }

    @After
    public void tearDown() throws Exception {
        m_cluster.shutDown();
        System.clearProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME);
    }

    @Test
    public void testMultipleNodeKill() throws Exception {
        int iters = 0;
        final int numIters = 4;
        do {
            m_cluster.killSingleHost(3);
            m_cluster.killSingleHost(4);
            Thread.sleep(2000);
            m_cluster.recoverOne(3, 0, "");
            m_cluster.recoverOne(4, 0, "");
            Thread.sleep(2000);
        } while (++iters < numIters);
    }
}