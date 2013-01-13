/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestOverReplicationFail extends RejoinTestBase {

    protected final boolean m_useIv2 = true;

    @Test
    public void testDontStart() throws Exception {
        LocalCluster cluster = null;
        Client client = null;

        try {
            VoltProjectBuilder builder = getBuilderForTest();
            builder.setSecurityEnabled(true);

            // This cluster config has an extra node.  Should fail.
            cluster = new LocalCluster("rejoin.jar", 1, 3, 1,
                    BackendTarget.NATIVE_EE_JNI, false, false);
            cluster.overrideAnyRequestForValgrind();
            cluster.setHasLocalServer(false);
            cluster.setMaxHeap(256);
            boolean success = cluster.compile(builder);
            assertTrue(success);
            MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

            boolean threw = false;
            try {
                cluster.startUp();
            }
            catch (RuntimeException rte) {
                if (rte.getMessage().contains("failed to start")) {
                    threw = true;
                }
            }
            assertTrue(threw);

        }
        finally {
            if (client != null) client.close();
            if (cluster != null) cluster.shutDown();
        }
    }

    @Test
    public void testDontStart2() throws Exception {
        LocalCluster cluster = null;
        Client client = null;

        try {
            VoltProjectBuilder builder = getBuilderForTest();
            builder.setSecurityEnabled(true);

            // This cluster config only needs 2 of the 3 sites on the third node.
            cluster = new LocalCluster("rejoin.jar", 3, 3, 1,
                    BackendTarget.NATIVE_EE_JNI, false, false);
            cluster.overrideAnyRequestForValgrind();
            cluster.setHasLocalServer(false);
            cluster.setMaxHeap(256);
            boolean success = cluster.compile(builder);
            assertTrue(success);
            MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

            boolean threw = false;
            try {
                cluster.startUp();
            }
            catch (RuntimeException rte) {
                if (rte.getMessage().contains("failed to start")) {
                    threw = true;
                }
            }
            assertTrue(threw);

        }
        finally {
            if (client != null) client.close();
            if (cluster != null) cluster.shutDown();
        }
    }

}
