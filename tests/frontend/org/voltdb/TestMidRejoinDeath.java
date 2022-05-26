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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestMidRejoinDeath extends RejoinTestBase {

    @Test
    public void testMidRejoinDeath() throws Exception {
        LocalCluster cluster = null;
        Client client = null;

        try {
            System.out.println("testMidRejoinDeath");
            VoltProjectBuilder builder = getBuilderForTest();
            builder.setSecurityEnabled(true, true);

            cluster = new LocalCluster("rejoin.jar", 3, 2, 1, BackendTarget.NATIVE_EE_JNI);
            //TODO: Till we figure out snapshot save on same machine.
            cluster.setJavaProperty("rejoindeathtest", null);
            cluster.overrideAnyRequestForValgrind();
            cluster.setMaxHeap(1024);
            boolean success = cluster.compile(builder);
            assertTrue(success);
            MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

            // WARNING: This may never be set to true or it will trigger an assert in StreamSnapshotDataTarget
            final boolean DONT_USE_LOCAL_SERVER_THREAD = false;
            cluster.setHasLocalServer(DONT_USE_LOCAL_SERVER_THREAD);

            cluster.startUp();

            client = ClientFactory.createClient(m_cconfig);
            client.createConnection("localhost", cluster.port(0));

            cluster.killSingleHost(1);
            Thread.sleep(3000);

            TableHelper helper = new TableHelper();
            VoltTable t = TableHelper.quickTable("TEST_INLINED_STRING (PKEY:INTEGER, VALUE:VARCHAR36-N, VALUE1:VARCHAR17700-N) P(0)");
            helper.fillTableWithBigintPkey(t, 512, 0, client, 0, 1);

            // try to rejoin, but expect this to fail after 10-15 seconds
            // because the "rejoindeathtest" property is set and that will
            // disable acking of streamed snapshots
            cluster.recoverOne(1, 0, MiscUtils.isPro());

            // try to snapshot to make sure it still works
            client.callProcedure("@SnapshotSave", "{uripath:\"file:///tmp\",nonce:\"mydb\",block:true,format:\"csv\"}");

            // make sure only one node is running if using snapshot code to rejoin
            // IV1 community uses a different path that doesn't fail
            if (MiscUtils.isPro()) {
                assertEquals(1, cluster.getLiveNodeCount());

                cluster.recoverOne(1, 0, MiscUtils.isPro());

                assertEquals(2, cluster.getLiveNodeCount());

                cluster.killSingleHost(1);

                cluster.setJavaProperty("rejoindeathtestonrejoinside", null);

                cluster.recoverOne(1, 0, MiscUtils.isPro());

                assertEquals(1, cluster.getLiveNodeCount());

                cluster.setJavaProperty("rejoindeathtestcancel", null);

                cluster.recoverOne(1, 0, MiscUtils.isPro());

                assertEquals(2, cluster.getLiveNodeCount());
            }
        }
        finally {
            if (client != null) {
                client.close();
            }
            if (cluster != null) {
                cluster.shutDown();
            }
        }
    }

}
