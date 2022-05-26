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

import java.util.Collections;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.voltdb.FlakyTestRule.Flaky;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.NullCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestReplicatedTableSnapshotRestore extends JUnit4LocalClusterTest {
    @Rule
    public FlakyTestRule ftRule = new FlakyTestRule();

    @After
    public void tearDown() throws Exception {
        System.gc();
        System.runFinalization();
    }

    /**
     * https://issues.voltdb.com/browse/ENG-15174
     */
    @Test
    @Flaky(description="TestReplicatedTableSnapshotRestore.testMultiBlockSnapshotRestore")
    public void testMultiBlockSnapshotRestore() throws Exception {
        // Create a table with a very large VARCHAR column, so it will be very easy for
        // the snapshot to have more than one block during restore.
        String ddl = "create table t (s1 varchar(1048576), s2 varchar(1048560));\n";
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(ddl);
        // Must have k-factor here.
        int sitesPerHost = 4, hostCount = 4, kFactor = 3;
        LocalCluster cluster = new LocalCluster("eng15174.jar", sitesPerHost,
                hostCount, kFactor, BackendTarget.NATIVE_EE_JNI);
        if (cluster.isValgrind()) {
            return;
        }
        assertTrue(cluster.compile(builder));
        MiscUtils.copyFile(builder.getPathToDeployment(), VoltDB.Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);
        cluster.startUp();

        Client client = cluster.createClient(new ClientConfig());
        NullCallback callback = new NullCallback();
        System.out.println("Inserting some rows into table T.");
        String text = String.join("", Collections.nCopies(1048560, "a"));
        for (int i = 1; i <= 30; i++) {
            client.callProcedure(callback, "T.insert", text, text);
        }
        client.drain();
        assertEquals(30, client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM T;").getResults()[0].asScalarLong());

        System.out.println("Kill node 1 and 2");
        cluster.killSingleHost(1);
        cluster.killSingleHost(2);
        Thread.sleep(1000);

        System.out.println("Take a snapshot.");
        client.callProcedure("@SnapshotSave", "{nonce:\"eng15174\",block:true}");

        System.out.println("Shutdown the cluster.");
        client.close();
        cluster.shutDown();

        System.out.println("Recover the cluster.");
        cluster.startUp(false);
        client = cluster.createClient(new ClientConfig());
        Thread.sleep(1000);
        assertEquals(30, client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM T;").getResults()[0].asScalarLong());
    }
}
