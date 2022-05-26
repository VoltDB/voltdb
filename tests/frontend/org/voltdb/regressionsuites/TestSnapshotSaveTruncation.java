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

package org.voltdb.regressionsuites;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONObject;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.SnapshotCompletionInterest;
import org.voltdb.SnapshotCompletionMonitor;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures
 */
public class TestSnapshotSaveTruncation extends SaveRestoreBase {
    public TestSnapshotSaveTruncation(String name) {
        super(name);
    }

    private class SnapshotCompletionIntr implements SnapshotCompletionInterest {

        private final Semaphore snapshotCompleted = new Semaphore(0);
        private long txnId;
        private Map<Integer, Long> partitionTxnIds;
        private boolean truncationSnapshot;

        @Override
        public CountDownLatch snapshotCompleted(SnapshotCompletionEvent event) {
            this.txnId = event.multipartTxnId;
            this.truncationSnapshot = event.truncationSnapshot;
            this.partitionTxnIds = event.partitionTxnIds;
            snapshotCompleted.release();
            return null;
        }

    }

    /*
     * Test that if a snapshot is requested for truncation
     * that it is reported as a truncation snapshot by SnapshotCompletionMonitor.
     * Check that if the nonce doesn't match the truncation request that it is not
     * reported as a truncation snapshot. Make sure that reusing a truncation nonce when
     * there is no outstanding request also comes out as not a truncation snapshot.
     */
    @Test
    public void testSnapshotSaveZKTruncation() throws Exception {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();

        Client client = getClient();

        SnapshotCompletionMonitor monitor = VoltDB.instance().getSnapshotCompletionMonitor();
        SnapshotCompletionIntr interest = new SnapshotCompletionIntr();
        monitor.addInterest(interest);

        zk.create(VoltZK.request_truncation_snapshot_node, null, Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT_SEQUENTIAL);

        interest.snapshotCompleted.acquire();
        assertTrue(interest.truncationSnapshot);

        while (true) {
            if (zk.getChildren(VoltZK.request_truncation_snapshot, false).isEmpty()) {
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(zk.getChildren(VoltZK.request_truncation_snapshot, false).isEmpty());

        ClientResponse response = client.callProcedure("@SnapshotSave", TMPDIR,
                TESTNONCE,
                (byte)1);
        String statusString = response.getAppStatusString();

        JSONObject obj = new JSONObject(statusString);
        long txnId = obj.getLong("txnId");
        interest.snapshotCompleted.acquire();
        assertEquals(interest.txnId, txnId);
        assertFalse(interest.truncationSnapshot);

        statusString = client.callProcedure("@SnapshotSave", TMPDIR,
                TESTNONCE + "5",
                (byte)1).getAppStatusString();

        obj = new JSONObject(statusString);
        txnId = obj.getLong("txnId");
        interest.snapshotCompleted.acquire();
        assertEquals(interest.txnId, txnId);
        assertFalse(interest.truncationSnapshot);

        /*
         * Test that old completed snapshot notices are deleted
         */
        for (int ii = 0; ii < 35; ii++) {
            zk.create(VoltZK.completed_snapshots + "/" + ii, null,
                      Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        response = client.callProcedure("@SnapshotSave", TMPDIR,
                TESTNONCE + "6",
                (byte)1);
        statusString = response.getAppStatusString();
        Thread.sleep(300);
        assertEquals( 30, zk.getChildren(VoltZK.completed_snapshots, false).size());
    }

    /**
     * Build a list of the tests to be run. Use the regression suite
     * helpers to allow multiple back ends.
     * JUnit magic that uses the regression suite helper classes.
     */
    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSnapshotSaveTruncation.class);
        if (LocalCluster.isMemcheckDefined()) {
            return builder;
        }
        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();

        VoltServerConfig config =
            new CatalogChangeSingleProcessServer(JAR_NAME, 3,
                                                 BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
