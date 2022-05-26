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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONObject;
import org.junit.Test;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;

public class TestSnapshotDaemonLeaderElection extends TestSnapshotDaemon {

    /*
     * Quick smoke test
     * that leader election work, it scans for snapshots,
     * and then deletes the extra snapshots.
     *
     * Since we only do leader election in the enterprise version for
     * command logging, gate this test on whether we're enterprise
     */
    @Test
    public void testLeaderElectionAndEverythingElse() throws Exception {
        getSnapshotDaemon(false);

        ZooKeeper zk = m_mockVoltDB.getHostMessenger().getZK();
        zk.create(VoltZK.snapshot_truncation_master, null, Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT);

        Thread.sleep(100);

        assertNull(m_initiator.procedureName);

        byte pathBytes[] = "/tmp".getBytes("UTF-8");
//        zk.create(VoltZK.truncation_snapshot_path, pathBytes,
//                  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String truncationRequest = zk.create(VoltZK.request_truncation_snapshot_node, null,
                  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        Thread.sleep(100);
        assertNull(m_initiator.procedureName);

        zk.delete(truncationRequest, -1);
        zk.create(VoltZK.test_scan_path, pathBytes, Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT);
        zk.delete(VoltZK.snapshot_truncation_master, -1);
        Thread.sleep(300);
        assertNotNull(zk.exists(VoltZK.snapshot_truncation_master, false));

        assertTrue(m_initiator.procedureName.equals("@SnapshotScan"));
        long handle = m_initiator.clientData;
        m_initiator.clear();

        m_daemon.processClientResponse(getSuccessfulScanThreeResults(handle)).get();

        truncationRequest = zk.create(VoltZK.request_truncation_snapshot_node, null, Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL_SEQUENTIAL);

        while (m_initiator.procedureName == null) {
            Thread.sleep(100);
        }

        JSONObject jsObj = new JSONObject((String)m_initiator.params[0]);
        assertTrue(jsObj.getString("path").equals("/tmp"));
        assertTrue(jsObj.length() == 5);

        handle = m_initiator.clientData;
        m_initiator.clear();
        assertTrue(ByteBuffer.wrap(zk.getData(VoltZK.request_truncation_snapshot,
                                              false, null)).getLong()
                                              > 0);

        m_daemon.processClientResponse(getSuccessResponse(32L, handle)).get();
        for (int ii = 0; ii < 20; ii++) {
            Thread.sleep(100);
            if (zk.getChildren(VoltZK.request_truncation_snapshot, false).isEmpty()) {
                break;
            }
        }
        assertTrue(zk.getChildren(VoltZK.request_truncation_snapshot, false).isEmpty());

        m_daemon.snapshotCompleted(SnapshotCompletionEvent.newInstanceForTest("",
                        SnapshotPathType.SNAP_PATH,
                        "",
                        32,
                        Collections.<Integer, Long>emptyMap(),
                        true, false, /*DRProducerProtocol.PROTOCOL_VERSION*/0,
                        VoltDB.instance().getClusterCreateTime())).await();
        assertTrue(m_initiator.procedureName.equals("@SnapshotDelete"));

        String nonces[] = (String[])m_initiator.params[1];
        List<String> approvedNonces = new LinkedList<String>(Arrays.asList("woobie_5", "woobie_2", "woobie_3"));
        for (String nonce : nonces) {
            assertTrue(approvedNonces.contains(nonce));
        }
        m_initiator.clear();

        m_daemon.shutdown();
        m_mockVoltDB.shutdown(null);
    }
}
