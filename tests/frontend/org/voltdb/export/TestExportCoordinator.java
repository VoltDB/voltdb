/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.export;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKTestBase;
import org.voltcore.zk.ZKUtil;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * @author rdykiel
 *
 */
public class TestExportCoordinator extends ZKTestBase {

    VoltLogger log = new VoltLogger("HOST");
    private final int NUM_AGREEMENT_SITES = 4;
    private final String stateMachineManagerRoot = "/test/db/export_coordination";
    private final String TEST_TABLE = "TEST_TABLE";
    private final int TEST_PARTITION = 7;

    // Mocked EDS
    ExportDataSource eds0 = null;
    ExportDataSource eds1 = null;
    ExportDataSource eds2 = null;
    ExportDataSource eds3 = null;

    // ExecutorServices
    ListeningExecutorService m_es0 = null;
    ListeningExecutorService m_es1 = null;
    ListeningExecutorService m_es2 = null;
    ListeningExecutorService m_es3 = null;

    // Handles to set mocked trackers on each EDS
    ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
    ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
    ExportSequenceNumberTracker tracker2 = new ExportSequenceNumberTracker();
    ExportSequenceNumberTracker tracker3 = new ExportSequenceNumberTracker();

    @Before
    public void setUp() throws Exception {
        setUpZK(NUM_AGREEMENT_SITES);
        ZooKeeper zk = m_messengers.get(0).getZK();
        ZKUtil.addIfMissing(zk, "/test", CreateMode.PERSISTENT, null);
        ZKUtil.addIfMissing(zk, "/test/db", CreateMode.PERSISTENT, null);
        ZKUtil.addIfMissing(zk, stateMachineManagerRoot, CreateMode.PERSISTENT, null);

        eds0 = mockDataSource(0, tracker0);
        eds1 = mockDataSource(0, tracker1);
        eds2 = mockDataSource(0, tracker2);
        eds3 = mockDataSource(0, tracker3);
    }

    private ExportDataSource mockDataSource(
            int siteId,
            ExportSequenceNumberTracker tracker) {

        ListeningExecutorService es = CoreUtils.getListeningExecutorService("ExportDataSource for table " +
                TEST_TABLE + " partition " + TEST_PARTITION, 1);

        ExportDataSource mocked = mock(ExportDataSource.class);
        when(mocked.toString()).thenReturn("Mocked EDS " + siteId);
        when(mocked.getExecutorService()).thenReturn(es);
        when(mocked.getTracker()).thenReturn(tracker.duplicate());
        when(mocked.getLastReleaseSeqNo()).thenReturn(0L);
        when(mocked.getPartitionId()).thenReturn(TEST_PARTITION);
        when(mocked.getTableName()).thenReturn(TEST_TABLE);
        return mocked;
    }

    @After
    public void tearDown() throws Exception {
        tearDownZK();
        if (m_es0 != null) {
            m_es0.shutdown();
        }
        if (m_es1 != null) {
            m_es1.shutdown();
        }
        if (m_es2 != null) {
            m_es2.shutdown();
        }
        if (m_es3 != null) {
            m_es3.shutdown();
        }
    }

    @Test
    public void testSingleNodeNoGaps() throws InterruptedException {

        try {
            tracker0.truncateAfter(0L);
            tracker0.append(1L, 100L);

            ExportCoordinator ec0 = new ExportCoordinator(
                    m_messengers.get(0).getZK(),
                    stateMachineManagerRoot,
                    0,
                    eds0);

            ec0.becomeLeader();
            while(!ec0.isLeader()) {
                Thread.yield();
            }

            // Wait for the mastership to be resolved
            while(!ec0.isExportMaster(1L)) {
                Thread.yield();
            }

            // Check the leader is export master forever even
            // past the initial tracker info
            assertTrue(ec0.isExportMaster(10L));
            assertTrue(ec0.isExportMaster(100L));
            assertTrue(ec0.isExportMaster(101L));
            assertTrue(ec0.isExportMaster(1000L));

            ec0.shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

}
