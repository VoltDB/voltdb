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

import static org.junit.Assert.assertFalse;
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
 * Test {@code ExportCoordinator} using a mocked {@code ExportDataSource}.
 *
 * NOTE: the JUnit test cases violate the contract that requires only invoking
 * the public methods of an ExportCoordinator through an ExportDataSource runnable.
 * Therefore the test cases rely on a special synchronization mechanism ensuring that
 * the exchange of tracker information has been completed before starting invoking the
 * public methods. Also these tests don't exercise membership changes that would result
 * in additional tracker exchanges.
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
    }

    // Mock a data source: note, the tracker must have been set for the test case
    // prior to calling this method.
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
    public void testSingleNodeNoGaps() {

        try {
            tracker0.truncateAfter(0L);
            tracker0.append(1L, 100L);
            eds0 = mockDataSource(0, tracker0);

            // Note: use the special constructor for JUnit
            ExportCoordinator ec0 = new ExportCoordinator(
                    m_messengers.get(0).getZK(),
                    stateMachineManagerRoot,
                    0,
                    eds0,
                    true);

            ec0.becomeLeader();
            while(!ec0.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isLeader());

            // Check the leader is export master forever even
            // past the initial tracker info
            assertTrue(ec0.isExportMaster(1L));
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

    @Test
    public void testSingleNodeWithGap() {

        try {
            tracker0.truncateAfter(0L);
            tracker0.append(1L, 100L);
            tracker0.append(200L, 300L);
            eds0 = mockDataSource(0, tracker0);

            // Note: use the special constructor for JUnit
            ExportCoordinator ec0 = new ExportCoordinator(
                    m_messengers.get(0).getZK(),
                    stateMachineManagerRoot,
                    0,
                    eds0,
                    true);

            ec0.becomeLeader();
            while(!ec0.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isLeader());

            // Check the leader is always export master since we have no replica
            // (EDS trying to poll would become BLOCKED)
            assertTrue(ec0.isExportMaster(1L));
            assertTrue(ec0.isExportMaster(10L));
            assertTrue(ec0.isExportMaster(100L));
            assertTrue(ec0.isExportMaster(101L));
            assertTrue(ec0.isExportMaster(200L));
            assertTrue(ec0.isExportMaster(300L));
            assertTrue(ec0.isExportMaster(301L));
            assertTrue(ec0.isExportMaster(1000L));

            ec0.shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void test2NodesWithGap() {

        try {
            tracker0.truncateAfter(0L);
            tracker0.append(1L, 100L);
            tracker0.append(200L, 300L);
            eds0 = mockDataSource(0, tracker0);

            tracker1.truncateAfter(0L);
            tracker1.append(1L, 300L);
            eds1 = mockDataSource(0, tracker1);

            // Note: use the special constructors for JUnit
            ExportCoordinator ec0 = new ExportCoordinator(
                    m_messengers.get(0).getZK(),
                    stateMachineManagerRoot,
                    0,
                    eds0,
                    true);

            ExportCoordinator ec1 = new ExportCoordinator(
                    m_messengers.get(1).getZK(),
                    stateMachineManagerRoot,
                    1,
                    eds1,
                    true);

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isLeader());

            // Check the leader is export master up to the gap
            // and verify replica becomes master, then leader
            // gets mastership past the gap
            // NOTE: this test does not verify the ack path
            assertTrue(ec0.isExportMaster(1L));
            assertFalse(ec1.isExportMaster(1L));

            assertTrue(ec0.isExportMaster(10L));
            assertFalse(ec1.isExportMaster(10L));

            assertTrue(ec0.isExportMaster(100L));
            assertFalse(ec1.isExportMaster(100L));

            assertFalse(ec0.isExportMaster(101L));
            assertTrue(ec1.isExportMaster(101L));

            assertFalse(ec0.isExportMaster(200L));
            assertTrue(ec1.isExportMaster(200L));

            assertTrue(ec0.isExportMaster(201L));
            assertFalse(ec1.isExportMaster(201L));

            assertTrue(ec0.isExportMaster(300L));
            assertFalse(ec1.isExportMaster(300L));

            assertTrue(ec0.isExportMaster(301L));
            assertFalse(ec1.isExportMaster(301L));

            assertTrue(ec0.isExportMaster(1000L));
            assertFalse(ec1.isExportMaster(1000L));

            ec0.shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void test3NodesWithGapLowestHostIdWins() {

        try {
            tracker0.truncateAfter(0L);
            tracker0.append(1L, 100L);
            tracker0.append(200L, 300L);
            eds0 = mockDataSource(0, tracker0);

            tracker1.truncateAfter(0L);
            tracker1.append(1L, 300L);
            eds1 = mockDataSource(0, tracker1);

            tracker2.truncateAfter(0L);
            tracker2.append(1L, 300L);
            eds2 = mockDataSource(0, tracker2);

            // Note: use the special constructors for JUnit
            ExportCoordinator ec0 = new ExportCoordinator(
                    m_messengers.get(0).getZK(),
                    stateMachineManagerRoot,
                    0,
                    eds0,
                    true);

            ExportCoordinator ec1 = new ExportCoordinator(
                    m_messengers.get(1).getZK(),
                    stateMachineManagerRoot,
                    1,
                    eds1,
                    true);

            ExportCoordinator ec2 = new ExportCoordinator(
                    m_messengers.get(2).getZK(),
                    stateMachineManagerRoot,
                    2,
                    eds2,
                    true);

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady() || !ec2.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isLeader());

            // Check the leader is export master up to the gap
            // and verify replicas aren't masters until the lowest
            // site becomes becomes master
            assertTrue(ec0.isExportMaster(1L));
            assertFalse(ec1.isExportMaster(1L));
            assertFalse(ec2.isExportMaster(1L));

            assertTrue(ec0.isExportMaster(10L));
            assertFalse(ec1.isExportMaster(10L));
            assertFalse(ec2.isExportMaster(10L));

            assertTrue(ec0.isExportMaster(100L));
            assertFalse(ec1.isExportMaster(100L));
            assertFalse(ec2.isExportMaster(100L));

            // Leader hits gap, lowest site must win
            assertFalse(ec0.isExportMaster(101L));
            assertTrue(ec1.isExportMaster(101L));
            assertFalse(ec2.isExportMaster(101L));

            assertFalse(ec0.isExportMaster(200L));
            assertTrue(ec1.isExportMaster(200L));
            assertFalse(ec2.isExportMaster(200L));

            // Leader regains mastership
            assertTrue(ec0.isExportMaster(201L));
            assertFalse(ec1.isExportMaster(201L));
            assertFalse(ec2.isExportMaster(101L));

            assertTrue(ec0.isExportMaster(300L));
            assertFalse(ec1.isExportMaster(300L));
            assertFalse(ec2.isExportMaster(300L));

            assertTrue(ec0.isExportMaster(301L));
            assertFalse(ec1.isExportMaster(301L));
            assertFalse(ec2.isExportMaster(301L));

            assertTrue(ec0.isExportMaster(1000L));
            assertFalse(ec1.isExportMaster(1000L));
            assertFalse(ec2.isExportMaster(1000L));

            ec0.shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }
}
