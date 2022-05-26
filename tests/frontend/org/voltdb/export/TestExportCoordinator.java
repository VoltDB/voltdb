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
    }

    @Test
    public void testSingleNodeNoGaps() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(1L, 100L);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            // Note: use the special constructor for JUnit
            ExportCoordinator ec0 = new ExportCoordinator(
                    m_messengers.get(0).getZK(),
                    stateMachineManagerRoot,
                    0,
                    eds0,
                    true);

            ec0.initialize();
            ec0.becomeLeader();
            while(!ec0.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

            // Check the leader is export master forever even
            // past the initial tracker info
            assertTrue(ec0.isExportMaster(1L));
            assertTrue(ec0.isExportMaster(10L));
            assertTrue(ec0.isExportMaster(100L));
            assertTrue(ec0.isExportMaster(101L));
            assertTrue(ec0.isExportMaster(1000L));

            ec0.shutdown();
            eds0.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testSingleNodeWithGap() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(1L, 100L);
            tracker0.append(200L, 300L);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            // Note: use the special constructor for JUnit
            ExportCoordinator ec0 = new ExportCoordinator(
                    m_messengers.get(0).getZK(),
                    stateMachineManagerRoot,
                    0,
                    eds0,
                    true);

            ec0.initialize();
            ec0.becomeLeader();
            while(!ec0.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

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
            eds0.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void test2NodesWithGap() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(1L, 100L);
            tracker0.append(200L, 300L);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
            tracker1.append(1L, 300L);
            ExportDataSource eds1 = mockDataSource(0, tracker1);

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

            ec0.initialize();
            ec1.initialize();

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

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

            assertTrue(ec0.isExportMaster(200L));
            assertFalse(ec1.isExportMaster(200L));

            assertTrue(ec0.isExportMaster(201L));
            assertFalse(ec1.isExportMaster(201L));

            assertTrue(ec0.isExportMaster(300L));
            assertFalse(ec1.isExportMaster(300L));

            assertTrue(ec0.isExportMaster(301L));
            assertFalse(ec1.isExportMaster(301L));

            assertTrue(ec0.isExportMaster(1000L));
            assertFalse(ec1.isExportMaster(1000L));

            ec0.shutdown();
            ec1.shutdown();

            eds0.getExecutorService().shutdown();
            eds1.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    // THis is an image of an actual test failure
    @Test
    public void test2NodesWithGap1() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(973L, 973L);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
            tracker1.append(973L, 991L);
            ExportDataSource eds1 = mockDataSource(0, tracker1);

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

            // the initial sequence number would be restored from snapshot, so the initial gap can be detected
            ec0.setInitialSequenceNumber(991L);

            ec0.initialize();
            ec1.initialize();

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());

            assertFalse(ec0.isExportMaster(974L));
            assertTrue(ec1.isExportMaster(974L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());

            // Simulate ack at 991
            assertTrue(ec0.isSafePoint(991L));
            assertTrue(ec1.isSafePoint(991L));

            // Leader is back to master
            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());

            assertTrue(ec0.isExportMaster(992L));
            assertFalse(ec1.isExportMaster(992L));

            ec0.shutdown();
            ec1.shutdown();

            eds0.getExecutorService().shutdown();
            eds1.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void test2NodesWithGapSafePoints() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(1L, 100L);
            tracker0.append(200L, 300L);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
            tracker1.append(1L, 300L);
            ExportDataSource eds1 = mockDataSource(0, tracker1);

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

            ec0.initialize();
            ec1.initialize();

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

            // Check the leader is export master up to the gap
            // and verify replica becomes master, then leader
            // gets mastership past the gap. Simulate the ack paths
            assertTrue(ec0.isExportMaster(1L));
            assertFalse(ec1.isExportMaster(1L));

            // Simulate safe point management via acks and verify mastership
            assertFalse(ec0.isSafePoint(10L));
            assertFalse(ec1.isSafePoint(10L));

            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());

            assertTrue(ec0.isSafePoint(100L));
            assertTrue(ec1.isSafePoint(100L));

            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());

            // Ec1 covers the gap until safe point 200L
            assertFalse(ec0.isExportMaster(101L));
            assertTrue(ec1.isExportMaster(101L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());

            assertFalse(ec0.isSafePoint(150L));
            assertFalse(ec1.isSafePoint(150L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());

            // Ec0 (leader) regains mastership
            assertTrue(ec0.isSafePoint(199L));
            assertTrue(ec1.isSafePoint(199L));

            assertTrue(ec0.isExportMaster(200L));
            assertFalse(ec1.isExportMaster(200L));

            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());

            assertTrue(ec0.isExportMaster(300L));
            assertFalse(ec1.isExportMaster(300L));

            assertFalse(ec0.isSafePoint(300L));
            assertFalse(ec1.isSafePoint(300L));

            assertTrue(ec0.isExportMaster(1000L));
            assertFalse(ec1.isExportMaster(1000L));

            assertFalse(ec0.isSafePoint(1000L));
            assertFalse(ec1.isSafePoint(1000L));

            ec0.shutdown();
            ec1.shutdown();

            eds0.getExecutorService().shutdown();
            eds1.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }



    @Test
    public void test2NodesWithLeaderInitialGap() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(200L, 300L);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
            tracker1.append(1L, 300L);
            ExportDataSource eds1 = mockDataSource(0, tracker1);

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

            ec0.initialize();
            ec1.initialize();

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

            // Check the leader is export master only after
            // the initial gap
            assertFalse(ec0.isExportMaster(1L));
            assertTrue(ec1.isExportMaster(1L));

            assertFalse(ec0.isExportMaster(10L));
            assertTrue(ec1.isExportMaster(10L));

            assertTrue(ec0.isExportMaster(200L));
            assertFalse(ec1.isExportMaster(200L));

            assertTrue(ec0.isExportMaster(201L));
            assertFalse(ec1.isExportMaster(201L));

            assertTrue(ec0.isExportMaster(300L));
            assertFalse(ec1.isExportMaster(300L));

            assertTrue(ec0.isExportMaster(301L));
            assertFalse(ec1.isExportMaster(301L));

            assertTrue(ec0.isExportMaster(1000L));
            assertFalse(ec1.isExportMaster(1000L));

            ec0.shutdown();
            ec1.shutdown();

            eds0.getExecutorService().shutdown();
            eds1.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void test3NodesWithGapLowestHostIdWins() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(1L, 100L);
            tracker0.append(200L, 300L);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
            tracker1.truncateAfter(0L);
            tracker1.append(1L, 300L);
            ExportDataSource eds1 = mockDataSource(0, tracker1);

            ExportSequenceNumberTracker tracker2 = new ExportSequenceNumberTracker();
            tracker2.append(1L, 300L);
            ExportDataSource eds2 = mockDataSource(0, tracker2);

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

            ec0.initialize();
            ec1.initialize();
            ec2.initialize();

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady() || !ec2.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

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

            assertFalse(ec0.isExportMaster(199L));
            assertTrue(ec1.isExportMaster(199L));
            assertFalse(ec2.isExportMaster(199L));

            // Leader regains mastership
            assertTrue(ec0.isExportMaster(200L));
            assertFalse(ec1.isExportMaster(200L));
            assertFalse(ec2.isExportMaster(200L));

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
            ec1.shutdown();
            ec2.shutdown();

            eds0.getExecutorService().shutdown();
            eds1.getExecutorService().shutdown();
            eds2.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    // Test handing over mastership 0 -> 1 -> 2 -> 0
    @Test
    public void test3Nodes_0_1_2_0() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(1L, 100L);
            tracker0.append(200L, 400L);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
            tracker1.append(1L, 130L);
            tracker1.append(250L, 400L);
            ExportDataSource eds1 = mockDataSource(0, tracker1);

            ExportSequenceNumberTracker tracker2 = new ExportSequenceNumberTracker();
            tracker2.append(1L, 70L);
            tracker2.append(100L, 400L);
            ExportDataSource eds2 = mockDataSource(0, tracker2);

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

            ec0.initialize();
            ec1.initialize();
            ec2.initialize();

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady() || !ec2.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

            // Check the leader is export master up to the gap,
            // Check that host 1 starts filling the gap until its own gap
            assertTrue(ec0.isExportMaster(1L));
            assertFalse(ec1.isExportMaster(1L));
            assertFalse(ec2.isExportMaster(1L));

            assertTrue(ec0.isSafePoint(100L));
            assertTrue(ec1.isSafePoint(100L));
            assertTrue(ec2.isSafePoint(100L));

            assertFalse(ec0.isExportMaster(101L));
            assertTrue(ec1.isExportMaster(101L));
            assertFalse(ec2.isExportMaster(101L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());
            assertFalse(ec2.isMaster());

            assertFalse(ec0.isExportMaster(110L));
            assertTrue(ec1.isExportMaster(110L));
            assertFalse(ec2.isExportMaster(110L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());
            assertFalse(ec2.isMaster());

            assertTrue(ec0.isSafePoint(130L));
            assertTrue(ec1.isSafePoint(130L));
            assertTrue(ec2.isSafePoint(130L));

            // Host 1 hits his gap, host 2 fills until leader safe point
            assertFalse(ec0.isExportMaster(131L));
            assertFalse(ec1.isExportMaster(131L));
            assertTrue(ec2.isExportMaster(131L));

            assertFalse(ec0.isMaster());
            assertFalse(ec1.isMaster());
            assertTrue(ec2.isMaster());

            assertFalse(ec0.isExportMaster(150L));
            assertFalse(ec1.isExportMaster(150L));
            assertTrue(ec2.isExportMaster(150L));

            assertFalse(ec0.isMaster());
            assertFalse(ec1.isMaster());
            assertTrue(ec2.isMaster());

            assertTrue(ec0.isSafePoint(199L));
            assertTrue(ec1.isSafePoint(199L));
            assertTrue(ec2.isSafePoint(199L));

            // Leader regains mastership until forever
            assertTrue(ec0.isExportMaster(200L));
            assertFalse(ec1.isExportMaster(200L));
            assertFalse(ec2.isExportMaster(200L));

            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());
            assertFalse(ec2.isMaster());

            assertTrue(ec0.isExportMaster(2000L));
            assertFalse(ec1.isExportMaster(2000L));
            assertFalse(ec2.isExportMaster(2000L));

            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());
            assertFalse(ec2.isMaster());


            ec0.shutdown();
            ec1.shutdown();
            ec2.shutdown();

            eds0.getExecutorService().shutdown();
            eds1.getExecutorService().shutdown();
            eds2.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    // Test handing over mastership 0 -> 1 -> 2 -> 1 -> 0
    @Test
    public void test3Nodes_0_1_2_1_0() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(1L, 100L);
            tracker0.append(200L, 400L);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
            tracker1.append(1L, 130L);
            tracker1.append(170L, 400L);
            ExportDataSource eds1 = mockDataSource(0, tracker1);

            ExportSequenceNumberTracker tracker2 = new ExportSequenceNumberTracker();
            tracker2.append(1L, 70L);
            tracker2.append(90L, 180L);
            tracker2.append(300L, 400L);
            ExportDataSource eds2 = mockDataSource(0, tracker2);

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

            ec0.initialize();
            ec1.initialize();
            ec2.initialize();

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady() || !ec2.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

            // Check the leader is export master up to the gap,
            // Check that host 1 starts filling the gap until its own gap
            assertTrue(ec0.isExportMaster(1L));
            assertFalse(ec1.isExportMaster(1L));
            assertFalse(ec2.isExportMaster(1L));

            assertTrue(ec0.isSafePoint(100L));
            assertTrue(ec1.isSafePoint(100L));
            assertTrue(ec2.isSafePoint(100L));

            assertFalse(ec0.isExportMaster(101L));
            assertTrue(ec1.isExportMaster(101L));
            assertFalse(ec2.isExportMaster(101L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());
            assertFalse(ec2.isMaster());

            assertFalse(ec0.isExportMaster(110L));
            assertTrue(ec1.isExportMaster(110L));
            assertFalse(ec2.isExportMaster(110L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());
            assertFalse(ec2.isMaster());

            assertTrue(ec0.isSafePoint(130L));
            assertTrue(ec1.isSafePoint(130L));
            assertTrue(ec2.isSafePoint(130L));

            // Host 1 hits his gap, host 2 fills until its own safe point
            assertFalse(ec0.isExportMaster(131L));
            assertFalse(ec1.isExportMaster(131L));
            assertTrue(ec2.isExportMaster(131L));

            assertFalse(ec0.isMaster());
            assertFalse(ec1.isMaster());
            assertTrue(ec2.isMaster());

            assertFalse(ec0.isExportMaster(150L));
            assertFalse(ec1.isExportMaster(150L));
            assertTrue(ec2.isExportMaster(150L));

            assertFalse(ec0.isMaster());
            assertFalse(ec1.isMaster());
            assertTrue(ec2.isMaster());

            assertTrue(ec0.isSafePoint(180L));
            assertTrue(ec1.isSafePoint(180L));
            assertTrue(ec2.isSafePoint(180L));

            // Host 1 resumes mastership until leader safe point
            assertFalse(ec0.isExportMaster(181L));
            assertTrue(ec1.isExportMaster(181L));
            assertFalse(ec2.isExportMaster(181L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());
            assertFalse(ec2.isMaster());

            assertFalse(ec0.isExportMaster(190L));
            assertTrue(ec1.isExportMaster(190L));
            assertFalse(ec2.isExportMaster(190L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());
            assertFalse(ec2.isMaster());

            assertTrue(ec0.isSafePoint(199L));
            assertTrue(ec1.isSafePoint(199L));
            assertTrue(ec2.isSafePoint(199L));

            // Leader regains mastership until forever
            assertTrue(ec0.isExportMaster(200L));
            assertFalse(ec1.isExportMaster(200L));
            assertFalse(ec2.isExportMaster(200L));

            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());
            assertFalse(ec2.isMaster());

            assertTrue(ec0.isExportMaster(2000L));
            assertFalse(ec1.isExportMaster(2000L));
            assertFalse(ec2.isExportMaster(2000L));

            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());
            assertFalse(ec2.isMaster());


            ec0.shutdown();
            ec1.shutdown();
            ec2.shutdown();

            eds0.getExecutorService().shutdown();
            eds1.getExecutorService().shutdown();
            eds2.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    // Contrived corner case where the initial row is missing
    @Test
    public void testENG17776() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(2, ExportSequenceNumberTracker.INFINITE_SEQNO);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
            tracker1.append(1, ExportSequenceNumberTracker.INFINITE_SEQNO);
            ExportDataSource eds1 = mockDataSource(0, tracker1);

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

            // the initial sequence number would be restored from snapshot, so the initial gap can be detected
            ec0.setInitialSequenceNumber(1);

            ec0.initialize();
            ec1.initialize();

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

            assertFalse(ec0.isExportMaster(1L));
            assertTrue(ec1.isExportMaster(1L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());

            // Simulate ack at 1
            assertTrue(ec0.isSafePoint(1L));
            assertTrue(ec1.isSafePoint(1L));

            // Leader is back to master
            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());

            assertTrue(ec0.isExportMaster(2L));
            assertFalse(ec1.isExportMaster(2L));

            ec0.shutdown();
            ec1.shutdown();

            eds0.getExecutorService().shutdown();
            eds1.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }

    // Contrived corner case where the second row is missing
    @Test
    public void testENG17776_2() {

        try {
            ExportSequenceNumberTracker tracker0 = new ExportSequenceNumberTracker();
            tracker0.append(2, ExportSequenceNumberTracker.INFINITE_SEQNO);
            ExportDataSource eds0 = mockDataSource(0, tracker0);

            ExportSequenceNumberTracker tracker1 = new ExportSequenceNumberTracker();
            tracker1.append(1, 1);
            tracker1.append(3, ExportSequenceNumberTracker.INFINITE_SEQNO);
            ExportDataSource eds1 = mockDataSource(0, tracker1);

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

            // the initial sequence number would be restored from snapshot, so the initial gap can be detected
            ec0.setInitialSequenceNumber(1);

            ec0.initialize();
            ec1.initialize();

            ec0.becomeLeader();
            while(!ec0.isTestReady() || !ec1.isTestReady()) {
                Thread.yield();
            }

            // Check leadership
            assertTrue(ec0.isPartitionLeader());

            assertFalse(ec0.isExportMaster(1L));
            assertTrue(ec1.isExportMaster(1L));

            assertFalse(ec0.isMaster());
            assertTrue(ec1.isMaster());

            // Simulate ack at 1
            assertTrue(ec0.isSafePoint(1L));
            assertTrue(ec1.isSafePoint(1L));

            // Leader is back to master
            assertTrue(ec0.isMaster());
            assertFalse(ec1.isMaster());

            assertTrue(ec0.isExportMaster(2L));
            assertFalse(ec1.isExportMaster(2L));

            ec0.shutdown();
            ec1.shutdown();

            eds0.getExecutorService().shutdown();
            eds1.getExecutorService().shutdown();
        }
        catch (InterruptedException e) {
            fail();
        }
    }
}
