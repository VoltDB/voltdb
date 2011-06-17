/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.fault;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.io.File;

import junit.framework.TestCase;

import org.voltdb.*;
import org.voltdb.VoltDB;
import org.voltdb.fault.FaultDistributorInterface.PPDPolicyDecision;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.catalog.SnapshotSchedule;

public class TestFaultDistributor extends TestCase
{
    public class MockFaultHandler implements FaultHandler
    {
        FaultType m_faultType;
        boolean m_gotFault;
        int m_order;
        OrderTracker m_orderTracker;
        Semaphore m_handledFaults = new Semaphore(0);
        Semaphore m_clearedFaults = new Semaphore(0);

        public MockFaultHandler(FaultType type)
        {
            m_faultType = type;
            m_gotFault = false;
        }

        public MockFaultHandler(FaultType type, int order,
                                OrderTracker orderTracker)
        {
            m_faultType = type;
            m_gotFault = false;
            m_order = order;
            m_orderTracker = orderTracker;
        }

        @Override
        public void faultOccured(Set<VoltFault> faults)
        {
            for (VoltFault fault : faults) {
                if (fault.getFaultType() == m_faultType)
                {
                    m_gotFault = true;
                }
                if (m_orderTracker != null)
                {
                    m_orderTracker.updateOrder(m_order);
                }
            }
            m_handledFaults.release();
        }

        @Override
        public void faultCleared(Set<VoltFault> faults) {
            m_clearedFaults.release();
        }
    }

    class OrderTracker
    {
        boolean m_goodOrder;
        int m_lastOrder;

        OrderTracker(int startingOrder)
        {
            m_goodOrder = true;
            m_lastOrder = startingOrder;
        }

        void updateOrder(int newOrder)
        {
            if (newOrder < m_lastOrder)
            {
                m_goodOrder = false;
            }
            m_lastOrder = newOrder;
        }
    }

    MockVoltDB m_voltdb;

    @Override
    public void setUp() {
        m_voltdb = new MockVoltDB();
    }

    @Override
    public void tearDown() throws Exception {
        m_voltdb.shutdown(null);
    }

    public void testBasicDispatch() throws Exception
    {
        FaultDistributor dut = new FaultDistributor(m_voltdb);
        MockFaultHandler unk_handler = new MockFaultHandler(FaultType.UNKNOWN);
        MockFaultHandler node_handler = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(1, unk_handler, FaultType.UNKNOWN);
        dut.registerFaultHandler(1, node_handler, FaultType.NODE_FAILURE);
        dut.reportFault(new VoltFault(FaultType.UNKNOWN));
        unk_handler.m_handledFaults.acquire();
        assertTrue(unk_handler.m_gotFault);
        assertFalse(node_handler.m_gotFault);
        dut.reportFault(new VoltFault(FaultType.NODE_FAILURE));
        node_handler.m_handledFaults.acquire();
        assertTrue(node_handler.m_gotFault);
    }

    // multiple handlers for same type
    public void testMultiHandler() throws Exception
    {
        FaultDistributor dut = new FaultDistributor(m_voltdb);
        MockFaultHandler node_handler1 = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(1, node_handler1, FaultType.NODE_FAILURE);
        MockFaultHandler node_handler2 = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(1, node_handler2, FaultType.NODE_FAILURE);
        dut.reportFault(new VoltFault(FaultType.NODE_FAILURE));
        node_handler1.m_handledFaults.acquire();
        node_handler2.m_handledFaults.acquire();
        assertTrue(node_handler1.m_gotFault);
        assertTrue(node_handler2.m_gotFault);
    }

    // no handler installed for type
    public void testNoHandler() throws Exception
    {
        FaultDistributor dut = new FaultDistributor(m_voltdb);
        // We lie a little bit here to get the NODE_FAILURE routed to UNKNOWN
        // but still set the checking bool correctly
        MockFaultHandler unk_handler = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerDefaultHandler(unk_handler);
        dut.reportFault(new VoltFault(FaultType.NODE_FAILURE));
        unk_handler.m_handledFaults.acquire();
        assertTrue(unk_handler.m_gotFault);
    }

    public void testSingleTypeOrder() throws Exception
    {
        FaultDistributor dut = new FaultDistributor(m_voltdb);
        OrderTracker order_tracker = new OrderTracker(-1);
        MockFaultHandler node_handler1 =
            new MockFaultHandler(FaultType.NODE_FAILURE, 1, order_tracker);
        MockFaultHandler node_handler2 =
            new MockFaultHandler(FaultType.NODE_FAILURE, 2, order_tracker);
        MockFaultHandler node_handler2a =
            new MockFaultHandler(FaultType.NODE_FAILURE, 2, order_tracker);
        MockFaultHandler node_handler5 =
            new MockFaultHandler(FaultType.NODE_FAILURE, 5, order_tracker);
        MockFaultHandler node_handler5a =
            new MockFaultHandler(FaultType.NODE_FAILURE, 5, order_tracker);
        MockFaultHandler node_handler7 =
            new MockFaultHandler(FaultType.NODE_FAILURE, 7, order_tracker);
        // register handlers in non-sequential order to avoid getting lucky
        // with insertion order
        dut.registerFaultHandler(7, node_handler7, FaultType.NODE_FAILURE);
        dut.registerFaultHandler(2, node_handler2a, FaultType.NODE_FAILURE);
        dut.registerFaultHandler(5, node_handler5, FaultType.NODE_FAILURE);
        dut.registerFaultHandler(1, node_handler1, FaultType.NODE_FAILURE);
        dut.registerFaultHandler(5, node_handler5a, FaultType.NODE_FAILURE);
        dut.registerFaultHandler(2, node_handler2, FaultType.NODE_FAILURE);
        dut.reportFault(new VoltFault(FaultType.NODE_FAILURE));
        node_handler7.m_handledFaults.acquire();
        assertTrue(node_handler1.m_gotFault);
        assertTrue(node_handler2.m_gotFault);
        assertTrue(node_handler2a.m_gotFault);
        assertTrue(node_handler5.m_gotFault);
        assertTrue(node_handler5a.m_gotFault);
        assertTrue(node_handler7.m_gotFault);
        assertTrue(order_tracker.m_goodOrder);
    }

    public void testFaultClearing() throws Exception
    {
        FaultDistributor dut = new  FaultDistributor(m_voltdb);
        OrderTracker orderTracker = new OrderTracker(-1);
        MockFaultHandler mh1 = new MockFaultHandler(FaultType.NODE_FAILURE, 1, orderTracker);
        MockFaultHandler mh2 = new MockFaultHandler(FaultType.NODE_FAILURE, 1, orderTracker);
        dut.registerFaultHandler(1, mh1, FaultType.NODE_FAILURE);
        dut.registerFaultHandler(1, mh2, FaultType.NODE_FAILURE);
        VoltFault theFault = new VoltFault(FaultType.NODE_FAILURE);
        dut.reportFault(theFault);

        // this is really a race against the other thread. but
        // will test momentarily that the clear() api is called.
        // if this fails intermittently at all, will have to (fix
        // the software) and write a more deterministic test
        assertEquals(0, mh1.m_clearedFaults.availablePermits());
        assertEquals(0, mh2.m_clearedFaults.availablePermits());

        dut.reportFaultCleared(theFault);
        mh1.m_clearedFaults.acquire();
        mh2.m_clearedFaults.acquire();
    }

    // trigger PPD
    public void testPartitionDetectionTrigger() throws Exception
    {
        // need to add live sites and generate a context for mock functionality
        MockVoltDB voltdb = m_voltdb;
        FaultDistributor dut = new FaultDistributor(voltdb, true);
        voltdb.setFaultDistributor(dut);
        VoltDB.replaceVoltDBInstanceForTest(voltdb);

        voltdb.addPartition(1);

        voltdb.addHost(0);
        voltdb.addSite(100, 0, 1, true);
        voltdb.addSite(1000, 0, 1, false);

        voltdb.addHost(1);
        voltdb.addSite(101, 0, 1, true);
        voltdb.addSite(1010, 0, 1, false);
        voltdb.getCatalogContext();

        // set sites at host 0 down... as if the catalog were updated
        // and the surviving set was not the blessed host id.
        voltdb.killSite(100);
        voltdb.killSite(1000);

        HashSet<Integer> failedSiteIds = new HashSet<Integer>();
        failedSiteIds.add(100);
        failedSiteIds.add(1000);

        // should get a PPD now.
        PPDPolicyDecision makePPDPolicyDecisions = dut.makePPDPolicyDecisions(failedSiteIds);
        assertEquals(PPDPolicyDecision.PartitionDetection, makePPDPolicyDecisions);
    }

    // do not trigger PPD
    public void testPartitionDetectionNoTrigger() throws Exception
    {
        // need to add live sites and generate a context for mock functionality
        MockVoltDB voltdb = m_voltdb;
        FaultDistributor dut = new FaultDistributor(voltdb, true);
        voltdb.setFaultDistributor(dut);
        VoltDB.replaceVoltDBInstanceForTest(voltdb);

        voltdb.addPartition(1);

        voltdb.addHost(0);
        voltdb.addSite(100, 0, 1, true);
        voltdb.addSite(1000, 0, 1, false);

        voltdb.addHost(1);
        voltdb.addSite(101, 1, 1, true);
        voltdb.addSite(1010, 1, 1, false);
        voltdb.getCatalogContext();

        // set sites at host 1 down... as if the catalog were updated
        // and the surviving set contained the blessed host id.
        voltdb.killSite(101);
        voltdb.killSite(1010);

        HashSet<Integer> failedSiteIds = new HashSet<Integer>();
        failedSiteIds.add(101);
        failedSiteIds.add(1010);

        // should get a PPD now.
        PPDPolicyDecision makePPDPolicyDecisions = dut.makePPDPolicyDecisions(failedSiteIds);
        assertEquals(PPDPolicyDecision.NodeFailure, makePPDPolicyDecisions);
    }

    // Bad PPD directory
    public void testPartitionDetectionDirectoryCheck() throws Exception
    {
        // need to add live sites and generate a context for mock functionality
        MockVoltDB voltdb = m_voltdb;
        FaultDistributor dut = new FaultDistributor(voltdb, true);
        voltdb.setFaultDistributor(dut);
        VoltDB.replaceVoltDBInstanceForTest(voltdb);
        SnapshotSchedule schedule = new SnapshotSchedule();
        File badpath = new File("/tmp/doesnotexists");
        badpath.delete();
        schedule.setPath(badpath.getCanonicalPath());
        schedule.setPrefix("foo");
        assertFalse(dut.testPartitionDetectionDirectory(schedule));
        assertTrue(badpath.createNewFile());
        assertFalse(dut.testPartitionDetectionDirectory(schedule));
        assertTrue(badpath.delete());
        assertTrue(badpath.mkdir());
        badpath.setWritable(false);
        badpath.setExecutable(false);
        badpath.setReadable(false);
        assertFalse(dut.testPartitionDetectionDirectory(schedule));
        badpath.setWritable(true);
        badpath.setExecutable(true);
        badpath.setReadable(true);
        assertTrue(dut.testPartitionDetectionDirectory(schedule));
    }

}
