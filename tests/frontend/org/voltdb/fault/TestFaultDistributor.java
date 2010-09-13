/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

import org.voltdb.MockVoltDB;
import org.voltdb.fault.VoltFault.FaultType;

import junit.framework.TestCase;

import java.util.*;
import java.util.concurrent.Semaphore;

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

    public void testBasicDispatch() throws Exception
    {
        FaultDistributor dut = new FaultDistributor(new MockVoltDB());
        MockFaultHandler unk_handler = new MockFaultHandler(FaultType.UNKNOWN);
        MockFaultHandler node_handler = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(FaultType.UNKNOWN, unk_handler, 1);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler, 1);
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
        FaultDistributor dut = new FaultDistributor(new MockVoltDB());
        MockFaultHandler node_handler1 = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler1, 1);
        MockFaultHandler node_handler2 = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler2, 1);
        dut.reportFault(new VoltFault(FaultType.NODE_FAILURE));
        node_handler1.m_handledFaults.acquire();
        node_handler2.m_handledFaults.acquire();
        assertTrue(node_handler1.m_gotFault);
        assertTrue(node_handler2.m_gotFault);
    }

    // no handler installed for type
    public void testNoHandler() throws Exception
    {
        FaultDistributor dut = new FaultDistributor(new MockVoltDB());
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
        FaultDistributor dut = new FaultDistributor(new MockVoltDB());
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
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler7, 7);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler2a, 2);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler5, 5);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler1, 1);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler5a, 5);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler2, 2);
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
        FaultDistributor dut = new  FaultDistributor(new MockVoltDB());
        OrderTracker orderTracker = new OrderTracker(-1);
        MockFaultHandler mh1 = new MockFaultHandler(FaultType.NODE_FAILURE, 1, orderTracker);
        MockFaultHandler mh2 = new MockFaultHandler(FaultType.NODE_FAILURE, 1, orderTracker);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, mh1, 1);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, mh2, 1);
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
        MockVoltDB voltdb = new MockVoltDB();
        voltdb.addHost(0);
        voltdb.addHost(1);

        // enable possible partition detection (PPD)
        FaultDistributor dut = new FaultDistributor(voltdb, true);
        MockFaultHandler nodeFaultHdlr = new MockFaultHandler(FaultType.NODE_FAILURE);
        MockFaultHandler clusterPartHdlr = new MockFaultHandler(FaultType.CLUSTER_PARTITION);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, nodeFaultHdlr, 1);
        dut.registerFaultHandler(FaultType.CLUSTER_PARTITION, clusterPartHdlr, 2);

        // verify node fault was promoted to cluster partition fault
        dut.reportFault(new NodeFailureFault(0, "hostname"));
        clusterPartHdlr.m_handledFaults.acquire();
        assertTrue(clusterPartHdlr.m_gotFault);

        // no node faults were produced. this handler has higher priority
        // than the cluster handler; presumably it ran if cluster semaphore
        // was acquired.
        assertFalse(nodeFaultHdlr.m_handledFaults.tryAcquire());

        // clear the cluster partition fault and verify it
        // reported cleared to the cluster partition handler,
        // not the node failure handler
        ClusterPartitionFault cpf = new ClusterPartitionFault(new NodeFailureFault(0, "ignored"));
        dut.reportFaultCleared(cpf);
        clusterPartHdlr.m_clearedFaults.acquire();
        assertFalse(nodeFaultHdlr.m_clearedFaults.tryAcquire());
    }

    // do not trigger PPD
    public void testPartitionDetectionNoTrigger() throws Exception
    {
        MockVoltDB voltdb = new MockVoltDB();
        voltdb.addHost(0);
        voltdb.addHost(1);
        voltdb.addHost(3);

        // enable possible partition detection (PPD)
        FaultDistributor dut = new FaultDistributor(voltdb, true);
        MockFaultHandler nodeFaultHdlr = new MockFaultHandler(FaultType.NODE_FAILURE);
        MockFaultHandler clusterPartHdlr = new MockFaultHandler(FaultType.CLUSTER_PARTITION);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, nodeFaultHdlr, 1);
        dut.registerFaultHandler(FaultType.CLUSTER_PARTITION, clusterPartHdlr, 2);

        // verify node fault was not promoted to cluster partition fault
        dut.reportFault(new NodeFailureFault(0, "hostname"));
        nodeFaultHdlr.m_handledFaults.acquire();
        assertTrue(nodeFaultHdlr.m_gotFault);
        assertFalse(clusterPartHdlr.m_handledFaults.tryAcquire());
    }


}
