/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.voltdb.fault.VoltFault.FaultType;

import junit.framework.TestCase;

public class TestFaultDistributor extends TestCase
{
    public class MockFaultHandler implements FaultHandler
    {
        FaultType m_faultType;
        boolean m_gotFault;
        int m_order;
        OrderTracker m_orderTracker;

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
        public void faultOccured(VoltFault fault)
        {
            if (fault.getFaultType() == m_faultType)
            {
                m_gotFault = true;
            }
            if (m_orderTracker != null)
            {
                m_orderTracker.updateOrder(m_order);
            }
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

    public void testBasicDispatch()
    {
        FaultDistributor dut = new FaultDistributor();
        MockFaultHandler unk_handler = new MockFaultHandler(FaultType.UNKNOWN);
        MockFaultHandler node_handler = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(FaultType.UNKNOWN, unk_handler, 1);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler, 1);
        dut.reportFault(new VoltFault(FaultType.UNKNOWN));
        assertTrue(unk_handler.m_gotFault);
        assertFalse(node_handler.m_gotFault);
        dut.reportFault(new VoltFault(FaultType.NODE_FAILURE));
        assertTrue(node_handler.m_gotFault);
    }

    // multiple handlers for same type
    public void testMultiHandler()
    {
        FaultDistributor dut = new FaultDistributor();
        MockFaultHandler node_handler1 = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler1, 1);
        MockFaultHandler node_handler2 = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler2, 1);
        dut.reportFault(new VoltFault(FaultType.NODE_FAILURE));
        assertTrue(node_handler1.m_gotFault);
        assertTrue(node_handler2.m_gotFault);
    }

    // no handler installed for type
    public void testNoHandler()
    {
        FaultDistributor dut = new FaultDistributor();
        // We lie a little bit here to get the NODE_FAILURE routed to UNKNOWN
        // but still set the checking bool correctly
        MockFaultHandler unk_handler = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerDefaultHandler(unk_handler);
        dut.reportFault(new VoltFault(FaultType.NODE_FAILURE));
        assertTrue(unk_handler.m_gotFault);
    }

    public void testSingleTypeOrder()
    {
        FaultDistributor dut = new FaultDistributor();
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
        assertTrue(node_handler1.m_gotFault);
        assertTrue(node_handler2.m_gotFault);
        assertTrue(node_handler2a.m_gotFault);
        assertTrue(node_handler5.m_gotFault);
        assertTrue(node_handler5a.m_gotFault);
        assertTrue(node_handler7.m_gotFault);
        assertTrue(order_tracker.m_goodOrder);
    }

    // threadedness?
}
