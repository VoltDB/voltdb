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

public class TestFaultManager extends TestCase
{
    public class MockFaultHandler implements FaultHandler
    {
        FaultType m_faultType;
        boolean m_gotFault;

        public MockFaultHandler(FaultType type)
        {
            m_faultType = type;
            m_gotFault = false;
        }

        @Override
        public void faultOccured(VoltFault fault)
        {
            if (fault.getFaultType() == m_faultType)
            {
                m_gotFault = true;
            }
        }
    }

    public void testBasicDispatch()
    {
        FaultDistributor dut = new FaultDistributor();
        MockFaultHandler unk_handler = new MockFaultHandler(FaultType.UNKNOWN);
        MockFaultHandler node_handler = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(FaultType.UNKNOWN, unk_handler);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler);
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
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler1);
        MockFaultHandler node_handler2 = new MockFaultHandler(FaultType.NODE_FAILURE);
        dut.registerFaultHandler(FaultType.NODE_FAILURE, node_handler2);
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

    // threadedness?
}
