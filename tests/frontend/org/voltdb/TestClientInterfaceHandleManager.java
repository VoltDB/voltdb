/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.voltcore.network.Connection;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestClientInterfaceHandleManager {

    @Test
    public void testGetAndFind() throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        ClientInterfaceHandleManager dut = new ClientInterfaceHandleManager();
        long handle = dut.getHandle(true, 7, 31337, mockConnection, false, 10, 10l);
        assertEquals(7, ClientInterfaceHandleManager.getPartIdFromHandle(handle));
        assertEquals(0, ClientInterfaceHandleManager.getSeqNumFromHandle(handle));
        ClientInterfaceHandleManager.Iv2InFlight inflight = dut.findHandle(handle);
        assertEquals(handle, inflight.m_ciHandle);
        assertEquals(31337, inflight.m_clientHandle);

        handle = dut.getHandle(false, 12, 31338, mockConnection, false, 10, 10l);
        assertEquals(ClientInterfaceHandleManager.MP_PART_ID,
                ClientInterfaceHandleManager.getPartIdFromHandle(handle));
        assertEquals(0, ClientInterfaceHandleManager.getSeqNumFromHandle(handle));
        inflight = dut.findHandle(handle);
        assertEquals(handle, inflight.m_ciHandle);
        assertEquals(31338, inflight.m_clientHandle);
    }

    @Test
    public void testGetAndRemove() throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        ClientInterfaceHandleManager dut = new ClientInterfaceHandleManager();
        List<Long> handles = new ArrayList<Long>();
        for (int i = 0; i < 10; i++) {
            handles.add(dut.getHandle(true, 7, 31337 + i, mockConnection, false, 10, 10l));
        }
        System.out.println("Removing handle: " + handles.get(5));
        dut.removeHandle(handles.get(5));
        for (int i = 0; i < 10; i++) {
            System.out.println("Looking for handle: " + handles.get(i));
            ClientInterfaceHandleManager.Iv2InFlight inflight = dut.findHandle(handles.get(i));
            if (i != 5)
            {
                assertEquals((long)handles.get(i), inflight.m_ciHandle);
            }
            else {
                assertEquals(null, inflight);
            }
        }
    }

    @Test
    public void testGetSkipMissingHandles() throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        ClientInterfaceHandleManager dut = new ClientInterfaceHandleManager();
        List<Long> handles = new ArrayList<Long>();
        for (int i = 0; i < 10; i++) {
            handles.add(dut.getHandle(true, 7, 31337 + i, mockConnection, false, 10, 10l));
        }
        // pretend handles 0-4 were lost
        for (int i = 5; i < 10; i++) {
            ClientInterfaceHandleManager.Iv2InFlight inflight = dut.findHandle(handles.get(i));
            assertEquals((long)handles.get(i), inflight.m_ciHandle);
            assertEquals(31337 + i, inflight.m_clientHandle);
        }
    }
}
