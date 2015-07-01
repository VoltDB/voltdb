/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltcore.network;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;

import junit.framework.TestCase;

/*
 * Not a lot of functionality in VoltPort. The only tricky bit
 * is setting interests - write a short test to verify that functionality.
 */

public class TestVoltPort extends TestCase {

    // stub some addToChangeList for test
    private static class MockVoltNetwork extends VoltNetwork {

        public MockVoltNetwork() {
            super(0, null, "Test");
        }
    }

    // implement abstract run() method.
    private static class MockVoltPort extends VoltPort {
        MockVoltPort(VoltNetwork vn, Channel channel) throws UnknownHostException {
            super (vn, null, new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 21212), vn.m_pool );
        }
    }


    public void testSetInterests() throws Exception {
        MockVoltNetwork vn = new MockVoltNetwork();
        MockVoltPort vp = new MockVoltPort(vn, null);

        // no interests initially
        assertEquals(0, vp.interestOps());

        // can set one interest.
        vp.setInterests(SelectionKey.OP_READ, 0);
        assertEquals(SelectionKey.OP_READ, vp.interestOps());

        // can set a second non-destructively with the first
        vp.setInterests(SelectionKey.OP_WRITE, 0);
        assertEquals(SelectionKey.OP_READ|SelectionKey.OP_WRITE, vp.interestOps());

        // can set an interest twice without detriment.
        vp.setInterests(SelectionKey.OP_READ, 0);
        assertEquals(SelectionKey.OP_READ|SelectionKey.OP_WRITE, vp.interestOps());

        // can add and remove interests in the same command
        vp.setInterests(SelectionKey.OP_CONNECT, SelectionKey.OP_READ);
        assertEquals(SelectionKey.OP_CONNECT|SelectionKey.OP_WRITE, vp.interestOps());

        // can clear all interests
        vp.setInterests(0, SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE);
        assertEquals(0, vp.interestOps());

        // Unnecessary un-set does no harm
        vp.setInterests(SelectionKey.OP_READ, 0);
        vp.setInterests(0, SelectionKey.OP_WRITE);
        assertEquals(SelectionKey.OP_READ, vp.interestOps());
    }
}
