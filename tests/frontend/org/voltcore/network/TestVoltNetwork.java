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

package org.voltcore.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Set;

import io.netty.buffer.CompositeByteBuf;
import jsr166y.ThreadLocalRandom;
import junit.framework.TestCase;

public class TestVoltNetwork extends TestCase {


    private static class MockVoltPort extends VoltPort {
        MockVoltPort(VoltNetwork vn, InputHandler handler) throws UnknownHostException {
            super (vn, handler, new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 21212), vn.m_pool);
        }

        @Override
        public void run() {
            m_running = false;
        }
    }

    private static class MockInputHandler implements InputHandler {

        @Override
        public int getMaxRead() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) {
            // TODO Auto-generated method stub

        }

        @Override
        public ByteBuffer retrieveNextMessage(NIOReadStream c) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ByteBuffer retrieveNextMessage(CompositeByteBuf bb) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int getNextMessageLength() {
            return 0;
        }

        @Override
        public void started(Connection c) {
            // TODO Auto-generated method stub

        }

        @Override
        public void starting(Connection c) {
            // TODO Auto-generated method stub

        }

        @Override
        public void stopped(Connection c) {
            // TODO Auto-generated method stub

        }

        @Override
        public void stopping(Connection c) {
            // TODO Auto-generated method stub

        }

        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {}
            };
        }

        @Override
        public Runnable onBackPressure() {
            return new Runnable() {
                @Override
                public void run() {}
            };
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return null;
        }

        @Override
        public long connectionId() {
            return 0;
        }
    }

    public static class MockSelectionKey extends SelectionKey {
        @Override
        public
        void cancel() {
        }

        @Override
        public SelectableChannel channel() {
            return null;
        }

        @Override
        public int interestOps() {
            return m_interestOps;
        }

        @Override
        public SelectionKey interestOps(int interestOps) {
            m_interestOps = interestOps;
            return this;
        }

        public SelectionKey readyOps(int readyOps) {
            m_readyOps = readyOps;
            return this;
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public int readyOps() {
            return m_readyOps;
        }

        @Override
        public Selector selector() {
            return null;
        }

        public int m_interestOps;
        public int m_readyOps;
        public Object m_fakeAttachment;
    }

    public static class MockSelector extends Selector {
        public SelectionKey m_fakeKey = null;

        MockSelector() {
        }

        void setFakeKey(SelectionKey fakeKey) {
            m_fakeKey = fakeKey;
        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub
        }

        @Override
        public boolean isOpen() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public Set<SelectionKey> keys() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public SelectorProvider provider() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int select() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int select(long timeout) throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int selectNow() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public Set<SelectionKey> selectedKeys() {
            Set<SelectionKey> aset = new HashSet<SelectionKey>();
            aset.add(m_fakeKey);
            return aset;
        }

        @Override
        public Selector wakeup() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private void runInstallInterests (VoltNetwork voltNetwork, VoltPort voltPort) {
        SelectionKey sk = new MockSelectionKey();
        voltPort.m_selectionKey = sk;

        // add the port to the changelist set and run install interests.
        // the ports desired ops should be set to the selection key.
        voltNetwork.addToChangeList(voltPort);
        voltNetwork.installInterests(voltPort);
        assertEquals(sk.interestOps(), voltPort.interestOps());

        // should be able to wash, rinse and repeat this a few times.
        // interesting as voltnetwork recycles some lists underneath
        // the covers.
        voltPort.setInterests(SelectionKey.OP_WRITE, 0);
        voltNetwork.addToChangeList(voltPort);
        voltNetwork.installInterests(voltPort);
        assertEquals(sk.interestOps(), SelectionKey.OP_WRITE);

        voltPort.setInterests(SelectionKey.OP_WRITE | SelectionKey.OP_READ, 0);
        voltNetwork.addToChangeList(voltPort);
        voltNetwork.installInterests(voltPort);
        assertEquals(sk.interestOps(), voltPort.interestOps());
    }

    public void testInstallInterests() throws Exception {
        VoltNetwork vn = new VoltNetwork( 0, null, "Test");
        MockVoltPort vp = new MockVoltPort(vn, new MockInputHandler());
        runInstallInterests(vn, vp);
    }

    private void runInvokeCallbacks(MockSelector baseSelector, VoltNetwork vn, VoltPort vp) throws Exception {
        MockSelectionKey selectionKey = new MockSelectionKey();   // fake selection key

        // glue the key, the selector and the port together.
        selectionKey.interestOps(SelectionKey.OP_WRITE);
        baseSelector.setFakeKey(selectionKey);
        vp.m_selectionKey = selectionKey;
        selectionKey.attach(vp);
        selectionKey.readyOps(SelectionKey.OP_WRITE);

        // invoke call backs and see that the volt port has the expected
        // selected operations.
        vn.invokeCallbacks(ThreadLocalRandom.current());
        assertEquals(SelectionKey.OP_WRITE, vp.readyOps());

        // and another time through, should have the new interests selected
        vp.setInterests(SelectionKey.OP_ACCEPT, 0);
        selectionKey.readyOps(SelectionKey.OP_ACCEPT);
        vn.installInterests(vp);
        vn.invokeCallbacks(ThreadLocalRandom.current());
        vn.shutdown();
        assertEquals(SelectionKey.OP_ACCEPT, vp.readyOps());
    }

    public void testInvokeCallbacks() throws Exception {
        MockSelector selector = new MockSelector();
        VoltNetwork vn = new VoltNetwork(selector);               // network with fake selector
        MockVoltPort vp = new MockVoltPort(vn, new MockInputHandler());             // implement abstract run()
        runInvokeCallbacks(selector, vn, vp);
    }

}
