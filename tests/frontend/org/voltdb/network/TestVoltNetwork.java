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

package org.voltdb.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;
import java.util.HashSet;
import junit.framework.*;

public class TestVoltNetwork extends TestCase {


    private static class MockVoltPort extends VoltPort {
        MockVoltPort(VoltNetwork vn, InputHandler handler) {
            super (vn, handler, handler.getExpectedOutgoingMessageSize(), "");
        }

        @Override
        public VoltPort call() {
            m_running = false;
            return null;
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
        public ByteBuffer retrieveNextMessage(Connection c) {
            // TODO Auto-generated method stub
            return null;
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
        public int getExpectedOutgoingMessageSize() {
            return 2048;
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

    public void testInstallInterests() throws InterruptedException {
        new MockSelector();
        VoltNetwork vn = new VoltNetwork();
        MockVoltPort vp = new MockVoltPort(vn, new MockInputHandler());
        MockSelectionKey selectionKey = new MockSelectionKey();
        vp.m_selectionKey = selectionKey;

        // add the port to the changelist set and run install interests.
        // the ports desired ops should be set to the selection key.
        vn.addToChangeList(vp);
        vn.installInterests();
        assertEquals(selectionKey.interestOps(), vp.interestOps());

        // should be able to wash, rinse and repeat this a few times.
        // interesting as voltnetwork recycles some lists underneath
        // the covers.
        vp.setInterests(SelectionKey.OP_WRITE, 0);
        vn.addToChangeList(vp);
        vn.installInterests();
        assertEquals(selectionKey.interestOps(), SelectionKey.OP_WRITE);

        vp.setInterests(SelectionKey.OP_WRITE | SelectionKey.OP_READ, 0);
        vn.addToChangeList(vp);
        vn.installInterests();
        assertEquals(selectionKey.interestOps(), vp.interestOps());
    }

    public void testInvokeCallbacks() throws InterruptedException{
        MockSelector selector = new MockSelector();
        VoltNetwork vn = new VoltNetwork(selector);               // network with fake selector
        MockVoltPort vp = new MockVoltPort(vn, new MockInputHandler());             // implement abstract run()
        MockSelectionKey selectionKey = new MockSelectionKey();   // fake selection key

        // glue the key, the selector and the port together.
        selectionKey.interestOps(SelectionKey.OP_WRITE);
        selector.setFakeKey(selectionKey);
        vp.m_selectionKey = selectionKey;
        selectionKey.attach(vp);
        selectionKey.readyOps(SelectionKey.OP_WRITE);

        // invoke call backs and see that the volt port has the expected
        // selected operations.
        vn.invokeCallbacks();
        assertEquals(SelectionKey.OP_WRITE, vp.readyOps());

        // and another time through, should have the new interests selected
        vp.setInterests(SelectionKey.OP_ACCEPT, 0);
        selectionKey.readyOps(SelectionKey.OP_ACCEPT);
        vn.installInterests();
        vn.invokeCallbacks();
        vn.shutdown();
        assertEquals(SelectionKey.OP_ACCEPT, vp.readyOps());
    }
}
