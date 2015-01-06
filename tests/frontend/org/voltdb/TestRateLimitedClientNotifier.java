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

package org.voltdb;

import com.google_voltpatches.common.base.Predicates;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.cache.Cache;
import com.google_voltpatches.common.cache.CacheBuilder;
import com.google_voltpatches.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.network.MockConnection;
import org.voltcore.network.MockWriteStream;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.RateLimitedClientNotifier.Node;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestRateLimitedClientNotifier {
    private RateLimitedClientNotifier notifier = null;
    private MockConnection connection;
    private MockWriteStream mws;
    private ClientInterfaceHandleManager cihm;
    private BlockingQueue messages;

    public static Supplier<DeferredSerialization> getSupplier(final ByteBuffer buf) {
        return new Supplier<DeferredSerialization>() {
            @Override
            public DeferredSerialization get() {
                 return new DeferredSerialization() {
                    @Override
                    public void serialize(final ByteBuffer outbuf) throws IOException {
                        outbuf.put(buf);
                    }
                    @Override
                    public void cancel() {}
                    @Override
                    public int getSerializedSize() {
                        return buf.remaining();
                    }
                };
            }
        };
    }

    /*
     * Check that equality and hash code work as expected
     * where nodes that are heads of identical lists are identical
     * and nodes that have the same supplier and next node are also
     * equal
     */
    @Test
    public void testNode() throws Exception {
        Supplier<DeferredSerialization> sup = getSupplier(null);
        Node n = new Node(sup, null);
        Node n2 = new Node(sup, null);
        Node diffSup = new Node(getSupplier(null), null);
        assertFalse(n.equals(null));
        assertTrue(n.equals(n2));
        assertEquals(n.hashCode(), n2.hashCode());
        assertTrue(n2.equals(n));
        assertTrue(n.equals(n));
        assertFalse(n.equals(diffSup));
        assertFalse(n.hashCode() == diffSup.hashCode());

        Supplier<DeferredSerialization> sup2 = getSupplier(null);
        Node n3 = new Node(sup2, n);
        assertFalse(n3.equals(n2));
        assertFalse(n3.hashCode() == n2.hashCode());
        Node n3_2 = new Node(sup2, n);

        Supplier<DeferredSerialization> sup3 = getSupplier(null);
        Node n4 = new Node(sup3, n3);
        Node n4_2 = new Node(sup3, n3_2);
        assertTrue(n4.equals(n4_2));
        assertTrue(n4.hashCode() == n4_2.hashCode());
    }

    Cache<Node, Node> cache =
            CacheBuilder.newBuilder()
                    .maximumSize(10000).concurrencyLevel(1).build();
    private Node getNode(Supplier<DeferredSerialization> s, Node n) throws Exception {
        Node node = new Node(s, n);
        return cache.get(node, node);
    }

    /*
     * Check caching postfixes of nodes works
     */
    @Test
    public void testNodeCaching() throws Exception {
        Supplier<DeferredSerialization> sup = getSupplier(null);
        Node n = getNode(sup, null);
        Node n2 = getNode(sup, null);
        Node diffSup = getNode(getSupplier(null), null);
        assertFalse(n.equals(null));
        assertTrue(n.equals(n2));
        assertEquals(n.hashCode(), n2.hashCode());
        assertTrue(n2.equals(n));
        assertTrue(n.equals(n));
        assertFalse(n.equals(diffSup));
        assertFalse(n.hashCode() == diffSup.hashCode());

        Supplier<DeferredSerialization> sup2 = getSupplier(null);
        Node n3 = getNode(sup2, n);
        assertFalse(n3.equals(n2));
        assertFalse(n3.hashCode() == n2.hashCode());
        Node n3_2 = getNode(sup2, n);

        Supplier<DeferredSerialization> sup3 = getSupplier(null);
        Node n4 = getNode(sup3, n3);
        Node n4_2 = getNode(sup3, n3_2);
        assertTrue(n4.equals(n4_2));
        assertTrue(n4.hashCode() == n4_2.hashCode());
        assertEquals(cache.size(), 4);
    }

    /*
     * Quick test to see if you can get a notification in and out, and that a notification
     * is not delivered if the predicate fails
     */
    @Test
    public void testNotification() throws Exception
    {
        notifier.start();
        ByteBuffer buf = ByteBuffer.allocate(1);
        Supplier<DeferredSerialization> sup = getSupplier(buf);

        notifier.queueNotification(ImmutableList.of(cihm), sup, Predicates.<ClientInterfaceHandleManager>alwaysFalse());
        assertNull(messages.poll(50, TimeUnit.MILLISECONDS));

        notifier.queueNotification(ImmutableList.of(cihm), sup, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        messages.take();
    }

    /*
     * Test that submitting a dupe notification is skipped
     * Multiple dedupe tests follow that cover different internal code paths
     * for deduping
     */
    @Test
    public void testDedupe() throws Exception
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        Supplier<DeferredSerialization> sup = getSupplier(buf);

        notifier.queueNotification(ImmutableList.of(cihm), sup, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        notifier.queueNotification(ImmutableList.of(cihm), sup, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        notifier.start();
        mws.m_messages.take();
        assertNull(mws.m_messages.poll(50, TimeUnit.MILLISECONDS));
    }

    /*
     * Test that submitting a dupe of a second notification is skipped
     */
    @Test
    public void testDedupeMultiple() throws Exception
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        Supplier<DeferredSerialization> sup = getSupplier(buf);
        Supplier<DeferredSerialization> sup2 = getSupplier(buf);

        notifier.queueNotification(ImmutableList.of(cihm), sup, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        notifier.queueNotification(ImmutableList.of(cihm), sup2, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        notifier.queueNotification(ImmutableList.of(cihm), sup2, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        notifier.start();
        messages.take();
        messages.take();
        assertNull(mws.m_messages.poll(50, TimeUnit.MILLISECONDS));
    }

    /*
     * Test that the 2nd notification is skipped, but the third is delivered
     */
    @Test
    public void testDedupeMultiple2() throws Exception
    {
        ByteBuffer buf = ByteBuffer.allocate(1);
        Supplier<DeferredSerialization> sup = getSupplier(buf);
        Supplier<DeferredSerialization> sup2 = getSupplier(buf);
        Supplier<DeferredSerialization> sup3 = getSupplier(buf);

        notifier.queueNotification(ImmutableList.of(cihm), sup, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        notifier.queueNotification(ImmutableList.of(cihm), sup2, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        notifier.queueNotification(ImmutableList.of(cihm), sup, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        notifier.queueNotification(ImmutableList.of(cihm), sup3, Predicates.<ClientInterfaceHandleManager>alwaysTrue());
        notifier.start();
        messages.take();
        messages.take();
        messages.take();
        assertNull(mws.m_messages.poll(50, TimeUnit.MILLISECONDS));
    }

    @Before
    public void setUp() throws Exception {
        RateLimitedClientNotifier.WARMUP_MS = 0;
        notifier = new RateLimitedClientNotifier();
        connection = new MockConnection();
        mws = connection.m_writeStream;
        messages = mws.m_messages;
        cihm = new ClientInterfaceHandleManager(false, connection, null );
    }

    @After
    public void tearDown() throws Exception {
        notifier.shutdown();
    }


}
