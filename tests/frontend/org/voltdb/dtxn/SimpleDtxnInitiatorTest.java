/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.dtxn;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.MockMailbox;
import org.voltdb.network.Connection;
import org.voltdb.network.NIOReadStream;
import org.voltdb.network.WriteStream;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.CommandLog;

public class SimpleDtxnInitiatorTest extends TestCase {
    MockMessenger m_messenger;
    MockMailbox mqueue;
    SimpleDtxnInitiator initiator;
    int[] partitions0;
    int numPartitions0 = 0;
    int[] partitions0And1;
    int numPartitions0And1 = 0;

    static final StoredProcedureInvocation work = new StoredProcedureInvocation();
    static final ByteBuffer workResult = ByteBuffer.allocate(10);

    static final int NUM_PARTITIONS = 2;
    static final int LOCAL_SITE = 3;
    static final int LOCAL_MAILBOX = 0;

    /*private class WasRun implements Runnable {
        @SuppressWarnings("unused")
        private boolean wasRun = false;
        @Override
        public void run() {
            wasRun = true;
        }
    }*/

    //private WasRun onBackPressure;
    //private WasRun offBackPressure;

    class MockMessenger implements Messenger
    {
        @Override
        public Mailbox createMailbox(int siteId, int mailboxId, boolean log)
        {
            return new MockMailbox();
        }

        @Override
        public void createMailbox(int siteId, int mailboxId, Mailbox mailbox) {
            throw new UnsupportedOperationException();
        }
    }

    /*@Override
    public void setUp() {
        m_messenger = new MockMessenger();
        onBackPressure = new WasRun();
        offBackPressure = new WasRun();
        initiator = new SimpleDtxnInitiator(m_messenger, 0, LOCAL_SITE, 0,
                                            onBackPressure, offBackPressure);
        partitions0 = new int[50];
        numPartitions0 = 0;
        partitions0[numPartitions0++] = 0;
        partitions0And1 = new int[50];
        numPartitions0And1 = 0;
        partitions0And1[numPartitions0And1++] = 1;
        partitions0And1[numPartitions0And1++] = 0;

        work.setProcName("foo");
    }*/

    @SuppressWarnings("unused")
    private final class DummyConnection implements Connection {
        private final DummyWriteStream writeStream = new DummyWriteStream();
        @Override
        public void disableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NIOReadStream readStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WriteStream writeStream() {
            return writeStream;
        }

        @Override
        public String getHostnameOrIP()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void scheduleRunnable(Runnable r) {
            // TODO Auto-generated method stub

        }

        @Override
        public void unregister() {
            // TODO Auto-generated method stub

        }

        @Override
        public long connectionId()
        {
            // TODO Auto-generated method stub
            return -1;
        }

    }

    private final class DummyWriteStream implements WriteStream {

        @SuppressWarnings("unused")
        private ClientResponse response;

        @Override
        public boolean enqueue(final BBContainer c) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean enqueue(final FastSerializable f) {
            response = (ClientResponse)f;
            return true;
        }

        @Override
        public boolean enqueue(final FastSerializable f, final int expectedSize) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean enqueue(final DeferredSerialization ds) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean enqueue(final ByteBuffer b) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean hadBackPressure() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public int calculatePendingWriteDelta(long now) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean isEmpty() {
            throw new RuntimeException("Not implemented.");
        }

        @Override
        public int getOutstandingMessageCount()
        {
            // TODO Auto-generated method stub
            return 0;
        }

    }

//    public void testCreateTransactionComplete() {
//        final DummyConnection c = new DummyConnection();
//        initiator.createTransaction( 42, "dummy", work, true, true, new int[] { LOCAL_SITE }, 1, c,  32, 0);
//        assertTrue(mqueue.lastEquals(LOCAL_SITE, 0));
//        final InitiateTask request = (InitiateTask) mqueue.popLastMessage();
//        assertEquals(LOCAL_SITE, request.getInitiatorSiteId());
//        //assertEquals(LOCAL_MAILBOX, request.getRequestingMailboxId());
//        assertEquals(work.getProcName(), request.getStoredProcedureName());
//        assertTrue(request.isSinglePartition());
//
//        mqueue.deliver(new InitiateResponse(request));
//
//        final ClientResponse response = c.writeStream.response;
//        assertNotNull(response);
//        final Iterator<Object> i = initiator.m_stats.getStatsRowKeyIterator();
//        assertNotNull(i);
//        assertTrue(i.hasNext());
//        final Object rowKey = i.next();
//        assertFalse(i.hasNext());
//        assertNotNull(rowKey);
//        assertTrue(rowKey instanceof String);
//        assertTrue(((String)rowKey).equals("foo$42"));
//        final Object statsRows[][] = initiator.m_stats.getStatsRows();
//        assertNotNull(statsRows);
//        assertEquals(1, statsRows.length);
//        assertNotNull(statsRows[0]);
//        assertEquals(statsRows[0][1], new Integer(LOCAL_SITE));
//        assertEquals(statsRows[0][2], new Integer(42));
//        assertEquals(statsRows[0][3], "foo");
//        assertEquals(statsRows[0][4], new Integer(1));
//        //assertTrue(response.wasCommitted());
//        //assertEquals(workResult, response.getClientResponseData());
//    }

    /*public void testTwoSinglePartitionTransactions() {
        initiator.createTransaction(42, 79, work, partitions0, numPartitions0);
        assertTrue(mqueue.lastEquals(0, 0));
        RemoteWorkRequest request =
                mqueue.popLastMessage().getWork(MockFastSerializable.class, null);
        long txnId1 = request.getTxnId();
        initiator.createTransaction(43, 0, work, partitions0, numPartitions0);
        assertTrue(mqueue.lastEquals(0, 0));
        request = mqueue.popLastMessage().getWork(MockFastSerializable.class, null);
        long txnId2 = request.getTxnId();

        mqueue.next = new SimpleDtxnUnion(
                new RemoteWorkResponse(txnId1, WorkUnit.Result.SUCCESS, workResult));
        Response response = initiator.getCompletedTransaction();
        assertEquals(42, response.connectionId);
        assertEquals(79, response.connectionSeqNo);
        assertTrue(response.committed);

        mqueue.next = new SimpleDtxnUnion(
                new RemoteWorkResponse(txnId2, WorkUnit.Result.ABORT, workResult));
        response = initiator.getCompletedTransaction();
        assertEquals(43, response.connectionId);
        assertEquals(0, response.connectionSeqNo);
        assertFalse(response.committed);
    }

    public void testMultiPartitionAndSinglePartition() {
        initiator.createTransaction(42, 79, work, partitions0And1, numPartitions0And1);
        assertTrue(mqueue.lastEquals(1, 0));
        RemoteWorkRequest request =
                mqueue.popLastMessage().getWork(MockFastSerializable.class, null);
        long txnId1 = request.getTxnId();
        assertTrue(request.isMultiPartition());

        // Single partition transaction
        initiator.createTransaction(43, 0, work, partitions0, numPartitions0);
        assertTrue(mqueue.lastEquals(0, 0));
        request = mqueue.popLastMessage().getWork(MockFastSerializable.class, null);
        long txnId2 = request.getTxnId();
        assertFalse(request.isMultiPartition());

        mqueue.next = new SimpleDtxnUnion(
                new RemoteWorkResponse(txnId2, WorkUnit.Result.SUCCESS, workResult));
        Response response = initiator.getCompletedTransaction();
        assertEquals(43, response.connectionId);
        assertEquals(0, response.connectionSeqNo);
        assertTrue(response.committed);

        // multi-partition transaction completes
        mqueue.next = new SimpleDtxnUnion(
                new RemoteWorkResponse(txnId1, WorkUnit.Result.ABORT, workResult));
        response = initiator.getCompletedTransaction();
        assertEquals(42, response.connectionId);
        assertEquals(79, response.connectionSeqNo);
        assertFalse(response.committed);
    }

    public void testTwoMultiPartitionTransactions() {
        initiator.createTransaction(42, 79, work, partitions0And1, numPartitions0And1);
        assertTrue(mqueue.lastEquals(1, 0));
        RemoteWorkRequest request =
                mqueue.popLastMessage().getWork(MockFastSerializable.class, null);
        long txnId1 = request.getTxnId();
        assertTrue(request.isMultiPartition());

        // The 2nd multi-partition transaction is *not* sent out
        initiator.createTransaction(43, 0, work, partitions0And1, numPartitions0And1);
        assertTrue(mqueue.noSentMessages());

        mqueue.next = new SimpleDtxnUnion(
                new RemoteWorkResponse(txnId1, WorkUnit.Result.SUCCESS, workResult));
        Response response = initiator.getCompletedTransaction();
        assertEquals(42, response.connectionId);
        assertEquals(79, response.connectionSeqNo);
        assertTrue(response.committed);

        // One multi-partition transaction completes, another executes
        assertTrue(mqueue.lastEquals(1, 0));
        request = mqueue.popLastMessage().getWork(MockFastSerializable.class, null);
        long txnId2 = request.getTxnId();
        assertTrue(request.isMultiPartition());

        mqueue.next = new SimpleDtxnUnion(
                new RemoteWorkResponse(txnId2, WorkUnit.Result.ABORT, workResult));
        response = initiator.getCompletedTransaction();
        assertEquals(43, response.connectionId);
        assertEquals(0, response.connectionSeqNo);
        assertFalse(response.committed);
    }*/

    /*public void testDeserializeResponse() {
        initiator.createTransaction(42, 79, work, partitions0, numPartitions0);
        long txnId = initiator.getMostRecentTxnId();
        // requires payload type to deserialize
        mqueue.next = FastSerializableTestUtil.roundTrip(new SimpleDtxnUnion(
                new RemoteWorkResponse(txnId, WorkUnit.Result.SUCCESS, workResult)));
        assertNotNull(initiator.getCompletedTransaction());
    }*/
}
