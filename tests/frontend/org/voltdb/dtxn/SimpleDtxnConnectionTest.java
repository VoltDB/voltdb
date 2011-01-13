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

import junit.framework.TestCase;

public class SimpleDtxnConnectionTest extends TestCase {
    /*MockMailbox mqueue;
    SimpleDtxnConnection connection;
    int nextUndo;

    static final MockFastSerializable work1 = new MockFastSerializable();
    static final MockFastSerializable work2 = new MockFastSerializable();
    static final MockFastSerializable.Other fragment = new MockFastSerializable.Other();
    static final MockFastSerializable o1 = new MockFastSerializable();
    static final MockFastSerializable o2 = new MockFastSerializable();
    static final int LOCAL_SITE = 0;
    static final int CLIENT_SITE = 1;
    static final int REMOTE_SITE = 105;
    static final int MAILBOX_ID = 42;
    static final int SP_TXN_ID = 99;
    static final int MP_TXN_ID = -42;
    static final int FIRST_UNDO = 77;
    static final InititateTask singlePartition = new InitiateTask(CLIENT_SITE,
            SP_TXN_ID, work1, new int[0],
            RemoteWorkRequest.Type.SINGLE_PARTITION);
    static final SimpleDtxnUnion singlePartition2 = new SimpleDtxnUnion(new RemoteWorkRequest(
            CLIENT_SITE, MAILBOX_ID,
            SP_TXN_ID + 1, work2, new int[0],
            RemoteWorkRequest.Type.SINGLE_PARTITION));
    static final SimpleDtxnUnion multiParticipant = new SimpleDtxnUnion(new RemoteWorkRequest(
            CLIENT_SITE, MAILBOX_ID, MP_TXN_ID, fragment, new int[0],
            RemoteWorkRequest.Type.MULTI_PARTITION_PARTICIPANT));
    static final SimpleDtxnUnion multiParticipantDependencies = new SimpleDtxnUnion(
            new RemoteWorkRequest(CLIENT_SITE, MAILBOX_ID, MP_TXN_ID, fragment, new int[]{1, 2},
                    RemoteWorkRequest.Type.MULTI_PARTITION_PARTICIPANT));
    static final SimpleDtxnUnion multiInitiator = new SimpleDtxnUnion(new RemoteWorkRequest(
            CLIENT_SITE, MAILBOX_ID,
            MP_TXN_ID, work2, new int[0],
            RemoteWorkRequest.Type.MULTI_PARTITION_INITIATOR));
    static final ByteBuffer response = ByteBuffer.allocate(10);
    static final SimpleDtxnUnion dependency1 =
            new SimpleDtxnUnion(new RemoteDependency(MP_TXN_ID, 1, o1));
    static final SimpleDtxnUnion dependency2 =
            new SimpleDtxnUnion(new RemoteDependency(MP_TXN_ID, 2, o2));

    public void setUp() {
        mqueue = new MockMailbox<SimpleDtxnUnion>();
        connection = new SimpleDtxnConnection(LOCAL_SITE, mqueue, MockFastSerializable.class,
                MockFastSerializable.Other.class, MockFastSerializable.class);
        nextUndo = FIRST_UNDO;
    }

    public void testNoWork() {
        WorkUnit work = connection.getNextWorkUnit();
        assertNull(work);
    }

    public void testGetNextPending() {
        mqueue.next = singlePartition;
        assertEquals(work1, connection.getNextWorkUnit().getPayload());

        // Getting the next work unit triggers an assert while there is a pending work unit
        try {
            connection.getNextWorkUnit();
            fail("assertion expected");
        } catch (AssertionError e) {}
    }

    private void verifyCompletedTransaction(WorkUnit w, WorkUnit.Result result, int txnId) {
        // Return a response and verify it
        connection.completedWorkUnit(result, response, null, nextUndo++);

        assertTrue(mqueue.lastEquals(CLIENT_SITE, MAILBOX_ID));
        RemoteWorkResponse r = mqueue.popLastMessage().getResponse();
        assertEquals(txnId, r.getTxnId());
        assertEquals(result, r.getResult());
        assertEquals(response, r.getPayload());
    }

    private void verifyAbortedTransaction(int numUndone) {
        WorkUnit.Undo u = (WorkUnit.Undo) connection.getNextWorkUnit();
        long[] undone = new long[numUndone];
        for (int i = 0; i < undone.length; ++i) {
            undone[i] = nextUndo - 1 - i;
        }
        assertTrue(Arrays.equals(undone, u.getUndoLogIds()));
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, 0);
    }

    public void testSinglePartitionSuccess() {
        mqueue.next = singlePartition;
        WorkUnit w = connection.getNextWorkUnit();
        assertEquals(work1, w.getPayload());
        verifyCompletedTransaction(w, WorkUnit.Result.SUCCESS, SP_TXN_ID);
    }

    public void testSinglePartitionAbort() {
        mqueue.next = singlePartition;
        WorkUnit w = connection.getNextWorkUnit();
        verifyCompletedTransaction(w, WorkUnit.Result.ABORT, SP_TXN_ID);
        verifyAbortedTransaction(1);
    }

    public void testBadCreateWork() {
        mqueue.next = singlePartition;
        connection.getNextWorkUnit();

        // Try to create work for another partition
        try {
            connection.createWorkUnit(42, work1, null);
            fail("assertion expected");
        } catch (AssertionError e) {}

        // Complete the work unit
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, response, null, -1);
    }

    public void testSinglePartitionMultipleWorkSuccess() {
        mqueue.next = singlePartition;
        WorkUnit w = connection.getNextWorkUnit();
        assertEquals(work1, w.getPayload());

        // Add a transaction to the queue to ensure the right one(s) are selected
        mqueue.next = singlePartition2;

        // Create some new work units.
        connection.createWorkUnit(LOCAL_SITE, work2, null);
        connection.createWorkUnit(LOCAL_SITE, work1, new int[] { 42, 7 });
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, -1);

        WorkUnit next = connection.getNextWorkUnit();
        assertEquals(work2, next.getPayload());
        connection.sendData(o1, 7);
        connection.sendData(o2, 42);
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, -1);

        next = connection.getNextWorkUnit();
        assertEquals(work1, next.getPayload());
        assertEquals(o1, next.getDependency(7).get(0));
        assertEquals(o2, next.getDependency(42).get(0));
        verifyCompletedTransaction(next, WorkUnit.Result.SUCCESS, SP_TXN_ID);
    }

    public void testSinglePartitionMultipleWorkAbort() {
        mqueue.next = singlePartition;
        WorkUnit w = connection.getNextWorkUnit();
        assertEquals(work1, w.getPayload());

        // Add a transaction to the queue to ensure the right one(s) are selected
        mqueue.next = singlePartition2;

        // Create some new work units.
        connection.createWorkUnit(LOCAL_SITE, work2, null);
        connection.createWorkUnit(LOCAL_SITE, work1, new int[] { 42, 7 });
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, nextUndo++);

        // Abort the next work unit: message should get sent
        WorkUnit next = connection.getNextWorkUnit();
        assertEquals(work2, next.getPayload());
        connection.completedWorkUnit(WorkUnit.Result.ABORT, response, null, nextUndo++);
        RemoteWorkResponse r = mqueue.popLastMessage().getResponse();
        assertEquals(WorkUnit.Result.ABORT, r.getResult());
        assertEquals(response, r.getPayload());

        // Abort = undo work
        verifyAbortedTransaction(2);

        // The next work should be the queued work unit
        w = connection.getNextWorkUnit();
        assertEquals(work2, w.getPayload());
    }

    public void testMultiPartitionParticipantSuccess() {
        mqueue.next = multiParticipant;
        WorkUnit w = connection.getNextWorkUnit();
        assertEquals(fragment, w.getPayload());

        // As a participant, we must send output, not give a response
        connection.sendData(work1, 1);
        assertTrue(mqueue.lastEquals(CLIENT_SITE, 0));
        RemoteDependency d = mqueue.popLastMessage().getDependency(MockFastSerializable.class);
        assertEquals(MP_TXN_ID, d.getTxnId());
        assertEquals(1, d.getDependencyId());
        assertEquals(work1, d.getPayload());

        // Completing the work unit doesn't actually do anything
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, -1);
        assertTrue(mqueue.noSentMessages());

        // TODO: Test adding another work unit as part of this transaction

        // Add a single partition txn: can't get executed while multi-partition is active.
        mqueue.next = singlePartition;
        assertNull(connection.getNextWorkUnit());

        // Commit the transaction: the single partition txn can now be executed
        mqueue.next = new SimpleDtxnUnion(
                new RemoteWorkResponse(MP_TXN_ID, WorkUnit.Result.SUCCESS, null));
        assertEquals(work1, connection.getNextWorkUnit().getPayload());
    }

    public void testMultiPartitionParticipantAbort() {
        mqueue.next = multiParticipant;
        WorkUnit w = connection.getNextWorkUnit();
        assertEquals(fragment, w.getPayload());

        // Participant aborts the transaction
        connection.completedWorkUnit(WorkUnit.Result.ABORT, null, null, nextUndo++);
        assertTrue(mqueue.lastEquals(CLIENT_SITE, MAILBOX_ID));
        RemoteWorkResponse r = mqueue.popLastMessage().getResponse();
        assertEquals(MP_TXN_ID, r.getTxnId());
        assertEquals(WorkUnit.Result.ABORT, r.getResult());
        assertNull(r.getPayload());

        // Another work unit arrives and is ignored: the txn has been aborted
        mqueue.next = multiParticipant;
        assertNull(connection.getNextWorkUnit());

        // A dependency arrives and is ignored: the txn has been aborted
        mqueue.next = new SimpleDtxnUnion(new RemoteDependency(MP_TXN_ID, 1, o1));
        assertNull(connection.getNextWorkUnit());

        // Add a single partition txn: MP still active so it can't run
        // TODO: Change this for the abort case.
        mqueue.next = singlePartition;
        assertNull(connection.getNextWorkUnit());

        // Now the MP is aborted: get the undo buffer
        mqueue.next = new SimpleDtxnUnion(
                new RemoteWorkResponse(MP_TXN_ID, WorkUnit.Result.ABORT, null));
        verifyAbortedTransaction(1);

        // the next work unit is finally dequeued
        assertEquals(work1, connection.getNextWorkUnit().getPayload());
    }

    public void testWorkThenDependencies() {
        // Can't dispatch it: no dependencies
        mqueue.next = multiParticipantDependencies;
        WorkUnit w = connection.getNextWorkUnit();
        assertNull(w);

        // Give it a dependency
        mqueue.next = dependency1;
        w = connection.getNextWorkUnit();
        assertNull(w);

        // Last dependency: actually gives us the work
        mqueue.next = dependency2;
        w = connection.getNextWorkUnit();
        assertEquals(fragment, w.getPayload());
        assertEquals(o1, w.getDependency(1).get(0));
        assertEquals(o2, w.getDependency(2).get(0));
    }

    public void testDependenciesThenWork() {
        // Give dependencies for work that does not yet exist
        mqueue.next = dependency1;
        WorkUnit w = connection.getNextWorkUnit();
        assertNull(w);
        mqueue.next = dependency2;
        w = connection.getNextWorkUnit();
        assertNull(w);

        // Have the dependencies already: can dispatch
        mqueue.next = multiParticipantDependencies;
        w = connection.getNextWorkUnit();
        assertEquals(fragment, w.getPayload());
        assertEquals(o1, w.getDependency(1).get(0));
        assertEquals(o2, w.getDependency(2).get(0));
    }

    private WorkUnit startMultiPartitionCoordinator() {
        mqueue.next = multiInitiator;
        WorkUnit w = connection.getNextWorkUnit();
        assertEquals(work2, w.getPayload());

        // Create a "resume" work unit: a dependency and a NULL payload
        connection.createWorkUnit(LOCAL_SITE, null, new int[]{1});
        assertTrue(mqueue.noSentMessages());

        // Send a remote work unit
        connection.createWorkUnit(REMOTE_SITE, work2, null);
        assertTrue(mqueue.lastEquals(REMOTE_SITE, 0));
        RemoteWorkRequest r = mqueue.popLastMessage().getWork(
                MockFastSerializable.class, MockFastSerializable.Other.class);
        assertEquals(MP_TXN_ID, r.getTxnId());
        assertEquals(work2, r.getWork());
        assertTrue(r.isMultiPartition());

        return w;
    }

    public void testMultiPartitionCoordinatorSuccess() {
        startMultiPartitionCoordinator();

        // No work until dependency is resolved
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, -1);
        assertNull(connection.getNextWorkUnit());
        mqueue.next = dependency1;
        WorkUnit w2 = connection.getNextWorkUnit();
        assertEquals(1, w2.getDependencies().size());

        // Completing the work unit, completes the transaction. This sends multiple messages
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, response, null, -1);

        // The last message is to the participant
        assertTrue(mqueue.lastEquals(REMOTE_SITE, 0));
        RemoteWorkResponse res = mqueue.popLastMessage().getResponse();
        assertEquals(MP_TXN_ID, res.getTxnId());
        assertEquals(WorkUnit.Result.SUCCESS, res.getResult());
        assertNull(res.getPayload());

        // The first message is to the client
        assertTrue(mqueue.lastEquals(CLIENT_SITE, MAILBOX_ID));
        res = mqueue.popLastMessage().getResponse();
        assertEquals(MP_TXN_ID, res.getTxnId());
        assertEquals(WorkUnit.Result.SUCCESS, res.getResult());
        assertEquals(response, res.getPayload());
    }

    public void testMultiPartitionCoordinatorLocalAbort() {
        startMultiPartitionCoordinator();

        // Abort the local work unit
        connection.completedWorkUnit(WorkUnit.Result.ABORT, null, null, -1);

        // Connection sends abort message to participant
        assertTrue(mqueue.lastEquals(REMOTE_SITE, 0));
        RemoteWorkResponse r = mqueue.popLastMessage().getResponse();
        assertEquals(MP_TXN_ID, r.getTxnId());
        assertEquals(WorkUnit.Result.ABORT, r.getResult());

        // Next work = undo
        WorkUnit.Undo undo = (WorkUnit.Undo) connection.getNextWorkUnit();
        assertTrue(Arrays.equals(new long[]{-1}, undo.getUndoLogIds()));
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, 0);

        // Participant sends abort that arrives late
        mqueue.next = new SimpleDtxnUnion(r);
        assertNull(connection.getNextWorkUnit());
    }

    public void testMultiPartitionCoordinatorRemoteAbort() {
        startMultiPartitionCoordinator();

        // Abort the remote work unit
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, -1);
        assertNull(connection.getNextWorkUnit());
        mqueue.next = new SimpleDtxnUnion(
                new RemoteWorkResponse(MP_TXN_ID, WorkUnit.Result.ABORT, null));

        // Next work = undo
        WorkUnit.Undo undo = (WorkUnit.Undo) connection.getNextWorkUnit();
        assertTrue(Arrays.equals(new long[]{-1}, undo.getUndoLogIds()));

        // Abort message goes out to client
        assertTrue(mqueue.lastEquals(CLIENT_SITE, MAILBOX_ID));
        RemoteWorkResponse r = mqueue.popLastMessage().getResponse();
        assertEquals(MP_TXN_ID, r.getTxnId());
        assertEquals(WorkUnit.Result.ABORT, r.getResult());

        // abort message also goes to participant
        assertTrue(mqueue.lastEquals(REMOTE_SITE, 0));
        r = mqueue.popLastMessage().getResponse();
        assertEquals(MP_TXN_ID, r.getTxnId());
        assertEquals(WorkUnit.Result.ABORT, r.getResult());

        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, 0);
    }

    public void testMultiPartitionCoordinatorDependencyAfterAbort() {
        startMultiPartitionCoordinator();

        // Abort the local work unit
        connection.completedWorkUnit(WorkUnit.Result.ABORT, null, null, -1);
        WorkUnit.Undo undo = (WorkUnit.Undo) connection.getNextWorkUnit();
        assertTrue(Arrays.equals(new long[]{-1}, undo.getUndoLogIds()));
        connection.completedWorkUnit(WorkUnit.Result.SUCCESS, null, null, 0);

        // Dependency arrives: It should be ignored; it belongs to an aborted transaction
        assertNull(connection.getNextWorkUnit());
        mqueue.next = new SimpleDtxnUnion(
                new RemoteDependency(MP_TXN_ID, 0, new MockFastSerializable()));
        assertNull(connection.getNextWorkUnit());

        // Single partition work arrives and is dispatched
        mqueue.next = singlePartition;
        assertNotNull(connection.getNextWorkUnit());
    }

    public void testDeserializeTransaction() {
        // requires the transaction payload type
        mqueue.next = FastSerializableTestUtil.roundTrip(singlePartition);
        WorkUnit w = connection.getNextWorkUnit();
        assertNotNull(w.getPayload());
        assertTrue(w.getPayload() instanceof MockFastSerializable);
    }

    public void testDeserializeFragment() {
        // requires the fragment payload type
        mqueue.next = FastSerializableTestUtil.roundTrip(multiParticipant);
        WorkUnit w = connection.getNextWorkUnit();
        assertNotNull(w.getPayload());
        assertTrue(w.getPayload() instanceof MockFastSerializable.Other);
    }

    public void testDeserializeResponse() {
        // requires the response payload type
        SimpleDtxnUnion msg = new SimpleDtxnUnion(
                new RemoteWorkResponse(MP_TXN_ID, WorkUnit.Result.SUCCESS, null));
        mqueue.next = FastSerializableTestUtil.roundTrip(msg);
        assertNull(connection.getNextWorkUnit());
    }

    public void testDeserializeDependency() {
        // requires the dependency payload type
        mqueue.next = FastSerializableTestUtil.roundTrip(dependency1);
        assertNull(connection.getNextWorkUnit());
    }*/

    public void testNothing() {
        assertTrue(true);
    }
}
