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
package org.voltdb.dtxn;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltdb.ClientResponseImpl;
import org.voltdb.MockVoltDB;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.fault.SiteFailureFault;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

public class TestDtxnInitiatorMailbox extends TestCase
{
    static int INITIATOR_SITE_ID = 5;
    static int HOST_ID = 0;
    static int MESSAGE_SIZE = 13;

    MockVoltDB m_mockVolt = null;

    private final Map<Long, Integer> m_siteMap = new HashMap<Long, Integer>();

    class MockWriteStream extends org.voltcore.network.MockWriteStream
    {
        boolean m_gotResponse;

        MockWriteStream()
        {
            m_gotResponse = false;
        }

        void reset()
        {
            m_gotResponse = false;
        }

        boolean gotResponse()
        {
            return m_gotResponse;
        }

        @Override
        public synchronized void enqueue(ByteBuffer buf)
        {
            m_gotResponse = true;
            notify();
        }
    }

    private final HostMessenger m_mockMessenger = new HostMessenger() {

    };

    class MockConnection extends org.voltcore.network.MockConnection
    {
        MockWriteStream m_writeStream;

        MockConnection(MockWriteStream writeStream)
        {
            m_writeStream = writeStream;
        }

        @Override
        public WriteStream writeStream()
        {
            return m_writeStream;
        }
    }

    class MockInitiator extends TransactionInitiator
    {
        int m_reduceSize;
        int m_reduceCount;

        MockInitiator()
        {
            m_reduceSize = 0;
            m_reduceCount = 0;
        }

        @Override
        public boolean createTransaction(long connectionId,
                                      String connectionHostname,
                                      boolean adminConnection,
                                      StoredProcedureInvocation invocation,
                                      boolean isReadOnly,
                                      boolean isSinglePartition,
                                      boolean isEveryPartition,
                                      int[] partitions, int numPartitions,
                                      Object clientData, int messageSize,
                                      long now,
                                      boolean allowMismatchedResults)
        {
            return true;
        }

        @Override
        public boolean createTransaction(long connectionId,
                                      String connectionHostname,
                                      boolean adminConnection,
                                      long txnId,
                                      long timestamp,
                                      StoredProcedureInvocation invocation,
                                      boolean isReadOnly,
                                      boolean isSinglePartition,
                                      boolean isEverySite,
                                      int[] partitions,
                                      int numPartitions,
                                      Object clientData,
                                      int messageSize,
                                      long now,
                                      boolean allowMismatchedResults)
        {
            return true;
        }

        @Override
        public long getMostRecentTxnId()
        {
            return 0;
        }

        @Override
        protected void increaseBackpressure(int messageSize)
        {
        }

        @Override
        protected void reduceBackpressure(int messageSize)
        {
            m_reduceCount++;
            m_reduceSize += messageSize;
        }

        @Override
        public long tick()
        {
            return 0;
        }

        @Override
        public void notifyExecutionSiteRejoin(ArrayList<Long> executorSiteIds) {}

        @Override
        public Map<Long, long[]> getOutstandingTxnStats()
        {
            return null;
        }

        @Override
        public void setSendHeartbeats(boolean val) {}

        @Override
        public void sendHeartbeat(long txnId) {}

        @Override
        public boolean isOnBackPressure() {
            return false;
        }

        @Override
        public void removeConnectionStats(long connectionId) {}

        @Override
        public void sendSentinel(long txnId, int partitionId) {}

        @Override
        public void sendEOLMessage(int partitionId) {}
    }

    InFlightTxnState createTxnState(long txnId, int[] coordIds, boolean readOnly,
                                    boolean isSinglePart)
    {
        long now = EstTime.currentTimeMillis();
        InFlightTxnState retval = new InFlightTxnState(
                txnId, coordIds[0], null, new long[]{}, readOnly,
                isSinglePart, new StoredProcedureInvocation(),
                m_testConnect, MESSAGE_SIZE, now, 0, "", false, false);
        if (coordIds.length > 1) {
            for (int i = 1; i < coordIds.length; i++)
                retval.addCoordinator(coordIds[i]);
        }
        return retval;
    }

    VoltTable[] createResultSet(String thing)
    {
        VoltTable[] retval = new VoltTable[1];

        retval[0] = new VoltTable(new ColumnInfo("thing", VoltType.STRING));
        retval[0].addRow(thing);

        return retval;
    }

    InitiateResponseMessage createInitiateResponse(long txnId, int coordId,
                                            boolean readOnly, boolean isSinglePart, boolean recovering,
                                            VoltTable[] results)
    {
        InitiateTaskMessage task = new InitiateTaskMessage(INITIATOR_SITE_ID, coordId, txnId,
                                             readOnly, isSinglePart,
                                             new StoredProcedureInvocation(),
                                             Long.MAX_VALUE);
        InitiateResponseMessage response = new InitiateResponseMessage(task);
        response.setResults(new ClientResponseImpl((byte) 0, results, ""));
        response.setRecovering(recovering);
        return response;
    }

    @Override
    public void setUp()
    {
        m_mockVolt = new MockVoltDB();
        VoltDB.crashMessage = null;
        VoltDB.wasCrashCalled = false;
        VoltDB.ignoreCrash = true;

        Long site1 = CoreUtils.getHSIdFromHostAndSite(HOST_ID, 1);
        Long site0 = CoreUtils.getHSIdFromHostAndSite(HOST_ID, 0);
        Long site2 = CoreUtils.getHSIdFromHostAndSite(HOST_ID, 2);
        m_mockVolt.addSite( site1, 0);
        m_mockVolt.addSite( site0, 0);
        m_mockVolt.addSite( site2, MailboxType.Initiator);
        m_siteMap.clear();
        m_siteMap.put(site0, 0);
        m_siteMap.put(site1, 0);
        m_siteMap.put(site2, 0);
        m_mockVolt.setFaultDistributor(new FaultDistributor(m_mockVolt));
        VoltDB.replaceVoltDBInstanceForTest(m_mockVolt);
    }

    @Override
    public void tearDown() throws Exception {
        m_mockVolt.shutdown(null);
    }

    public void testNonReplicatedBasicOps()
    {
        MockInitiator initiator = new MockInitiator();
        ExecutorTxnIdSafetyState safetyState = new ExecutorTxnIdSafetyState(m_siteMap);
        DtxnInitiatorMailbox dim = new DtxnInitiatorMailbox(safetyState, m_mockMessenger);
        dim.setInitiator(initiator);
        dim.setHSId(INITIATOR_SITE_ID);
        m_testStream.reset();
        // Single-partition read-only txn
        dim.addPendingTxn(createTxnState(0, new int[] {0}, true, true));
        dim.deliver(createInitiateResponse(0, 0, true, true, false, createResultSet("dude")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(1, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE, initiator.m_reduceSize);
        m_testStream.reset();
        // multi-partition read-only txn
        dim.addPendingTxn(createTxnState(1, new int[] {0}, true, false));
        dim.deliver(createInitiateResponse(1, 0, true, false, false, createResultSet("dude")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(2, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE * 2, initiator.m_reduceSize);
        m_testStream.reset();
        // Single-partition read-write txn
        dim.addPendingTxn(createTxnState(2, new int[] {0}, false, true));
        dim.deliver(createInitiateResponse(2, 0, false, true, false, createResultSet("dude")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(3, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE * 3, initiator.m_reduceSize);
        m_testStream.reset();
        // multi-partition read-write txn
        dim.addPendingTxn(createTxnState(3, new int[] {0}, false, false));
        dim.deliver(createInitiateResponse(3, 0, false, false, false, createResultSet("dude")));
        assertEquals(4, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE * 4, initiator.m_reduceSize);
        assertTrue(m_testStream.gotResponse());
    }

    // Multi-partition transactions don't differ in behavior at the initiator
    // so we'll only throw in the single-partition cases
    public void testReplicatedBasicOps()
    {
        MockInitiator initiator = new MockInitiator();
        ExecutorTxnIdSafetyState safetyState = new ExecutorTxnIdSafetyState(m_siteMap);
        DtxnInitiatorMailbox dim = new DtxnInitiatorMailbox(safetyState, m_mockMessenger);
        dim.setInitiator(initiator);
        dim.setHSId(INITIATOR_SITE_ID);
        m_testStream.reset();
        // Single-partition read-only txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, true, true));
        dim.deliver(createInitiateResponse(0, 0, true, true, false, createResultSet("dude")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(0, initiator.m_reduceCount);
        assertEquals(0, initiator.m_reduceSize);
        m_testStream.reset();
        dim.deliver(createInitiateResponse(0, 1, true, true, false, createResultSet("dude")));
        assertFalse(m_testStream.gotResponse());
        assertEquals(1, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE, initiator.m_reduceSize);
        m_testStream.reset();
        // Single-partition read-write txn
        dim.addPendingTxn(createTxnState(2, new int[] {0,1}, false, true));
        dim.deliver(createInitiateResponse(2, 0, false, true, false, createResultSet("dude")));
        assertFalse(m_testStream.gotResponse());
        assertEquals(1, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE, initiator.m_reduceSize);
        dim.deliver(createInitiateResponse(2, 1, false, true, false, createResultSet("dude")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(2, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE * 2, initiator.m_reduceSize);
    }

    // Test responsing where some things are recovering
    // Again, only matters for single-partition work
    public void testRecoveringBasicOps()
    {
        MockInitiator initiator = new MockInitiator();
        ExecutorTxnIdSafetyState safetyState = new ExecutorTxnIdSafetyState(m_siteMap);
        DtxnInitiatorMailbox dim = new DtxnInitiatorMailbox(safetyState, m_mockMessenger);
        dim.setInitiator(initiator);
        dim.setHSId(INITIATOR_SITE_ID);

        m_testStream.reset();
        // Single-partition read-only txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, true, true));
        // recovering message
        dim.deliver(createInitiateResponse(0, 0, true, true, true, createResultSet("fido")));
        assertFalse(m_testStream.gotResponse());
        assertEquals(0, initiator.m_reduceCount);
        assertEquals(0, initiator.m_reduceSize);

        m_testStream.reset();
        // Single-partition read-only txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, true, true));
        // recovering message
        dim.deliver(createInitiateResponse(0, 0, true, true, true, createResultSet("fido")));
        // valid message gets sent out
        dim.deliver(createInitiateResponse(0, 1, true, true, false, createResultSet("dude")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(1, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE, initiator.m_reduceSize);

        m_testStream.reset();
        // Single-partition read-only txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, true, true));
        // valid message gets sent out
        dim.deliver(createInitiateResponse(0, 0, true, true, false, createResultSet("dude")));
        // recovering message
        dim.deliver(createInitiateResponse(0, 1, true, true, true, createResultSet("fido")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(2, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE * 2, initiator.m_reduceSize);

        m_testStream.reset();
        // Single-partition read-only txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, false, true));
        // valid message gets sent out
        dim.deliver(createInitiateResponse(0, 0, false, true, false, createResultSet("dude")));
        // recovering message
        dim.deliver(createInitiateResponse(0, 1, false, true, true, createResultSet("fido")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(3, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE * 3, initiator.m_reduceSize);

        m_testStream.reset();
        // Single-partition read-only txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, false, true));
        // recovering message
        dim.deliver(createInitiateResponse(0, 0, false, true, true, createResultSet("fido")));
        // valid message gets sent out
        dim.deliver(createInitiateResponse(0, 1, false, true, false, createResultSet("dude")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(4, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE * 4, initiator.m_reduceSize);
    }

    public void testInconsistentResults()
    {
        MockInitiator initiator = new MockInitiator();
        ExecutorTxnIdSafetyState safetyState = new ExecutorTxnIdSafetyState(m_siteMap);
        DtxnInitiatorMailbox dim = new DtxnInitiatorMailbox(safetyState, m_mockMessenger);
        dim.setInitiator(initiator);
        dim.setHSId(INITIATOR_SITE_ID);

        m_testStream.reset();

        // Single-partition read-only txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, true, true));
        dim.deliver(createInitiateResponse(0, 0, true, true, false, createResultSet("dude")));
        assertTrue(m_testStream.gotResponse());
        m_testStream.reset();
        boolean caught = false;
        try
        {
            dim.deliver(createInitiateResponse(0, 1, true, true, false, createResultSet("sweet")));
        }
        catch (AssertionError e)
        {
            caught = true;
        }
        assertTrue(caught);
        m_testStream.reset();

        // Single-partition read-write txn
        dim.addPendingTxn(createTxnState(2, new int[] {0,1}, false, true));
        dim.deliver(createInitiateResponse(2, 0, false, true, false, createResultSet("dude")));
        assertFalse(m_testStream.gotResponse());
        caught = false;
        try
        {
            dim.deliver(createInitiateResponse(2, 1, true, true, false, createResultSet("sweet")));
        }
        catch (AssertionError e)
        {
            caught = true;
        }
        assertTrue(caught);
    }

    // Failure cases to test:
    // for read/write:
    // add two pending txns
    // -- receive one, fail the second site, verify that we get enqueue
    // -- fail the second site, receive one, verify that we get enqueue
    // -- fail both, verify ?? (exception/crash of some sort?)
    // -- receive both, fail one, verify ??
    // replace two with three or for for tricksier cases?
    // have two or three different outstanding txn IDs
    // read-only harder since stuff lingers and there's no way to look at it
    //

    public void testEarlyReadWriteFailure()
    {
        MockInitiator initiator = new MockInitiator();
        ExecutorTxnIdSafetyState safetyState = new ExecutorTxnIdSafetyState(m_siteMap);
        DtxnInitiatorMailbox dim = new DtxnInitiatorMailbox(safetyState, m_mockMessenger);
        dim.setInitiator(initiator);
        dim.setHSId(INITIATOR_SITE_ID);

        m_testStream.reset();
        // Single-partition read-write txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, false, true));
        dim.removeSite(0);
        dim.deliver(createInitiateResponse(0, 1, true, true, false, createResultSet("dude")));
        assertTrue(m_testStream.gotResponse());
        assertEquals(1, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE, initiator.m_reduceSize);
    }

    public void testMidReadWriteFailure()
    {
        MockInitiator initiator = new MockInitiator();
        ExecutorTxnIdSafetyState safetyState = new ExecutorTxnIdSafetyState(m_siteMap);
        DtxnInitiatorMailbox dim = new DtxnInitiatorMailbox(safetyState, m_mockMessenger);
        dim.setInitiator(initiator);
        dim.setHSId(INITIATOR_SITE_ID);

        m_testStream.reset();
        // Single-partition read-write txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, false, true));
        dim.deliver(createInitiateResponse(0, 1, true, true, false, createResultSet("dude")));
        dim.removeSite(0);
        assertTrue(m_testStream.gotResponse());
        assertEquals(1, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE, initiator.m_reduceSize);
    }

    public void testMultipleTxnIdMidFailure()
    {
        MockInitiator initiator = new MockInitiator();
        ExecutorTxnIdSafetyState safetyState = new ExecutorTxnIdSafetyState(m_siteMap);
        DtxnInitiatorMailbox dim = new DtxnInitiatorMailbox(safetyState, m_mockMessenger);
        dim.setInitiator(initiator);
        dim.setHSId(INITIATOR_SITE_ID);

        m_testStream.reset();
        // Single-partition read-write txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, false, true));
        dim.addPendingTxn(createTxnState(1, new int[] {0,1}, false, true));
        dim.deliver(createInitiateResponse(0, 1, true, true, false, createResultSet("dude")));
        dim.deliver(createInitiateResponse(1, 1, true, true, false, createResultSet("sweet")));
        dim.removeSite(0);
        assertTrue(m_testStream.gotResponse());
        assertEquals(2, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE * 2, initiator.m_reduceSize);
    }

//    public void testTotalFailure()
//    {
//        MockInitiator initiator = new MockInitiator();
//        DtxnInitiatorQueue dut = new DtxnInitiatorQueue(SITE_ID);
//        dut.setInitiator(initiator);
//        m_testStream.reset();
//        // Single-partition read-only txn
//        dut.addPendingTxn(createTxnState(0, 0, false, true));
//        dut.addPendingTxn(createTxnState(0, 1, false, true));
//        dut.removeSite(0);
//        dut.removeSite(1);
//    }

    public void testFaultNotification() throws Exception
    {
        MockInitiator initiator = new MockInitiator();
        ExecutorTxnIdSafetyState safetyState = new ExecutorTxnIdSafetyState(m_siteMap);
        DtxnInitiatorMailbox dim = new DtxnInitiatorMailbox(safetyState, m_mockMessenger);
        dim.setInitiator(initiator);
        dim.setHSId(INITIATOR_SITE_ID);

        m_testStream.reset();
        // Single-partition read-write txn
        dim.addPendingTxn(createTxnState(0, new int[] {0,1}, false, true));
        dim.deliver(createInitiateResponse(0, 1, true, true, false, createResultSet("dude")));

        synchronized (m_testStream) {
            SiteFailureFault site_failure = new SiteFailureFault(
                    Arrays.asList(new Long[] { 0L })// the non exec site in setUp()
                    );
            VoltDB.instance().getFaultDistributor().reportFault(site_failure);
            m_testStream.wait(10000);
        }

        Thread.sleep(100);
        assertTrue(m_testStream.gotResponse());
        assertEquals(1, initiator.m_reduceCount);
        assertEquals(MESSAGE_SIZE, initiator.m_reduceSize);
    }

    MockWriteStream m_testStream = new MockWriteStream();
    MockConnection m_testConnect = new MockConnection(m_testStream);
}
