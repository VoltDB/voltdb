/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.iv2;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltcore.utils.Pair;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest;
import org.voltdb.SnapshotCompletionMonitor;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.RejoinMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public class TestJoinProducer {
    private static final long m_coordinatorHSId = 10l;
    private static final long m_HSId = 101l;

    private JoinProducer m_producer;
    private SiteTaskerQueue m_taskerQueue;
    private InitiatorMailbox m_mb;
    private SnapshotCompletionMonitor m_snapshotMonitor;

    @Before
    public void setUp()
    {
        m_snapshotMonitor = mock(SnapshotCompletionMonitor.class);
        VoltDBInterface volt = mock(VoltDBInterface.class);
        when(volt.getSnapshotCompletionMonitor()).thenReturn(m_snapshotMonitor);
        VoltDB.replaceVoltDBInstanceForTest(volt);

        m_mb = mock(InitiatorMailbox.class);
        when(m_mb.getHSId()).thenReturn(m_HSId);

        m_taskerQueue = new SiteTaskerQueue();
        m_producer = new JoinProducer(2, m_taskerQueue);
        m_producer.setMailbox(m_mb);
    }

    @Test
    public void testInitiation() throws IOException
    {
        SiteProcedureConnection site = mock(SiteProcedureConnection.class);
        assertTrue(m_producer.acceptPromotion());

        // Once initiated, it should insert itself into the tasker queue
        RejoinMessage rm = new RejoinMessage(m_coordinatorHSId, RejoinMessage.Type.INITIATION,
                "test-snapshot");
        m_producer.deliver(rm);
        assertFalse(m_taskerQueue.isEmpty());

        m_taskerQueue.poll().runForRejoin(site, null);
        assertFalse(m_taskerQueue.isEmpty());
    }

    @Test
    public void testReceiveFirstFragment() throws IOException
    {
        SiteProcedureConnection site = mock(SiteProcedureConnection.class);

        testInitiation();
        m_producer.logTask(mock(FragmentTaskMessage.class));
        m_taskerQueue.poll().runForRejoin(site, null);

        // Check if the rejoin message is sent to the coordinator
        ArgumentCaptor<Long> hsidCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<RejoinMessage> msgCaptor = ArgumentCaptor.forClass(RejoinMessage.class);
        verify(m_mb).send(hsidCaptor.capture(), msgCaptor.capture());
        assertEquals(m_coordinatorHSId, hsidCaptor.getValue().longValue());
        RejoinMessage rm = msgCaptor.getValue();
        assertEquals(RejoinMessage.Type.FIRST_FRAGMENT_RECEIVED, rm.getType());

        assertFalse(m_taskerQueue.isEmpty());
    }

    @Test
    public void testSnapshotDataTransfer() throws IOException
    {
        SiteProcedureConnection site = mock(SiteProcedureConnection.class);

        testReceiveFirstFragment();

        // Fake a table block
        VoltTable table = TableHelper.quickTable("T (PKEY:INTEGER, VALUE:VARCHAR36-N, VALUE1:VARCHAR17700-N) P(0)");
        table.addRow(3, "hello", "world");

        // deliver a snapshot block
        RejoinMessage rm = new RejoinMessage(m_coordinatorHSId, 4, table.getBuffer());
        m_producer.deliver(rm);

        // run the producer again to load the snapshot data
        m_taskerQueue.poll().runForRejoin(site, null);

        ArgumentCaptor<Integer> tableIdCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(site).loadTable(anyLong(), tableIdCaptor.capture(), any(VoltTable.class));
        assertEquals(4, tableIdCaptor.getValue().intValue());

        assertFalse(m_taskerQueue.isEmpty());
    }

    @Test
    public void testSnapshotFinish() throws IOException
    {
        testSnapshotDataTransfer();

        // Get the snapshot interest
        ArgumentCaptor<SnapshotCompletionInterest> interestCaptor =
                ArgumentCaptor.forClass(SnapshotCompletionInterest.class);
        verify(m_snapshotMonitor).addInterest(interestCaptor.capture());
        SnapshotCompletionInterest interest = interestCaptor.getValue();

        // Fake snapshot completion event
        Map<Integer, Long> partitionTxnIds = new HashMap<Integer, Long>();
        partitionTxnIds.put(2, 3l);
        Map<String, Map<Integer, Pair<Long, Long>>> exportSeqNums =
                new HashMap<String, Map<Integer, Pair<Long, Long>>>();
        SnapshotCompletionInterest.SnapshotCompletionEvent event =
                new SnapshotCompletionInterest.SnapshotCompletionEvent("test-snapshot", 1,
                        partitionTxnIds, false, null, exportSeqNums);
        interest.snapshotCompleted(event);

        reset(m_mb);
        SiteProcedureConnection site = mock(SiteProcedureConnection.class);
        m_taskerQueue.poll().runForRejoin(site, null);

        // Check if the rejoin message is sent to the coordinator
        ArgumentCaptor<Long> hsidCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<RejoinMessage> msgCaptor = ArgumentCaptor.forClass(RejoinMessage.class);
        verify(m_mb).send(hsidCaptor.capture(), msgCaptor.capture());
        assertEquals(m_coordinatorHSId, hsidCaptor.getValue().longValue());
        RejoinMessage rm = msgCaptor.getValue();
        assertEquals(RejoinMessage.Type.SNAPSHOT_FINISHED, rm.getType());
    }

    @Test
    public void testFirstFragmentBeforeInitiation() throws IOException
    {
        m_producer.logTask(mock(FragmentTaskMessage.class));
        assertNull(m_taskerQueue.poll());

        // before the coordinator sends the initiation, the producer doesn't know the HSId of the
        // coordinator, so just remember that it has received the first fragment.
        verify(m_mb, never()).send(anyLong(), any(RejoinMessage.class));

        testReceiveFirstFragment();
    }
}
