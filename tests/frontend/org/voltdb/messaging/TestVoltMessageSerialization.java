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

package org.voltdb.messaging;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.exceptions.EEException;
import org.voltdb.utils.DBBPool;

public class TestVoltMessageSerialization extends TestCase {

    VoltMessage checkVoltMessage(VoltMessage msg, final DBBPool pool) {
        msg.flattenToBuffer( pool );
        ByteBuffer buf1 = msg.m_buffer;

        VoltMessage msg2 = VoltMessage.createMessageFromBuffer(buf1, false);
        msg2.flattenToBuffer( pool );
        ByteBuffer buf2 = msg2.m_buffer;

        assertTrue(buf1.compareTo(buf2) == 0);

        return msg2;
    }

    public void testInitiateTask() {
        DBBPool pool = new DBBPool();
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("johnisgreat");
        spi.setParams(57, "gooniestoo");

        InitiateTaskMessage itask = new InitiateTaskMessage(23, 8, 100045, true, false, spi, 2101);
        itask.setNonCoordinatorSites(new int[] { 5, 2003 });

        InitiateTaskMessage itask2 = (InitiateTaskMessage) checkVoltMessage(itask, pool);

        assertEquals(itask.getInitiatorSiteId(), itask2.getInitiatorSiteId());
        assertEquals(itask.getTxnId(), itask2.getTxnId());
        assertEquals(itask.isReadOnly(), itask2.isReadOnly());
        assertEquals(itask.isSinglePartition(), itask2.isSinglePartition());
        assertEquals(itask.getStoredProcedureName(), itask2.getStoredProcedureName());
        assertEquals(itask.getParameterCount(), itask2.getParameterCount());
        assertEquals(itask.getLastSafeTxnId(), itask2.getLastSafeTxnId());

        itask.discard();
        itask2.discard();
        pool.clear();
    }

    public void testInitiateResponse() {
        DBBPool pool = new DBBPool();
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("elmerfudd");
        spi.setParams(57, "wrascallywabbit");

        InitiateTaskMessage itask = new InitiateTaskMessage(23, 8, 100045, true, false, spi, 2101);
        itask.setNonCoordinatorSites(new int[] { 5, 2003 });

        VoltTable table = new VoltTable(
                new VoltTable.ColumnInfo("foobar", VoltType.STRING)
        );
        table.addRow("howmanylicksdoesittaketogettothecenterofatootsiepop");

        InitiateResponseMessage iresponse = new InitiateResponseMessage(itask);
        iresponse.setResults( new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                new VoltTable[] { table }, "knockknockbananna", new EEException(1)));
        iresponse.setClientHandle(99);

        InitiateResponseMessage iresponse2 = (InitiateResponseMessage) checkVoltMessage(iresponse, pool);

        assertEquals(iresponse.getTxnId(), iresponse2.getTxnId());
        assertEquals(iresponse.getLastReceivedTxnId(), iresponse2.getLastReceivedTxnId());
        iresponse.discard();
        iresponse2.discard();
        pool.clear();
    }

    public void testFragmentTask() {
        DBBPool pool = new DBBPool();
        FragmentTaskMessage ft = new FragmentTaskMessage(9, 70654312, -75, true,
            new long[] { 5 }, new int[] { 12 }, new ByteBuffer[] { ByteBuffer.allocate(0) }, true);
        ft.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_PARTITION);

        FragmentTaskMessage ft2 = (FragmentTaskMessage) checkVoltMessage(ft, pool);

        assertEquals(ft.getInitiatorSiteId(), ft2.getInitiatorSiteId());
        assertEquals(ft.getCoordinatorSiteId(), ft2.getCoordinatorSiteId());
        assertEquals(ft.getTxnId(), ft2.getTxnId());
        assertEquals(ft.isReadOnly(), ft2.isReadOnly());

        assertEquals(ft.getFragmentCount(), ft2.getFragmentCount());

        assertEquals(ft.isFinalTask(), ft2.isFinalTask());
        assertEquals(ft.isSysProcTask(), ft2.isSysProcTask());
        ft.discard();
        ft2.discard();
        pool.clear();
    }

    public void testFragmentResponse() {
        DBBPool pool = new DBBPool();
        FragmentTaskMessage ft = new FragmentTaskMessage(
                15, 12, 37,
                false, new long[0], null,
                new ByteBuffer[0], false);

        VoltTable table = new VoltTable(
                new VoltTable.ColumnInfo("bearhugg", VoltType.STRING)
        );
        table.addRow("sandimashighschoolfootballrules");

        FragmentResponseMessage fr = new FragmentResponseMessage(ft, 23);
        fr.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new EEException(1));
        fr.addDependency(99, table);

        FragmentResponseMessage fr2 = (FragmentResponseMessage) checkVoltMessage(fr, pool);

        assertEquals(fr.getExecutorSiteId(), fr2.getExecutorSiteId());
        assertEquals(fr.getDestinationSiteId(), fr2.getDestinationSiteId());
        assertEquals(fr.getTxnId(), fr2.getTxnId());
        assertEquals(fr.getStatusCode(), fr2.getStatusCode());
        assertEquals(fr.getTableCount(), fr2.getTableCount());

        VoltTable t1 = fr.getTableAtIndex(0);
        VoltTable t2 = fr2.getTableAtIndex(0);
        assertEquals(t1.fetchRow(0).getString(0), t2.fetchRow(0).getString(0));
        ft.discard();
        fr2.discard();
        fr.discard();
        pool.clear();
    }

    public void testMembershipNotice() {
        DBBPool pool = new DBBPool();
        MultiPartitionParticipantMessage mn = new MultiPartitionParticipantMessage(100222, -75, 555555555555L, false);

        MultiPartitionParticipantMessage mn2 = (MultiPartitionParticipantMessage) checkVoltMessage(mn, pool);

        assertEquals(mn.getInitiatorSiteId(), mn2.getInitiatorSiteId());
        assertEquals(mn.getCoordinatorSiteId(), mn2.getCoordinatorSiteId());
        assertEquals(mn.getTxnId(), mn2.getTxnId());
        assertEquals(mn.isReadOnly(), mn2.isReadOnly());
        mn.discard();
        mn2.discard();
        pool.clear();
    }

    public void testHeartbeat() {
        DBBPool pool = new DBBPool();
        HeartbeatMessage mn = new HeartbeatMessage(100222, 555555555555L, 97L);

        HeartbeatMessage mn2 = (HeartbeatMessage) checkVoltMessage(mn, pool);

        assertEquals(mn.getInitiatorSiteId(), mn2.getInitiatorSiteId());
        assertEquals(mn.getCoordinatorSiteId(), mn2.getCoordinatorSiteId());
        assertEquals(mn.getTxnId(), mn2.getTxnId());
        assertEquals(mn.isReadOnly(), mn2.isReadOnly());
        assertEquals(mn.getLastSafeTxnId(), mn2.getLastSafeTxnId());
        mn.discard();
        mn2.discard();
        pool.clear();
    }

    public void testHeartbeatResponse() {
        DBBPool pool = new DBBPool();
        HeartbeatResponseMessage mn = new HeartbeatResponseMessage(55, 100222);

        HeartbeatResponseMessage mn2 = (HeartbeatResponseMessage) checkVoltMessage(mn, pool);

        assertEquals(mn.getExecSiteId(), mn2.getExecSiteId());
        assertEquals(mn.getLastReceivedTxnId(), mn2.getLastReceivedTxnId());
        mn.discard();
        mn2.discard();
        pool.clear();
    }

}
