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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltcore.messaging.HeartbeatMessage;
import org.voltcore.messaging.HeartbeatResponseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.EEException;

public class TestVoltMessageSerialization extends TestCase {

    VoltMessage checkVoltMessage(VoltMessage msg) throws IOException {
        ByteBuffer buf1 = ByteBuffer.allocate(msg.getSerializedSize());
        msg.flattenToBuffer(buf1);
        buf1.flip();

        VoltDbMessageFactory vdbmf = new VoltDbMessageFactory();

        VoltMessage msg2 = vdbmf.createMessageFromBuffer(buf1, -1);
        ByteBuffer buf2 = ByteBuffer.allocate(msg2.getSerializedSize());
        msg2.flattenToBuffer(buf2);
        buf1.rewind();
        buf2.rewind();

        assertTrue(buf1.compareTo(buf2) == 0);

        return msg2;
    }

    public void testInitiateTask() throws IOException {
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("johnisgreat");
        spi.setParams(57, "gooniestoo", "dudemandude");

        InitiateTaskMessage itask = new InitiateTaskMessage(23, 8, 100045, true, false, spi, 2101);

        InitiateTaskMessage itask2 = (InitiateTaskMessage) checkVoltMessage(itask);

        assertEquals(itask.getInitiatorHSId(), itask2.getInitiatorHSId());
        assertEquals(itask.getTxnId(), itask2.getTxnId());
        assertEquals(itask.isReadOnly(), itask2.isReadOnly());
        assertEquals(itask.isSinglePartition(), itask2.isSinglePartition());
        assertEquals(itask.getStoredProcedureName(), itask2.getStoredProcedureName());
        assertEquals(itask.getParameterCount(), itask2.getParameterCount());
        assertEquals(itask.getLastSafeTxnId(), itask2.getLastSafeTxnId());
    }

    public void testInitiateResponse() throws IOException {
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("elmerfudd");
        spi.setParams(57, "wrascallywabbit");

        InitiateTaskMessage itask = new InitiateTaskMessage(23, 8, 100045, true, false, spi, 2101);

        VoltTable table = new VoltTable(
                new VoltTable.ColumnInfo("foobar", VoltType.STRING)
        );
        table.addRow("howmanylicksdoesittaketogettothecenterofatootsiepop");

        InitiateResponseMessage iresponse = new InitiateResponseMessage(itask);
        iresponse.setResults( new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                new VoltTable[] { table, table }, "knockknockbananna", new EEException(1)));
        iresponse.setClientHandle(99);

        InitiateResponseMessage iresponse2 = (InitiateResponseMessage) checkVoltMessage(iresponse);

        assertEquals(iresponse.getTxnId(), iresponse2.getTxnId());
    }

    public void testFragmentTask() throws IOException {
        FragmentTaskMessage ft = new FragmentTaskMessage(9, 70654312, -75, true, true);
        ft.addFragment(5, 12, ByteBuffer.allocate(0));
        ft.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_PARTITION);

        FragmentTaskMessage ft2 = (FragmentTaskMessage) checkVoltMessage(ft);

        assertEquals(ft.getInitiatorHSId(), ft2.getInitiatorHSId());
        assertEquals(ft.getCoordinatorHSId(), ft2.getCoordinatorHSId());
        assertEquals(ft.getTxnId(), ft2.getTxnId());
        assertEquals(ft.isReadOnly(), ft2.isReadOnly());

        assertEquals(ft.getFragmentCount(), ft2.getFragmentCount());

        assertEquals(ft.isFinalTask(), ft2.isFinalTask());
        assertEquals(ft.isSysProcTask(), ft2.isSysProcTask());
    }

    public void testFragmentTaskWithTwoFrags() throws IOException {
        Object[] params1 = {(Integer)10, (Double)10.1};
        Object[] params2 = {(Integer)20, (Double)20.2};

        ParameterSet param_set1 = new ParameterSet();
        param_set1.setParameters(params1);
        ParameterSet param_set2 = new ParameterSet();
        param_set2.setParameters(params2);

        FastSerializer param1_fs = new FastSerializer();
        param1_fs.writeObject(param_set1);
        ByteBuffer param1_buf = param1_fs.getBuffer();

        FastSerializer param2_fs = new FastSerializer();
        param2_fs.writeObject(param_set2);
        ByteBuffer param2_buf = param2_fs.getBuffer();

        FragmentTaskMessage ft = new FragmentTaskMessage(9, 70654312, -75, true, true);
        ft.addFragment(5, 12, param1_buf);
        ft.addFragment(10, 24, param2_buf);
        ft.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_PARTITION);

        FragmentTaskMessage ft2 = (FragmentTaskMessage) checkVoltMessage(ft);

        assertEquals(ft.getInitiatorHSId(), ft2.getInitiatorHSId());
        assertEquals(ft.getCoordinatorHSId(), ft2.getCoordinatorHSId());
        assertEquals(ft.getTxnId(), ft2.getTxnId());
        assertEquals(ft.isReadOnly(), ft2.isReadOnly());

        assertEquals(ft.getFragmentCount(), ft2.getFragmentCount());

        assertEquals(ft.isFinalTask(), ft2.isFinalTask());
        assertEquals(ft.isSysProcTask(), ft2.isSysProcTask());

        assertEquals(2, ft2.getFragmentCount());
        ParameterSet params = null;
        ByteBuffer paramData = ft2.getParameterDataForFragment(0);
        if (paramData != null) {
            final FastDeserializer fds = new FastDeserializer(paramData);
            params = fds.readObject(ParameterSet.class);
            assertEquals(10, params.toArray()[0]);
            assertEquals(10.1, params.toArray()[1]);
        }

        params = null;
        paramData = ft2.getParameterDataForFragment(1);
        if (paramData != null) {
            final FastDeserializer fds = new FastDeserializer(paramData);
            params = fds.readObject(ParameterSet.class);
            assertEquals(20, params.toArray()[0]);
            assertEquals(20.2, params.toArray()[1]);
        }
    }


    public void testFragmentResponse() throws IOException {
        FragmentTaskMessage ft = new FragmentTaskMessage(15, 12, 37, false, false);

        VoltTable table = new VoltTable(
                new VoltTable.ColumnInfo("bearhugg", VoltType.STRING)
        );
        table.addRow("sandimashighschoolfootballrules");

        FragmentResponseMessage fr = new FragmentResponseMessage(ft, 23);
        fr.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new EEException(1));
        fr.addDependency(99, table);

        FragmentResponseMessage fr2 = (FragmentResponseMessage) checkVoltMessage(fr);

        assertEquals(fr.getExecutorSiteId(), fr2.getExecutorSiteId());
        assertEquals(fr.getDestinationSiteId(), fr2.getDestinationSiteId());
        assertEquals(fr.getTxnId(), fr2.getTxnId());
        assertEquals(fr.getStatusCode(), fr2.getStatusCode());
        assertEquals(fr.getTableCount(), fr2.getTableCount());

        VoltTable t1 = fr.getTableAtIndex(0);
        VoltTable t2 = fr2.getTableAtIndex(0);
        assertEquals(t1.fetchRow(0).getString(0), t2.fetchRow(0).getString(0));
    }

    public void testMembershipNotice() throws IOException {
        MultiPartitionParticipantMessage mn = new MultiPartitionParticipantMessage(100222, -75, 555555555555L, false);

        MultiPartitionParticipantMessage mn2 =
            (MultiPartitionParticipantMessage) checkVoltMessage(mn);

        assertEquals(mn.getInitiatorHSId(), mn2.getInitiatorHSId());
        assertEquals(mn.getCoordinatorHSId(), mn2.getCoordinatorHSId());
        assertEquals(mn.getTxnId(), mn2.getTxnId());
        assertEquals(mn.isReadOnly(), mn2.isReadOnly());
    }

    public void testHeartbeat() throws IOException {
        HeartbeatMessage mn = new HeartbeatMessage(100222, 555555555555L, 97L);

        HeartbeatMessage mn2 = (HeartbeatMessage) checkVoltMessage(mn);

        assertEquals(mn.getInitiatorHSId(), mn2.getInitiatorHSId());
        assertEquals(mn.getCoordinatorHSId(), mn2.getCoordinatorHSId());
        assertEquals(mn.getTxnId(), mn2.getTxnId());
        assertEquals(mn.isReadOnly(), mn2.isReadOnly());
        assertEquals(mn.getLastSafeTxnId(), mn2.getLastSafeTxnId());
    }

    public void testHeartbeatResponse() throws IOException {
        HeartbeatResponseMessage mn = new HeartbeatResponseMessage(55, 100222, true);

        HeartbeatResponseMessage mn2 = (HeartbeatResponseMessage) checkVoltMessage(mn);

        assertEquals(mn.getExecHSId(), mn2.getExecHSId());
        assertEquals(mn.getLastReceivedTxnId(), mn2.getLastReceivedTxnId());
        assertEquals(mn.isBlocked(), mn2.isBlocked());
    }

    public void testCompleteTransactionMessage() throws IOException
    {
        CompleteTransactionMessage ctm =
            new CompleteTransactionMessage(12345, 54321, 67890, false, false,
                                           true);

        CompleteTransactionMessage ctm2 = (CompleteTransactionMessage) checkVoltMessage(ctm);
        assertEquals(ctm.m_isRollback, ctm2.m_isRollback);
        assertEquals(ctm.m_requiresAck, ctm2.m_requiresAck);
    }

    public void testCompleteTransactionResponseMessage() throws IOException
    {
        CompleteTransactionMessage ctm =
            new CompleteTransactionMessage(12345, 54321, 67890, false, false,
                                           true);

        CompleteTransactionResponseMessage ctrm =
            new CompleteTransactionResponseMessage(ctm, Long.MAX_VALUE - 4);

        CompleteTransactionResponseMessage ctrm2 =
            (CompleteTransactionResponseMessage) checkVoltMessage(ctrm);
        assertEquals(ctrm.getExecutionSiteId(), ctrm.getExecutionSiteId());
        assertEquals(ctrm.getTxnId(), ctrm2.getTxnId());
    }
}
