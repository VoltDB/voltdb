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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google_voltpatches.common.collect.Sets;
import junit.framework.TestCase;

import org.voltcore.messaging.HeartbeatMessage;
import org.voltcore.messaging.HeartbeatResponseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.Pair;
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

        assertEquals(buf1.remaining(), buf2.remaining());
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

    public void testIv2InitiateTask() throws IOException {
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("johnisgreat");
        spi.setParams(57, "gooniestoo", "dudemandude");

        Iv2InitiateTaskMessage itask = new Iv2InitiateTaskMessage(23, 8, 10L, 100045, 99, true, false, spi, 2101, 3101, true);
        itask.setSpHandle(31337);
        Iv2InitiateTaskMessage itask2 = (Iv2InitiateTaskMessage) checkVoltMessage(itask);

        assertEquals(10L, itask.getTruncationHandle());
        assertEquals(itask.getInitiatorHSId(), itask2.getInitiatorHSId());
        assertEquals(itask.getTruncationHandle(), itask2.getTruncationHandle());
        assertEquals(itask.getTxnId(), itask2.getTxnId());
        assertEquals(itask.getUniqueId(), itask2.getUniqueId());
        assertEquals(itask.isReadOnly(), itask2.isReadOnly());
        assertEquals(itask.isSinglePartition(), itask2.isSinglePartition());
        assertEquals(itask.getStoredProcedureName(), itask2.getStoredProcedureName());
        assertEquals(itask.getParameterCount(), itask2.getParameterCount());
        assertEquals(itask.getClientInterfaceHandle(), itask2.getClientInterfaceHandle());
        assertEquals(itask.getClientInterfaceHandle(), 2101);
        assertEquals(itask.getConnectionId(), 3101);
        assertEquals(itask.getSpHandle(), itask2.getSpHandle());
        assertEquals(31337, itask.getSpHandle());
        assertTrue(itask.isForReplay());
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
                new VoltTable[] { table, table }, "knockknockbananna"));
        iresponse.setClientHandle(99);

        InitiateResponseMessage iresponse2 = (InitiateResponseMessage) checkVoltMessage(iresponse);

        assertEquals(iresponse.getTxnId(), iresponse2.getTxnId());
    }

    public void testInitiateResponseForIv2() throws IOException {
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("elmerfudd");
        spi.setParams(57, "wrascallywabbit");

        Iv2InitiateTaskMessage itask = new Iv2InitiateTaskMessage(23, 8, 10L, 100045, 99, true, false, spi, 2101, 3101, true);

        VoltTable table = new VoltTable(
                new VoltTable.ColumnInfo("foobar", VoltType.STRING)
        );
        table.addRow("howmanylicksdoesittaketogettothecenterofatootsiepop");

        InitiateResponseMessage iresponse = new InitiateResponseMessage(itask);
        iresponse.setResults( new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                new VoltTable[] { table, table }, "knockknockbananna"));
        iresponse.setClientHandle(99);

        InitiateResponseMessage iresponse2 = (InitiateResponseMessage) checkVoltMessage(iresponse);

        assertEquals(iresponse.getTxnId(), iresponse2.getTxnId());
        assertTrue(iresponse2.isReadOnly());
    }

    public void testMispartitionedResponse() throws IOException {
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("elmerfudd");
        spi.setParams(57, "wrascallywabbit");

        Iv2InitiateTaskMessage itask = new Iv2InitiateTaskMessage(23, 8, 10L, 100045, 99, true, false, spi, 2101, 3101, true);

        InitiateResponseMessage iresponse = new InitiateResponseMessage(itask);
        iresponse.setMispartitioned(true, spi, Pair.of(3l, new byte[] {1, 2, 3}));
        iresponse.setClientHandle(99);

        InitiateResponseMessage iresponse2 = (InitiateResponseMessage) checkVoltMessage(iresponse);

        assertEquals(iresponse.getTxnId(), iresponse2.getTxnId());
        assertTrue(iresponse2.isReadOnly());
        assertTrue(iresponse2.isMispartitioned());
        assertFalse(iresponse2.shouldCommit());
        assertNotNull(iresponse2.getInvocation());
        assertNotNull(iresponse2.getCurrentHashinatorConfig());
        assertEquals(ClientResponse.TXN_RESTART, iresponse2.getClientResponseData().getStatus());
    }

    public void testFragmentTask() throws IOException {
        FragmentTaskMessage ft = new FragmentTaskMessage(9, 70654312, -75, 99, true, true, false);
        ft.addFragment(new byte[20], 12, ByteBuffer.allocate(0));
        ft.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_PARTITION);
        ft.setBatch(75);

        FragmentTaskMessage ft2 = (FragmentTaskMessage) checkVoltMessage(ft);

        assertEquals(ft.getInitiatorHSId(), ft2.getInitiatorHSId());
        assertEquals(ft.getCoordinatorHSId(), ft2.getCoordinatorHSId());
        assertEquals(ft.getTxnId(), ft2.getTxnId());
        assertEquals(ft.getUniqueId(), ft2.getUniqueId());
        assertEquals(ft.isReadOnly(), ft2.isReadOnly());
        assertEquals(ft.isForReplay(), ft2.isForReplay());

        assertEquals(ft.getFragmentCount(), ft2.getFragmentCount());

        assertEquals(ft.isFinalTask(), ft2.isFinalTask());
        assertEquals(ft.isSysProcTask(), ft2.isSysProcTask());
        assertEquals(ft.getCurrentBatchIndex(), ft2.getCurrentBatchIndex());
    }

    public void testFragmentTaskWithTwoFrags() throws IOException {
        Object[] params1 = {10, 10.1};
        Object[] params2 = {20, 20.2};

        ParameterSet param_set1 = ParameterSet.fromArrayNoCopy(params1);
        ParameterSet param_set2 = ParameterSet.fromArrayNoCopy(params2);

        ByteBuffer param1_buf = ByteBuffer.allocate(param_set1.getSerializedSize());
        param_set1.flattenToBuffer(param1_buf);
        param1_buf.flip();

        ByteBuffer param2_buf = ByteBuffer.allocate(param_set2.getSerializedSize());
        param_set2.flattenToBuffer(param2_buf);
        param2_buf.flip();

        FragmentTaskMessage ft = new FragmentTaskMessage(9, 70654312, -75, 99, true, true, false);
        ft.addFragment(new byte[20], 12, param1_buf);
        ft.addFragment(new byte[20], 24, param2_buf);
        ft.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_PARTITION);

        FragmentTaskMessage ft2 = (FragmentTaskMessage) checkVoltMessage(ft);

        assertEquals(ft.getInitiatorHSId(), ft2.getInitiatorHSId());
        assertEquals(ft.getCoordinatorHSId(), ft2.getCoordinatorHSId());
        assertEquals(ft.getTxnId(), ft2.getTxnId());
        assertEquals(ft.getUniqueId(), ft2.getUniqueId());
        assertEquals(ft.isReadOnly(), ft2.isReadOnly());
        assertEquals(ft.isForReplay(), ft2.isForReplay());

        assertEquals(ft.getFragmentCount(), ft2.getFragmentCount());

        assertEquals(ft.isFinalTask(), ft2.isFinalTask());
        assertEquals(ft.isSysProcTask(), ft2.isSysProcTask());

        assertEquals(2, ft2.getFragmentCount());
        ParameterSet params = null;
        ByteBuffer paramData = ft2.getParameterDataForFragment(0);
        if (paramData != null) {
            params = ParameterSet.fromByteBuffer(paramData);
            assertEquals(10, params.toArray()[0]);
            assertEquals(10.1, params.toArray()[1]);
        }

        params = null;
        paramData = ft2.getParameterDataForFragment(1);
        if (paramData != null) {
            params = ParameterSet.fromByteBuffer(paramData);
            assertEquals(20, params.toArray()[0]);
            assertEquals(20.2, params.toArray()[1]);
        }
    }

    public void testFragmentTaskWithInitiateTask() throws IOException {
        // The fragment task.
        FragmentTaskMessage ft = new FragmentTaskMessage(9, 70654312, -75, 99, true, true, false);
        ft.addFragment(new byte[20], 12, ByteBuffer.allocate(0));
        ft.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_PARTITION);

        // The initiate task.
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("johnisgreat");
        spi.setParams(57, "gooniestoo", "dudemandude");

        Iv2InitiateTaskMessage itask = new Iv2InitiateTaskMessage(23, 8, 10L, 100045, 99, true, false, spi, 2101, 3101, true);
        itask.setSpHandle(31337);

        // this is the important part.
        ft.setStateForDurability(itask, Sets.newHashSet(0, 1, 2));
        assertTrue(ft.getInitiateTask() != null);
        assertTrue(ft.m_initiateTaskBuffer != null);
        assertTrue(ft.m_initiateTaskBuffer.remaining() > 0);

        FragmentTaskMessage ft2 = (FragmentTaskMessage) checkVoltMessage(ft);

        assertTrue(ft2.getInitiateTask() != null);
        assertEquals(ft.getInitiatorHSId(), ft2.getInitiatorHSId());
        assertEquals(ft.getCoordinatorHSId(), ft2.getCoordinatorHSId());
        assertEquals(ft.getTxnId(), ft2.getTxnId());
        assertEquals(ft.getUniqueId(), ft2.getUniqueId());
        assertEquals(ft.isReadOnly(), ft2.isReadOnly());
        assertEquals(ft.isForReplay(), ft2.isForReplay());

        assertEquals(ft.getFragmentCount(), ft2.getFragmentCount());

        assertEquals(ft.isFinalTask(), ft2.isFinalTask());
        assertEquals(ft.isSysProcTask(), ft2.isSysProcTask());

        Iv2InitiateTaskMessage itask2 = ft2.getInitiateTask();
        assertEquals(10L, itask.getTruncationHandle());
        assertEquals(itask.getInitiatorHSId(), itask2.getInitiatorHSId());
        assertEquals(itask.getTruncationHandle(), itask2.getTruncationHandle());
        assertEquals(itask.getTxnId(), itask2.getTxnId());
        assertEquals(itask.getUniqueId(), itask2.getUniqueId());
        assertEquals(itask.isReadOnly(), itask2.isReadOnly());
        assertEquals(itask.isSinglePartition(), itask2.isSinglePartition());
        assertEquals(itask.getStoredProcedureName(), itask2.getStoredProcedureName());
        assertEquals(itask.getParameterCount(), itask2.getParameterCount());
        assertEquals(itask.getClientInterfaceHandle(), itask2.getClientInterfaceHandle());
        assertEquals(itask.getClientInterfaceHandle(), 2101);
        assertEquals(itask.getConnectionId(), 3101);
        assertEquals(itask.getSpHandle(), itask2.getSpHandle());
        assertEquals(31337, itask.getSpHandle());
        assertTrue(itask.isForReplay());
    }


    public void testFragmentResponse() throws IOException {
        FragmentTaskMessage ft = new FragmentTaskMessage(15, 12, 37, 99, false, false, false);

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
            new CompleteTransactionMessage(12345, 54321, 67890, false, 77, false,
                                           true, false, true);

        CompleteTransactionMessage ctm2 = (CompleteTransactionMessage) checkVoltMessage(ctm);
        assertEquals(ctm.m_isRollback, ctm2.m_isRollback);
        assertEquals(ctm.m_requiresAck, ctm2.m_requiresAck);
        assertEquals(ctm.m_rollbackForFault, ctm2.m_rollbackForFault);
        assertEquals(ctm.m_hash, ctm2.m_hash);
    }

    public void testCompleteTransactionResponseMessage() throws IOException
    {
        CompleteTransactionMessage ctm =
            new CompleteTransactionMessage(12345, 54321, 67890, false, 0, false,
                                           true, false, true);

        CompleteTransactionResponseMessage ctrm =
            new CompleteTransactionResponseMessage(ctm, Long.MAX_VALUE - 4);

        CompleteTransactionResponseMessage ctrm2 =
            (CompleteTransactionResponseMessage) checkVoltMessage(ctrm);
        assertEquals(ctrm.getExecutionSiteId(), ctrm.getExecutionSiteId());
        assertEquals(ctrm.getTxnId(), ctrm2.getTxnId());
    }

    public void testIv2RepairLogRequestMessage() throws IOException
    {
        Iv2RepairLogRequestMessage rlm = new Iv2RepairLogRequestMessage(100, Iv2RepairLogRequestMessage.SPREQUEST);
        Iv2RepairLogRequestMessage rlm2 = (Iv2RepairLogRequestMessage) checkVoltMessage(rlm);
        assertEquals(rlm.getRequestId(), rlm2.getRequestId());
        assertEquals(rlm.isMPIRequest(), rlm2.isMPIRequest());
    }

    public void testIv2RepairLogResponseMessage() throws Exception
    {
        // make a first itask
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("johnisgreat");
        spi.setParams(57, "gooniestoo", "dudemandude");

        Iv2InitiateTaskMessage itask =
                new Iv2InitiateTaskMessage(23, 8, 100044, 100045, 99, true, false, spi, 2101, 3101, false);
        itask.setSpHandle(31337);

        Iv2RepairLogResponseMessage r1 = new Iv2RepairLogResponseMessage(0, 1, 2, 3L, 3L, itask);
        Iv2RepairLogResponseMessage r2 = (Iv2RepairLogResponseMessage)checkVoltMessage(r1);
        assertEquals(r1.getOfTotal(), r2.getOfTotal());
        assertEquals(r1.getHandle(), r2.getHandle());
        assertEquals(r1.getTxnId(), r2.getTxnId());
        assertEquals(r1.getRequestId(), r2.getRequestId());
        assertEquals(r1.getSequence(), r2.getSequence());
        assertFalse(r1.hasHashinatorConfig());

        // make sure the payload was round-tripped correctly.
        Iv2InitiateTaskMessage itask2 = (Iv2InitiateTaskMessage)r2.getPayload();
        assertEquals(itask.getInitiatorHSId(), itask2.getInitiatorHSId());
        assertEquals(itask.getTxnId(), itask2.getTxnId());
        assertEquals(itask.getUniqueId(), itask2.getUniqueId());
        assertEquals(itask.isReadOnly(), itask2.isReadOnly());
        assertEquals(itask.isSinglePartition(), itask2.isSinglePartition());
        assertEquals(itask.getStoredProcedureName(), itask2.getStoredProcedureName());
        assertEquals(itask.getParameterCount(), itask2.getParameterCount());
        assertEquals(itask.getClientInterfaceHandle(), itask2.getClientInterfaceHandle());
        assertEquals(itask.getClientInterfaceHandle(), 2101);
        assertEquals(itask.getConnectionId(), 3101);
        assertEquals(itask.getSpHandle(), itask2.getSpHandle());
        assertEquals(31337, itask.getSpHandle());
        assertFalse(itask.isForReplay());
    }

    public void testFirstIv2RepairLogResponseMessage() throws Exception
    {
        // simulate the first message in the sequence, sequence must be 0
        Iv2RepairLogResponseMessage r1 = new Iv2RepairLogResponseMessage(
                0, 10, Long.MAX_VALUE, Long.MAX_VALUE,
                Pair.<Long, byte[]>of(2L, new byte[] {(byte)1,(byte)2,(byte)3})
                );
        Iv2RepairLogResponseMessage r2 = (Iv2RepairLogResponseMessage)checkVoltMessage(r1);
        assertEquals(r1.getOfTotal(), r2.getOfTotal());
        assertEquals(r1.getHandle(), r2.getHandle());
        assertEquals(r1.getTxnId(), r2.getTxnId());
        assertEquals(r1.getRequestId(), r2.getRequestId());
        assertEquals(r1.getSequence(), r2.getSequence());
        assertTrue(r1.hasHashinatorConfig());
        assertEquals(r1.getHashinatorVersionedConfig().getFirst(),new Long(2));
    }
}
