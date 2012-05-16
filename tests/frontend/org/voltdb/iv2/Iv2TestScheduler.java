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

package org.voltdb.iv2;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.voltdb.ProcedureRunner;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ParameterSet;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class Iv2TestScheduler extends TestCase
{
    Mailbox mbox;
    SiteTracker clerk;
    LoadedProcedureSet procs;
    VoltDBInterface vdbi;
    ProcedureRunner runner;

    Scheduler dut;

    static final String MockSPName = "MOCKSP";

    @Override
    public void setUp()
    {
        mbox = mock(Mailbox.class);
        clerk = mock(SiteTracker.class);

        // Mock a procedure set that always returns a proc with name MockSPName
        // that is never a system procedure.
        procs = mock(LoadedProcedureSet.class);
        runner = mock(ProcedureRunner.class);
        when(runner.isSystemProcedure()).thenReturn(false);
        when(procs.getProcByName(MockSPName)).thenReturn(runner);

        dut = new Scheduler(clerk);
        dut.setMailbox(mbox);
        dut.setProcedureSet(procs);

        vdbi = mock(VoltDBInterface.class);
        VoltDB.replaceVoltDBInstanceForTest(vdbi);
        when(vdbi.getSiteTracker()).thenReturn(clerk);
    }

    private Iv2InitiateTaskMessage createMsg(long txnId, boolean readOnly,
                                             boolean singlePart)
    {
        // Mock an invocation for MockSPName.
        StoredProcedureInvocation spi = mock(StoredProcedureInvocation.class);
        when(spi.getProcName()).thenReturn(MockSPName);
        Iv2InitiateTaskMessage task =
            new Iv2InitiateTaskMessage(Long.MIN_VALUE, // don't care?
                                       Long.MIN_VALUE, // don't care
                                       txnId,
                                       readOnly, // readonly
                                       singlePart, // single-part
                                       spi, // invocation
                                       Long.MAX_VALUE); // client interface handle
        return task;
    }

    private FragmentTaskMessage createFrag(long txnId, boolean readOnly)
    {
        FragmentTaskMessage frag =
            new FragmentTaskMessage(Long.MIN_VALUE, // don't care
                                    Long.MIN_VALUE, // don't care
                                    txnId,
                                    readOnly,
                                    new long[] {},
                                    new int[] {},
                                    new ByteBuffer[] {},
                                    false);
        return frag;
    }

    @Test
    public void testSpTaskCreate() throws InterruptedException
    {
        long txnid = 1234l;

        Iv2InitiateTaskMessage sptask = createMsg(txnid, true, true);
        dut.handleIv2InitiateTaskMessage(sptask);

        SpProcedureTask task = (SpProcedureTask)dut.getQueue().poll();
        // This txnid validation is a bit of a hack until we have real TXN IDs
        // since Scheduler is generating the local id for ordering
        assertEquals(txnid, ((SpTransactionState)task.m_txn).m_task.getTxnId());
    }

    @Test
    public void testMpTaskCreate() throws InterruptedException
    {
        long txnid = 1234l;

        Iv2InitiateTaskMessage mptask = createMsg(txnid, true, false);
        dut.handleIv2InitiateTaskMessage(mptask);

        MpProcedureTask task = (MpProcedureTask)dut.getQueue().poll();
        // This txnid validation is a bit of a hack until we have real TXN IDs
        // since Scheduler is generating the local id for ordering
        assertEquals(txnid, ((MpTransactionState)task.m_txn).m_task.getTxnId());
    }

    @Test
    public void testFragTaskCreate() throws InterruptedException
    {
        long txnid = 1234l;

        FragmentTaskMessage fragtask = createFrag(txnid, true);
        dut.handleFragmentTaskMessage(fragtask);

        FragmentTask task = (FragmentTask)dut.getQueue().poll();
        // This txnid validation is a bit of a hack until we have real TXN IDs
        // since Scheduler is generating the local id for ordering
        assertEquals(txnid, ((ParticipantTransactionState)task.m_txn).m_task.getTxnId());
    }
}
