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

import org.json_voltpatches.JSONException;

import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;

import org.voltcore.zk.MapCache;

import org.voltdb.ProcedureRunner;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltcore.messaging.Mailbox;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDBInterface;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import org.json_voltpatches.JSONObject;

public class Iv2TestScheduler extends TestCase
{
    Mailbox mbox;
    MapCache iv2masters;
    LoadedProcedureSet procs;
    VoltDBInterface vdbi;
    ProcedureRunner runner;
    Scheduler dut;

    static final String MockSPName = "MOCKSP";

    public void createObjs(boolean isMPI) throws JSONException
    {
        mbox = mock(Mailbox.class);
        iv2masters = mock(MapCache.class);

        // make fake MapCache of iv2masters
        HashMap<String,JSONObject> fakecache = new HashMap<String, JSONObject>();
        fakecache.put("0", new JSONObject("{hsid:0}"));
        when(iv2masters.pointInTimeCache()).thenReturn(ImmutableMap.copyOf(fakecache));

        // Mock a procedure set that always returns a proc with name MockSPName
        // that is never a system procedure.
        procs = mock(LoadedProcedureSet.class);
        runner = mock(ProcedureRunner.class);
        when(runner.isSystemProcedure()).thenReturn(false);
        when(procs.getProcByName(MockSPName)).thenReturn(runner);

        if (isMPI) {
            dut = new MpScheduler(iv2masters);
        }
        else {
            dut = new SpScheduler();
        }
        dut.setMailbox(mbox);
        dut.setProcedureSet(procs);

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
    public void testSpTaskCreate() throws Exception
    {
        long txnid = 1234l;

        createObjs(false);
        Iv2InitiateTaskMessage sptask = createMsg(txnid, true, true);
        dut.handleIv2InitiateTaskMessage(sptask);

        SpProcedureTask task = (SpProcedureTask)dut.getQueue().poll();
        // This txnid validation is a bit of a hack until we have real TXN IDs
        // since Scheduler is generating the local id for ordering
        assertEquals(txnid, ((SpTransactionState)task.m_txn).m_task.getTxnId());
    }

    @Test
    public void testMpTaskCreate() throws Exception
    {
        long txnid = 1234l;

        createObjs(true);
        Iv2InitiateTaskMessage mptask = createMsg(txnid, true, false);
        dut.handleIv2InitiateTaskMessage(mptask);

        MpProcedureTask task = (MpProcedureTask)dut.getQueue().poll();
        // This txnid validation is a bit of a hack until we have real TXN IDs
        // since Scheduler is generating the local id for ordering
        assertEquals(txnid, ((MpTransactionState)task.m_txn).m_task.getTxnId());
    }

    @Test
    public void testFragTaskCreate() throws Exception
    {
        long txnid = 1234l;

        createObjs(false);
        FragmentTaskMessage fragtask = createFrag(txnid, true);
        dut.handleFragmentTaskMessage(fragtask);

        FragmentTask task = (FragmentTask)dut.getQueue().poll();
        // This txnid validation is a bit of a hack until we have real TXN IDs
        // since Scheduler is generating the local id for ordering
        assertEquals(txnid, ((ParticipantTransactionState)task.m_txn).m_task.getTxnId());
    }
}
