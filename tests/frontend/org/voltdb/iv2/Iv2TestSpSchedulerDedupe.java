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

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONException;

import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;

import org.voltcore.zk.MapCache;

import org.voltdb.ClientResponseImpl;

import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.InitiateResponseMessage;

import org.voltdb.ParameterSet;
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

public class Iv2TestSpSchedulerDedupe extends TestCase
{
    Mailbox mbox;
    MapCache iv2masters;
    LoadedProcedureSet procs;
    VoltDBInterface vdbi;
    ProcedureRunner runner;
    Scheduler dut;

    static final String MockSPName = "MOCKSP";
    static final long dut_hsid = 11223344l;

    public void createObjs() throws JSONException
    {
        mbox = mock(Mailbox.class);
        when(mbox.getHSId()).thenReturn(dut_hsid);
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

        dut = new SpScheduler();
        dut.setMailbox(mbox);
        dut.setProcedureSet(procs);
    }

    private Iv2InitiateTaskMessage createMsg(long txnId, boolean readOnly,
                                             boolean singlePart, long destHSId)
    {
        // Mock an invocation for MockSPName.
        StoredProcedureInvocation spi = mock(StoredProcedureInvocation.class);
        when(spi.getProcName()).thenReturn(MockSPName);
        ParameterSet bleh = mock(ParameterSet.class);
        when(spi.getParams()).thenReturn(bleh);
        Iv2InitiateTaskMessage task =
            new Iv2InitiateTaskMessage(destHSId, // don't care?
                                       Long.MIN_VALUE, // don't care
                                       txnId,
                                       readOnly, // readonly
                                       singlePart, // single-part
                                       spi, // invocation
                                       Long.MAX_VALUE); // client interface handle
        return task;
    }

    private FragmentTaskMessage createFrag(long txnId, boolean readOnly, long destHSId)
    {
        FragmentTaskMessage frag =
            new FragmentTaskMessage(destHSId, // don't care
                                    destHSId, // don't care
                                    txnId,
                                    readOnly,
                                    new long[] {},
                                    new int[] {},
                                    new ByteBuffer[] {},
                                    false);
        return frag;
    }

    @Test
    public void testReplicaInitiateTaskResponse() throws Exception
    {
        long txnid = 1234l;
        long primary_hsid = 1111l;

        createObjs();
        Iv2InitiateTaskMessage sptask = createMsg(txnid, true, true, primary_hsid);
        dut.deliver(sptask);
        // verify no response sent yet
        verify(mbox, times(0)).send(anyLong(), (VoltMessage)anyObject());
        verify(mbox, times(0)).send(new long[] {anyLong()}, (VoltMessage)anyObject());
        InitiateResponseMessage resp = new InitiateResponseMessage(sptask);
        dut.deliver(resp);
        verify(mbox, times(1)).send(eq(primary_hsid), eq(resp));
    }

    @Test
    public void testReplicaFragmentTaskResponse() throws Exception
    {
        long txnid = 1234l;
        long primary_hsid = 1111l;

        createObjs();
        FragmentTaskMessage sptask = createFrag(txnid, true, primary_hsid);
        dut.deliver(sptask);
        // verify no response sent yet
        verify(mbox, times(0)).send(anyLong(), (VoltMessage)anyObject());
        verify(mbox, times(0)).send(new long[] {anyLong()}, (VoltMessage)anyObject());
        FragmentResponseMessage resp = new FragmentResponseMessage(sptask, 0l);
        dut.deliver(resp);
        verify(mbox, times(1)).send(eq(primary_hsid), eq(resp));
    }

    @Test
    public void testPrimaryInitiateTaskResponseNoReplicas() throws Exception
    {
        long txnid = 1234l;
        long primary_hsid = 1111l;

        createObjs();
        dut.setLeaderState(true);
        dut.updateReplicas(new ArrayList<Long>());
        Iv2InitiateTaskMessage sptask = createMsg(txnid, true, true, primary_hsid);
        dut.deliver(sptask);
        // verify no response sent yet
        verify(mbox, times(0)).send(anyLong(), (VoltMessage)anyObject());
        verify(mbox, times(0)).send(new long[] {anyLong()}, (VoltMessage)anyObject());
        InitiateResponseMessage resp = new InitiateResponseMessage(sptask);
        dut.deliver(resp);
        verify(mbox, times(1)).send(eq(primary_hsid), eq(resp));
    }

    @Test
    public void testPrimaryFragmentTaskResponseNoReplicas() throws Exception
    {
        long txnid = 1234l;
        long primary_hsid = 1111l;

        createObjs();
        dut.setLeaderState(true);
        dut.updateReplicas(new ArrayList<Long>());
        FragmentTaskMessage sptask = createFrag(txnid, true, primary_hsid);
        dut.deliver(sptask);
        // verify no response sent yet
        verify(mbox, times(0)).send(anyLong(), (VoltMessage)anyObject());
        verify(mbox, times(0)).send(new long[] {anyLong()}, (VoltMessage)anyObject());
        FragmentResponseMessage resp = new FragmentResponseMessage(sptask, 0l);
        dut.deliver(resp);
        verify(mbox, times(1)).send(eq(primary_hsid), eq(resp));
    }

    @Test
    public void testPrimaryInitiateTaskResponseReplicas() throws Exception
    {
        long txnid = 1234l;
        long primary_hsid = 1111l;

        createObjs();
        dut.setLeaderState(true);
        List<Long> replicas = new ArrayList<Long>();
        replicas.add(2l);
        dut.updateReplicas(replicas);
        Iv2InitiateTaskMessage sptask = createMsg(txnid, true, true, primary_hsid);
        dut.deliver(sptask);
        verify(mbox, times(0)).send(anyLong(), (VoltMessage)anyObject());
        // Capture the InitiateTaskMessage that gets sent to the replica so we can test it,
        // use it for response construction, etc.
        ArgumentCaptor<Iv2InitiateTaskMessage> replmsg = ArgumentCaptor.forClass(Iv2InitiateTaskMessage.class);
        verify(mbox, times(1)).send(eq(new long[] {2}), replmsg.capture());
        assertEquals(dut_hsid, replmsg.getValue().getInitiatorHSId());
        InitiateResponseMessage resp = new InitiateResponseMessage(sptask);
        ClientResponseImpl cr = mock(ClientResponseImpl.class);
        resp.setResults(cr);
        InitiateResponseMessage replresp = new InitiateResponseMessage(replmsg.getValue());
        replresp.setResults(cr);
        dut.deliver(resp);
        dut.deliver(replresp);
        verify(mbox, times(1)).send(eq(primary_hsid), eq(resp));
    }

    @Test
    public void testPrimaryFragmentTaskResponseReplicas() throws Exception
    {
        long txnid = 1234l;
        long primary_hsid = 1111l;

        createObjs();
        dut.setLeaderState(true);
        List<Long> replicas = new ArrayList<Long>();
        replicas.add(2l);
        dut.updateReplicas(replicas);
        FragmentTaskMessage sptask = createFrag(txnid, true, primary_hsid);
        dut.deliver(sptask);
        // verify no response sent yet
        verify(mbox, times(0)).send(anyLong(), (VoltMessage)anyObject());
        ArgumentCaptor<FragmentTaskMessage> replmsg = ArgumentCaptor.forClass(FragmentTaskMessage.class);
        verify(mbox, times(1)).send(eq(new long[] {2}), replmsg.capture());
        assertEquals(dut_hsid, replmsg.getValue().getCoordinatorHSId());
        FragmentResponseMessage resp = new FragmentResponseMessage(sptask, 0l);
        dut.deliver(resp);
        FragmentResponseMessage replresp = new FragmentResponseMessage(replmsg.getValue(), 0l);
        dut.deliver(replresp);
        verify(mbox, times(1)).send(eq(primary_hsid), eq(resp));
    }
}
