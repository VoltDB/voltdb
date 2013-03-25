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

package org.voltdb.dtxn;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.voltcore.messaging.MockMailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ExecutionSite;
import org.voltdb.ParameterSet;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskLogMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

/**
 * This test is a horrible mysterious beast, but it does run through
 * the MultiPartitionParticipantTxnState and the ReplayTxnState
 *
 */
public class TestReplayTxnState extends TestCase {

    final static StoredProcedureInvocation invocation = new StoredProcedureInvocation();
    final static SiteTracker tracker = mock(SiteTracker.class);
    final static MockMailbox mbox = new MockMailbox();
    final static VoltTable result = new VoltTable(new ColumnInfo("foo", VoltType.BIGINT));
    static MultiPartitionParticipantMessage participantNotice = new MultiPartitionParticipantMessage(0, 2, 1, true);
    final static FragmentTaskMessage ftmR = new FragmentTaskMessage(0, 2, 1, System.currentTimeMillis(), true, true, false);
    final static FragmentTaskMessage ftmW = new FragmentTaskMessage(0, 2, 1, System.currentTimeMillis(), false, false, false);
    final static FragmentTaskMessage ftmCol = new FragmentTaskMessage(0, 1, 2, System.currentTimeMillis(), false, false, false);
    final static FragmentTaskMessage ftmAgg = new FragmentTaskMessage(0, 1, 2, System.currentTimeMillis(), false, false, false);
    final static ParameterSet params = new ParameterSet();
    static ByteBuffer paramsBuf;
    static FragmentResponseMessage frmR, frmW;
    static InitiateResponseMessage irm;

    static {
        invocation.setProcName("ryanlikesyankees");
        invocation.setParams(new Object[0]);
        result.addRow(1);
        params.setParameters(new Object[0]);
        paramsBuf = ByteBuffer.allocate(params.getSerializedSize());
        try { params.flattenToBuffer(paramsBuf); } catch (IOException e) {}
        ftmR.addFragment(0, 0, paramsBuf);
        ftmW.addFragment(1, 1, paramsBuf);
        frmR = new FragmentResponseMessage(ftmR, 1);
        frmR.setStatus(FragmentResponseMessage.SUCCESS, null);
        frmR.addDependency(0, result);
        frmW = new FragmentResponseMessage(ftmW, 1);
        frmW.setStatus(FragmentResponseMessage.SUCCESS, null);
        frmW.addDependency(0, result);

        ftmCol.addFragment(0, 13, paramsBuf);
        ftmAgg.addFragment(1, 0, paramsBuf);
        ftmAgg.addInputDepId(0, 13);

        when(tracker.getAllSitesExcluding(anyLong())).thenReturn(new long[] { 2 });
    }

    public void testReturnInvocation() {
        ExecutionSite site = mock(ExecutionSite.class);
        InitiateTaskMessage task = new InitiateTaskMessage(0, 0, 0, false, true, invocation, 0);
        ReplayedTxnState txnState = new ReplayedTxnState(site, task);
        assertNotNull(txnState.getInvocation());
    }
}
