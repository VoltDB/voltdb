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

package org.voltdb.iv2;

import org.junit.Test;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.sysprocs.SysProcFragmentId;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestSite {
    @Test
    public void testReplayFilter()
    {
        // Replay normal SPs
        assertFalse(Site.filter(makeInit("Blah")));

        // @LoadSinglePartitionTable a durable sysproc
        assertFalse(Site.filter(makeInit("@LoadSinglePartitionTable")));

        // Replay a sysproc thats not durable
        assertTrue(Site.filter(makeInit("@Quiesce")));

        // Replay @BalancePartitions fragments
        assertFalse(Site.filter(makeFrag(true, SysProcFragmentId.PF_prepBalancePartitions)));
        assertFalse(Site.filter(makeFrag(true, SysProcFragmentId.PF_balancePartitions)));
        assertFalse(Site.filter(makeFrag(true, SysProcFragmentId.PF_balancePartitionsData)));
        // Replay @LoadMultipartitionTable fragments
        assertFalse(Site.filter(makeFrag(true, SysProcFragmentId.PF_distribute)));
        // Replay filter rejects @SnapshotRestore fragments
        assertTrue(Site.filter(makeFrag(true, SysProcFragmentId.PF_restoreAsyncRunLoop)));

        // Replay complete msgs
        assertFalse(Site.filter(makeComplete()));

        // Reject all other sysproc fragments
        assertTrue(Site.filter(makeFrag(true, SysProcFragmentId.PF_createSnapshotTargets)));

    }

    private static FragmentTaskMessage makeFrag(boolean sysproc, long id)
    {
        FragmentTaskMessage msg = mock(FragmentTaskMessage.class);
        doReturn(sysproc).when(msg).isSysProcTask();
        doReturn(VoltSystemProcedure.fragIdToHash(id)).when(msg).getPlanHash(anyInt());
        return msg;
    }

    private static Iv2InitiateTaskMessage makeInit(String procName)    {
        Iv2InitiateTaskMessage msg = mock(Iv2InitiateTaskMessage.class);
        doReturn(procName).when(msg).getStoredProcedureName();
        return msg;
    }

    private static CompleteTransactionMessage makeComplete()
    {
        return mock(CompleteTransactionMessage.class);
    }
}
