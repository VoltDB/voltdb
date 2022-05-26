/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.voltdb.messaging.CompleteTransactionMessage;

public class TestScoreboard {


    private FragmentTask createFrag(long txnId, long timestamp) {
        FragmentTask task = mock(FragmentTask.class);
        when(task.getTxnId()).thenReturn(txnId);
        when(task.getTimestamp()).thenReturn(timestamp);
        return task;
    }

    private CompleteTransactionTask createComp(long txnId, long timestamp) {
        CompleteTransactionMessage msg = mock(CompleteTransactionMessage.class);
        when(msg.isRollback()).thenReturn(!MpRestartSequenceGenerator.isForRestart(timestamp));
        CompleteTransactionTask task = mock(CompleteTransactionTask.class);
        when(task.getMsgTxnId()).thenReturn(txnId);
        when(task.getTimestamp()).thenReturn(timestamp);
        when(task.getCompleteMessage()).thenReturn(msg);
        return task;
    }

    @Test
    public void testFragmentTask() {
        Scoreboard scoreboard = new Scoreboard();
        FragmentTask ft1 = createFrag(1000L, CompleteTransactionMessage.INITIAL_TIMESTAMP);
        scoreboard.addFragmentTask(ft1);
        FragmentTask ft2 = createFrag(1001L, CompleteTransactionMessage.INITIAL_TIMESTAMP);
        scoreboard.addFragmentTask(ft2);
        assertEquals(1001L, scoreboard.getFragmentTask().getTxnId());
        assertEquals(CompleteTransactionMessage.INITIAL_TIMESTAMP, scoreboard.getFragmentTask().getTimestamp());
    }

    @Test
    public void testRepairStepsOnInitialCompletion() {
        Scoreboard scoreboard = new Scoreboard();
        MpRestartSequenceGenerator repairGen = new MpRestartSequenceGenerator(1, false);

        // Message with newer timestamp steps on older one
        CompleteTransactionTask comp1 = createComp(1000L, CompleteTransactionMessage.INITIAL_TIMESTAMP);
        scoreboard.addCompletedTransactionTask(comp1, false);
        long expectedTimestamp = repairGen.getNextSeqNum();
        CompleteTransactionTask comp2 = createComp(1000L, expectedTimestamp);
        scoreboard.addCompletedTransactionTask(comp2, false);

        assertEquals(1000L, scoreboard.peekFirst().getFirst().getMsgTxnId());
        assertEquals(expectedTimestamp, scoreboard.peekFirst().getFirst().getTimestamp());

        // Message from different transaction can't step on each other
        CompleteTransactionTask comp3 = createComp(2000L, CompleteTransactionMessage.INITIAL_TIMESTAMP);
        scoreboard.addCompletedTransactionTask(comp3, false);

        assertEquals(1000L, scoreboard.peekFirst().getFirst().getMsgTxnId());
        assertEquals(expectedTimestamp, scoreboard.peekFirst().getFirst().getTimestamp());
        assertEquals(2000L, scoreboard.peekLast().getFirst().getMsgTxnId());
        assertEquals(CompleteTransactionMessage.INITIAL_TIMESTAMP, scoreboard.peekLast().getFirst().getTimestamp());
    }

    @Test
    public void testRestartCompletionStepsOnFragment() {
        Scoreboard scoreboard = new Scoreboard();
        MpRestartSequenceGenerator repairGen = new MpRestartSequenceGenerator(1, true);
        FragmentTask ft1 = createFrag(1000L, CompleteTransactionMessage.INITIAL_TIMESTAMP);
        scoreboard.addFragmentTask(ft1);
        long expectedTimestamp = repairGen.getNextSeqNum();
        CompleteTransactionTask comp1 = createComp(1000L, expectedTimestamp);
        scoreboard.addCompletedTransactionTask(comp1, false);
        assertEquals(1000L, scoreboard.peekFirst().getFirst().getMsgTxnId());
        assertNull(scoreboard.getFragmentTask());
    }

    @Test
    public void testRepairFollowedByRestart() {
        Scoreboard scoreboard = new Scoreboard();
        MpRestartSequenceGenerator repairGen = new MpRestartSequenceGenerator(1, false);
        MpRestartSequenceGenerator restartGen = new MpRestartSequenceGenerator(1, true);

        long expectedTimestamp = repairGen.getNextSeqNum();
        CompleteTransactionTask comp1 = createComp(1000L, expectedTimestamp);
        scoreboard.addCompletedTransactionTask(comp1, false);

        long restartTimestamp = restartGen.getNextSeqNum();
        CompleteTransactionTask comp2 = createComp(2000L, restartTimestamp);
        scoreboard.addCompletedTransactionTask(comp2, false);

        assertEquals(1000L, scoreboard.peekFirst().getFirst().getMsgTxnId());
        assertEquals(expectedTimestamp, scoreboard.peekFirst().getFirst().getTimestamp());
        assertEquals(2000L, scoreboard.peekLast().getFirst().getMsgTxnId());
        assertEquals(restartTimestamp, scoreboard.peekLast().getFirst().getTimestamp());
    }

    @Test
    public void testDeadMPISendStaleInitialCompletion() {
        Scoreboard scoreboard = new Scoreboard();
        MpRestartSequenceGenerator repairGen = new MpRestartSequenceGenerator(1, false);
        MpRestartSequenceGenerator restartGen = new MpRestartSequenceGenerator(1, true);

        long expectedTimestamp = repairGen.getNextSeqNum();
        CompleteTransactionTask comp1 = createComp(1000L, expectedTimestamp);
        scoreboard.addCompletedTransactionTask(comp1, false);

        // stale initial completion doesn't overwrite repair
        CompleteTransactionTask comp2 = createComp(1000L, CompleteTransactionMessage.INITIAL_TIMESTAMP);
        scoreboard.addCompletedTransactionTask(comp2, false);

        assertEquals(1000L, scoreboard.peekFirst().getFirst().getMsgTxnId());
        assertEquals(expectedTimestamp, scoreboard.peekFirst().getFirst().getTimestamp());

        long restartTimestamp = restartGen.getNextSeqNum();
        CompleteTransactionTask comp3 = createComp(2000L, restartTimestamp);
        scoreboard.addCompletedTransactionTask(comp3, false);

        // stale initial completion doesn't overwrite restart completion
        scoreboard.addCompletedTransactionTask(comp2, false);

        assertEquals(1000L, scoreboard.peekFirst().getFirst().getMsgTxnId());
        assertEquals(expectedTimestamp, scoreboard.peekFirst().getFirst().getTimestamp());
        assertEquals(2000L, scoreboard.peekLast().getFirst().getMsgTxnId());
        assertEquals(restartTimestamp, scoreboard.peekLast().getFirst().getTimestamp());
    }

    @Test
    public void testOutOfSequenceRepairAndRestartCompletion() {
        Scoreboard scoreboard = new Scoreboard();
        MpRestartSequenceGenerator repairGen = new MpRestartSequenceGenerator(1, false);
        MpRestartSequenceGenerator restartGen = new MpRestartSequenceGenerator(1, true);

        long restartTimestamp = restartGen.getNextSeqNum();
        CompleteTransactionTask comp1 = createComp(2000L, restartTimestamp);
        scoreboard.addCompletedTransactionTask(comp1, false);

        long expectedTimestamp = repairGen.getNextSeqNum();
        CompleteTransactionTask comp2 = createComp(1000L, expectedTimestamp);
        scoreboard.addCompletedTransactionTask(comp2, false);

        assertEquals(1000L, scoreboard.peekFirst().getFirst().getMsgTxnId());
        assertEquals(expectedTimestamp, scoreboard.peekFirst().getFirst().getTimestamp());
        assertEquals(2000L, scoreboard.peekLast().getFirst().getMsgTxnId());
        assertEquals(restartTimestamp, scoreboard.peekLast().getFirst().getTimestamp());
    }

    @Test
    public void testMissingCompletion() {
        Scoreboard scoreboard = new Scoreboard();
        MpRestartSequenceGenerator repairGen = new MpRestartSequenceGenerator(1, false);

        CompleteTransactionTask comp1 = createComp(1000L, CompleteTransactionMessage.INITIAL_TIMESTAMP);
        scoreboard.addCompletedTransactionTask(comp1, false);

        long expectedTimestamp = repairGen.getNextSeqNum();
        CompleteTransactionTask comp2 = createComp(1000L, expectedTimestamp);
        scoreboard.addCompletedTransactionTask(comp2, true);

        assertEquals(1000L, scoreboard.peekFirst().getFirst().getMsgTxnId());
        assertEquals(expectedTimestamp, scoreboard.peekFirst().getFirst().getTimestamp());
    }

    @Test
    public void testTwoRepairsFollowedByRestart() {
        Scoreboard scoreboard = new Scoreboard();
        MpRestartSequenceGenerator repairGen = new MpRestartSequenceGenerator(1, false);
        MpRestartSequenceGenerator restartGen = new MpRestartSequenceGenerator(1, true);

        long expectedTimestamp = repairGen.getNextSeqNum();
        scoreboard.addCompletedTransactionTask(createComp(1000L, expectedTimestamp), false);
        scoreboard.addCompletedTransactionTask(createComp(1100L, repairGen.getNextSeqNum()), false);

        long restartTimestamp = restartGen.getNextSeqNum();
        scoreboard.addCompletedTransactionTask(createComp(2000L, restartTimestamp), false);

        assertEquals(1000L, scoreboard.peekFirst().getFirst().getMsgTxnId());
        assertEquals(expectedTimestamp, scoreboard.peekFirst().getFirst().getTimestamp());
        assertEquals(2000L, scoreboard.peekLast().getFirst().getMsgTxnId());
        assertEquals(restartTimestamp, scoreboard.peekLast().getFirst().getTimestamp());
    }
}
