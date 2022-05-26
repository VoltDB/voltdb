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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltdb.CommandLog.CompletionChecks;
import org.voltdb.SnapshotCompletionMonitor;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.collect.Lists;

public class TestDurabilityListener {
    private SpScheduler m_sched;
    private TransactionTaskQueue m_taskQueue;

    private SimpleListener m_listener;
    private SpDurabilityListener dut;

    class SimpleListener implements SpScheduler.DurableUniqueIdListener {
        public long m_spUniqueId = Long.MIN_VALUE;
        public long m_mpUniqueId = Long.MIN_VALUE;
        public boolean m_notified = false;

        @Override
        public void lastUniqueIdsMadeDurable(long spUniqueId, long mpUniqueId)
        {
            m_spUniqueId = spUniqueId;
            m_mpUniqueId = mpUniqueId;
            m_notified = true;
        }

        public void resetNotified()
        {
            m_notified = false;
        }
    }

    @Before
    public void setup() {
        m_sched = mock(SpScheduler.class);
        m_taskQueue = mock(TransactionTaskQueue.class);
        m_listener = new SimpleListener();

        dut = new SpDurabilityListener(m_sched, m_taskQueue);
        dut.configureUniqueIdListener(m_listener, true);
    }

    @Test
    public void testSp()
    {
        for (boolean isSync : new boolean[] {false, true}) {
            dut.createFirstCompletionCheck(isSync, true);

            final List<Long> spUniqIds = logSp(0, 1, 2);

            dut.startNewTaskList(dut.getNumberOfTasks()).processChecks();

            assertEquals(spUniqIds.get(2).longValue(), m_listener.m_spUniqueId);
            assertEquals(Long.MIN_VALUE, m_listener.m_mpUniqueId);
        }
    }

    @Test
    public void testSpThenEverywhere()
    {
        for (boolean isSync : new boolean[] {false, true}) {
            dut.createFirstCompletionCheck(isSync, true);

            final List<Long> spUniqIds = logSp(10);
            final List<Long> mpUniqIds = logEverywhere(5);

            dut.startNewTaskList(dut.getNumberOfTasks()).processChecks();

            assertEquals(spUniqIds.get(0).longValue(), m_listener.m_spUniqueId);
            assertEquals(mpUniqIds.get(0).longValue(), m_listener.m_mpUniqueId);
        }
    }

    @Test
    public void testMp()
    {
        for (boolean isSync : new boolean[] {false, true}) {
            dut.createFirstCompletionCheck(isSync, true);

            final List<Long> mpUniqIds = logMp(0, 1, 2);

            dut.startNewTaskList(dut.getNumberOfTasks()).processChecks();

            assertEquals(Long.MIN_VALUE, m_listener.m_spUniqueId);
            assertEquals(mpUniqIds.get(2).longValue(), m_listener.m_mpUniqueId);
        }
    }

    @Test
    public void testInitializeID()
    {
        for (boolean isSync : new boolean[] {false, true}) {
            dut.createFirstCompletionCheck(isSync, true);

            dut.initializeLastDurableUniqueId(UniqueIdGenerator.makeIdFromComponents(5, 0, 0));
            dut.startNewTaskList(dut.getNumberOfTasks()).processChecks();

            assertEquals(UniqueIdGenerator.makeIdFromComponents(5, 0, 0), m_listener.m_spUniqueId);
        }
    }

    @Test
    public void testNoDuplicateNotifications()
    {
        for (boolean isSync : new boolean[] {false, true}) {
            dut.createFirstCompletionCheck(isSync, true);

            final List<Long> spUniqIds = logSp(0, 1, 2);
            dut.startNewTaskList(dut.getNumberOfTasks()).processChecks();

            assertEquals(spUniqIds.get(2).longValue(), m_listener.m_spUniqueId);
            assertEquals(Long.MIN_VALUE, m_listener.m_mpUniqueId);
            assertTrue(m_listener.m_notified);

            // No new txns before this sync, should not have notified the listener
            m_listener.resetNotified();
            dut.startNewTaskList(dut.getNumberOfTasks()).processChecks();
            assertFalse(m_listener.m_notified);
        }
    }

    @Test
    public void testProcessDurabilityChecks()
    {
        SiteTaskerQueue stq = mock(SiteTaskerQueue.class);
        SnapshotCompletionMonitor scm = mock(SnapshotCompletionMonitor.class);
        SpScheduler sched = spy(new SpScheduler(1, stq, scm, true));
        sched.setLock(new Object());

        TransactionTaskQueue taskQueue = mock(TransactionTaskQueue.class);
        SimpleListener listener = new SimpleListener();
        SpDurabilityListener dut = new SpDurabilityListener(sched, taskQueue);
        dut.configureUniqueIdListener(listener, true);

        int cnt = 0;
        ArgumentCaptor<SiteTasker.SiteTaskerRunnable> captor =
                ArgumentCaptor.forClass(SiteTasker.SiteTaskerRunnable.class);
        for (boolean isSync : new boolean[] {false, true}) {
            dut.createFirstCompletionCheck(isSync, true);
            CompletionChecks completionChecks = dut.startNewTaskList(dut.getNumberOfTasks());
            dut.processDurabilityChecks(completionChecks);
            verify(sched, never()).processDurabilityChecks(completionChecks);

            final List<Long> spUniqIds = logSp(dut, taskQueue, 0, 1, 2);
            completionChecks = dut.startNewTaskList(dut.getNumberOfTasks());
            dut.processDurabilityChecks(completionChecks);
            verify(sched).processDurabilityChecks(completionChecks);
            cnt++;
            verify(stq, times(cnt)).offer(captor.capture());
            captor.getValue().run();

            assertEquals(spUniqIds.get(2).longValue(), listener.m_spUniqueId);
            assertEquals(Long.MIN_VALUE, listener.m_mpUniqueId);
            assertTrue(listener.m_notified);

            final List<Long> mpUniqIds = logMp(dut, taskQueue, 0, 1, 2);
            completionChecks = dut.startNewTaskList(dut.getNumberOfTasks());
            dut.processDurabilityChecks(completionChecks);
            verify(sched).processDurabilityChecks(completionChecks);
            cnt++;
            verify(stq, times(cnt)).offer(captor.capture());
            captor.getValue().run();

            assertEquals(spUniqIds.get(2).longValue(), listener.m_spUniqueId);
            assertEquals(mpUniqIds.get(2).longValue(), listener.m_mpUniqueId);
            assertTrue(listener.m_notified);
        }
    }

    private List<Long> logSp(int...timestamps) {
        return logSp(dut, m_taskQueue, timestamps);
    }

    private static List<Long> logSp(SpDurabilityListener dut, TransactionTaskQueue taskQueue, int...timestamps)
    {
        List<Long> uniqIds = Lists.newArrayList();

        for (int ts : timestamps) {
            final long id = UniqueIdGenerator.makeIdFromComponents(ts, 0, 0);
            uniqIds.add(id);
            dut.addTransaction(newInitMsg(true, taskQueue, id));
        }

        return uniqIds;
    }

    private List<Long> logMp(int... timestamps) {
        return logMp(dut, m_taskQueue, timestamps);
    }

    private static List<Long> logMp(SpDurabilityListener dut, TransactionTaskQueue taskQueue, int... timestamps)
    {
        List<Long> uniqIds = Lists.newArrayList();

        for (int ts : timestamps) {
            final long id = UniqueIdGenerator.makeIdFromComponents(ts, 0, MpInitiator.MP_INIT_PID);
            uniqIds.add(id);
            dut.addTransaction(newInitMsg(false, taskQueue, id));
        }

        return uniqIds;
    }

    private List<Long> logEverywhere(int... timestamps) {
        return logEverywhere(dut, m_taskQueue, timestamps);
    }

    private static List<Long> logEverywhere(SpDurabilityListener dut, TransactionTaskQueue taskQueue, int... timestamps)
    {
        List<Long> uniqIds = Lists.newArrayList();

        for (int ts : timestamps) {
            final long id = UniqueIdGenerator.makeIdFromComponents(ts, 0, MpInitiator.MP_INIT_PID);
            uniqIds.add(id);
            dut.addTransaction(newInitMsg(true, taskQueue, id));
        }

        return uniqIds;
    }

    private static SpProcedureTask newInitMsg(boolean isSp, TransactionTaskQueue taskQueue, long uniqId)
    {
        final Iv2InitiateTaskMessage msg = new Iv2InitiateTaskMessage(0, 0, 0, 0, uniqId,
                                                                      false, isSp, false, new StoredProcedureInvocation(),
                                                                      0, 0, false);
        return SpProcedureTask.create(null, "Hello", taskQueue, msg);
    }
}
