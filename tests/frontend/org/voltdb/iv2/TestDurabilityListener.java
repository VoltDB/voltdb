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

import com.google_voltpatches.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

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
        dut.setUniqueIdListener(m_listener);
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

    private List<Long> logSp(int...timestamps)
    {
        List<Long> uniqIds = Lists.newArrayList();

        for (int ts : timestamps) {
            final long id = UniqueIdGenerator.makeIdFromComponents(ts, 0, 0);
            uniqIds.add(id);
            dut.addTransaction(newInitMsg(true, id));
        }

        return uniqIds;
    }

    private List<Long> logMp(int... timestamps)
    {
        List<Long> uniqIds = Lists.newArrayList();

        for (int ts : timestamps) {
            final long id = UniqueIdGenerator.makeIdFromComponents(ts, 0, MpInitiator.MP_INIT_PID);
            uniqIds.add(id);
            dut.addTransaction(newInitMsg(false, id));
        }

        return uniqIds;
    }

    private List<Long> logEverywhere(int... timestamps)
    {
        List<Long> uniqIds = Lists.newArrayList();

        for (int ts : timestamps) {
            final long id = UniqueIdGenerator.makeIdFromComponents(ts, 0, MpInitiator.MP_INIT_PID);
            uniqIds.add(id);
            dut.addTransaction(newInitMsg(true, id));
        }

        return uniqIds;
    }

    private SpProcedureTask newInitMsg(boolean isSp, long uniqId)
    {
        final Iv2InitiateTaskMessage msg = new Iv2InitiateTaskMessage(0, 0, 0, 0, uniqId,
                                                                      false, isSp, new StoredProcedureInvocation(),
                                                                      0, 0, false, FairSiteTaskerQueue.DEFAULT_QUEUE);
        return new SpProcedureTask(null, "Hello", m_taskQueue, msg, null);
    }
}
