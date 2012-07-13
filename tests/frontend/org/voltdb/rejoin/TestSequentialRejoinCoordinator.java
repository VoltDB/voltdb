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

package org.voltdb.rejoin;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.utils.VoltFile;

public class TestSequentialRejoinCoordinator {

    // spied on using Mockito
    private SequentialRejoinCoordinator m_coordinator;
    private static VoltDBInterface m_volt;
    private File m_overflow = null;

    File getTempDir() throws IOException {
        File overflowDir = File.createTempFile("test-tasklog", "");
        overflowDir.delete();
        assertTrue(overflowDir.mkdir());
        return overflowDir;
    }

    @BeforeClass
    public static void setUpOnce() throws IOException {
        m_volt = mock(VoltDBInterface.class);
        VoltDB.replaceVoltDBInstanceForTest(m_volt);
        VoltDB.ignoreCrash = true;
    }

    @Before
    public void setUp() throws IOException {
        ArrayList<Long> sites = new ArrayList<Long>();
        sites.add(1l);
        sites.add(2l);

        m_overflow = getTempDir();
        HostMessenger messenger = mock(HostMessenger.class);
        doReturn(1000l).when(messenger).generateMailboxId(null);
        m_coordinator = spy(new SequentialRejoinCoordinator(messenger, sites, m_overflow.getAbsolutePath()));
    }

    @After
    public void tearDown() throws IOException {
        m_coordinator.close();
        VoltFile.recursivelyDelete(m_overflow);

        m_coordinator = null;
        reset(m_volt);
        VoltDB.wasCrashCalled = false;
    }

    protected void verifySent(long hsId, RejoinMessage expected) {
        ArgumentCaptor<Long> hsIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<VoltMessage> msgCaptor = ArgumentCaptor.forClass(VoltMessage.class);
        verify(m_coordinator).send(hsIdCaptor.capture(), msgCaptor.capture());
        assertEquals(hsId, hsIdCaptor.getValue().longValue());
        RejoinMessage msg = (RejoinMessage) msgCaptor.getValue();
        assertEquals(expected.getType(), msg.getType());
        assertEquals(expected.m_sourceHSId, msg.m_sourceHSId);
    }

    @Test
    public void testBasic() {
        m_coordinator.startRejoin();
        RejoinMessage msg = new RejoinMessage(1000, RejoinMessage.Type.INITIATION);
        verifySent(1l, msg);
        verify(m_volt, never()).onExecutionSiteRejoinCompletion(anyLong());
    }

    @Test
    public void testSwitchToNextSite() {
        testBasic();
        reset(m_coordinator);

        // fake response
        RejoinMessage msg = new RejoinMessage(1l, RejoinMessage.Type.SNAPSHOT_FINISHED);
        m_coordinator.deliver(msg);

        // verify the second site is started
        RejoinMessage expected = new RejoinMessage(1000, RejoinMessage.Type.INITIATION);
        verifySent(2l, expected);

        verify(m_volt, never()).onExecutionSiteRejoinCompletion(anyLong());
    }

    @Test
    public void testFinish() {
        testSwitchToNextSite();

        // fake a replay finished response for site 1
        RejoinMessage msg1 = new RejoinMessage(1l, RejoinMessage.Type.REPLAY_FINISHED);
        m_coordinator.deliver(msg1);

        // fake a snapshot finished response for site 2
        RejoinMessage msg2 = new RejoinMessage(2l, RejoinMessage.Type.SNAPSHOT_FINISHED);
        m_coordinator.deliver(msg2);

        // fake a replay finished response for site 2
        RejoinMessage msg3 = new RejoinMessage(2l, RejoinMessage.Type.REPLAY_FINISHED);
        m_coordinator.deliver(msg3);

        verify(m_volt).onExecutionSiteRejoinCompletion(anyLong());
    }

    @Test
    public void testReplayFinishedBeforeSnapshot() {
        m_coordinator.startRejoin();

        // fake a replay finished response for site 2 before snapshot stream finishes
        RejoinMessage msg3 = new RejoinMessage(2l, RejoinMessage.Type.REPLAY_FINISHED);
        m_coordinator.deliver(msg3);

        // crash should be called
        assertTrue(VoltDB.wasCrashCalled);
    }
}
