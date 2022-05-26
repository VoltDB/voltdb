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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.json_voltpatches.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltdb.messaging.SnapshotCheckRequestMessage;
import org.voltdb.messaging.SnapshotCheckResponseMessage;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

public class TestSnapshotIOAgent {
    private MockVoltDB m_mockVoltDB;

    @Before
    public void setUp()
    {
        m_mockVoltDB = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        m_mockVoltDB.addSite(CoreUtils.getHSIdFromHostAndSite(0, 1), 0);
        m_mockVoltDB.addTable("partitioned", false);
    }

    @After
    public void tearDown() throws InterruptedException
    {
        m_mockVoltDB.shutdown(null);
        m_mockVoltDB = null;
    }

    @Test
    public void testSnapshotIOAgentSuccess() throws JSONException
    {
        final Mailbox mb = m_mockVoltDB.getHostMessenger().createMailbox();
        final SnapshotIOAgentImpl agent = new SnapshotIOAgentImpl(m_mockVoltDB.getHostMessenger(), 0);

        final SnapshotInitiationInfo snapshotRequest = new SnapshotInitiationInfo("/tmp", "woobie", false,
                SnapshotFormat.NATIVE, SnapshotPathType.SNAP_PATH, null);
        final SnapshotCheckRequestMessage checkMsg = new SnapshotCheckRequestMessage(snapshotRequest.getJSONObjectForZK().toString());
        checkMsg.m_sourceHSId = mb.getHSId();
        agent.deliver(checkMsg);

        final SnapshotCheckResponseMessage resp = (SnapshotCheckResponseMessage) mb.recvBlocking();
        assertEquals(snapshotRequest.getPath(), resp.getPath());
        assertEquals(snapshotRequest.getNonce(), resp.getNonce());
        assertTrue(SnapshotUtil.didSnapshotRequestSucceed(new VoltTable[]{resp.getResponse()}));
    }

    @Test
    public void testSnapshotIOAgentFailInProgress() throws JSONException
    {
        final Mailbox mb = m_mockVoltDB.getHostMessenger().createMailbox();
        final SnapshotIOAgentImpl agent = new SnapshotIOAgentImpl(m_mockVoltDB.getHostMessenger(), 0);

        // Fake a snapshot is still in progress on one site
        SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.add(this);

        final SnapshotInitiationInfo snapshotRequest = new SnapshotInitiationInfo("/tmp", "woobie", false,
                SnapshotFormat.NATIVE, SnapshotPathType.SNAP_PATH, null);
        final SnapshotCheckRequestMessage checkMsg = new SnapshotCheckRequestMessage(snapshotRequest.getJSONObjectForZK().toString());
        checkMsg.m_sourceHSId = mb.getHSId();
        agent.deliver(checkMsg);

        final SnapshotCheckResponseMessage resp = (SnapshotCheckResponseMessage) mb.recvBlocking();
        assertEquals(snapshotRequest.getPath(), resp.getPath());
        assertEquals(snapshotRequest.getNonce(), resp.getNonce());
        assertTrue(SnapshotUtil.isSnapshotInProgress(new VoltTable[]{resp.getResponse()}));

        SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.clear();
    }
}
