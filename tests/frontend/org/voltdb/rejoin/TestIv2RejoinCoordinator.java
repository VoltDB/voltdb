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

package org.voltdb.rejoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.network.Connection;
import org.voltdb.ClientInterface;
import org.voltdb.ClientResponseImpl;
import org.voltdb.RealVoltDB;
import org.voltdb.SnapshotDaemon;
import org.voltdb.SnapshotInitiationInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Database;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.RejoinMessage;

public class TestIv2RejoinCoordinator {

    // spied on using Mockito
    private Iv2RejoinCoordinator m_coordinator;
    private static VoltDBInterface m_volt;
    private static SnapshotDaemon m_snapshotDaemon;
    private static ClientInterface m_clientInterface = null;
    private static Database m_catalog = null;
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
        doReturn(RealVoltDB.extractBuildInfo(new VoltLogger("HOST"))[0]).when(m_volt).getVersionString();
        VoltDB.replaceVoltDBInstanceForTest(m_volt);
        VoltDB.ignoreCrash = true;

        m_catalog = TPCCProjectBuilder.getTPCCSchemaCatalog()
                                      .getClusters().get("cluster")
                                      .getDatabases().get("database");
    }

    @Before
    public void setUp() {
        // Add mock for ClientInterface and provide the stub to
        // VoltDBInterface mock
        m_clientInterface = mock(ClientInterface.class);
        doReturn(m_clientInterface).when(m_volt).getClientInterface();

        // Mock up SnapshotDaemon returned by mock client interface
        m_snapshotDaemon = mock(SnapshotDaemon.class);
        doReturn(m_snapshotDaemon).when(m_clientInterface).getSnapshotDaemon();
    }

    public void createCoordinator(boolean live) throws IOException
    {
        ArrayList<Long> sites = new ArrayList<Long>();
        sites.add(1l);
        sites.add(2l);

        m_overflow = getTempDir();
        HostMessenger messenger = mock(HostMessenger.class);
        doReturn(10000l).when(messenger).generateMailboxId(null);
        doReturn(mock(ZooKeeper.class)).when(messenger).getZK();
        m_coordinator = spy(new Iv2RejoinCoordinator(messenger, sites, m_overflow.getAbsolutePath(), live));
    }

    @After
    public void tearDown() throws IOException {
        m_coordinator.close();
        FileUtils.deleteDirectory(m_overflow);

        m_coordinator = null;
        m_snapshotDaemon = null;
        m_clientInterface = null;
        reset(m_volt);
        VoltDB.wasCrashCalled = false;
    }

    protected void verifySent(List<Long> hsIds, RejoinMessage expected) {
        ArgumentCaptor<long[]> hsIdCaptor = ArgumentCaptor.forClass(long[].class);
        ArgumentCaptor<VoltMessage> msgCaptor = ArgumentCaptor.forClass(VoltMessage.class);
        verify(m_coordinator).send(hsIdCaptor.capture(), msgCaptor.capture());
        assertEquals(hsIds.size(), hsIdCaptor.getValue().length);
        for (long HSId: hsIdCaptor.getValue()) {
            assertTrue(hsIds.contains(HSId));
        }
        RejoinMessage msg = (RejoinMessage) msgCaptor.getValue();
        assertEquals(expected.getType(), msg.getType());
        assertEquals(expected.m_sourceHSId, msg.m_sourceHSId);
    }

    protected void verifySnapshotRequest(boolean expected) throws Exception
    {
        ArgumentCaptor<Long> clientHandleCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Connection> connectionCaptor = ArgumentCaptor.forClass(Connection.class);
        ArgumentCaptor<SnapshotInitiationInfo> infoCaptor = ArgumentCaptor.forClass(SnapshotInitiationInfo.class);
        ArgumentCaptor<Boolean> notifyCaptor = ArgumentCaptor.forClass(Boolean.class);
        if (expected) {
            // this gets called from a new thread.  Wait for a long time
            verify(m_snapshotDaemon, timeout(120000)).createAndWatchRequestNode(clientHandleCaptor.capture(),
                    connectionCaptor.capture(),
                    infoCaptor.capture(),
                    notifyCaptor.capture());
            System.out.println("JSON: " + infoCaptor.getValue().getJSONBlob());
            VoltTable fake = new VoltTable(new VoltTable.ColumnInfo("ERR_MSG", VoltType.STRING),
                    new VoltTable.ColumnInfo("RESULT", VoltType.STRING));
            fake.addRow("", "SUCCESS");
            ClientResponseImpl resp = new ClientResponseImpl(ClientResponse.SUCCESS, Byte.MIN_VALUE, "enough",
                    new VoltTable[] {fake}, "");
            ByteBuffer buf = ByteBuffer.allocate(resp.getSerializedSize() + 4);
            buf.putInt(resp.getSerializedSize());
            resp.flattenToBuffer(buf);
            connectionCaptor.getValue().writeStream().enqueue(buf);
        }
        else {
            Thread.sleep(1000);  // yes, ugh
            verify(m_snapshotDaemon, never()).createAndWatchRequestNode(clientHandleCaptor.capture(),
                    connectionCaptor.capture(),
                    infoCaptor.capture(),
                    notifyCaptor.capture());
        }
    }

    @Test
    public void testBlockingBasic() throws Exception {
        createCoordinator(false);
        m_coordinator.startJoin(m_catalog);
        // verify the first site is started
        List<Long> hsids = new ArrayList<Long>();
        hsids.add(1l);
        hsids.add(2l);
        RejoinMessage msg = new RejoinMessage(10000l, RejoinMessage.Type.INITIATION_COMMUNITY, "Rejoin_3",
                null, null);
        verifySent(hsids, msg);

        verify(m_volt, never()).onExecutionSiteRejoinCompletion(anyLong());

        // Generate a fake INITIATION_RESPONSE from site 1.
        msg = new RejoinMessage(1l, 1001l, 1002l);
        m_coordinator.deliver(msg);

        // Verify that this does not trigger a snapshot request
        verifySnapshotRequest(false);

        // fake an INITIATION_RESPONSE for site 2
        msg = new RejoinMessage(2l, 2001l, 2002l);
        m_coordinator.deliver(msg);

        // Verify this triggers a snapshot request
        verifySnapshotRequest(true);

        // Generate a fake SNAPSHOT_FINISHED from site 1.
        msg = new RejoinMessage(1l, RejoinMessage.Type.SNAPSHOT_FINISHED);
        m_coordinator.deliver(msg);

        // Shouldn't be done yet.
        verify(m_volt, never()).onExecutionSiteRejoinCompletion(anyLong());

        // fake a snapshot finished response for site 2
        RejoinMessage msg2 = new RejoinMessage(2l, RejoinMessage.Type.SNAPSHOT_FINISHED);
        m_coordinator.deliver(msg2);

        // fake a replay finished response for site 1
        RejoinMessage msg1 = new RejoinMessage(1l, RejoinMessage.Type.REPLAY_FINISHED);
        m_coordinator.deliver(msg1);

        // fake a replay finished response for site 2
        RejoinMessage msg3 = new RejoinMessage(2l, RejoinMessage.Type.REPLAY_FINISHED);
        m_coordinator.deliver(msg3);

        // NOW we're done
        verify(m_volt).onExecutionSiteRejoinCompletion(anyLong());
    }

    @Test
    public void testReplayFinishedBeforeSnapshot() throws Exception {
        createCoordinator(false);
        m_coordinator.startJoin(m_catalog);

        // fake a replay finished response for site 2 before snapshot stream finishes
        RejoinMessage msg3 = new RejoinMessage(2l, RejoinMessage.Type.REPLAY_FINISHED);
        boolean threw = false;
        try {
            m_coordinator.deliver(msg3);
        }
        catch (AssertionError ae) {
            threw = true;
        }
        assertTrue(threw);
        // crash should be called
        assertTrue(VoltDB.wasCrashCalled);
    }
}
