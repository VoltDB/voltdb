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

package org.voltdb.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.voltdb.export.ExportMatchers.ackMbxMessageIs;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Connector;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.export.ExportMatchers.AckPayloadMessage;
import org.voltdb.export.processors.GuestProcessor;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableMap;

public class TestExportGeneration {

    static {
        org.voltdb.NativeLibraryLoader.loadVoltDB();
    }

    static String testout_jar;
    static CatalogMap<Connector> m_connectors;
    static String m_streamName;
    static File m_tempRoot;

    @BeforeClass
    static public void onClassLoad() throws Exception {
        String userName = System.getProperty("user.name","elGuapoNonEsistente");
        String tempDN = System.getProperty("java.io.tmpdir", "/tmp");

        File directory = new File(tempDN);
        m_tempRoot = new File(directory,userName);
        m_tempRoot.mkdir();

        testout_jar = m_tempRoot.getCanonicalPath() + File.separatorChar + "testout.jar";

        String schemaDDL =
                "create stream e1 export to target e1 (id integer, f1 varchar(16)); ";

        VoltCompiler compiler = new VoltCompiler(false);
        boolean success = compiler.compileDDLString(schemaDDL, testout_jar);
        assertTrue("failed compilation",success);

        m_connectors = compiler
                .getCatalog().getClusters().get("cluster")
                .getDatabases().get("database")
                .getConnectors();
        Connector defaultConnector = m_connectors.get("e1");
        defaultConnector.setEnabled(true);

        m_streamName = defaultConnector
                .getTableinfo()
                .getIgnoreCase("e1")
                .getTable()
                .getTypeName();
    }

    File m_dataDirectory;
    MockVoltDB m_mockVoltDB;
    ExportGeneration m_exportGeneration;
    ExportDataSource m_expDs;
    ZooKeeper m_zk;
    String m_zkPartitionDN;
    int m_host = 0;
    int m_site = 1;
    int m_part = 2;

    final AtomicReference<CountDownLatch> m_mbxNotifyCdlRef =
            new AtomicReference<>(null);
    final AtomicReference<Matcher<VoltMessage>> m_ackMatcherRef =
            new AtomicReference<>();
    LocalMailbox m_mbox;

    private ExportDataProcessor getProcessor() {
        Map<String, Pair<Properties, Set<String>>> config = new HashMap<>();
        Properties props = new Properties();
        props.put("nonce", "mynonce");
        props.put("type", "csv");
        props.put("__EXPORT_TO_TYPE__", "org.voltdb.exportclient.ExportToFileClient");
        props.put("replicated", false);
        props.put("outdir", m_tempRoot.getAbsolutePath() + "/my_exports");
        Set<String> tables = new HashSet<>();
        tables.add("e1");
        config.put("e1", new Pair<>(props, tables));
        ExportDataProcessor processor = new GuestProcessor();
        processor.setProcessorConfig(config);
        // Do NOT start the actual processor as we don't have an actual
        // export client
        // processor.startPolling();
        return processor;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Before
    public void setUp() throws Exception {

        MiscUtils.deleteRecursively(m_tempRoot);

        m_dataDirectory = new File(
                m_tempRoot,
                Long.toString(System.identityHashCode(this), Character.MAX_RADIX)
                );

        m_mockVoltDB = new MockVoltDB();
        m_mockVoltDB.addSite(CoreUtils.getHSIdFromHostAndSite(m_host, m_site), m_part);
        m_mockVoltDB.addTable("e1", false);
        m_mockVoltDB.addColumnToTable("e1", "id", VoltType.INTEGER, true, "AA", VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("e1", "f1", VoltType.STRING, true, "AA", VoltType.STRING);

        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);

        m_exportGeneration = new ExportGeneration(m_dataDirectory, m_mockVoltDB.getHostMessenger());

        ExportDataProcessor processor = getProcessor();
        processor.setExportGeneration(m_exportGeneration);

        m_exportGeneration.initializeGenerationFromCatalog(m_mockVoltDB.getCatalogContext(),
                m_connectors, getProcessor(), m_mockVoltDB.m_hostId,
                ImmutableMap.of(m_part, CoreUtils.getSiteIdFromHSId(m_site)), false);

        m_mbox = new LocalMailbox(m_mockVoltDB.getHostMessenger()) {
            @Override
            public void deliver(VoltMessage message) {
                if (message instanceof BinaryPayloadMessage) {
                    BinaryPayloadMessage bpm = (BinaryPayloadMessage)message;
                    ByteBuffer buf = ByteBuffer.wrap(bpm.m_payload);
                    final byte msgType = buf.get();
                    // Skip non-related messages
                    if (msgType != ExportManager.RELEASE_BUFFER) {
                        return;
                    }
                }
                assertThat( message, m_ackMatcherRef.get());
                m_mbxNotifyCdlRef.get().countDown();
            }
        };
        m_mockVoltDB.getHostMessenger().createMailbox(null, m_mbox);
        m_zk = m_mockVoltDB.getHostMessenger().getZK();

        SiteTracker siteTracker = m_mockVoltDB.getSiteTrackerForSnapshot();

        List<ZKUtil.StringCallback> cbs = new ArrayList<>();
        for (Long site : siteTracker.getSitesForHost(m_mockVoltDB.m_hostId)) {
            Integer partition = siteTracker.getPartitionForSite(site);
            String zkPath = VoltZK.exportGenerations +
                    "/mailboxes" +
                    "/" + partition +
                    "/" + m_mbox.getHSId()
                    ;
            cbs.add(ZKUtil.asyncMkdirs( m_zk, zkPath));
        }
        for( ZKUtil.StringCallback cb: cbs) {
            cb.get();
        }

        m_expDs = m_exportGeneration.getDataSourceByPartition().get(m_part).get(m_streamName);

        // Assign a mock {@code ExportCoordinator}
        m_expDs.m_coordinator = getMockCoordinator();

        // Make sure the test EDS is always ready for polling
        m_expDs.setReadyForPolling(true);

        m_zkPartitionDN =  VoltZK.exportGenerations + "/mailboxes" + "/" + m_part;
    }

    private ExportCoordinator getMockCoordinator() {
        ExportCoordinator mock = mock(ExportCoordinator.class);
        when(mock.isCoordinatorInitialized()).thenReturn(true);
        when(mock.isPartitionLeader()).thenReturn(true);
        when(mock.isMaster()).thenReturn(true);
        when(mock.isSafePoint(anyLong())).thenReturn(true);
        when(mock.isExportMaster(anyLong())).thenReturn(true);
        return mock;
    }

    @After
    public void tearDown() throws Exception {
        m_exportGeneration.shutdown();
        m_mockVoltDB.shutdown(null);
        VoltDB.replaceVoltDBInstanceForTest(null);
    }

    @Test
    public void testAckReceipt() throws Exception {
        ByteBuffer foo = ByteBuffer.allocate(20 + StreamBlock.HEADER_SIZE);

        // Promote the data source to be master first, otherwise it won't send acks.
        m_exportGeneration.becomeLeader(m_part);

        int retries = 4000;
        long seqNo = 1L;
        boolean active = false;


        while( --retries >= 0 && ! active) {

            m_exportGeneration.pushExportBuffer(
                    m_part,
                    m_streamName,
                    seqNo,
                    seqNo,
                    1,
                    0L,
                    DBBPool.wrapBB(foo.duplicate())
                    );
            AckingContainer cont = m_expDs.poll().get();
            cont.updateStartTime(System.currentTimeMillis());

            m_mbxNotifyCdlRef.set( new CountDownLatch(1));
            m_ackMatcherRef.set(ackMbxMessageIs(m_part, m_streamName, seqNo, m_expDs.getGenerationIdCreated()));

            cont.discard();

            active = m_mbxNotifyCdlRef.get().await(2, TimeUnit.MILLISECONDS);
            seqNo++;
        }
        assertTrue( "timeout on ack message receipt", retries >= 0);
    }

    @Test
    public void testAckDelivery() throws Exception {
        ByteBuffer foo = ByteBuffer.allocate(20 + StreamBlock.HEADER_SIZE);

        int retries = 4000;
        long size = m_expDs.sizeInBytes();

        m_exportGeneration.pushExportBuffer(
                m_part,
                m_streamName,
                /*seqNo*/1L,
                1L,
                1,
                0L,
                DBBPool.wrapBB(foo.duplicate())
                );

        while( --retries >= 0 && size == m_expDs.sizeInBytes()) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException iex) {
                Throwables.propagate(iex);
            }
        }
        assertTrue("timeout on data source size poll", retries >= 0);
        assertEquals("unexpected data sources size", foo.capacity() - StreamBlock.HEADER_SIZE, m_expDs.sizeInBytes());

        retries = 4000;
        size = m_expDs.sizeInBytes();

        Long hsid = getOtherMailboxHsid();
        assertNotNull( "other mailbox not listed in zookeeper",  hsid);

        m_mbox.send(
                hsid,
                new AckPayloadMessage(m_part, m_streamName, 1L, m_expDs.getGenerationIdCreated()).asVoltMessage()
                );

        while( --retries >= 0 && size == m_expDs.sizeInBytes()) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException iex) {
                Throwables.propagate(iex);
            }
        }
        assertTrue("timeout on data source size poll", retries >= 0);
        assertEquals("unexpected data sources size", 0, m_expDs.sizeInBytes());
    }

    @Test
    public void testStaleAckDelivery() throws Exception {
        ByteBuffer foo = ByteBuffer.allocate(20 + StreamBlock.HEADER_SIZE);

        int retries = 4000;
        long size = m_expDs.sizeInBytes();

        m_exportGeneration.pushExportBuffer(
                m_part,
                m_streamName,
                /*seqNo*/1L,
                1L,
                1,
                0L,
                DBBPool.wrapBB(foo.duplicate())
                );

        while( --retries >= 0 && size == m_expDs.sizeInBytes()) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException iex) {
                Throwables.propagate(iex);
            }
        }
        assertTrue("timeout on data source size poll", retries >= 0);
        assertEquals("unexpected data sources size", foo.capacity() - StreamBlock.HEADER_SIZE, m_expDs.sizeInBytes());

        retries = 1000;
        size = m_expDs.sizeInBytes();

        Long hsid = getOtherMailboxHsid();
        assertNotNull( "other mailbox not listed in zookeeper",  hsid);

        // Send an ack message with a catalogVersion < the EDS catalogVersion
        m_mbox.send(
                hsid,
                new AckPayloadMessage(m_part,
                        m_streamName, 1L,
                        m_expDs.getGenerationIdCreated() - 1) // stale catalogVersion
                    .asVoltMessage()
                );

        while( --retries >= 0) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException iex) {
                Throwables.propagate(iex);
            }
        }

        // The ack should have been ignored
        assertEquals("unexpected data sources size", size, m_expDs.sizeInBytes());
    }

    private Long getOtherMailboxHsid() throws Exception {
        Long otherHsid = null;

        ZKUtil.ChildrenCallback callback = new ZKUtil.ChildrenCallback();
        m_zk.getChildren(m_zkPartitionDN, null, callback, null);

        for ( String child: callback.get()) {
            long asLong = Long.parseLong(child);
            if( asLong != m_mbox.getHSId()) {
                otherHsid = asLong;
                break;
            }
        }

        return otherHsid;
    }
}
