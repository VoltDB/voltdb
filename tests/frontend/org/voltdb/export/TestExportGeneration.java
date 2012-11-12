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

package org.voltdb.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.voltdb.export.ExportMatchers.ackMbxMessageIs;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Connector;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.export.ExportDataSource.AckingContainer;
import org.voltdb.export.ExportMatchers.AckPayloadMessage;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.utils.MiscUtils;

import com.google.common.base.Throwables;

public class TestExportGeneration {

    static {
        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
    }

    static String testout_jar;
    static Connector m_connector;
    static String m_tableSignature;
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
                "create table e1 (id integer, f1 varchar(16)); " +
                        "export table e1;";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schemaDDL.toString());
        String schemaPath = schemaFile.getPath();

        String projectXML =
                "<?xml version=\"1.0\"?>\n" +
                        "<project>" +
                        "<database name='database'>" +
                        "<schemas><schema path='" + schemaPath + "' /></schemas>" +
                        "</database>" +
                        "</project>";

        File projectFile = VoltProjectBuilder.writeStringToTempFile(projectXML);
        String projectPath = projectFile.getPath();

        VoltCompiler compiler = new VoltCompiler();

        boolean success = compiler.compile(projectPath, testout_jar);
        assertTrue("failed compilation",success);

        m_connector = compiler
                .getCatalog().getClusters().get("cluster")
                .getDatabases().get("database")
                .getConnectors().get("0");

        m_tableSignature = m_connector
                .getTableinfo()
                .getIgnoreCase("e1")
                .getTable()
                .getSignature();
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

    final AtomicReference<CountDownLatch> m_drainCdlRef =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));

    Runnable m_doOnDrain = new Runnable() {
        @Override
        public void run() {
            m_drainCdlRef.get().countDown();
        }
    };

    final AtomicReference<CountDownLatch> m_mbxNotifyCdlRef =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));
    final AtomicReference<Matcher<VoltMessage>> m_ackMatcherRef =
            new AtomicReference<Matcher<VoltMessage>>();
    LocalMailbox m_mbox;

    @Before
    public void setUp() throws Exception {

        MiscUtils.deleteRecursively(m_tempRoot);

        m_dataDirectory = new File(
                m_tempRoot,
                Long.toString(System.identityHashCode(this), Character.MAX_RADIX)
                );

        m_mockVoltDB = new MockVoltDB();
        m_mockVoltDB.addSite(CoreUtils.getHSIdFromHostAndSite(m_host, m_site), m_part);

        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);

        m_exportGeneration = new ExportGeneration( 0L, m_doOnDrain, m_dataDirectory);
        m_exportGeneration.initializeGenerationFromCatalog(
                m_connector, m_mockVoltDB.m_hostId, m_mockVoltDB.getHostMessenger()
                );

        m_mbox = new LocalMailbox(m_mockVoltDB.getHostMessenger()) {
            @Override
            public void deliver(VoltMessage message) {
                assertThat( message, m_ackMatcherRef.get());
                m_mbxNotifyCdlRef.get().countDown();
            }
        };
        m_mockVoltDB.getHostMessenger().createMailbox(null, m_mbox);
        m_mockVoltDB.getHostMessenger().registerMailbox(m_mbox);
        m_zk = m_mockVoltDB.getHostMessenger().getZK();

        SiteTracker siteTracker = m_mockVoltDB.getSiteTracker();

        List<ZKUtil.StringCallback> cbs = new ArrayList<ZKUtil.StringCallback>();
        for (Long site : siteTracker.getSitesForHost(m_mockVoltDB.m_hostId)) {
            Integer partition = siteTracker.getPartitionForSite(site);
            String zkPath = VoltZK.exportGenerations +
                    "/0/mailboxes" +
                    "/" + partition +
                    "/" + m_mbox.getHSId()
                    ;
            cbs.add(ZKUtil.asyncMkdirs( m_zk, zkPath));
        }
        for( ZKUtil.StringCallback cb: cbs) {
            cb.get();
        }

        m_expDs = m_exportGeneration.m_dataSourcesByPartition.get(m_part).get(m_tableSignature);
        m_zkPartitionDN =  VoltZK.exportGenerations + "/0/mailboxes" + "/" + m_part;
    }

    @After
    public void tearDown() throws Exception {
        m_exportGeneration.closeAndDelete();
        m_mockVoltDB.shutdown(null);
        VoltDB.replaceVoltDBInstanceForTest(null);
    }

    @Test
    public void testAckReceipt() throws Exception {
        ByteBuffer foo = ByteBuffer.allocate(20);

        int retries = 4000;
        long uso = 0L;
        boolean active = false;

        while( --retries >= 0 && ! active) {
            m_exportGeneration.pushExportBuffer(
                    m_part,
                    m_tableSignature,
                    uso, /*cptr*/0,
                    foo.duplicate(),
                    false, false
                    );
            AckingContainer cont = (AckingContainer)m_expDs.poll().get();

            m_ackMatcherRef.set(ackMbxMessageIs(m_part, m_tableSignature, uso + foo.capacity()));
            m_mbxNotifyCdlRef.set( new CountDownLatch(1));

            cont.discard();

            active = m_mbxNotifyCdlRef.get().await(2, TimeUnit.MILLISECONDS);
            uso += foo.capacity();
        }
        assertTrue( "timeout on ack message receipt", retries >= 0);
    }

    @Test
    public void testAckDelivery() throws Exception {
        ByteBuffer foo = ByteBuffer.allocate(20);

        int retries = 4000;
        long size = m_expDs.sizeInBytes();

        m_exportGeneration.pushExportBuffer(
                m_part,
                m_tableSignature,
                /*uso*/0, /*cptr*/0,
                foo.duplicate(),
                false, false
                );

        while( --retries >= 0 && size == m_expDs.sizeInBytes()) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException iex) {
                Throwables.propagate(iex);
            }
        }
        assertTrue("timeout on data source size poll", retries >= 0);
        assertEquals("unexpected data sources size", foo.capacity(), m_expDs.sizeInBytes());

        retries = 4000;
        size = m_expDs.sizeInBytes();

        Long hsid = getOtherMailboxHsid();
        assertNotNull( "other mailbox not listed in zookeeper",  hsid);

        m_mbox.send(
                hsid,
                new AckPayloadMessage(m_part, m_tableSignature, foo.capacity()).asVoltMessage()
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

    private Long getOtherMailboxHsid() throws Exception {
        Long otherHsid = null;

        ZKUtil.ChildrenCallback callback = new ZKUtil.ChildrenCallback();
        m_zk.getChildren(m_zkPartitionDN, null, callback, null);

        for ( String child: callback.getChildren()) {
            long asLong = Long.parseLong(child);
            if( asLong != m_mbox.getHSId()) {
                otherHsid = asLong;
                break;
            }
        }

        return otherHsid;
    }
}
