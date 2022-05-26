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

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.messaging.SnapshotCheckRequestMessage;
import org.voltdb.messaging.SnapshotCheckResponseMessage;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltSnapshotFile;

import org.apache.commons.io.FileUtils;

public class TestSnapshotDirectory extends JUnit4LocalClusterTest {

    private static LocalCluster m_config;
    private Client m_client;
    private MockVoltDB m_mockVoltDB;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        FileUtils.deleteDirectory(new VoltSnapshotFile("/tmp/" + System.getProperty("user.name")));
        m_config = new LocalCluster("tpcc.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        m_config.setHasLocalServer(false);
        m_config.setEnableVoltSnapshotPrefix(true);
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addDefaultProcedures();
        if (MiscUtils.isPro()) {
            project.configureLogging( null, null, true, true, 200, Integer.MAX_VALUE, null);
        }
        m_config.compile(project);
    }

    @Before
    public void setUp() throws Exception {
        m_config.startUp();
        m_client = getClient();
        m_mockVoltDB = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        m_mockVoltDB.addSite(CoreUtils.getHSIdFromHostAndSite(0, 1), 0);
        m_mockVoltDB.addTable("partitioned", false);
    }

    @After
    public void tearDown() throws InterruptedException
    {
        m_config.shutDown();
        m_mockVoltDB.shutdown(null);
        m_config.shutDown();
        m_mockVoltDB = null;
    }

    @Test
    public void testSnapshotDirectoryNotExist() throws Exception {
        System.out.println("............      \n testSnapshotDirectoryNotExist \n ............");

        VoltTable[] result =
                m_client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name") + "/temp1/", "Lannister", 0).getResults();
        VoltTable table = result[0];
        while (table.advanceRow()) {
            assertEquals("SUCCESS", table.getString("RESULT"));
            assertEquals("", table.getString("ERR_MSG"));
        }
        m_config.shutDown();
    }

    @Test
    public void testSnapshotDirectoryNonceDuplicate() throws Exception {
        System.out.println("............      \n testSnapshotDirectoryNonceDuplicate \n ............");

        m_client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name") + "/temp2/", "Stark", 1).getResults();
        Thread.sleep(200);
        VoltTable[] result =
                m_client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name") + "/temp2/", "Stark", 1).getResults();
        VoltTable table = result[0];
        while (table.advanceRow()) {
            assertEquals("FAILURE", table.getString("RESULT"));
            assertEquals("SNAPSHOT FILE WITH SAME NONCE ALREADY EXISTS", table.getString("ERR_MSG"));
        }
        m_config.shutDown();
    }

    @Test
    public void testSnapshotDirectoryNonceDuplicateNonBlocking() throws Exception {
        System.out.println("............      \n testSnapshotDirectoryNonceDuplicateNonBlocking \n ............");

        m_client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name") + "/temp4/", "Stark", 0).getResults();
        VoltTable[] result =
                m_client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name") + "/temp4/", "Stark", 0).getResults();
        VoltTable table = result[0];
        while (table.advanceRow()) {
            assertEquals("SUCCESS", table.getString("RESULT"));
            assertEquals("SNAPSHOT REQUEST QUEUED", table.getString("ERR_MSG"));
        }
        m_config.shutDown();
    }

    @Test
    public void testSnapshotDirectoryParentDirInvalid() throws Exception {
        System.out.println("............      \n testSnapshotDirectoryParentDirInvalid \n ............");

        String parentDir = "testtest/" + System.getProperty("user.name");
        m_client.callProcedure("@SnapshotSave", parentDir + "/temp5", "Stark", 1);
        VoltTable[] result = m_client.callProcedure("@SnapshotSave", parentDir + "/temp5/Stark-host_1.digest/", "Stark", 1).getResults();
        VoltTable table = result[0];
        while (table.advanceRow()) {
            assertEquals("FAILURE", table.getString("RESULT"));
            assertEquals(true, table.getString("ERR_MSG").contains("FILE LOCATION UNWRITABLE"));
        }
        m_config.shutDown();
    }

    @Test
    public void testSnapshotDirectoryParentDirNotPermit() throws Exception {
        System.out.println("............      \n testSnapshotDirectoryParentDirNotPermit \n ............");

        final Mailbox mb = m_mockVoltDB.getHostMessenger().createMailbox();
        final SnapshotIOAgentImpl agent = new SnapshotIOAgentImpl(m_mockVoltDB.getHostMessenger(), 0);

        // Fake a snapshot is still in progress on one site
        SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.add(this);

        File dir = new File("/tmp");
        dir.mkdirs();
        dir.setWritable(false);
        dir.setReadable(false);
        dir.setExecutable(false);
        final SnapshotInitiationInfo snapshotRequest = new SnapshotInitiationInfo("/tmp", "woobie", true,
                SnapshotFormat.CSV, SnapshotPathType.SNAP_PATH, null);
        final SnapshotCheckRequestMessage checkMsg = new SnapshotCheckRequestMessage(snapshotRequest.getJSONObjectForZK().toString());
        checkMsg.m_sourceHSId = mb.getHSId();
        agent.deliver(checkMsg);
        dir.setWritable(true);
        dir.setReadable(true);
        dir.setExecutable(true);
        final SnapshotCheckResponseMessage resp = (SnapshotCheckResponseMessage) mb.recvBlocking();
        assertEquals(snapshotRequest.getPath(), resp.getPath());
        assertEquals(snapshotRequest.getNonce(), resp.getNonce());
        assertTrue(SnapshotUtil.isSnapshotInProgress(new VoltTable[]{resp.getResponse()}));
        VoltTable table = resp.getResponse();
        while (table.advanceRow()) {
            System.out.println("Result: " + table.getString("RESULT"));
            System.out.println("ERR_MSG: " + table.getString("ERR_MSG"));
            assertEquals("FAILURE", table.getString("RESULT"));
            assertEquals(true, table.getString("ERR_MSG").contains("FILE LOCATION UNWRITABLE"));
        }
        SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.clear();
    }

    private Client getClient() throws Exception {
        final Client client = ClientFactory.createClient();
        client.createConnection("localhost", m_config.port(0));
        return client;
    }
}
