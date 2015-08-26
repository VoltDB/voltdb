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

package org.voltdb;

import java.io.File;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientUtils;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.FakeStatsProducer;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.SystemStatsCollector.Datum;

public class TestMemoryResourceMonitor extends TestCase
{
    private static final int DEFAULT_MONITORING_INTERVAL=60;
    private static final int MONITORING_INTERVAL = 2;

    private ServerThread m_localServer;
    private VoltDB.Configuration m_config;
    private Client m_client;
    private TestStatsProducer m_mockStatsProducer;

    public void setUpServer(boolean setRssLimit, boolean setMonitoringInterval) throws Exception
    {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        if (setRssLimit) {
            builder.setRssLimit(0.5);
        }
        if (setMonitoringInterval) {
            builder.setResourceCheckInterval(MONITORING_INTERVAL);
        }
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("resourcemonitor.jar"), 1, 1, 0);
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("resourcemonitor.xml"));

        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = Configuration.getPathToCatalogForTest("resourcemonitor.jar");
        m_config.m_pathToDeployment = Configuration.getPathToCatalogForTest("resourcemonitor.xml");
        m_localServer = new ServerThread(m_config);
        m_localServer.start();
        m_localServer.waitForInitialization();

        m_mockStatsProducer = new TestStatsProducer();
        SystemStatsCollector.setFakeStatsProducer(m_mockStatsProducer);

        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());
        m_client = ClientFactory.createClient();
        m_client.createConnection("localhost:" + m_config.m_adminPort);
    }

    @Override
    public void tearDown() throws Exception {
        m_client.close();
        m_localServer.shutdown();
    }

    public void testNoRssLimit() throws Exception
    {
        setUpServer(false, true);

        // Wait for monitoring interval time and verify server is still in running mode
        m_mockStatsProducer.m_rss = 2048L*1024*1024;
        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());
    }

    // Disabling this test because it takes long. Enable it and run manually to test.
    public void notestNoMonitoringInterval() throws Exception
    {
        setUpServer(true, false);

        // Wait for monitoring interval time and verify server is still in running mode
        m_mockStatsProducer.m_rss = 2048L*1024*1024;
        resumeAndWait(DEFAULT_MONITORING_INTERVAL+1);
        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());
    }

    public void testLimitNotExceeded() throws Exception
    {
        setUpServer(true, true);

        m_mockStatsProducer.m_rss = 10*1024*1024;
        assertEquals(m_mockStatsProducer.m_rss, SystemStatsCollector.getRSSMB());

        // Wait for monitoring interval time and verify server is still in running mode
        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());
    }

    public void testLimitExceededWithResumePauseAgain() throws Exception
    {
        setUpServer(true, true);

        // Go above limit, wait for more than configured amt of time and verify server is paused
        m_mockStatsProducer.m_rss = 512*1024*1024;
        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.PAUSED, VoltDB.instance().getMode());

        // Resume and verify that server again goes into paused.
        m_client.callProcedure("@Resume");
        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.PAUSED, VoltDB.instance().getMode());
    }

    public void testLimitExceededWithResume() throws Exception
    {
        setUpServer(true, true);

        // Go above limit, wait for more than configured amt of time and verify server is paused
        m_mockStatsProducer.m_rss = 512*1024*1024;
        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.PAUSED, VoltDB.instance().getMode());

        // Don't go above limit in mock, resume and make sure server does not go back into paused.
        m_mockStatsProducer.m_rss = 1024*1024;
        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());
    }

    public void testCatalogUpdate_PauseAfterUpdate() throws Exception
    {
        setUpServer(false, true); // set up server with no rss limit
        m_mockStatsProducer.m_rss = 2048L*1024*1024;
        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());

        // update server with rss limit
        String newDepFile = getDeploymentPathWithRss(1);
        String depBytes = new String(ClientUtils.fileToBytes(new File(newDepFile)), Constants.UTF8ENCODING);
        VoltTable[] results = m_client.callProcedure("@UpdateApplicationCatalog", null, depBytes).getResults();
        assertTrue(results.length == 1);
        Thread.sleep(5000); // wait to make sure new deployment file takes effect

        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.PAUSED, VoltDB.instance().getMode());
    }

    public void testCatalogUpdate_ResumeAfterUpdate() throws Exception
    {
        setUpServer(true, true); // set up server with rss limit
        m_mockStatsProducer.m_rss = 2048L*1024*1024;
        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.PAUSED, VoltDB.instance().getMode());

        // update server with rss limit
        String newDepFile = getDeploymentPathWithRss(0);
        String depBytes = new String(ClientUtils.fileToBytes(new File(newDepFile)), Constants.UTF8ENCODING);
        VoltTable[] results = m_client.callProcedure("@UpdateApplicationCatalog", null, depBytes).getResults();
        assertTrue(results.length == 1);
        Thread.sleep(5000); // wait to make sure new deployment file takes effect

        resumeAndWait(MONITORING_INTERVAL+1);
        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());
    }

    private String getDeploymentPathWithRss(int rssLimit) throws Exception
    {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.setRssLimit(rssLimit);
        builder.setResourceCheckInterval(MONITORING_INTERVAL);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("updatedresourcemonitor.jar"), 1, 1, 0);
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("updatedresourcemonitor.xml"));

        return Configuration.getPathToCatalogForTest("updatedresourcemonitor.xml");
    }

    // time in seconds
    private void resumeAndWait(long time) throws Exception
    {
        // first wait for system stats collector interval of 5 seconds
        // to make sure that a stats collection is run.
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis()-startTime < 5000) {
            try { Thread.sleep(5000); } catch(InterruptedException e) { }
        }

        m_client.callProcedure("@Resume");

        // now sleep for specified time
        startTime = System.currentTimeMillis();
        while (System.currentTimeMillis()-startTime < time*1000) {
            try { Thread.sleep(time*1000); } catch(InterruptedException e) { }
        }
    }

    private static class TestStatsProducer implements FakeStatsProducer
    {
        volatile long m_rss;

        @Override
        public Datum getCurrentStatsData()
        {
            return new Datum(m_rss);
        }
    }
}
